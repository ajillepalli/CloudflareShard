import { DurableObject } from "cloudflare:workers";
import {
  CURRENT_PROTOCOL_VERSION,
  CURRENT_MANIFEST_ENUMERATION_FORMAT_VERSION,
  RESTORE_PLAN_FORMAT_VERSION,
  RESTORE_PROTOCOL_VERSION,
  RESTORE_REQUEST_FORMAT_VERSION,
  RESTORE_STATUS_FORMAT_VERSION,
  RestoreContractViolation,
  hashCanonicalJson,
  hashFinalizedRedoEnvelope,
  hashManifestRecordV2,
  hashManifestRequest,
  hashRestorePlanBody,
  hashRestorePreviewParameters,
  restoreError,
  validateRestoreExecuteRequest,
  validateRestorePreviewRequest,
  validateRestoreReconcileRequest,
  validateRestoreRollbackRequest,
  validateRestoreStatusRequest,
  validateManifestEnumerationResult,
  validateRedoEnvelope,
  type JsonValue,
  type ManifestEnumerationCursorV2,
  type ManifestEnumerationRequestV2,
  type ManifestEnumerationResultV2,
  type ManifestRecordV2,
  type ReadableRedoEnvelope,
  type RestoreExecuteRequestV1,
  type RestorePlanBodyV1,
  type RestorePlanV1,
  type RestorePreviewRequestV1,
  type RestoreProtocolError,
  type RestoreRollbackRequestV1,
  type RestoreStatusV1,
} from "../packages/contracts/src/index.js";
import type { CatalogDO } from "./catalog.js";
import type { CoordinatorDO } from "./coordinator.js";
import type { ShardDO } from "./shard.js";
import type { RestoreManifestService } from "../workers/control-plane/src/manifest-types.js";
import { json } from "./http.js";
import { log } from "./log.js";

const PITR_WINDOW_MS = 30 * 24 * 60 * 60_000;
const PITR_SAFETY_MARGIN_MS = 24 * 60 * 60_000;
const PLAN_EXECUTION_WINDOW_MS = 15 * 60_000;
const RESTORE_ALARM_DELAY_MS = 250;
const MANIFEST_PAGE_SIZE = 128;
const LOSS_PAGE_SIZE = 128;
const MAX_CATALOG_SHARDS = 128;
const COORDINATOR_REGISTRY_RETENTION_MS = 35 * 24 * 60 * 60_000;
const TERMINAL_RESTORE_STAGES = ["previewed", "complete", "rolled_back", "manual_repair_required", "failed"] as const;
const MAX_RESTORE_RETRY_ATTEMPTS = 10;
const MAX_RESTORE_RETRY_DELAY_MS = 30_000;

type JsonObject = Record<string, unknown>;

export interface RestoreCoordinatorEnv {
  CATALOG: DurableObjectNamespace<CatalogDO>;
  SHARD: DurableObjectNamespace<ShardDO>;
  COORDINATOR: DurableObjectNamespace<CoordinatorDO>;
  RESTORE_COORDINATOR: DurableObjectNamespace<RestoreCoordinatorDO>;
  CONTROL_PLANE?: RestoreManifestService;
  DEPLOYMENT_FLEET_ID?: string;
  CATALOG_SHARD_COUNT?: string;
  FAULT_INJECTION_ENABLED?: string;
  RESTORE_REHEARSAL_FAIL_AFTER_PARTICIPANTS?: string;
}

type OperationRow = {
  restore_id: string;
  fleet_id: string;
  cutoff: string;
  idempotency_key: string;
  parameter_hash: string;
  phase: string;
  stage: string;
  plan_json: string | null;
  plan_hash: string | null;
  previewed_at: string;
  execute_before: string;
  topology_epoch: number | null;
  topology_hash: string | null;
  catalog_proof_json: string | null;
  manifest_pin_json: string | null;
  manifest_cursor_json: string | null;
  fence_generation: number | null;
  fence_installed_at: string | null;
  started_at: string | null;
  completed_at: string | null;
  blocker_json: string | null;
  report_json: string | null;
  resume_phase: string | null;
  resume_stage: string | null;
  retry_count: number;
  retry_started_at_ms: number | null;
  retry_not_before_ms: number | null;
  updated_at: string;
};

type ParticipantRow = {
  restore_id: string;
  participant_id: string;
  participant_kind: "shard" | "coordinator";
  object_name: string;
  target_bookmark: string | null;
  target_checkpoint_at: string | null;
  preview_bookmark: string | null;
  coverage_start: string | null;
  status: string;
  undo_bookmark: string | null;
  pre_fence_bookmark: string | null;
  closed_through: string | null;
  loss_cursor_json: string | null;
  last_error: string | null;
};

type GateRow = {
  active: number;
  restore_id: string | null;
  generation: number;
  phase: string | null;
  activated_at: string | null;
};

type CatalogProof = {
  topology_epoch: number;
  topology_hash: string;
  shard_ids: string[];
  coverage_start: string;
  raw: JsonValue;
};

function isObject(value: unknown): value is JsonObject {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function canonicalTimestamp(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const parsed = new Date(value);
  return Number.isFinite(parsed.getTime()) && parsed.toISOString() === value ? value : null;
}

function stringField(value: JsonObject, ...names: string[]): string | null {
  for (const name of names) {
    if (typeof value[name] === "string" && value[name].length > 0) return value[name] as string;
  }
  return null;
}

function numberField(value: JsonObject, ...names: string[]): number | null {
  for (const name of names) {
    if (typeof value[name] === "number" && Number.isSafeInteger(value[name])) return value[name] as number;
  }
  return null;
}

function restoreResponse(error: RestoreProtocolError): Response {
  return json({ ok: false, status: error.retryable ? "unavailable" : "rejected", error }, error.http_status);
}

function failure(code: RestoreProtocolError["code"], message: string, details?: Record<string, JsonValue>): RestoreContractViolation {
  return new RestoreContractViolation(restoreError(code, message, details));
}

async function responseJson(response: Response, context: string): Promise<JsonObject> {
  let body: unknown;
  try {
    body = await response.json();
  } catch {
    throw failure("RESTORE_UNAVAILABLE", `${context} returned a non-JSON response.`);
  }
  if (!isObject(body)) {
    throw failure("RESTORE_UNAVAILABLE", `${context} rejected the restore operation.`, {
      http_status: response.status,
    });
  }
  if (!response.ok) {
    const nested = isObject(body.error) ? body.error : body;
    const remoteCode = stringField(nested, "code") ?? "";
    const remoteMessage = stringField(nested, "message") ?? `${context} rejected the restore operation.`;
    if ([408, 425, 429].includes(response.status)) {
      throw failure("RESTORE_UNAVAILABLE", remoteMessage, { http_status: response.status, remote_code: remoteCode });
    }
    if (response.status === 404 && context === "/redo-envelope") {
      throw failure("RESTORE_MANIFEST_GAP", "A manifest record references a redo envelope that is unavailable.");
    }
    if (response.status >= 400 && response.status < 500 && response.status !== 409) {
      if (remoteCode === "RESTORE_VERSION_UNSUPPORTED" || remoteCode.endsWith("VERSION_UNSUPPORTED")) {
        throw failure("RESTORE_VERSION_UNSUPPORTED", remoteMessage);
      }
      throw failure("RESTORE_INVALID_REQUEST", remoteMessage, { http_status: response.status, remote_code: remoteCode });
    }
    if (response.status === 409) {
      if (remoteCode.includes("BOOKMARK")) throw failure("RESTORE_BOOKMARK_MISSING", remoteMessage);
      if (remoteCode.includes("COVERAGE") || remoteCode.includes("GAP") || remoteCode.includes("INCOMPLETE")) {
        throw failure("RESTORE_ENUMERATION_INCOMPLETE", remoteMessage);
      }
      if (remoteCode.includes("HASH") || remoteCode.includes("ENVELOPE")) {
        throw failure("RESTORE_HASH_CONTRADICTION", remoteMessage);
      }
      if (remoteCode.includes("VERIFY") || remoteCode.includes("INVARIANT")) {
        throw failure("RESTORE_INVARIANT_FAILED", remoteMessage);
      }
      if (remoteCode.includes("TOPOLOGY") || remoteCode.includes("STALE")) {
        throw failure("RESTORE_PLAN_STALE", remoteMessage);
      }
      throw failure("RESTORE_CONFLICT", remoteMessage, { remote_code: remoteCode });
    }
    throw failure("RESTORE_UNAVAILABLE", remoteMessage, {
      http_status: response.status,
      remote_code: remoteCode,
    });
  }
  return body;
}

async function postToStub(stub: DurableObjectStub, path: string, body: unknown): Promise<JsonObject> {
  let response: Response;
  try {
    response = await stub.fetch(new Request(`https://restore.internal${path}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    }));
  } catch {
    throw failure("RESTORE_UNAVAILABLE", `${path} is temporarily unavailable.`);
  }
  return responseJson(response, path);
}

/**
 * One RestoreCoordinatorDO is addressed by a stable deployment authority name.
 * Keeping the gate and coordinator registry in the same non-restored object
 * makes the authority discoverable to every participant after a PITR restart.
 */
export class RestoreCoordinatorDO extends DurableObject<RestoreCoordinatorEnv> {
  private readonly sql: SqlStorage;
  private schemaEnsured = false;
  private mutationTail: Promise<void> = Promise.resolve();

  constructor(ctx: DurableObjectState, env: RestoreCoordinatorEnv) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
  }

  private ensureSchema(): void {
    if (this.schemaEnsured) return;
    this.schemaEnsured = true;
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS restore_operations (
        restore_id TEXT PRIMARY KEY,
        fleet_id TEXT NOT NULL,
        cutoff TEXT NOT NULL,
        idempotency_key TEXT NOT NULL UNIQUE,
        parameter_hash TEXT NOT NULL,
        phase TEXT NOT NULL,
        stage TEXT NOT NULL,
        plan_json TEXT,
        plan_hash TEXT,
        previewed_at TEXT NOT NULL,
        execute_before TEXT NOT NULL,
        topology_epoch INTEGER,
        topology_hash TEXT,
        catalog_proof_json TEXT,
        manifest_pin_json TEXT,
        manifest_cursor_json TEXT,
        fence_generation INTEGER,
        fence_installed_at TEXT,
        started_at TEXT,
        completed_at TEXT,
        blocker_json TEXT,
        report_json TEXT,
        resume_phase TEXT,
        resume_stage TEXT,
        retry_count INTEGER NOT NULL DEFAULT 0,
        retry_started_at_ms INTEGER,
        retry_not_before_ms INTEGER,
        updated_at TEXT NOT NULL
      )
    `);
    const operationColumns = this.many<{ name: string }>("PRAGMA table_info(restore_operations)");
    if (!operationColumns.some((column) => column.name === "retry_count")) {
      this.sql.exec("ALTER TABLE restore_operations ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0");
    }
    if (!operationColumns.some((column) => column.name === "retry_started_at_ms")) {
      this.sql.exec("ALTER TABLE restore_operations ADD COLUMN retry_started_at_ms INTEGER");
    }
    if (!operationColumns.some((column) => column.name === "retry_not_before_ms")) {
      this.sql.exec("ALTER TABLE restore_operations ADD COLUMN retry_not_before_ms INTEGER");
    }
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS restore_participants (
        restore_id TEXT NOT NULL,
        participant_id TEXT NOT NULL,
        participant_kind TEXT NOT NULL,
        object_name TEXT NOT NULL,
        target_bookmark TEXT,
        target_checkpoint_at TEXT,
        preview_bookmark TEXT,
        coverage_start TEXT,
        status TEXT NOT NULL,
        undo_bookmark TEXT,
        pre_fence_bookmark TEXT,
        closed_through TEXT,
        loss_cursor_json TEXT,
        last_error TEXT,
        PRIMARY KEY (restore_id, participant_id)
      )
    `);
    const participantColumns = this.many<{ name: string }>("PRAGMA table_info(restore_participants)");
    if (!participantColumns.some((column) => column.name === "target_checkpoint_at")) {
      this.sql.exec("ALTER TABLE restore_participants ADD COLUMN target_checkpoint_at TEXT");
    }
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS restore_coordinator_work (
        restore_id TEXT NOT NULL,
        coordinator_id TEXT NOT NULL,
        loss_cursor_json TEXT,
        closed_through TEXT,
        status TEXT NOT NULL,
        last_error TEXT,
        PRIMARY KEY (restore_id, coordinator_id)
      )
    `);
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS restore_manifest_records (
        restore_id TEXT NOT NULL,
        record_hash TEXT NOT NULL,
        tx_id TEXT NOT NULL,
        coordinator_id TEXT NOT NULL,
        commit_decided_at TEXT NOT NULL,
        envelope_hash TEXT NOT NULL,
        record_json TEXT NOT NULL,
        envelope_json TEXT,
        reconciliation_status TEXT NOT NULL DEFAULT 'pending',
        last_error TEXT,
        PRIMARY KEY (restore_id, record_hash)
      )
    `);
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS restore_manifest_evidence (
        restore_id TEXT NOT NULL,
        evidence_hash TEXT NOT NULL,
        evidence_json TEXT NOT NULL,
        PRIMARY KEY (restore_id, evidence_hash)
      )
    `);
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS restore_loss_entries (
        restore_id TEXT NOT NULL,
        participant_id TEXT NOT NULL,
        loss_hash TEXT NOT NULL,
        loss_json TEXT NOT NULL,
        PRIMARY KEY (restore_id, participant_id, loss_hash)
      )
    `);
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS restore_events (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
        restore_id TEXT NOT NULL,
        event TEXT NOT NULL,
        details_json TEXT NOT NULL,
        created_at TEXT NOT NULL
      )
    `);
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS coordinator_registry (
        coordinator_id TEXT PRIMARY KEY,
        fleet_id TEXT NOT NULL,
        registered_at TEXT NOT NULL,
        last_seen_at TEXT NOT NULL
      )
    `);
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS restore_metadata (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      )
    `);
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS fleet_restore_gate (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        active INTEGER NOT NULL,
        restore_id TEXT,
        generation INTEGER NOT NULL,
        phase TEXT,
        activated_at TEXT
      )
    `);
    this.sql.exec("INSERT OR IGNORE INTO fleet_restore_gate (singleton, active, generation) VALUES (1, 0, 0)");
  }

  private one<T extends object>(statement: string, ...params: unknown[]): T | null {
    for (const row of this.sql.exec(statement, ...params)) return row as T;
    return null;
  }

  private many<T extends object>(statement: string, ...params: unknown[]): T[] {
    return Array.from(this.sql.exec(statement, ...params)) as T[];
  }

  private operation(restoreId: string): OperationRow | null {
    return this.one<OperationRow>("SELECT * FROM restore_operations WHERE restore_id = ?", restoreId);
  }

  private gate(): GateRow {
    return this.one<GateRow>(
      "SELECT active, restore_id, generation, phase, activated_at FROM fleet_restore_gate WHERE singleton = 1",
    )!;
  }

  private deploymentFleetId(): string {
    return this.env.DEPLOYMENT_FLEET_ID?.trim() || "default";
  }

  private catalogShardCount(): number {
    const parsed = Number.parseInt(this.env.CATALOG_SHARD_COUNT ?? "4", 10);
    return Number.isSafeInteger(parsed) && parsed > 0 && parsed <= MAX_CATALOG_SHARDS ? parsed : 4;
  }

  private recordEvent(restoreId: string, event: string, details: Record<string, JsonValue> = {}): void {
    this.sql.exec(
      "INSERT INTO restore_events (restore_id, event, details_json, created_at) VALUES (?, ?, ?, ?)",
      restoreId,
      event,
      JSON.stringify(details),
      new Date().toISOString(),
    );
  }

  private async schedule(delayMs = RESTORE_ALARM_DELAY_MS): Promise<void> {
    const target = Date.now() + delayMs;
    const existing = await this.ctx.storage.getAlarm();
    if (existing === null || existing > target) await this.ctx.storage.setAlarm(target);
  }

  private runMutation<T>(work: () => Promise<T>): Promise<T> {
    const result = this.mutationTail.then(work, work);
    this.mutationTail = result.then(() => undefined, () => undefined);
    return result;
  }

  private async respond(path: string, request: Request): Promise<Response> {
    try {
      if (path === "/gate") return await this.handleGate(request);
      if (path === "/register-coordinator") return await this.handleRegisterCoordinator(request);
      if (path === "/preview") return await this.handlePreview(request);
      if (path === "/execute") return await this.handleExecute(request);
      if (path === "/status") return await this.handleStatus(request);
      if (path === "/reconcile") return await this.handleReconcile(request);
      if (path === "/rollback") return await this.handleRollback(request);
      return json({ error: "Unknown restore coordinator route." }, 404);
    } catch (error) {
      if (error instanceof RestoreContractViolation) return restoreResponse(error.protocolError);
      const message = error instanceof Error ? error.message : String(error);
      log("restore.unhandled_error", { path, message });
      return restoreResponse(restoreError("RESTORE_UNAVAILABLE", "Restore coordinator is temporarily unavailable."));
    }
  }

  async fetch(request: Request): Promise<Response> {
    this.ensureSchema();
    const path = new URL(request.url).pathname;
    if (request.method !== "POST") return json({ error: "Method not allowed." }, 405);
    // Gate checks are deliberately not queued: participant restore RPCs call
    // back into /gate while an exclusive advance is awaiting them. Status is
    // read-only and may expose the latest durable checkpoint during an advance.
    // Registration has its own synchronous durable critical section and must
    // remain callable while an advance awaits a CoordinatorDO. Queueing it
    // behind that advance can deadlock with the coordinator mutation drain.
    if (path === "/gate" || path === "/status" || path === "/register-coordinator") {
      return this.respond(path, request);
    }
    return this.runMutation(() => this.respond(path, request));
  }

  async alarm(): Promise<void> {
    this.ensureSchema();
    await this.runMutation(async () => {
      const gate = this.gate();
      const gatedOperation = gate.active === 1 && gate.restore_id !== null
        ? this.operation(gate.restore_id)
        : null;
      if (gatedOperation && TERMINAL_RESTORE_STAGES.includes(gatedOperation.stage as typeof TERMINAL_RESTORE_STAGES[number])) {
        return;
      }
      const operation = gatedOperation
        ?? (gate.active === 1 ? null
        : this.one<OperationRow>(
          `SELECT * FROM restore_operations
           WHERE stage NOT IN ('previewed', 'complete', 'rolled_back', 'manual_repair_required', 'failed')
           ORDER BY updated_at ASC LIMIT 1`,
        ));
      if (!operation) return;
      this.sql.exec(
        "UPDATE restore_operations SET updated_at = ? WHERE restore_id = ?",
        new Date().toISOString(),
        operation.restore_id,
      );
      try {
        await this.advance(operation.restore_id);
      } catch (error) {
        await this.retryOrPark(operation.restore_id, error, null, null);
      }
      const refreshedGate = this.gate();
      const pending = refreshedGate.active === 1 && refreshedGate.restore_id !== null
        ? this.operation(refreshedGate.restore_id)
        : this.one<OperationRow>(
            `SELECT * FROM restore_operations
             WHERE stage NOT IN ('previewed', 'complete', 'rolled_back', 'manual_repair_required', 'failed')
             ORDER BY updated_at ASC LIMIT 1`,
          );
      if (pending && !TERMINAL_RESTORE_STAGES.includes(pending.stage as typeof TERMINAL_RESTORE_STAGES[number])) {
        const delayMs = Math.max(RESTORE_ALARM_DELAY_MS, (pending.retry_not_before_ms ?? 0) - Date.now());
        await this.schedule(delayMs);
      }
    });
  }

  private async handleGate(request: Request): Promise<Response> {
    const body = await request.json().catch(() => ({})) as JsonObject;
    const fleetId = typeof body.fleet_id === "string" ? body.fleet_id : this.deploymentFleetId();
    if (fleetId !== this.deploymentFleetId()) {
      return restoreResponse(restoreError("RESTORE_CONFLICT", "Fleet ID is not this deployment's physical restore domain."));
    }
    const gate = this.gate();
    const sameRestore = gate.active === 1
      && body.restore_id === gate.restore_id
      && body.generation === gate.generation;
    return json({
      ok: true,
      active: gate.active === 1,
      allowed: gate.active === 0 || sameRestore,
      restore_id: gate.restore_id,
      generation: gate.generation,
      phase: gate.phase,
      activated_at: gate.activated_at,
    });
  }

  private async handleRegisterCoordinator(request: Request): Promise<Response> {
    const body = await request.json() as JsonObject;
    const fleetId = stringField(body, "fleet_id", "fleetId");
    // The physical Durable Object is addressed by txId even when the
    // protocol-level coordinator_id is customized for evidence identity.
    const coordinatorId = stringField(body, "tx_id", "txId", "coordinator_id", "coordinatorId");
    if (fleetId !== this.deploymentFleetId() || coordinatorId === null) {
      throw failure("RESTORE_INVALID_REQUEST", "Coordinator registration is invalid.");
    }
    const existingCreatedAt = body.existing_created_at === undefined
      ? null
      : canonicalTimestamp(body.existing_created_at);
    if (body.existing_created_at !== undefined && existingCreatedAt === null) {
      throw failure("RESTORE_INVALID_REQUEST", "Existing coordinator creation time is invalid.");
    }
    const gate = this.gate();
    const gatedOperation = gate.active === 1 && gate.restore_id !== null ? this.operation(gate.restore_id) : null;
    if (
      existingCreatedAt !== null
      && gatedOperation
      && (gatedOperation.stage === "releasing_participants" || gatedOperation.resume_stage === "releasing_participants")
    ) {
      // Release is the inventory linearization point. A coordinator that
      // missed the active restore retries after the gate opens, where the
      // completed-restore lookup below returns a discard directive when its
      // durable decision is post-cutoff.
      throw failure("RESTORE_UNAVAILABLE", "Fleet restore is finalizing coordinator inventory; retry registration.");
    }
    if (gate.active === 1 && existingCreatedAt === null) {
      throw failure("RESTORE_CONFLICT", "Fleet restore fence is active; new transactions are blocked.");
    }
    const alreadyInventoried = gate.active === 1 && gate.restore_id !== null
      ? this.one<{ status: string }>(
        "SELECT status FROM restore_coordinator_work WHERE restore_id = ? AND coordinator_id = ?",
        gate.restore_id,
        coordinatorId,
      )
      : null;
    const now = new Date().toISOString();
    this.ctx.storage.transactionSync(() => {
      this.sql.exec(
        "DELETE FROM coordinator_registry WHERE last_seen_at < ?",
        new Date(Date.now() - COORDINATOR_REGISTRY_RETENTION_MS).toISOString(),
      );
      this.sql.exec(
        `INSERT INTO coordinator_registry (coordinator_id, fleet_id, registered_at, last_seen_at)
         VALUES (?, ?, ?, ?)
         ON CONFLICT(coordinator_id) DO UPDATE SET last_seen_at = excluded.last_seen_at`,
        coordinatorId,
        fleetId,
        now,
        now,
      );
      this.sql.exec("INSERT OR IGNORE INTO restore_metadata (key, value) VALUES ('coordinator_coverage_start', ?)", now);
      if (gate.active === 1 && gate.restore_id !== null) {
        this.sql.exec(
          `INSERT OR IGNORE INTO restore_coordinator_work (restore_id, coordinator_id, status)
           VALUES (?, ?, 'pending_loss')`,
          gate.restore_id,
          coordinatorId,
        );
        if (alreadyInventoried === null) {
          this.sql.exec(
            `UPDATE restore_operations SET stage = 'materializing_loss', phase = 'restoring', updated_at = ?
             WHERE restore_id = ? AND stage IN
               ('restoring_participants', 'reconciling', 'verifying_participants',
                'discarding_coordinators', 'releasing_participants')`,
            now,
            gate.restore_id,
          );
          this.sql.exec(
            `UPDATE restore_operations SET resume_stage = 'materializing_loss', resume_phase = 'restoring', updated_at = ?
             WHERE restore_id = ? AND stage = 'manual_repair_required' AND resume_phase != 'rolling_back'`,
            now,
            gate.restore_id,
          );
        }
      }
    });
    if (gate.active === 1) {
      if (alreadyInventoried === null) await this.schedule();
      return json({
        ok: true,
        disposition: "registered",
        registered_at: now,
        active_restore_id: gate.restore_id,
        generation: gate.generation,
      });
    }
    if (existingCreatedAt !== null) {
      const missedRestore = this.one<{ restore_id: string; fence_generation: number; cutoff: string }>(
        `SELECT operation.restore_id, operation.fence_generation, operation.cutoff
         FROM restore_operations AS operation
         LEFT JOIN restore_coordinator_work AS work
           ON work.restore_id = operation.restore_id AND work.coordinator_id = ?
         WHERE operation.stage = 'complete'
           AND operation.fence_installed_at IS NOT NULL
           AND operation.fence_installed_at >= ?
           AND (work.coordinator_id IS NULL OR work.status = 'discarded')
         ORDER BY operation.completed_at ASC LIMIT 1`,
        coordinatorId,
        existingCreatedAt,
      );
      if (missedRestore) {
        return json({
          ok: true,
          disposition: "discard_required",
          registered_at: now,
          restore_id: missedRestore.restore_id,
          generation: missedRestore.fence_generation,
          cutoff: missedRestore.cutoff,
        });
      }
    }
    return json({ ok: true, disposition: "registered", registered_at: now });
  }

  private async handlePreview(request: Request): Promise<Response> {
    const raw: unknown = await request.json();
    validateRestorePreviewRequest(raw);
    const body: RestorePreviewRequestV1 = raw;
    if (body.fleet_id !== this.deploymentFleetId()) {
      throw failure(
        "RESTORE_CONFLICT",
        "A logical fleet cannot be restored independently inside this deployment's shared Durable Object namespaces.",
        { deployment_fleet_id: this.deploymentFleetId(), requested_fleet_id: body.fleet_id },
      );
    }
    const cutoffMs = new Date(body.cutoff).getTime();
    const nowMs = Date.now();
    if (cutoffMs > nowMs) throw failure("RESTORE_CUTOFF_IN_FUTURE", "Restore cutoff cannot be in the future.");
    if (cutoffMs < nowMs - PITR_WINDOW_MS + PITR_SAFETY_MARGIN_MS) {
      throw failure(
        "RESTORE_CUTOFF_OUTSIDE_PITR_WINDOW",
        "Restore cutoff is outside the safe provider PITR window.",
        { safety_margin_ms: PITR_SAFETY_MARGIN_MS },
      );
    }
    const coverage = this.one<{ value: string }>(
      "SELECT value FROM restore_metadata WHERE key = 'coordinator_coverage_start'",
    )?.value;
    if (coverage === undefined || new Date(coverage).getTime() > cutoffMs) {
      throw failure(
        "RESTORE_ENUMERATION_INCOMPLETE",
        "Coordinator inventory does not cover the requested cutoff.",
        { coordinator_coverage_start: coverage ?? "unavailable" },
      );
    }
    const parameterHash = await hashRestorePreviewParameters(body);
    const existing = this.one<OperationRow>(
      "SELECT * FROM restore_operations WHERE idempotency_key = ?",
      body.idempotency_key,
    );
    if (existing) {
      if (existing.parameter_hash !== parameterHash) {
        throw failure("RESTORE_CONFLICT", "Restore preview idempotency key was reused with different parameters.");
      }
      if (existing.plan_json !== null) {
        return json({ ok: true, status: "previewed", plan: JSON.parse(existing.plan_json) });
      }
      try {
        await this.advance(existing.restore_id);
      } catch (error) {
        await this.retryOrPark(existing.restore_id, error, null, null);
        throw error;
      }
      const refreshed = this.operation(existing.restore_id)!;
      if (refreshed.plan_json !== null) {
        return json({ ok: true, status: "previewed", plan: JSON.parse(refreshed.plan_json) });
      }
      if (refreshed.stage === "failed") {
        throw failure("RESTORE_CONFLICT", "This immutable preview attempt failed; correct the evidence gap and use a new idempotency key.");
      }
      return json({
        ok: true,
        status: "previewing",
        restore_id: existing.restore_id,
        fleet_id: existing.fleet_id,
        cutoff: existing.cutoff,
        retry_after_ms: RESTORE_ALARM_DELAY_MS,
      }, 202);
    }

    const restoreIdentityHash = await hashCanonicalJson([parameterHash, body.idempotency_key]);
    const restoreId = `restore-${restoreIdentityHash.slice(0, 32)}`;
    const previewedAt = new Date(nowMs).toISOString();
    const executeBefore = new Date(nowMs + PLAN_EXECUTION_WINDOW_MS).toISOString();
    this.sql.exec(
      `INSERT INTO restore_operations
       (restore_id, fleet_id, cutoff, idempotency_key, parameter_hash, phase, stage,
        previewed_at, execute_before, updated_at)
       VALUES (?, ?, ?, ?, ?, 'previewing', 'closing_manifest', ?, ?, ?)`,
      restoreId,
      body.fleet_id,
      body.cutoff,
      body.idempotency_key,
      parameterHash,
      previewedAt,
      executeBefore,
      previewedAt,
    );
    this.recordEvent(restoreId, "preview_started", { cutoff: body.cutoff });
    try {
      await this.advance(restoreId);
    } catch (error) {
      await this.retryOrPark(restoreId, error, null, null);
      throw error;
    }
    const operation = this.operation(restoreId)!;
    if (operation.plan_json !== null) {
      return json({ ok: true, status: "previewed", plan: JSON.parse(operation.plan_json) });
    }
    await this.schedule();
    return json({
      ok: true,
      status: "previewing",
      restore_id: restoreId,
      fleet_id: body.fleet_id,
      cutoff: body.cutoff,
      retry_after_ms: RESTORE_ALARM_DELAY_MS,
    }, 202);
  }

  private async readCatalogProof(operation: OperationRow): Promise<CatalogProof> {
    const proofs: JsonObject[] = [];
    const shardIds = new Set<string>();
    let topologyEpoch = 1;
    let coverageStartMs = 0;
    const expectedCatalogCount = this.catalogShardCount();
    for (let i = 0; i < expectedCatalogCount; i += 1) {
      const catalogId = `catalog-${i}`;
      const result = await postToStub(this.env.CATALOG.getByName(catalogId), "/restore-proof", {
        cutoff: operation.cutoff,
        restoreId: operation.restore_id,
      });
      const proof = isObject(result.proof) ? result.proof : result;
      if (proof.ok === false) {
        throw failure("RESTORE_PLAN_STALE", `Catalog ${catalogId} rejected restore proof.`);
      }
      const active = proof.active_operations ?? proof.activeOperations ?? [];
      const changes = proof.post_cutoff_changes ?? proof.postCutoffChanges ?? [];
      if ((Array.isArray(active) && active.length > 0) || (Array.isArray(changes) && changes.length > 0)) {
        throw failure(
          "RESTORE_PLAN_STALE",
          "Catalog topology or configuration is not cutoff-consistent.",
          { catalog_id: catalogId },
        );
      }
      const epoch = numberField(proof, "topology_epoch", "topologyEpoch", "metadata_version", "metadataVersion");
      if (epoch === null || epoch < 1) throw failure("RESTORE_ENUMERATION_INCOMPLETE", "Catalog topology epoch is missing.");
      topologyEpoch = Math.max(topologyEpoch, epoch);
      const topologyVector = isObject(proof.topology_vector)
        ? proof.topology_vector
        : isObject(proof.topologyVector) ? proof.topologyVector : null;
      const topologyConfig = topologyVector && isObject(topologyVector.config) ? topologyVector.config : null;
      const durableCatalogCount = topologyConfig === null
        ? null
        : numberField(topologyConfig, "catalog_shard_count", "catalogShardCount");
      if (durableCatalogCount !== expectedCatalogCount) {
        throw failure(
          "RESTORE_ENUMERATION_INCOMPLETE",
          "Configured catalog fan-out does not match the durable topology.",
          { catalog_id: catalogId, configured_count: expectedCatalogCount, durable_count: durableCatalogCount ?? -1 },
        );
      }
      const coverage = canonicalTimestamp(proof.coverage_start ?? proof.coverageStart);
      if (coverage === null) throw failure("RESTORE_ENUMERATION_INCOMPLETE", "Catalog restore evidence coverage is missing.");
      coverageStartMs = Math.max(coverageStartMs, new Date(coverage).getTime());
      const ids = proof.shard_ids ?? proof.shardIds ?? proof.physical_shard_ids ?? proof.physicalShardIds;
      if (!Array.isArray(ids) || ids.some((id) => typeof id !== "string" || id.length === 0)) {
        throw failure("RESTORE_ENUMERATION_INCOMPLETE", "Catalog physical shard inventory is invalid.");
      }
      for (const id of ids as string[]) shardIds.add(id);
      proofs.push({ catalog_id: catalogId, ...proof });
    }
    if (shardIds.size === 0) throw failure("RESTORE_ENUMERATION_INCOMPLETE", "Restore inventory contains no physical shards.");
    proofs.sort((a, b) => String(a.catalog_id).localeCompare(String(b.catalog_id)));
    return {
      topology_epoch: topologyEpoch,
      topology_hash: await hashCanonicalJson(proofs as JsonValue),
      shard_ids: [...shardIds].sort(),
      coverage_start: new Date(coverageStartMs).toISOString(),
      raw: proofs as JsonValue,
    };
  }

  private async closeManifest(operation: OperationRow): Promise<boolean> {
    if (!this.env.CONTROL_PLANE) throw failure("RESTORE_UNAVAILABLE", "Control-plane restore service is not configured.");
    const raw = await this.env.CONTROL_PLANE.closeFleetThrough({ fleet_id: operation.fleet_id, cutoff: operation.cutoff });
    if (!isObject(raw)) throw failure("RESTORE_UNAVAILABLE", "Manifest close returned an invalid response.");
    if (raw.ok === false) {
      throw failure("RESTORE_ENUMERATION_INCOMPLETE", "Manifest close could not certify the requested cutoff.");
    }
    if (raw.status !== "complete") {
      await this.schedule();
      return false;
    }
    const proof = await this.readCatalogProof(operation);
    if (new Date(proof.coverage_start).getTime() > new Date(operation.cutoff).getTime()) {
      throw failure("RESTORE_ENUMERATION_INCOMPLETE", "Catalog evidence starts after the restore cutoff.");
    }
    const closeKey = stringField(raw, "catalog_close_key", "close_key");
    const snapshotHash = stringField(raw, "snapshot_hash", "catalog_snapshot_hash");
    const fleetRootHash = stringField(raw, "fleet_root_hash");
    const partitionConfigHash = stringField(raw, "partition_config_hash");
    const generation = numberField(raw, "snapshot_generation", "catalog_generation");
    const manifestCoverage = canonicalTimestamp(raw.coverage_start) ?? proof.coverage_start;
    if (!closeKey || !snapshotHash || !fleetRootHash || !partitionConfigHash || generation === null) {
      throw failure("RESTORE_ENUMERATION_INCOMPLETE", "Manifest close omitted a required immutable evidence pin.");
    }
    const pin = {
      coverage_start: manifestCoverage,
      catalog_close_key: closeKey,
      catalog_generation: generation,
      catalog_snapshot_hash: snapshotHash,
      fleet_root_hash: fleetRootHash,
      partition_config_hash: partitionConfigHash,
      record_count: 0,
    };
    this.ctx.storage.transactionSync(() => {
      this.sql.exec(
        `UPDATE restore_operations SET stage = 'enumerating_manifest', topology_epoch = ?, topology_hash = ?,
          catalog_proof_json = ?, manifest_pin_json = ?, manifest_cursor_json = NULL, updated_at = ? WHERE restore_id = ?`,
        proof.topology_epoch,
        proof.topology_hash,
        JSON.stringify(proof),
        JSON.stringify(pin),
        new Date().toISOString(),
        operation.restore_id,
      );
      for (const shardId of proof.shard_ids) {
        this.sql.exec(
          `INSERT OR IGNORE INTO restore_participants
           (restore_id, participant_id, participant_kind, object_name, status)
           VALUES (?, ?, 'shard', ?, 'pending_preview')`,
          operation.restore_id,
          `shard:${shardId}`,
          shardId,
        );
      }
    });
    this.recordEvent(operation.restore_id, "manifest_closed", { fleet_root_hash: fleetRootHash });
    return true;
  }

  private async enumerateManifest(operation: OperationRow): Promise<boolean> {
    if (!this.env.CONTROL_PLANE) throw failure("RESTORE_UNAVAILABLE", "Control-plane restore service is not configured.");
    if (operation.manifest_pin_json === null) throw failure("RESTORE_ENUMERATION_INCOMPLETE", "Manifest pin is missing.");
    const pin = JSON.parse(operation.manifest_pin_json) as JsonObject;
    const cursor = operation.manifest_cursor_json === null
      ? null
      : JSON.parse(operation.manifest_cursor_json) as ManifestEnumerationCursorV2;
    const request: ManifestEnumerationRequestV2 = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: CURRENT_MANIFEST_ENUMERATION_FORMAT_VERSION,
      fleet_id: operation.fleet_id,
      coverage_start: String(pin.coverage_start),
      cutoff: operation.cutoff,
      partition_config_hash: String(pin.partition_config_hash),
      catalog_generation: Number(pin.catalog_generation),
      catalog_snapshot_hash: String(pin.catalog_snapshot_hash),
      catalog_close_key: String(pin.catalog_close_key),
      fleet_root_hash: String(pin.fleet_root_hash),
      limit: MANIFEST_PAGE_SIZE,
      cursor,
    };
    const requestHash = await hashManifestRequest(request);
    const raw = await this.env.CONTROL_PLANE.enumerateManifest(request);
    if (!isObject(raw) || raw.ok === false) {
      throw failure("RESTORE_ENUMERATION_INCOMPLETE", "Manifest enumeration failed.");
    }
    try {
      validateManifestEnumerationResult(raw);
    } catch {
      throw failure("RESTORE_ENUMERATION_INCOMPLETE", "Manifest page failed the exact V2 contract.");
    }
    const result = raw as ManifestEnumerationResultV2;
    if (
      result.format_version !== CURRENT_MANIFEST_ENUMERATION_FORMAT_VERSION
      || result.request_hash !== requestHash
      || result.catalog_close_key !== pin.catalog_close_key
      || result.fleet_root_hash !== pin.fleet_root_hash
    ) {
      throw failure("RESTORE_HASH_CONTRADICTION", "Manifest page is not bound to the pinned fleet close.");
    }
    const now = new Date().toISOString();
    for (const record of result.records) {
      const previouslyDiscarded = this.one<{ n: number }>(
        `SELECT COUNT(*) AS n FROM restore_coordinator_work
         WHERE coordinator_id = ? AND status = 'discarded'`,
        record.tx_id,
      )?.n ?? 0;
      if (previouslyDiscarded > 0) {
        this.recordEvent(operation.restore_id, "manifest_record_previously_discarded", { tx_id: record.tx_id });
        continue;
      }
      const recordHash = await hashManifestRecordV2(record as ManifestRecordV2);
      this.sql.exec(
        `INSERT OR IGNORE INTO restore_manifest_records
         (restore_id, record_hash, tx_id, coordinator_id, commit_decided_at, envelope_hash, record_json)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        operation.restore_id,
        recordHash,
        record.tx_id,
        record.coordinator_id,
        record.commit_decided_at,
        record.envelope_hash,
        JSON.stringify(record),
      );
    }
    for (const evidence of result.evidence) {
      const evidenceHash = await hashCanonicalJson(evidence as unknown as JsonValue);
      this.sql.exec(
        "INSERT OR IGNORE INTO restore_manifest_evidence (restore_id, evidence_hash, evidence_json) VALUES (?, ?, ?)",
        operation.restore_id,
        evidenceHash,
        JSON.stringify(evidence),
      );
    }
    if (result.next_cursor !== null) {
      this.sql.exec(
        "UPDATE restore_operations SET manifest_cursor_json = ?, updated_at = ? WHERE restore_id = ?",
        JSON.stringify(result.next_cursor),
        now,
        operation.restore_id,
      );
      await this.schedule();
      return false;
    }
    if (result.complete !== true || result.coverage !== "complete") {
      throw failure("RESTORE_ENUMERATION_INCOMPLETE", "Manifest enumeration exhausted without complete coverage.");
    }
    const recordCount = this.one<{ n: number }>(
      "SELECT COUNT(*) AS n FROM restore_manifest_records WHERE restore_id = ?",
      operation.restore_id,
    )?.n ?? 0;
    pin.record_count = recordCount;
    this.sql.exec(
      `UPDATE restore_operations SET stage = 'materializing_envelopes', manifest_pin_json = ?,
       manifest_cursor_json = NULL, updated_at = ? WHERE restore_id = ?`,
      JSON.stringify(pin),
      now,
      operation.restore_id,
    );
    this.recordEvent(operation.restore_id, "manifest_enumerated", { record_count: recordCount });
    return true;
  }

  private async materializeNextEnvelope(operation: OperationRow): Promise<boolean> {
    const row = this.one<{
      record_hash: string;
      tx_id: string;
      envelope_hash: string;
      record_json: string;
    }>(
      `SELECT record_hash, tx_id, envelope_hash, record_json FROM restore_manifest_records
       WHERE restore_id = ? AND envelope_json IS NULL ORDER BY commit_decided_at, tx_id LIMIT 1`,
      operation.restore_id,
    );
    if (!row) {
      this.sql.exec(
        "UPDATE restore_operations SET stage = 'previewing_participants', updated_at = ? WHERE restore_id = ?",
        new Date().toISOString(),
        operation.restore_id,
      );
      return true;
    }
    const result = await postToStub(this.env.COORDINATOR.getByName(row.tx_id), "/recovery-envelope", {
      restore_id: operation.restore_id,
      tx_id: row.tx_id,
      envelope_hash: row.envelope_hash,
    });
    const envelope = result.envelope;
    if (envelope === undefined) throw failure("RESTORE_MANIFEST_GAP", "Coordinator redo envelope is missing.", { tx_id: row.tx_id });
    try {
      await validateRedoEnvelope(envelope);
    } catch {
      throw failure("RESTORE_HASH_CONTRADICTION", "Coordinator redo envelope failed canonical validation.", { tx_id: row.tx_id });
    }
    if (!isObject(envelope)) throw failure("RESTORE_HASH_CONTRADICTION", "Coordinator redo envelope is malformed.");
    const actualHash = await hashFinalizedRedoEnvelope(envelope as unknown as ReadableRedoEnvelope);
    if (actualHash !== row.envelope_hash) {
      throw failure("RESTORE_HASH_CONTRADICTION", "Coordinator redo envelope does not match the manifest hash.", { tx_id: row.tx_id });
    }
    const manifestRecord = JSON.parse(row.record_json) as ManifestRecordV2;
    if (
      envelope.tx_id !== manifestRecord.tx_id
      || envelope.fleet_id !== manifestRecord.fleet_id
      || envelope.coordinator_id !== manifestRecord.coordinator_id
      || envelope.decision_epoch !== manifestRecord.decision_epoch
      || envelope.operation_hash !== manifestRecord.operation_hash
      || envelope.commit_decided_at !== manifestRecord.commit_decided_at
      || envelope.retention_deadline !== manifestRecord.retention_deadline
    ) {
      throw failure("RESTORE_HASH_CONTRADICTION", "Coordinator redo envelope contradicts its immutable manifest record.", { tx_id: row.tx_id });
    }
    this.sql.exec(
      "UPDATE restore_manifest_records SET envelope_json = ? WHERE restore_id = ? AND record_hash = ?",
      JSON.stringify(envelope),
      operation.restore_id,
      row.record_hash,
    );
    await this.schedule();
    return false;
  }

  private participantStub(participant: ParticipantRow): DurableObjectStub {
    return participant.participant_kind === "shard"
      ? this.shardStub(participant.object_name)
      : this.env.COORDINATOR.getByName(participant.object_name);
  }

  private shardStub(objectName: string): DurableObjectStub {
    return this.env.SHARD.getByName(objectName);
  }

  private async previewNextParticipant(operation: OperationRow): Promise<boolean> {
    const participant = this.one<ParticipantRow>(
      `SELECT * FROM restore_participants
       WHERE restore_id = ? AND status = 'pending_preview' ORDER BY participant_id LIMIT 1`,
      operation.restore_id,
    );
    if (!participant) {
      await this.finalizePlan(operation.restore_id);
      return true;
    }
    const result = await postToStub(this.participantStub(participant), "/pitr-preview", {
      fleet_id: operation.fleet_id,
      restore_id: operation.restore_id,
      cutoff: operation.cutoff,
    });
    const target = stringField(result, "target_bookmark", "targetBookmark");
    const current = stringField(result, "preview_bookmark", "previewBookmark", "current_bookmark", "currentBookmark");
    const coverage = canonicalTimestamp(result.coverage_start ?? result.coverageStart);
    const targetCheckpointAt = canonicalTimestamp(result.checkpoint_at ?? result.checkpointAt);
    if (!target || !current || !coverage || !targetCheckpointAt) {
      throw failure("RESTORE_BOOKMARK_MISSING", "Participant did not return certified exact bookmark evidence.", {
        participant_id: participant.participant_id,
      });
    }
    if (new Date(coverage).getTime() > new Date(operation.cutoff).getTime()) {
      throw failure("RESTORE_BOOKMARK_MISSING", "Participant checkpoint coverage starts after the requested cutoff.", {
        participant_id: participant.participant_id,
        coverage_start: coverage,
      });
    }
    this.sql.exec(
      `UPDATE restore_participants SET target_bookmark = ?, target_checkpoint_at = ?, preview_bookmark = ?, coverage_start = ?, status = 'previewed'
       WHERE restore_id = ? AND participant_id = ?`,
      target,
      targetCheckpointAt,
      current,
      coverage,
      operation.restore_id,
      participant.participant_id,
    );
    await this.schedule();
    return false;
  }

  private async finalizePlan(restoreId: string): Promise<void> {
    const operation = this.operation(restoreId);
    if (!operation || operation.manifest_pin_json === null || operation.topology_hash === null || operation.topology_epoch === null) {
      throw failure("RESTORE_ENUMERATION_INCOMPLETE", "Restore plan evidence is incomplete.");
    }
    const participants = this.many<ParticipantRow>(
      "SELECT * FROM restore_participants WHERE restore_id = ? ORDER BY participant_id",
      restoreId,
    );
    if (participants.length === 0 || participants.some((p) => !p.target_bookmark || !p.target_checkpoint_at || !p.preview_bookmark || p.status !== "previewed")) {
      throw failure("RESTORE_BOOKMARK_MISSING", "Restore plan is missing participant bookmarks.");
    }
    const manifest = JSON.parse(operation.manifest_pin_json) as RestorePlanBodyV1["manifest"];
    const previewedAt = new Date().toISOString();
    const executeBefore = new Date(Date.now() + PLAN_EXECUTION_WINDOW_MS).toISOString();
    const intentionalLossFrom = participants
      .map((participant) => participant.target_checkpoint_at!)
      .sort()[0]!;
    const limitations = [
      "PITR rewinds each physical participant independently; reconciliation is required before the fleet is released.",
      "Rollback is available only while the original fleet fence remains held and before post-cutoff coordinators are discarded.",
    ].sort();
    const body: RestorePlanBodyV1 = {
      protocol_version: RESTORE_PROTOCOL_VERSION,
      format_version: RESTORE_PLAN_FORMAT_VERSION,
      restore_id: restoreId,
      fleet_id: operation.fleet_id,
      cutoff: operation.cutoff,
      previewed_at: previewedAt,
      execute_before: executeBefore,
      parameter_hash: operation.parameter_hash,
      topology: {
        topology_epoch: operation.topology_epoch,
        topology_hash: operation.topology_hash,
      },
      manifest,
      participants: participants.map((participant) => ({
        participant_id: participant.participant_id,
        target_bookmark: participant.target_bookmark!,
        preview_bookmark: participant.preview_bookmark!,
      })),
      impact: {
        participant_count: participants.length,
        transaction_count: manifest.record_count,
        intentional_loss_from: intentionalLossFrom,
        intentional_loss_through: executeBefore,
      },
      rollback: {
        undo_supported: true,
        // Conservative relative to the provider's 30-day bookmark history:
        // the undo bookmark is created no later than execute_before.
        undo_expires_at: new Date(new Date(executeBefore).getTime() + PITR_WINDOW_MS - PITR_SAFETY_MARGIN_MS).toISOString(),
        limitations,
      },
    };
    const planHash = await hashRestorePlanBody(body);
    const plan: RestorePlanV1 = { ...body, plan_hash: planHash };
    this.sql.exec(
      `UPDATE restore_operations SET stage = 'previewed', phase = 'previewed', previewed_at = ?, execute_before = ?,
       plan_json = ?, plan_hash = ?, updated_at = ?
       WHERE restore_id = ?`,
      previewedAt,
      executeBefore,
      JSON.stringify(plan),
      planHash,
      new Date().toISOString(),
      restoreId,
    );
    this.recordEvent(restoreId, "preview_complete", { plan_hash: planHash, participant_count: participants.length });
  }

  private async handleExecute(request: Request): Promise<Response> {
    const raw: unknown = await request.json();
    validateRestoreExecuteRequest(raw);
    const body: RestoreExecuteRequestV1 = raw;
    const operation = this.operation(body.restore_id);
    if (!operation || operation.plan_json === null || operation.plan_hash === null) {
      throw failure("RESTORE_INVALID_REQUEST", "Restore plan does not exist or preview is incomplete.");
    }
    if (operation.plan_hash !== body.plan_hash) {
      throw failure("RESTORE_PLAN_HASH_MISMATCH", "Exact restore plan hash confirmation is required.");
    }
    if (operation.stage !== "previewed") {
      if (operation.stage === "complete") {
        return json({ ok: true, status: "already_started", restore_id: body.restore_id, plan_hash: body.plan_hash });
      }
      if (!['failed', 'manual_repair_required'].includes(operation.stage)) {
        return json({ ok: true, status: "already_started", restore_id: body.restore_id, plan_hash: body.plan_hash }, 202);
      }
      throw failure("RESTORE_CONFLICT", "Restore operation cannot be restarted from its current terminal state.");
    }
    if (Date.now() >= new Date(operation.execute_before).getTime()) {
      throw failure("RESTORE_PLAN_STALE", "Restore plan expired before execution.");
    }
    const gate = this.gate();
    if (gate.active === 1 && gate.restore_id !== body.restore_id) {
      throw failure("RESTORE_CONFLICT", "Another fleet restore already owns the deployment fence.");
    }
    const generation = gate.generation + 1;
    const now = new Date().toISOString();
    this.ctx.storage.transactionSync(() => {
      this.sql.exec(
        `UPDATE fleet_restore_gate SET active = 1, restore_id = ?, generation = ?, phase = 'fencing', activated_at = ?
         WHERE singleton = 1`,
        body.restore_id,
        generation,
        now,
      );
      this.sql.exec(
        `UPDATE restore_operations SET phase = 'fencing', stage = 'fencing_catalogs', fence_generation = ?,
          fence_installed_at = ?, started_at = ?, updated_at = ? WHERE restore_id = ?`,
        generation,
        now,
        now,
        now,
        body.restore_id,
      );
      this.sql.exec(
        `INSERT OR IGNORE INTO restore_coordinator_work (restore_id, coordinator_id, status)
         SELECT ?, coordinator_id, 'pending_loss' FROM coordinator_registry WHERE fleet_id = ?`,
        body.restore_id,
        operation.fleet_id,
      );
    });
    this.recordEvent(body.restore_id, "fleet_fence_activated", { generation });
    try {
      await this.advance(body.restore_id);
    } catch (error) {
      await this.retryOrPark(body.restore_id, error, null, null);
    }
    await this.schedule();
    return json({ ok: true, status: "accepted", restore_id: body.restore_id, plan_hash: body.plan_hash }, 202);
  }

  private async fenceCatalogs(operation: OperationRow): Promise<void> {
    let proof: CatalogProof;
    try {
      proof = await this.readCatalogProof(operation);
    } catch (error) {
      if (error instanceof RestoreContractViolation && !error.protocolError.retryable) {
        await this.failBeforeRestore(operation, error.protocolError.code, error.protocolError.message);
        return;
      }
      throw error;
    }
    if (proof.topology_hash !== operation.topology_hash || proof.topology_epoch !== operation.topology_epoch) {
      await this.failBeforeRestore(operation, "RESTORE_PLAN_STALE", "Catalog topology changed after preview.");
      return;
    }
    const pinned = operation.catalog_proof_json === null
      ? null
      : JSON.parse(operation.catalog_proof_json) as CatalogProof;
    const rawProofs = Array.isArray(pinned?.raw) ? pinned.raw : [];
    try {
      for (let i = 0; i < this.catalogShardCount(); i += 1) {
        const local = rawProofs.find((candidate) => isObject(candidate) && candidate.catalog_id === `catalog-${i}`);
        if (!isObject(local)) throw failure("RESTORE_ENUMERATION_INCOMPLETE", "Pinned catalog proof is missing.");
        await postToStub(this.env.CATALOG.getByName(`catalog-${i}`), "/restore-fence", {
          restoreId: operation.restore_id,
          generation: operation.fence_generation,
          action: "install",
          expectedTopologyHash: stringField(local, "topologyHash", "topology_hash"),
          expectedTopologyEpoch: numberField(local, "topologyEpoch", "topology_epoch"),
        });
      }
    } catch (error) {
      if (error instanceof RestoreContractViolation && !error.protocolError.retryable) {
        await this.failBeforeRestore(operation, error.protocolError.code, error.protocolError.message);
        return;
      }
      throw error;
    }
    this.sql.exec(
      "UPDATE restore_operations SET stage = 'fencing_participants', updated_at = ? WHERE restore_id = ?",
      new Date().toISOString(),
      operation.restore_id,
    );
    this.recordEvent(operation.restore_id, "catalog_fences_installed");
  }

  private async fenceNextParticipant(operation: OperationRow): Promise<void> {
    const participant = this.one<ParticipantRow>(
      `SELECT * FROM restore_participants WHERE restore_id = ? AND status = 'previewed'
       ORDER BY participant_id LIMIT 1`,
      operation.restore_id,
    );
    if (!participant) {
      if (Date.now() >= new Date(operation.execute_before).getTime()) {
        await this.failBeforeRestore(operation, "RESTORE_PLAN_STALE", "Restore fencing exceeded the immutable execution window.");
        return;
      }
      this.sql.exec(
        "UPDATE restore_operations SET stage = 'revalidating_participants', updated_at = ? WHERE restore_id = ?",
        new Date().toISOString(),
        operation.restore_id,
      );
      return;
    }
    let result: JsonObject;
    try {
      result = await postToStub(this.participantStub(participant), "/restore-fence", {
        fleet_id: operation.fleet_id,
        restore_id: operation.restore_id,
        generation: operation.fence_generation,
        action: "install",
      });
    } catch (error) {
      if (error instanceof RestoreContractViolation && !error.protocolError.retryable) {
        await this.failBeforeRestore(operation, error.protocolError.code, error.protocolError.message);
        return;
      }
      throw error;
    }
    const closedThrough = canonicalTimestamp(result.closed_through ?? result.closedThrough);
    const preFenceBookmark = stringField(result, "pre_fence_bookmark", "preFenceBookmark");
    if (closedThrough === null || preFenceBookmark === null) {
      throw failure("RESTORE_ENUMERATION_INCOMPLETE", "Participant fence omitted its pre-fence head or closed-through watermark.", {
        participant_id: participant.participant_id,
      });
    }
    this.sql.exec(
      `UPDATE restore_participants SET status = 'fenced', pre_fence_bookmark = ?, closed_through = ?
       WHERE restore_id = ? AND participant_id = ?`,
      preFenceBookmark,
      closedThrough,
      operation.restore_id,
      participant.participant_id,
    );
  }

  private async revalidateNextParticipant(operation: OperationRow): Promise<void> {
    const participant = this.one<ParticipantRow>(
      `SELECT * FROM restore_participants WHERE restore_id = ? AND status = 'fenced'
       ORDER BY participant_id LIMIT 1`,
      operation.restore_id,
    );
    if (!participant) {
      this.sql.exec(
        `UPDATE restore_operations SET phase = 'restoring', stage = 'materializing_loss', updated_at = ?
         WHERE restore_id = ?`,
        new Date().toISOString(),
        operation.restore_id,
      );
      this.sql.exec(
        "UPDATE fleet_restore_gate SET phase = 'restoring' WHERE singleton = 1 AND restore_id = ?",
        operation.restore_id,
      );
      return;
    }
    const current = await postToStub(this.participantStub(participant), "/pitr-preview", {
      fleet_id: operation.fleet_id,
      restore_id: operation.restore_id,
      generation: operation.fence_generation,
      cutoff: operation.cutoff,
    });
    const target = stringField(current, "target_bookmark", "targetBookmark");
    const head = stringField(current, "preview_bookmark", "previewBookmark", "current_bookmark", "currentBookmark");
    if (target !== participant.target_bookmark || head !== participant.pre_fence_bookmark) {
      await this.failBeforeRestore(operation, "RESTORE_PLAN_STALE", `Participant ${participant.participant_id} changed after preview.`);
      return;
    }
    this.sql.exec(
      "UPDATE restore_participants SET status = 'ready' WHERE restore_id = ? AND participant_id = ?",
      operation.restore_id,
      participant.participant_id,
    );
  }

  private async failBeforeRestore(operation: OperationRow, code: RestoreProtocolError["code"], message: string): Promise<void> {
    const participants = this.many<ParticipantRow>(
      "SELECT * FROM restore_participants WHERE restore_id = ? ORDER BY participant_id",
      operation.restore_id,
    );
    for (const participant of participants) {
      try {
        await postToStub(this.participantStub(participant), "/restore-fence", {
          fleet_id: operation.fleet_id,
          restore_id: operation.restore_id,
          generation: operation.fence_generation,
          action: "release",
        });
      } catch {
        // The external gate remains installed until every local release and
        // catalog release succeeds; a failed release is escalated below.
        await this.parkForManualRepair(operation.restore_id, failure("RESTORE_INTERRUPTED", message), participant.participant_id, null);
        return;
      }
    }
    for (let i = 0; i < this.catalogShardCount(); i += 1) {
      try {
        await postToStub(this.env.CATALOG.getByName(`catalog-${i}`), "/restore-fence", {
          restoreId: operation.restore_id,
          generation: operation.fence_generation,
          action: "release",
        });
      } catch {
        // CatalogDO release is idempotent when no local generation was ever
        // installed. A transport failure is still ambiguous, so retain the
        // external fleet gate and surface the exact catalog for repair.
        await this.parkForManualRepair(
          operation.restore_id,
          failure("RESTORE_INTERRUPTED", message),
          `catalog-${i}`,
          null,
        );
        return;
      }
    }
    const now = new Date().toISOString();
    this.ctx.storage.transactionSync(() => {
      this.sql.exec(
        "UPDATE fleet_restore_gate SET active = 0, restore_id = NULL, phase = NULL, activated_at = NULL WHERE singleton = 1 AND restore_id = ?",
        operation.restore_id,
      );
      this.sql.exec(
        "UPDATE restore_operations SET phase = 'failed', stage = 'failed', blocker_json = ?, completed_at = ?, updated_at = ? WHERE restore_id = ?",
        JSON.stringify([{ code, message, participant_id: null, tx_id: null }]),
        now,
        now,
        operation.restore_id,
      );
    });
    this.recordEvent(operation.restore_id, "execution_rejected_before_restore", { code });
  }

  private async materializeNextLossPage(operation: OperationRow): Promise<void> {
    const participant = this.one<ParticipantRow>(
      `SELECT * FROM restore_participants WHERE restore_id = ? AND participant_kind = 'shard'
       AND status = 'ready' ORDER BY participant_id LIMIT 1`,
      operation.restore_id,
    );
    if (participant) {
      const result = await postToStub(this.participantStub(participant), "/restore-loss-page", {
        fleet_id: operation.fleet_id,
        restore_id: operation.restore_id,
        generation: operation.fence_generation,
        cutoff: participant.target_checkpoint_at,
        through: participant.closed_through,
        limit: LOSS_PAGE_SIZE,
        after: participant.loss_cursor_json === null ? 0 : JSON.parse(participant.loss_cursor_json),
      });
      const entries = result.entries;
      if (!Array.isArray(entries)) throw failure("RESTORE_ENUMERATION_INCOMPLETE", "Participant loss page is invalid.");
      for (const entry of entries) {
        const lossHash = await hashCanonicalJson(entry as JsonValue);
        this.sql.exec(
          "INSERT OR IGNORE INTO restore_loss_entries (restore_id, participant_id, loss_hash, loss_json) VALUES (?, ?, ?, ?)",
          operation.restore_id,
          participant.participant_id,
          lossHash,
          JSON.stringify(entry),
        );
      }
      const complete = result.complete === true;
      const nextCursor = result.next_cursor ?? result.nextCursor ?? null;
      if (!complete && nextCursor === null) {
        throw failure("RESTORE_ENUMERATION_INCOMPLETE", "Participant loss enumeration ended without complete coverage.", {
          participant_id: participant.participant_id,
        });
      }
      this.sql.exec(
        `UPDATE restore_participants SET loss_cursor_json = ?, status = ?
         WHERE restore_id = ? AND participant_id = ?`,
        complete ? null : JSON.stringify(nextCursor),
        complete ? "loss_materialized" : "ready",
        operation.restore_id,
        participant.participant_id,
      );
      return;
    }

    const coordinator = this.one<{ coordinator_id: string; loss_cursor_json: string | null }>(
      `SELECT coordinator_id, loss_cursor_json FROM restore_coordinator_work
       WHERE restore_id = ? AND status = 'pending_loss' ORDER BY coordinator_id LIMIT 1`,
      operation.restore_id,
    );
    if (coordinator) {
      const participantId = `coordinator:${coordinator.coordinator_id}`;
      const result = await postToStub(this.env.COORDINATOR.getByName(coordinator.coordinator_id), "/restore-loss-page", {
        fleet_id: operation.fleet_id,
        restore_id: operation.restore_id,
        generation: operation.fence_generation,
        cutoff: operation.cutoff,
        through: new Date().toISOString(),
        limit: LOSS_PAGE_SIZE,
        after: coordinator.loss_cursor_json === null ? null : JSON.parse(coordinator.loss_cursor_json),
      });
      if (!Array.isArray(result.entries) || result.complete !== true) {
        throw failure("RESTORE_ENUMERATION_INCOMPLETE", "Coordinator loss evidence is incomplete.", {
          participant_id: participantId,
        });
      }
      const coordinatorClosedThrough = canonicalTimestamp(result.closed_through ?? result.closedThrough);
      if (coordinatorClosedThrough === null) {
        throw failure("RESTORE_ENUMERATION_INCOMPLETE", "Coordinator loss evidence omitted its drain watermark.", {
          participant_id: participantId,
        });
      }
      for (const entry of result.entries) {
        const lossHash = await hashCanonicalJson(entry as JsonValue);
        this.sql.exec(
          "INSERT OR IGNORE INTO restore_loss_entries (restore_id, participant_id, loss_hash, loss_json) VALUES (?, ?, ?, ?)",
          operation.restore_id,
          participantId,
          lossHash,
          JSON.stringify(entry),
        );
      }
      const requiresDiscard = result.requires_discard === true || result.entries.length > 0;
      this.sql.exec(
        `UPDATE restore_coordinator_work SET status = ?, closed_through = ?
         WHERE restore_id = ? AND coordinator_id = ?`,
        requiresDiscard ? "discard_required" : "retained",
        coordinatorClosedThrough,
        operation.restore_id,
        coordinator.coordinator_id,
      );
      return;
    }

    this.sql.exec(
      "UPDATE restore_operations SET stage = 'restoring_participants', updated_at = ? WHERE restore_id = ?",
      new Date().toISOString(),
      operation.restore_id,
    );
  }

  private async restoreNextParticipant(operation: OperationRow): Promise<void> {
    const participant = this.one<ParticipantRow>(
      `SELECT * FROM restore_participants WHERE restore_id = ? AND status NOT IN ('restored', 'verified', 'released')
       ORDER BY participant_id LIMIT 1`,
      operation.restore_id,
    );
    if (!participant) {
      this.sql.exec(
        `UPDATE restore_operations SET phase = 'reconciliation_pending', stage = 'reconciling', updated_at = ?
         WHERE restore_id = ?`,
        new Date().toISOString(),
        operation.restore_id,
      );
      this.sql.exec(
        "UPDATE fleet_restore_gate SET phase = 'reconciling' WHERE singleton = 1 AND restore_id = ?",
        operation.restore_id,
      );
      this.recordEvent(operation.restore_id, "participants_restored");
      return;
    }
    if (participant.status === "loss_materialized") {
      const result = await postToStub(this.participantStub(participant), "/pitr-stage", {
        fleet_id: operation.fleet_id,
        restore_id: operation.restore_id,
        generation: operation.fence_generation,
        target_bookmark: participant.target_bookmark,
      });
      const undo = stringField(result, "undo_bookmark", "undoBookmark");
      if (!undo) {
        throw failure("RESTORE_INTERRUPTED", "Participant did not return an undo bookmark after arming restore.", {
          participant_id: participant.participant_id,
        });
      }
      this.sql.exec(
        `UPDATE restore_participants SET status = 'staged', undo_bookmark = ?
         WHERE restore_id = ? AND participant_id = ?`,
        undo,
        operation.restore_id,
        participant.participant_id,
      );
      this.recordEvent(operation.restore_id, "participant_staged", { participant_id: participant.participant_id });
      return;
    }
    if (participant.status === "staged") {
      // Work-before-RPC: the expected session abort can sever the response.
      // Persist ambiguity first, then resolve it only through verification.
      this.sql.exec(
        `UPDATE restore_participants SET status = 'activation_requested'
         WHERE restore_id = ? AND participant_id = ?`,
        operation.restore_id,
        participant.participant_id,
      );
      try {
        await postToStub(this.participantStub(participant), "/pitr-apply", {
          fleet_id: operation.fleet_id,
          restore_id: operation.restore_id,
          generation: operation.fence_generation,
          target_bookmark: participant.target_bookmark,
        });
      } catch {
        // Expected when ctx.abort() starts the restored session. Verification
        // below is the sole authority for whether activation completed.
      }
      return;
    }
    if (participant.status === "activation_requested") {
      const result = await postToStub(this.participantStub(participant), "/pitr-verify", {
        fleet_id: operation.fleet_id,
        restore_id: operation.restore_id,
        generation: operation.fence_generation,
        target_bookmark: participant.target_bookmark,
        preview_bookmark: participant.preview_bookmark,
      });
      if (result.pending === true || result.status === "pending") {
        if (result.phase === "staged") {
          try {
            await postToStub(this.participantStub(participant), "/pitr-apply", {
              fleet_id: operation.fleet_id,
              restore_id: operation.restore_id,
              generation: operation.fence_generation,
            });
          } catch {
            // Verification on the next durable tick resolves the restart.
          }
        }
        return;
      }
      if (result.verified !== true) {
        throw failure("RESTORE_INVARIANT_FAILED", "Participant failed post-PITR verification.", {
          participant_id: participant.participant_id,
        });
      }
      this.sql.exec(
        `UPDATE restore_participants SET status = 'restored'
         WHERE restore_id = ? AND participant_id = ?`,
        operation.restore_id,
        participant.participant_id,
      );
      this.recordEvent(operation.restore_id, "participant_restored", { participant_id: participant.participant_id });
      const failAfter = Number.parseInt(this.env.RESTORE_REHEARSAL_FAIL_AFTER_PARTICIPANTS ?? "", 10);
      if (this.env.FAULT_INJECTION_ENABLED === "true" && Number.isSafeInteger(failAfter) && failAfter > 0) {
        const restoredCount = this.one<{ n: number }>(
          "SELECT COUNT(*) AS n FROM restore_participants WHERE restore_id = ? AND status = 'restored'",
          operation.restore_id,
        )?.n ?? 0;
        const marker = `rehearsal_fault:${operation.restore_id}`;
        if (restoredCount >= failAfter && this.one("SELECT key FROM restore_metadata WHERE key = ?", marker) === null) {
          this.sql.exec("INSERT INTO restore_metadata (key, value) VALUES (?, ?)", marker, new Date().toISOString());
          throw failure("RESTORE_INTERRUPTED", "Rehearsal-only interruption injected after participant restore.");
        }
      }
      return;
    }
    throw failure("RESTORE_INTERRUPTED", `Participant is in unexpected restore state ${participant.status}.`, {
      participant_id: participant.participant_id,
    });
  }

  private participantMessage(
    operation: OperationRow,
    envelope: JsonObject,
    phase: "prepare" | "status" | "recover",
    participant: JsonObject,
  ): JsonObject {
    return {
      protocol_version: envelope.protocol_version,
      tx_id: envelope.tx_id,
      epoch: participant.epoch ?? envelope.decision_epoch,
      phase,
      operation_hash: envelope.operation_hash,
      restore_id: operation.restore_id,
      generation: operation.fence_generation,
      ...(phase === "status" && typeof participant.prepare_bookmark === "string"
        ? { prepare_bookmark: participant.prepare_bookmark }
        : {}),
    };
  }

  private redoIntents(participant: JsonObject): JsonObject[] {
    if (!Array.isArray(participant.intents)) return [];
    return participant.intents.map((raw) => {
      if (!isObject(raw)) throw failure("RESTORE_HASH_CONTRADICTION", "Redo intent is malformed.");
      return {
        sql: raw.sql,
        params: raw.params,
        tenantId: raw.tenant_id,
        table: raw.table_name,
        partitionKey: raw.partition_key,
        ...(raw.vbucket === null || raw.vbucket === undefined ? {} : { vbucket: raw.vbucket }),
        ...(raw.operation === null || raw.operation === undefined ? {} : { op: raw.operation }),
        ...(raw.mirror_target_participant_id === null || raw.mirror_target_participant_id === undefined
          ? {}
          : { mirrorTargetShardId: raw.mirror_target_participant_id }),
      };
    });
  }

  private async reconcileNextTransaction(operation: OperationRow): Promise<void> {
    const record = this.one<{
      record_hash: string;
      tx_id: string;
      envelope_json: string | null;
    }>(
      `SELECT record_hash, tx_id, envelope_json FROM restore_manifest_records
       WHERE restore_id = ? AND reconciliation_status != 'complete'
       ORDER BY commit_decided_at, tx_id LIMIT 1`,
      operation.restore_id,
    );
    if (!record) {
      this.sql.exec(
        `UPDATE restore_operations SET phase = 'verifying', stage = 'verifying_participants', updated_at = ?
         WHERE restore_id = ?`,
        new Date().toISOString(),
        operation.restore_id,
      );
      this.sql.exec(
        "UPDATE fleet_restore_gate SET phase = 'verifying' WHERE singleton = 1 AND restore_id = ?",
        operation.restore_id,
      );
      return;
    }
    if (record.envelope_json === null) {
      throw failure("RESTORE_MANIFEST_GAP", "Materialized redo envelope is missing.", { tx_id: record.tx_id });
    }
    const envelope = JSON.parse(record.envelope_json) as JsonObject;
    if (!Array.isArray(envelope.participants)) {
      throw failure("RESTORE_HASH_CONTRADICTION", "Redo envelope participant set is invalid.", { tx_id: record.tx_id });
    }
    for (const rawParticipant of envelope.participants) {
      if (!isObject(rawParticipant) || typeof rawParticipant.participant_id !== "string") {
        throw failure("RESTORE_HASH_CONTRADICTION", "Redo participant is invalid.", { tx_id: record.tx_id });
      }
      const stub = this.shardStub(rawParticipant.participant_id);
      const statusMessage = this.participantMessage(operation, envelope, "status", rawParticipant);
      const status = await postToStub(stub, "/tx-status", statusMessage);
      const exactPrepareProof = typeof rawParticipant.prepare_bookmark === "string";
      if (exactPrepareProof && typeof status.prepare_bookmark_present !== "boolean") {
        throw failure("RESTORE_MANIFEST_GAP", "Participant did not prove whether the restored target contains prepare.", {
          tx_id: record.tx_id,
          participant_id: rawParticipant.participant_id,
        });
      }
      // `prepare_bookmark_present=false, found=true` is the expected durable
      // checkpoint after an earlier replayed /prepare whose response was lost.
      // /tx-status has already validated the incoming operation hash against
      // that found state, so it is safe to continue with /recover. The inverse
      // (the cutoff bookmark proves prepare, but the tx row is absent) is a
      // real restored-state contradiction.
      if (exactPrepareProof && status.prepare_bookmark_present === true && status.found !== true) {
        throw failure("RESTORE_HASH_CONTRADICTION", "Participant prepare marker contradicts its restored transaction state.", {
          tx_id: record.tx_id,
          participant_id: rawParticipant.participant_id,
        });
      }
      if (status.found === true && status.status === "aborted") {
        throw failure("RESTORE_HASH_CONTRADICTION", "Committed manifest decision conflicts with an aborted participant.", {
          tx_id: record.tx_id,
          participant_id: rawParticipant.participant_id,
        });
      }
      if (status.found !== true && (exactPrepareProof ? status.prepare_bookmark_present === false : true)) {
        const prepare = this.participantMessage(operation, envelope, "prepare", rawParticipant);
        prepare.intents = this.redoIntents(rawParticipant);
        await postToStub(stub, "/prepare", prepare);
      }
      if (status.status !== "committed") {
        const recover = this.participantMessage(operation, envelope, "recover", rawParticipant);
        recover.decision = "commit";
        await postToStub(stub, "/recover", recover);
      }
    }
    this.sql.exec(
      `UPDATE restore_manifest_records SET reconciliation_status = 'complete', last_error = NULL
       WHERE restore_id = ? AND record_hash = ?`,
      operation.restore_id,
      record.record_hash,
    );
    this.recordEvent(operation.restore_id, "transaction_reconciled", { tx_id: record.tx_id });
  }

  private async verifyNextParticipant(operation: OperationRow): Promise<void> {
    const participant = this.one<ParticipantRow>(
      `SELECT * FROM restore_participants WHERE restore_id = ? AND status = 'restored'
       ORDER BY participant_id LIMIT 1`,
      operation.restore_id,
    );
    if (!participant) {
      this.sql.exec(
        "UPDATE restore_operations SET stage = 'discarding_coordinators', updated_at = ? WHERE restore_id = ?",
        new Date().toISOString(),
        operation.restore_id,
      );
      return;
    }
    const result = await postToStub(this.participantStub(participant), "/pitr-verify", {
      fleet_id: operation.fleet_id,
      restore_id: operation.restore_id,
      generation: operation.fence_generation,
      target_bookmark: participant.target_bookmark,
      preview_bookmark: participant.preview_bookmark,
      require_invariants: true,
    });
    if (result.verified !== true) {
      throw failure("RESTORE_INVARIANT_FAILED", "Participant invariant verification failed.", {
        participant_id: participant.participant_id,
      });
    }
    this.sql.exec(
      "UPDATE restore_participants SET status = 'verified' WHERE restore_id = ? AND participant_id = ?",
      operation.restore_id,
      participant.participant_id,
    );
  }

  private async discardNextCoordinator(operation: OperationRow): Promise<void> {
    const coordinator = this.one<{ coordinator_id: string }>(
      `SELECT coordinator_id FROM restore_coordinator_work
       WHERE restore_id = ? AND status = 'discard_required' ORDER BY coordinator_id LIMIT 1`,
      operation.restore_id,
    );
    if (!coordinator) {
      this.sql.exec(
        "UPDATE restore_operations SET stage = 'releasing_participants', updated_at = ? WHERE restore_id = ?",
        new Date().toISOString(),
        operation.restore_id,
      );
      return;
    }
    const discarded = await postToStub(this.env.COORDINATOR.getByName(coordinator.coordinator_id), "/restore-discard", {
      fleet_id: operation.fleet_id,
      restore_id: operation.restore_id,
      generation: operation.fence_generation,
    });
    if (discarded.discarded_by_restore !== true) {
      throw failure("RESTORE_INVARIANT_FAILED", "Post-cutoff coordinator did not confirm durable discard.", {
        participant_id: `coordinator:${coordinator.coordinator_id}`,
      });
    }
    this.sql.exec(
      "UPDATE restore_coordinator_work SET status = 'discarded' WHERE restore_id = ? AND coordinator_id = ?",
      operation.restore_id,
      coordinator.coordinator_id,
    );
  }

  private async releaseNextParticipant(operation: OperationRow): Promise<void> {
    const participant = this.one<ParticipantRow>(
      `SELECT * FROM restore_participants WHERE restore_id = ? AND status = 'verified'
       ORDER BY participant_id LIMIT 1`,
      operation.restore_id,
    );
    if (participant) {
      await postToStub(this.participantStub(participant), "/pitr-release", {
        fleet_id: operation.fleet_id,
        restore_id: operation.restore_id,
        generation: operation.fence_generation,
      });
      this.sql.exec(
        "UPDATE restore_participants SET status = 'released' WHERE restore_id = ? AND participant_id = ?",
        operation.restore_id,
        participant.participant_id,
      );
      return;
    }
    await this.completeRestore(operation);
  }

  private async completeRestore(operation: OperationRow): Promise<void> {
    const incompleteParticipants = this.one<{ n: number }>(
      "SELECT COUNT(*) AS n FROM restore_participants WHERE restore_id = ? AND status != 'released'",
      operation.restore_id,
    )?.n ?? 0;
    const incompleteTransactions = this.one<{ n: number }>(
      "SELECT COUNT(*) AS n FROM restore_manifest_records WHERE restore_id = ? AND reconciliation_status != 'complete'",
      operation.restore_id,
    )?.n ?? 0;
    const incompleteCoordinators = this.one<{ n: number }>(
      `SELECT COUNT(*) AS n FROM restore_coordinator_work
       WHERE restore_id = ? AND status NOT IN ('retained', 'discarded')`,
      operation.restore_id,
    )?.n ?? 0;
    const current = this.operation(operation.restore_id);
    if (current?.stage !== "releasing_participants") {
      await this.schedule();
      return;
    }
    if (incompleteParticipants > 0 || incompleteTransactions > 0 || incompleteCoordinators > 0) {
      throw failure("RESTORE_INVARIANT_FAILED", "Restore cannot release the fleet with incomplete work.");
    }
    // Catalog fences release only after every restored participant has passed
    // invariants and released its local generation. The external fleet gate
    // remains active until the final transaction below.
    for (let i = 0; i < this.catalogShardCount(); i += 1) {
      await postToStub(this.env.CATALOG.getByName(`catalog-${i}`), "/restore-fence", {
        restoreId: operation.restore_id,
        generation: operation.fence_generation,
        action: "release",
      });
    }
    const lossHashes = this.many<{ loss_hash: string }>(
      "SELECT loss_hash FROM restore_loss_entries WHERE restore_id = ? ORDER BY participant_id, loss_hash",
      operation.restore_id,
    ).map((row) => row.loss_hash);
    const discardedWriteReportHash = await hashCanonicalJson(lossHashes);
    const now = new Date().toISOString();
    const closeWatermarks = [
      ...(this.many<{ closed_through: string | null }>(
        "SELECT closed_through FROM restore_participants WHERE restore_id = ?",
        operation.restore_id,
      ).map((row) => row.closed_through)),
      ...(this.many<{ closed_through: string | null }>(
        "SELECT closed_through FROM restore_coordinator_work WHERE restore_id = ?",
        operation.restore_id,
      ).map((row) => row.closed_through)),
    ].filter((value): value is string => value !== null);
    const closedThroughMs = Math.max(
      new Date(operation.fence_installed_at ?? now).getTime(),
      ...closeWatermarks.map((value) => new Date(value).getTime()),
    );
    const report = {
      discarded_write_count: lossHashes.length,
      discarded_write_report_hash: discardedWriteReportHash,
      discarded_write_report_complete: true,
      measured_rpo_ms: Math.max(0, closedThroughMs - new Date(operation.cutoff).getTime()),
      measured_rto_ms: Math.max(0, Date.now() - new Date(operation.started_at ?? now).getTime()),
      verified_at: now,
    };
    this.ctx.storage.transactionSync(() => {
      const current = this.operation(operation.restore_id);
      const pendingParticipants = this.one<{ n: number }>(
        "SELECT COUNT(*) AS n FROM restore_participants WHERE restore_id = ? AND status != 'released'",
        operation.restore_id,
      )?.n ?? 0;
      const pendingTransactions = this.one<{ n: number }>(
        "SELECT COUNT(*) AS n FROM restore_manifest_records WHERE restore_id = ? AND reconciliation_status != 'complete'",
        operation.restore_id,
      )?.n ?? 0;
      const pendingCoordinators = this.one<{ n: number }>(
        `SELECT COUNT(*) AS n FROM restore_coordinator_work
         WHERE restore_id = ? AND status NOT IN ('retained', 'discarded')`,
        operation.restore_id,
      )?.n ?? 0;
      if (current?.stage !== "releasing_participants") {
        throw failure("RESTORE_INVARIANT_FAILED", "Restore stage changed after final inventory linearization.");
      }
      if (pendingParticipants > 0 || pendingTransactions > 0 || pendingCoordinators > 0) {
        throw failure("RESTORE_INVARIANT_FAILED", "Restore became incomplete during final fleet release.");
      }
      this.sql.exec(
        `UPDATE restore_operations SET phase = 'complete', stage = 'complete', report_json = ?,
         completed_at = ?, updated_at = ?, blocker_json = NULL WHERE restore_id = ?`,
        JSON.stringify(report),
        now,
        now,
        operation.restore_id,
      );
      const clearedGate = this.sql.exec(
        `UPDATE fleet_restore_gate SET active = 0, restore_id = NULL, phase = NULL, activated_at = NULL
         WHERE singleton = 1 AND restore_id = ? AND generation = ?`,
        operation.restore_id,
        operation.fence_generation,
      );
      if (clearedGate.rowsWritten !== 1) {
        throw failure("RESTORE_INVARIANT_FAILED", "Restore completion did not durably clear the fleet gate.");
      }
    });
    this.recordEvent(operation.restore_id, "restore_complete", {
      discarded_write_count: lossHashes.length,
      measured_rpo_ms: report.measured_rpo_ms,
      measured_rto_ms: report.measured_rto_ms,
    });
  }

  private async handleStatus(request: Request): Promise<Response> {
    const raw: unknown = await request.json();
    validateRestoreStatusRequest(raw);
    const operation = this.operation(raw.restore_id);
    if (!operation) throw failure("RESTORE_INVALID_REQUEST", "Restore operation was not found.");
    if (operation.plan_hash === null && operation.stage === "failed") {
      throw failure("RESTORE_CONFLICT", "Restore preview failed before an immutable plan could be created.");
    }
    return json(await this.status(operation));
  }

  private async status(operation: OperationRow): Promise<RestoreStatusV1> {
    const participantsTotal = this.one<{ n: number }>(
      "SELECT COUNT(*) AS n FROM restore_participants WHERE restore_id = ?",
      operation.restore_id,
    )?.n ?? 0;
    const participantsRestored = this.one<{ n: number }>(
      `SELECT COUNT(*) AS n FROM restore_participants
       WHERE restore_id = ? AND status IN ('restored', 'verified', 'released')`,
      operation.restore_id,
    )?.n ?? 0;
    const transactionsTotal = this.one<{ n: number }>(
      "SELECT COUNT(*) AS n FROM restore_manifest_records WHERE restore_id = ?",
      operation.restore_id,
    )?.n ?? 0;
    const transactionsReconciled = this.one<{ n: number }>(
      "SELECT COUNT(*) AS n FROM restore_manifest_records WHERE restore_id = ? AND reconciliation_status = 'complete'",
      operation.restore_id,
    )?.n ?? 0;
    const blockers = operation.blocker_json === null ? [] : JSON.parse(operation.blocker_json);
    return {
      protocol_version: RESTORE_PROTOCOL_VERSION,
      format_version: RESTORE_STATUS_FORMAT_VERSION,
      restore_id: operation.restore_id,
      plan_hash: operation.plan_hash,
      fleet_id: operation.fleet_id,
      cutoff: operation.cutoff,
      phase: operation.phase as RestoreStatusV1["phase"],
      started_at: operation.started_at,
      updated_at: operation.updated_at,
      completed_at: operation.completed_at,
      progress: {
        participants_total: participantsTotal,
        participants_restored: participantsRestored,
        transactions_total: transactionsTotal,
        transactions_reconciled: transactionsReconciled,
      },
      blockers,
      report: operation.report_json === null ? null : JSON.parse(operation.report_json),
    };
  }

  private async handleRollback(request: Request): Promise<Response> {
    const raw: unknown = await request.json();
    validateRestoreRollbackRequest(raw);
    const body: RestoreRollbackRequestV1 = raw;
    const operation = this.operation(body.restore_id);
    if (!operation || operation.plan_hash !== body.plan_hash || operation.plan_json === null) {
      throw failure("RESTORE_PLAN_HASH_MISMATCH", "Restore rollback requires the exact immutable plan hash.");
    }
    if (operation.stage === "rolled_back") {
      return json({ ok: true, status: "already_started", restore_id: body.restore_id, plan_hash: body.plan_hash });
    }
    if (operation.phase === "rolling_back") {
      await this.schedule();
      return json({ ok: true, status: "already_started", restore_id: body.restore_id, plan_hash: body.plan_hash }, 202);
    }
    if (operation.stage === "manual_repair_required" && operation.resume_phase === "rolling_back") {
      const blockers = operation.blocker_json === null ? [] : JSON.parse(operation.blocker_json) as Array<{ code?: unknown }>;
      const retryableInterruption = blockers.length > 0
        && blockers.every((blocker) => blocker.code === "RESTORE_INTERRUPTED" || blocker.code === "RESTORE_UNAVAILABLE");
      if (!retryableInterruption || operation.resume_stage === null) {
        throw failure("RESTORE_CONFLICT", "Rollback is blocked by a non-retryable invariant or missing resume checkpoint.");
      }
      this.sql.exec(
        `UPDATE restore_operations SET phase = 'rolling_back', stage = resume_stage, blocker_json = NULL,
         resume_phase = NULL, resume_stage = NULL, updated_at = ? WHERE restore_id = ?`,
        new Date().toISOString(),
        operation.restore_id,
      );
      try {
        await this.advance(operation.restore_id);
      } catch (error) {
        await this.retryOrPark(operation.restore_id, error, null, null);
      }
      await this.schedule();
      return json({ ok: true, status: "already_started", restore_id: body.restore_id, plan_hash: body.plan_hash }, 202);
    }
    const plan = JSON.parse(operation.plan_json) as RestorePlanV1;
    if (
      plan.rollback.undo_supported !== true
      || plan.rollback.undo_expires_at === null
      || Date.now() >= new Date(plan.rollback.undo_expires_at).getTime()
    ) {
      throw failure("RESTORE_PLAN_STALE", "Restore undo bookmarks are unavailable or expired.");
    }
    if (operation.stage === "complete") {
      throw failure("RESTORE_CONFLICT", "A completed restore cannot be rolled back after its fleet fence is released.");
    }
    const releasedParticipants = this.one<{ n: number }>(
      "SELECT COUNT(*) AS n FROM restore_participants WHERE restore_id = ? AND status = 'released'",
      operation.restore_id,
    )?.n ?? 0;
    if (releasedParticipants > 0) {
      throw failure("RESTORE_CONFLICT", "Rollback is unavailable after any participant restore fence has been released.");
    }
    const discardedCoordinators = this.one<{ n: number }>(
      "SELECT COUNT(*) AS n FROM restore_coordinator_work WHERE restore_id = ? AND status = 'discarded'",
      operation.restore_id,
    )?.n ?? 0;
    if (discardedCoordinators > 0) {
      throw failure("RESTORE_CONFLICT", "Rollback is unavailable after irreversible post-cutoff coordinator discard.");
    }
    const gate = this.gate();
    if (gate.active !== 1 || gate.restore_id !== operation.restore_id || gate.generation !== operation.fence_generation) {
      throw failure("RESTORE_CONFLICT", "Rollback requires the original fleet fence to remain active.");
    }
    const undoCount = this.one<{ n: number }>(
      "SELECT COUNT(*) AS n FROM restore_participants WHERE restore_id = ? AND undo_bookmark IS NOT NULL",
      operation.restore_id,
    )?.n ?? 0;
    if (undoCount === 0) {
      throw failure("RESTORE_CONFLICT", "No participant restore has been armed, so there is nothing to roll back.");
    }
    const now = new Date().toISOString();
    this.ctx.storage.transactionSync(() => {
      this.sql.exec(
        `UPDATE restore_participants SET status = CASE
           WHEN undo_bookmark IS NULL THEN 'rollback_not_needed' ELSE 'rollback_pending' END
         WHERE restore_id = ? AND status != 'rollback_released'`,
        operation.restore_id,
      );
      this.sql.exec(
        `UPDATE restore_operations SET phase = 'rolling_back', stage = 'rollback_participants',
         blocker_json = NULL, resume_phase = NULL, resume_stage = NULL, updated_at = ? WHERE restore_id = ?`,
        now,
        operation.restore_id,
      );
      this.sql.exec(
        "UPDATE fleet_restore_gate SET phase = 'rolling_back' WHERE singleton = 1 AND restore_id = ?",
        operation.restore_id,
      );
    });
    this.recordEvent(operation.restore_id, "rollback_started", { participant_count: undoCount });
    try {
      await this.advance(operation.restore_id);
    } catch (error) {
      await this.retryOrPark(operation.restore_id, error, null, null);
    }
    await this.schedule();
    return json({ ok: true, status: "accepted", restore_id: body.restore_id, plan_hash: body.plan_hash }, 202);
  }

  private async rollbackNextParticipant(operation: OperationRow): Promise<void> {
    const participant = this.one<ParticipantRow>(
      `SELECT * FROM restore_participants WHERE restore_id = ?
       AND status IN ('rollback_pending', 'rollback_staged', 'rollback_activation_requested')
       ORDER BY participant_id LIMIT 1`,
      operation.restore_id,
    );
    if (!participant) {
      this.sql.exec(
        "UPDATE restore_operations SET stage = 'rollback_releasing_participants', updated_at = ? WHERE restore_id = ?",
        new Date().toISOString(),
        operation.restore_id,
      );
      return;
    }
    if (participant.undo_bookmark === null) {
      throw failure("RESTORE_INVARIANT_FAILED", "Rollback participant is missing its external undo bookmark.", {
        participant_id: participant.participant_id,
      });
    }
    if (participant.status === "rollback_pending") {
      await postToStub(this.participantStub(participant), "/pitr-undo", {
        fleet_id: operation.fleet_id,
        restore_id: operation.restore_id,
        generation: operation.fence_generation,
        target_bookmark: participant.undo_bookmark,
        mode: "undo",
      });
      this.sql.exec(
        "UPDATE restore_participants SET status = 'rollback_staged' WHERE restore_id = ? AND participant_id = ?",
        operation.restore_id,
        participant.participant_id,
      );
      return;
    }
    if (participant.status === "rollback_staged") {
      this.sql.exec(
        "UPDATE restore_participants SET status = 'rollback_activation_requested' WHERE restore_id = ? AND participant_id = ?",
        operation.restore_id,
        participant.participant_id,
      );
      try {
        await postToStub(this.participantStub(participant), "/pitr-apply", {
          fleet_id: operation.fleet_id,
          restore_id: operation.restore_id,
          generation: operation.fence_generation,
        });
      } catch {
        // The expected session abort is resolved through the durable marker
        // verification below, never from this ambiguous RPC response.
      }
      return;
    }
    const result = await postToStub(this.participantStub(participant), "/pitr-verify", {
      fleet_id: operation.fleet_id,
      restore_id: operation.restore_id,
      generation: operation.fence_generation,
      target_bookmark: participant.undo_bookmark,
      mode: "undo",
    });
    if (result.pending === true || result.status === "pending") {
      if (result.phase === "undo-staged") {
        try {
          await postToStub(this.participantStub(participant), "/pitr-apply", {
            fleet_id: operation.fleet_id,
            restore_id: operation.restore_id,
            generation: operation.fence_generation,
          });
        } catch {
          // Verification on the next durable tick resolves the restart.
        }
      }
      return;
    }
    if (result.verified !== true || result.mode !== "undo") {
      throw failure("RESTORE_INVARIANT_FAILED", "Participant undo verification failed.", {
        participant_id: participant.participant_id,
      });
    }
    this.sql.exec(
      "UPDATE restore_participants SET status = 'rollback_verified' WHERE restore_id = ? AND participant_id = ?",
      operation.restore_id,
      participant.participant_id,
    );
  }

  private async releaseNextRollbackParticipant(operation: OperationRow): Promise<void> {
    const participant = this.one<ParticipantRow>(
      `SELECT * FROM restore_participants WHERE restore_id = ?
       AND status IN ('rollback_verified', 'rollback_not_needed') ORDER BY participant_id LIMIT 1`,
      operation.restore_id,
    );
    if (participant) {
      await postToStub(this.participantStub(participant), "/pitr-release", {
        fleet_id: operation.fleet_id,
        restore_id: operation.restore_id,
        generation: operation.fence_generation,
      });
      this.sql.exec(
        "UPDATE restore_participants SET status = 'rollback_released' WHERE restore_id = ? AND participant_id = ?",
        operation.restore_id,
        participant.participant_id,
      );
      return;
    }
    const incomplete = this.one<{ n: number }>(
      "SELECT COUNT(*) AS n FROM restore_participants WHERE restore_id = ? AND status != 'rollback_released'",
      operation.restore_id,
    )?.n ?? 0;
    if (incomplete > 0) throw failure("RESTORE_INVARIANT_FAILED", "Rollback cannot release with incomplete participants.");
    for (let i = 0; i < this.catalogShardCount(); i += 1) {
      await postToStub(this.env.CATALOG.getByName(`catalog-${i}`), "/restore-fence", {
        restoreId: operation.restore_id,
        generation: operation.fence_generation,
        action: "release",
      });
    }
    const now = new Date().toISOString();
    this.ctx.storage.transactionSync(() => {
      this.sql.exec(
        `UPDATE restore_operations SET phase = 'rolled_back', stage = 'rolled_back', completed_at = ?,
         updated_at = ?, blocker_json = NULL WHERE restore_id = ?`,
        now,
        now,
        operation.restore_id,
      );
      this.sql.exec(
        `UPDATE fleet_restore_gate SET active = 0, restore_id = NULL, phase = NULL, activated_at = NULL
         WHERE singleton = 1 AND restore_id = ? AND generation = ?`,
        operation.restore_id,
        operation.fence_generation,
      );
    });
    this.recordEvent(operation.restore_id, "rollback_complete");
  }

  private async handleReconcile(request: Request): Promise<Response> {
    const raw: unknown = await request.json();
    validateRestoreReconcileRequest(raw);
    const operation = this.operation(raw.restore_id);
    if (!operation || operation.plan_hash !== raw.plan_hash) {
      throw failure("RESTORE_PLAN_HASH_MISMATCH", "Restore reconcile requires the exact immutable plan hash.");
    }
    if (operation.stage === "complete") {
      return json({ ok: true, status: "already_started", restore_id: operation.restore_id, plan_hash: operation.plan_hash });
    }
    if (this.gate().active !== 1 || this.gate().restore_id !== operation.restore_id) {
      throw failure("RESTORE_CONFLICT", "Restore fence is not active; reconciliation cannot safely resume.");
    }
    if (operation.stage === "manual_repair_required") {
      const blockers = operation.blocker_json === null ? [] : JSON.parse(operation.blocker_json) as Array<{ code?: unknown }>;
      const retryableInterruption = blockers.length > 0
        && blockers.every((blocker) => blocker.code === "RESTORE_INTERRUPTED" || blocker.code === "RESTORE_UNAVAILABLE");
      if (!retryableInterruption) {
        throw failure(
          "RESTORE_CONFLICT",
          "This blocker requires a versioned repair attestation or rollback; automatic reconcile is unsafe.",
        );
      }
      if (operation.resume_stage === null || operation.resume_phase === null) {
        throw failure("RESTORE_INTERRUPTED", "Restore operation has no durable resume checkpoint.");
      }
      this.sql.exec(
        `UPDATE restore_operations SET phase = resume_phase, stage = resume_stage, blocker_json = NULL,
          resume_phase = NULL, resume_stage = NULL, updated_at = ?
         WHERE restore_id = ?`,
        new Date().toISOString(),
        operation.restore_id,
      );
    }
    try {
      await this.advance(operation.restore_id);
    } catch (error) {
      await this.retryOrPark(operation.restore_id, error, null, null);
    }
    await this.schedule();
    return json({ ok: true, status: "already_started", restore_id: operation.restore_id, plan_hash: operation.plan_hash }, 202);
  }

  private async retryOrPark(
    restoreId: string,
    error: unknown,
    participantId: string | null,
    txId: string | null,
  ): Promise<void> {
    if (error instanceof RestoreContractViolation && error.protocolError.retryable) {
      const operation = this.operation(restoreId);
      if (!operation || TERMINAL_RESTORE_STAGES.includes(operation.stage as typeof TERMINAL_RESTORE_STAGES[number])) return;
      const attempt = operation.retry_count + 1;
      if (attempt < MAX_RESTORE_RETRY_ATTEMPTS) {
        const delayMs = Math.min(MAX_RESTORE_RETRY_DELAY_MS, RESTORE_ALARM_DELAY_MS * 2 ** (attempt - 1));
        const now = Date.now();
        this.sql.exec(
          `UPDATE restore_operations SET retry_count = ?, retry_started_at_ms = COALESCE(retry_started_at_ms, ?),
             retry_not_before_ms = ?, updated_at = ? WHERE restore_id = ?`,
          attempt,
          now,
          now + delayMs,
          new Date(now).toISOString(),
          restoreId,
        );
        this.recordEvent(restoreId, "restore_retry_scheduled", {
          code: error.protocolError.code,
          attempt,
          delay_ms: delayMs,
        });
        await this.schedule(delayMs);
        return;
      }
    }
    await this.parkForManualRepair(restoreId, error, participantId, txId);
  }

  private async parkForManualRepair(
    restoreId: string,
    error: unknown,
    participantId: string | null,
    txId: string | null,
  ): Promise<void> {
    const operation = this.operation(restoreId);
    if (!operation) return;
    if (TERMINAL_RESTORE_STAGES.includes(operation.stage as typeof TERMINAL_RESTORE_STAGES[number])) return;
    const protocolError = error instanceof RestoreContractViolation
      ? error.protocolError
      : restoreError("RESTORE_INTERRUPTED", error instanceof Error ? error.message : String(error));
    const fenced = operation.fence_generation !== null && this.gate().restore_id === restoreId;
    const phase = fenced ? "manual_repair_required" : "failed";
    const blocker = [{
      code: protocolError.code,
      message: protocolError.message,
      participant_id: participantId,
      tx_id: txId,
    }];
    const now = new Date().toISOString();
    this.sql.exec(
      `UPDATE restore_operations SET phase = ?, stage = ?, blocker_json = ?, resume_phase = ?, resume_stage = ?,
         retry_count = 0, retry_started_at_ms = NULL, retry_not_before_ms = NULL, updated_at = ?
       WHERE restore_id = ?`,
      phase,
      phase,
      JSON.stringify(blocker),
      fenced ? operation.phase : null,
      fenced ? operation.stage : null,
      now,
      restoreId,
    );
    if (fenced) {
      this.sql.exec(
        "UPDATE fleet_restore_gate SET phase = 'manual_repair_required' WHERE singleton = 1 AND restore_id = ?",
        restoreId,
      );
    }
    this.recordEvent(restoreId, phase, { code: protocolError.code });
    log("restore.manual_repair_required", { restoreId, code: protocolError.code, fenced });
  }

  private async advance(restoreId: string): Promise<void> {
    const operation = this.operation(restoreId);
    if (!operation) throw failure("RESTORE_INVALID_REQUEST", "Restore operation was not found.");
    if (operation.retry_not_before_ms !== null && operation.retry_not_before_ms > Date.now()) {
      await this.schedule(operation.retry_not_before_ms - Date.now());
      return;
    }
    switch (operation.stage) {
      case "closing_manifest":
        await this.closeManifest(operation);
        break;
      case "enumerating_manifest":
        await this.enumerateManifest(operation);
        break;
      case "materializing_envelopes":
        await this.materializeNextEnvelope(operation);
        break;
      case "previewing_participants":
        await this.previewNextParticipant(operation);
        break;
      case "previewed":
      case "complete":
      case "rolled_back":
      case "failed":
      case "manual_repair_required":
        return;
      case "fencing_catalogs":
        await this.fenceCatalogs(operation);
        break;
      case "fencing_participants":
        await this.fenceNextParticipant(operation);
        break;
      case "revalidating_participants":
        await this.revalidateNextParticipant(operation);
        break;
      case "materializing_loss":
        await this.materializeNextLossPage(operation);
        break;
      case "restoring_participants":
        await this.restoreNextParticipant(operation);
        break;
      case "reconciling":
        this.sql.exec(
          "UPDATE restore_operations SET phase = 'reconciling', updated_at = ? WHERE restore_id = ?",
          new Date().toISOString(),
          operation.restore_id,
        );
        await this.reconcileNextTransaction(operation);
        break;
      case "verifying_participants":
        await this.verifyNextParticipant(operation);
        break;
      case "discarding_coordinators":
        await this.discardNextCoordinator(operation);
        break;
      case "releasing_participants":
        await this.releaseNextParticipant(operation);
        break;
      case "rollback_participants":
        await this.rollbackNextParticipant(operation);
        break;
      case "rollback_releasing_participants":
        await this.releaseNextRollbackParticipant(operation);
        break;
      default:
        throw failure("RESTORE_INTERRUPTED", `Unknown durable restore stage ${operation.stage}.`);
    }
    this.sql.exec(
      `UPDATE restore_operations SET retry_count = 0, retry_started_at_ms = NULL, retry_not_before_ms = NULL
       WHERE restore_id = ?`,
      restoreId,
    );
    const refreshed = this.operation(restoreId);
    if (refreshed && !["previewed", "complete", "rolled_back", "failed", "manual_repair_required"].includes(refreshed.stage)) {
      await this.schedule();
    }
  }
}
