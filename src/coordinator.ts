import { DurableObject } from "cloudflare:workers";
import {
  COORDINATOR_RETENTION_DAYS,
  CURRENT_PROTOCOL_VERSION,
  DEFAULT_DURABLE_OBJECT_RETRY_AFTER_MS,
  MANIFEST_TERMINAL_INTENT_FORMAT_VERSION,
  MAX_DURABLE_OBJECT_RETRY_AFTER_MS,
  CURRENT_REDO_ENVELOPE_FORMAT_VERSION,
  REDO_ENVELOPE_V1_FORMAT_VERSION,
  TransactionContractViolation,
  assertReadableTransactionStateModelVersion,
  assertReadableProtocolVersion,
  assertTransactionTransition,
  canonicalJson,
  classifyDurableObjectFailure,
  createManifestRegistration,
  durableObjectUnavailableError,
  hashCanonicalJson,
  hashManifestFinalizeIntent,
  hashManifestRecordV2,
  hashManifestReservation,
  hashRedoEnvelope,
  hashParticipantOperations,
  isCommitDecidedOrLater,
  isTransactionState,
  reliabilitySloEvent,
  sha256Hex,
  transactionError,
  validateRedoEnvelope,
  validateRedoEnvelopeStructure,
  validateWritableRedoEnvelopeStructure,
  validateManifestCancelIntent,
  validateManifestFinalizeIntent,
  validateManifestRegistration,
  validateManifestRecordV2,
  validateManifestReservation,
  type JsonValue,
  type ManifestCancelIntentV1,
  type ManifestFinalizeIntentV1,
  type ManifestRecordV2,
  type ManifestReservationV1,
  type ManifestRegistrationV1,
  type ParticipantPhase,
  type ParticipantPhaseMessageV1,
  type RedoEnvelopeV1,
  type RedoEnvelopeV2,
  type ReadableRedoEnvelope,
  type RedoParticipantV1,
  type RedoParticipantV2,
  type TransactionProtocolError,
  type TransactionState,
} from "../packages/contracts/src/index.js";
import type {
  ManifestAdmissionResult,
  ManifestCancelRequestV1,
  ManifestCancelResult,
  ManifestFinalizeRequestV1,
  ManifestFinalizeResult,
  ManifestReleaseRequest,
  ManifestReleaseResult,
  ManifestReserveRequestV1,
  ManifestReserveResult,
  ManifestRouteAssignmentRequestV1,
  ManifestRouteAssignmentResult,
  ManifestServiceRegisterResult,
  ManifestV2ReleaseRequest,
  ManifestQuarantineResolutionRequestV1,
  ManifestQuarantineResolutionResult,
} from "../workers/control-plane/src/manifest-types.js";
import { MANIFEST_CIRCUIT_POLICY } from "../workers/control-plane/src/manifest-types.js";
import { json } from "./http";
import { log } from "./log";

type GeneratedControlPlaneService = Cloudflare.Env["CONTROL_PLANE"];

export interface TransactionManifestService {
  assignManifestRoute(request: ManifestRouteAssignmentRequestV1): Promise<ManifestRouteAssignmentResult>;
  reserveManifest(request: ManifestReserveRequestV1): Promise<ManifestReserveResult>;
  finalizeManifest(request: ManifestFinalizeRequestV1): Promise<ManifestFinalizeResult>;
  cancelManifest(request: ManifestCancelRequestV1): Promise<ManifestCancelResult>;
  resolveManifestQuarantine?(request: ManifestQuarantineResolutionRequestV1): Promise<ManifestQuarantineResolutionResult>;
  releaseManifestV2(request: ManifestV2ReleaseRequest): Promise<ManifestReleaseResult>;
  checkManifestAdmission(
    request: Parameters<GeneratedControlPlaneService["checkManifestAdmission"]>[0],
  ): Promise<ManifestAdmissionResult>;
  registerManifest(registration: ManifestRegistrationV1): Promise<ManifestServiceRegisterResult>;
  releaseManifestRetention(
    request: Parameters<GeneratedControlPlaneService["releaseManifestRetention"]>[0],
  ): Promise<ManifestReleaseResult>;
}

type Assert<T extends true> = T;
type _GeneratedControlPlaneBindingIsCompatible = Assert<
  Pick<
    GeneratedControlPlaneService,
    "assignManifestRoute" | "reserveManifest" | "finalizeManifest" | "cancelManifest" | "resolveManifestQuarantine" | "releaseManifestV2"
    | "checkManifestAdmission" | "registerManifest" | "releaseManifestRetention"
  > extends TransactionManifestService
    ? true
    : false
>;

type RestoreCoordinatorStub = { fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> };
type RestoreCoordinatorNamespace = { getByName(name: string): RestoreCoordinatorStub };
type CoordinatorRegistrationResult =
  | { disposition: "registered" }
  | { disposition: "discard_required"; restoreId: string; generation: number };
type RestoreGateSnapshot = {
  ok: true;
  state: "open" | "fenced";
  fleetId: string;
  restoreId: string | null;
  generation: number;
  phase?: string;
};

export interface CoordinatorPitrPort {
  getCurrentBookmark(): Promise<string>;
  getBookmarkForTime(timestamp: number | Date): Promise<string>;
  stageRestoreBookmark(bookmark: string): Promise<string>;
  abort(): void;
}

type CoordinatorEnv = Omit<Cloudflare.Env, "CONTROL_PLANE"> & {
  CONTROL_PLANE?: TransactionManifestService;
  RESTORE_COORDINATOR?: RestoreCoordinatorNamespace;
  DEPLOYMENT_FLEET_ID?: string;
};

type BeginIntent = {
  sql: string;
  params?: JsonValue[];
  tenantId: string;
  table: string;
  partitionKey: string;
  mirrorTargetShardId?: string;
  vbucket?: number;
  op?: "insert" | "update" | "delete" | "upsert";
};

type BeginParticipant = { shardId: string; intents: BeginIntent[] };
type BeginPayload = {
  txId: string;
  fleetId?: string;
  coordinatorId?: string;
  participants: BeginParticipant[];
};

type TxRow = {
  tx_id: string;
  status: string;
  created_at: string;
  participant_shards_json: string;
  operation_json: string;
  operation_hash: string;
  protocol_version: number;
  state_model_version: number;
  epoch: number;
  decision: string;
  fleet_id: string;
  coordinator_id: string;
  redo_envelope_json: string | null;
  manifest_registration_json: string | null;
  manifest_route_assignment_request_json: string | null;
  manifest_reservation_json: string | null;
  manifest_reservation_hash: string | null;
  manifest_finalize_request_json: string | null;
  manifest_cancel_request_json: string | null;
  redo_envelope_intent_json: string | null;
  manifest_record_json: string | null;
  manifest_record_hash: string | null;
  commit_decided_at_ms: number | null;
  decision_sequence: number | null;
  result_json: string | null;
  last_error: string | null;
};

type RecoveryAction = "reserve" | "finalize" | "cancel" | "manifest" | "commit" | "abort" | "legacy_abort" | "release";
type CoordinatorIdentity = "current" | "legacy" | "invalid";
const RECOVERY_BASE_DELAY_MS = 5_000;
const RECOVERY_MAX_DELAY_MS = 60_000;
const DAY_MS = 24 * 60 * 60 * 1_000;
/** New transactions use reservation/finalization state model 2. Explicit
 * predecessor adoption remains on model 1 so its V1 recovery bytes retain
 * their original interpretation. */
const COORDINATOR_WRITE_STATE_MODEL_VERSION = 2;
const LEGACY_ADOPTION_STATE_MODEL_VERSION = 1;
const STATE_MODEL_1_STATES: ReadonlySet<TransactionState> = new Set([
  "new",
  "preparing",
  "abort_decided",
  "aborting",
  "aborted",
  "prepared",
  "commit_decided",
  "commit_pending_manifest",
  "manifest_registered",
  "committing",
  "committed_pending_ack",
  "committed",
  "quarantined",
]);
const COORDINATOR_GATE_MUTATING_PATHS = new Set(["/begin", "/force-abort", "/resolve-manifest-quarantine"]);

class CoordinatorCasLost extends Error {
  constructor(readonly state: TransactionState) {
    super(`Coordinator state changed to ${state}.`);
  }
}

class RestoreGateDenied extends Error {}

function protocolResponse(error: TransactionProtocolError): Response {
  return json({ error }, error.http_status);
}

function contractResponse(error: unknown): Response | null {
  return error instanceof TransactionContractViolation ? protocolResponse(error.protocolError) : null;
}

function retentionDeadline(decidedAt: string): string {
  return new Date(new Date(decidedAt).getTime() + COORDINATOR_RETENTION_DAYS * DAY_MS).toISOString();
}

/** One durable coordinator per txId. All state changes are validated by the
 * shared contract. External effects happen only after their forward work and
 * irreversible decision are durable. */
export class CoordinatorDO extends DurableObject<CoordinatorEnv> {
  private readonly sql: SqlStorage;
  private readonly coordinatorEnv: CoordinatorEnv;
  private readonly routes: Record<string, (request: Request) => Promise<Response>>;
  private schemaEnsured = false;
  // Unreachable compatibility helpers below retain this adapter until their
  // private implementation is removed; no Coordinator PITR route is exposed.
  private pitrPort: CoordinatorPitrPort;
  private restoreCoordinatorOverride: RestoreCoordinatorNamespace | null = null;
  private activeMutations = 0;
  private mutationDrainWaiters: Array<() => void> = [];
  private recoveryInFlight = new Set<string>();
  private releaseRecoveryInFlight = new Map<string, Promise<void>>();

  constructor(ctx: DurableObjectState, env: CoordinatorEnv) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.coordinatorEnv = env;
    this.pitrPort = {
      getCurrentBookmark: () => ctx.storage.getCurrentBookmark(),
      getBookmarkForTime: (timestamp) => ctx.storage.getBookmarkForTime(timestamp),
      stageRestoreBookmark: (bookmark) => ctx.storage.onNextSessionRestoreBookmark(bookmark),
      abort: () => ctx.abort(),
    };
    this.routes = {
      "/tx-status": this.handleTxStatus.bind(this),
      "/begin": this.handleBegin.bind(this),
      "/force-abort": this.handleForceAbort.bind(this),
      "/resolve-manifest-quarantine": this.handleResolveManifestQuarantine.bind(this),
      "/stats": this.handleStats.bind(this),
      "/redo-envelope": this.handleRedoEnvelope.bind(this),
      "/recovery-envelope": this.handleRedoEnvelope.bind(this),
      "/restore-loss-page": this.handleRestoreLossPage.bind(this),
      "/restore-discard": this.handleRestoreDiscard.bind(this),
    };
  }

  private ensureColumn(table: string, column: string, definition: string): void {
    const existing = Array.from(this.sql.exec(`PRAGMA table_info(${table})`)) as Array<{ name: string }>;
    if (!existing.some((candidate) => candidate.name === column)) {
      this.sql.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
    }
  }

  private ensureSchema(): void {
    if (this.schemaEnsured) return;
    this.schemaEnsured = true;
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS transactions (
        tx_id TEXT PRIMARY KEY,
        status TEXT NOT NULL,
        participant_shards_json TEXT NOT NULL,
        operation_json TEXT NOT NULL,
        operation_hash TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        attempt_count INTEGER NOT NULL DEFAULT 0,
        last_error TEXT
      )
    `);
    this.ensureColumn("transactions", "operation_hash", "TEXT NOT NULL DEFAULT ''");
    this.ensureColumn("transactions", "protocol_version", "INTEGER NOT NULL DEFAULT 1");
    this.ensureColumn("transactions", "state_model_version", "INTEGER NOT NULL DEFAULT 1");
    this.ensureColumn("transactions", "epoch", "INTEGER NOT NULL DEFAULT 1");
    this.ensureColumn("transactions", "decision", "TEXT NOT NULL DEFAULT 'undecided'");
    this.ensureColumn("transactions", "fleet_id", "TEXT NOT NULL DEFAULT 'default'");
    this.ensureColumn("transactions", "coordinator_id", "TEXT NOT NULL DEFAULT ''");
    this.ensureColumn("transactions", "redo_envelope_json", "TEXT");
    this.ensureColumn("transactions", "manifest_registration_json", "TEXT");
    this.ensureColumn("transactions", "manifest_route_assignment_request_json", "TEXT");
    this.ensureColumn("transactions", "manifest_reservation_json", "TEXT");
    this.ensureColumn("transactions", "manifest_reservation_hash", "TEXT");
    this.ensureColumn("transactions", "manifest_finalize_request_json", "TEXT");
    this.ensureColumn("transactions", "manifest_cancel_request_json", "TEXT");
    this.ensureColumn("transactions", "redo_envelope_intent_json", "TEXT");
    this.ensureColumn("transactions", "manifest_record_json", "TEXT");
    this.ensureColumn("transactions", "manifest_record_hash", "TEXT");
    this.ensureColumn("transactions", "commit_decided_at_ms", "INTEGER");
    this.ensureColumn("transactions", "decision_sequence", "INTEGER");
    this.ensureColumn("transactions", "result_json", "TEXT");
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS transaction_participants (
        tx_id TEXT NOT NULL,
        shard_id TEXT NOT NULL,
        phase_status TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (tx_id, shard_id)
      )
    `);
    this.ensureColumn("transaction_participants", "epoch", "INTEGER NOT NULL DEFAULT 1");
    this.ensureColumn("transaction_participants", "operation_hash", "TEXT NOT NULL DEFAULT ''");
    this.ensureColumn("transaction_participants", "prepare_bookmark", "TEXT");
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS recovery_queue (
        tx_id TEXT PRIMARY KEY,
        action TEXT NOT NULL,
        next_attempt_at TEXT NOT NULL,
        attempt_count INTEGER NOT NULL DEFAULT 0
      )
    `);
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS manifest_admission_circuit (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        failure_count INTEGER NOT NULL DEFAULT 0,
        failure_window_started_at_ms INTEGER NOT NULL DEFAULT 0,
        open_until_ms INTEGER NOT NULL DEFAULT 0,
        open_count INTEGER NOT NULL DEFAULT 0,
        half_open_probe INTEGER NOT NULL DEFAULT 0,
        half_open_probe_until_ms INTEGER NOT NULL DEFAULT 0
      )
    `);
    this.sql.exec(
      `INSERT OR IGNORE INTO manifest_admission_circuit
        (singleton, failure_count, failure_window_started_at_ms, open_until_ms, open_count, half_open_probe, half_open_probe_until_ms)
       VALUES (1, 0, 0, 0, 0, 0, 0)`,
    );
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS coordinator_restore_discard (
        singleton       INTEGER PRIMARY KEY CHECK (singleton = 1),
        restore_id      TEXT NOT NULL,
        generation      INTEGER NOT NULL,
        discarded_at_ms INTEGER NOT NULL
      )
    `);
  }

  private one<T extends object>(statement: string, ...params: unknown[]): T | null {
    for (const row of this.sql.exec(statement, ...params)) return row as T;
    return null;
  }

  private loadTx(txId: string): TxRow | null {
    return this.one<TxRow>(
      `SELECT tx_id, status, created_at, participant_shards_json, operation_json, operation_hash,
              protocol_version, state_model_version, epoch, decision, fleet_id,
              coordinator_id, redo_envelope_json, manifest_registration_json,
              manifest_route_assignment_request_json, manifest_reservation_json,
              manifest_reservation_hash, manifest_finalize_request_json,
              manifest_cancel_request_json, redo_envelope_intent_json,
              manifest_record_json, manifest_record_hash, commit_decided_at_ms,
              decision_sequence, result_json, last_error
         FROM transactions WHERE tx_id = ?`,
      txId,
    );
  }

  private stateOf(row: TxRow): TransactionState {
    assertReadableProtocolVersion(row.protocol_version);
    assertReadableTransactionStateModelVersion(row.state_model_version);
    if (!isTransactionState(row.status)) {
      throw new TransactionContractViolation(
        transactionError("TX_VERSION_UNSUPPORTED", `Unknown durable transaction state ${row.status}.`),
      );
    }
    if (row.state_model_version === 1 && !STATE_MODEL_1_STATES.has(row.status)) {
      throw new TransactionContractViolation(
        transactionError("TX_VERSION_UNSUPPORTED", `Transaction state ${row.status} requires a newer state model.`),
      );
    }
    return row.status;
  }

  private participants(row: TxRow): BeginParticipant[] {
    const parsed = JSON.parse(row.operation_json) as BeginParticipant[];
    if (!Array.isArray(parsed)) {
      throw new TransactionContractViolation(transactionError("TX_ENVELOPE_INVALID", "Stored participant set is invalid."));
    }
    return parsed;
  }

  private normalizedParticipants(participants: BeginParticipant[]): BeginParticipant[] {
    // Participant order was caller-controlled in the predecessor. Shard
    // identity is a set, while each shard's intent order remains semantic.
    return [...participants].sort((left, right) => left.shardId.localeCompare(right.shardId));
  }

  private async coordinatorIdentity(row: TxRow): Promise<CoordinatorIdentity> {
    const participants = this.participants(row);
    const currentHash = await hashParticipantOperations(this.redoParticipants(participants, row.epoch || 1));
    if (row.operation_hash === currentHash) return "current";
    const hasNoV1DecisionArtifacts = row.redo_envelope_json === null && row.manifest_registration_json === null;
    if (!hasNoV1DecisionArtifacts) return "invalid";
    if (row.operation_hash === "") return "legacy";
    // The predecessor hashed the exact JSON serialization stored alongside
    // the digest. Only that verified shape qualifies for upgrade adoption;
    // arbitrary noncanonical hashes remain fail-closed.
    const predecessorHash = await sha256Hex(JSON.stringify(participants));
    return row.operation_hash === predecessorHash ? "legacy" : "invalid";
  }

  private participantMessage(row: TxRow, phase: ParticipantPhase): ParticipantPhaseMessageV1 {
    return {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      tx_id: row.tx_id,
      epoch: row.epoch,
      phase,
      operation_hash: row.operation_hash,
    };
  }

  private async callShard(
    row: TxRow,
    participant: BeginParticipant,
    phase: "prepare" | "commit" | "abort",
    options: { legacyPredecisionAbort?: boolean } = {},
  ): Promise<Response> {
    const stub = this.coordinatorEnv.SHARD.get(this.coordinatorEnv.SHARD.idFromName(participant.shardId));
    return stub.fetch(`https://shard.internal/${phase}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        ...this.participantMessage(row, phase),
        ...(phase === "prepare" ? { intents: participant.intents } : {}),
        ...(options.legacyPredecisionAbort ? { legacy_predecision_abort: true } : {}),
      }),
    });
  }

  private async reconcileLegacyRecovery(row: TxRow, action: "/commit" | "/abort"): Promise<boolean> {
    const outcomes = await Promise.allSettled(
      this.participants(row).map(async (participant) => {
        const stub = this.coordinatorEnv.SHARD.get(this.coordinatorEnv.SHARD.idFromName(participant.shardId));
        return stub.fetch(`https://shard.internal${action}`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ coordinatorTxId: row.tx_id }),
        });
      }),
    );
    const allAcknowledged = outcomes.every((outcome) => outcome.status === "fulfilled" && outcome.value.ok);
    if (allAcknowledged) this.sql.exec("DELETE FROM recovery_queue WHERE tx_id = ?", row.tx_id);
    return allAcknowledged;
  }

  private beginAdmissionAttempt(now: number):
    | { readonly state: "closed" | "probe" }
    | { readonly state: "open"; readonly retry_after_ms: number } {
    return this.ctx.storage.transactionSync(() => {
      const circuit = this.one<{
        open_until_ms: number;
        half_open_probe: number;
        half_open_probe_until_ms: number;
      }>("SELECT open_until_ms, half_open_probe, half_open_probe_until_ms FROM manifest_admission_circuit WHERE singleton = 1");
      if (!circuit) throw new Error("Manifest admission circuit row is missing.");
      if (circuit.open_until_ms > now) {
        return {
          state: "open",
          retry_after_ms: Math.min(
            MAX_DURABLE_OBJECT_RETRY_AFTER_MS,
            Math.max(1, Math.ceil(circuit.open_until_ms - now)),
          ),
        };
      }
      if (circuit.open_until_ms > 0) {
        if (circuit.half_open_probe === 1 && circuit.half_open_probe_until_ms > now) {
          return {
            state: "open",
            retry_after_ms: Math.min(
              DEFAULT_DURABLE_OBJECT_RETRY_AFTER_MS,
              Math.max(1, Math.ceil(circuit.half_open_probe_until_ms - now)),
            ),
          };
        }
        this.sql.exec(
          "UPDATE manifest_admission_circuit SET half_open_probe = 1, half_open_probe_until_ms = ? WHERE singleton = 1",
          now + MANIFEST_CIRCUIT_POLICY.failure_window_ms,
        );
        return { state: "probe" };
      }
      return { state: "closed" };
    });
  }

  private recordAdmissionSuccess(): void {
    this.sql.exec(
      `UPDATE manifest_admission_circuit
       SET failure_count = 0, failure_window_started_at_ms = 0, open_until_ms = 0,
           open_count = 0, half_open_probe = 0, half_open_probe_until_ms = 0
       WHERE singleton = 1`,
    );
  }

  private recordAdmissionNeutral(): void {
    this.sql.exec(
      `UPDATE manifest_admission_circuit
       SET half_open_probe = 0, half_open_probe_until_ms = 0
       WHERE singleton = 1`,
    );
  }

  private recordAdmissionFailure(now: number, attempt: "closed" | "probe"): void {
    this.ctx.storage.transactionSync(() => {
      const circuit = this.one<{
        failure_count: number;
        failure_window_started_at_ms: number;
        open_count: number;
      }>("SELECT failure_count, failure_window_started_at_ms, open_count FROM manifest_admission_circuit WHERE singleton = 1");
      if (!circuit) throw new Error("Manifest admission circuit row is missing.");
      const insideWindow = circuit.failure_window_started_at_ms > 0
        && now - circuit.failure_window_started_at_ms <= MANIFEST_CIRCUIT_POLICY.failure_window_ms;
      const failureCount = insideWindow ? circuit.failure_count + 1 : 1;
      const shouldOpen = attempt === "probe" || failureCount >= MANIFEST_CIRCUIT_POLICY.failure_threshold;
      if (shouldOpen) {
        const openCount = circuit.open_count + 1;
        const cooldown = Math.min(
          MANIFEST_CIRCUIT_POLICY.maximum_cooldown_ms,
          MANIFEST_CIRCUIT_POLICY.failure_window_ms * 2 ** Math.min(circuit.open_count, 10),
        );
        this.sql.exec(
          `UPDATE manifest_admission_circuit
           SET failure_count = 0, failure_window_started_at_ms = 0, open_until_ms = ?,
               open_count = ?, half_open_probe = 0, half_open_probe_until_ms = 0
           WHERE singleton = 1`,
          now + cooldown,
          openCount,
        );
      } else {
        this.sql.exec(
          `UPDATE manifest_admission_circuit
           SET failure_count = ?, failure_window_started_at_ms = ?, half_open_probe = 0,
               half_open_probe_until_ms = 0
           WHERE singleton = 1`,
          failureCount,
          insideWindow ? circuit.failure_window_started_at_ms : now,
        );
      }
    });
  }

  private async checkManifestAdmission(request: { fleet_id: string; tx_id: string; commit_decided_at: string }): Promise<ManifestAdmissionResult> {
    const now = Date.now();
    const attempt = this.beginAdmissionAttempt(now);
    if (attempt.state === "open") {
      log("reliability.slo", { ...reliabilitySloEvent({
        component: "coordinator",
        operation: "manifest_admission",
        outcome: "controlled_failure",
        classification: { overloaded: false, retryable: true, retry_after_ms: attempt.retry_after_ms },
      }) });
      return {
        ok: false,
        status: "unavailable",
        http_status: 503,
        error: transactionError(
          "TX_MANIFEST_UNAVAILABLE",
          "Manifest admission circuit is open; retry later.",
          undefined,
          { overloaded: false, retryable: true, retry_after_ms: attempt.retry_after_ms },
        ),
        circuit: {
          count_toward_open: true,
          failure_threshold: MANIFEST_CIRCUIT_POLICY.failure_threshold,
          failure_window_ms: MANIFEST_CIRCUIT_POLICY.failure_window_ms,
          maximum_cooldown_ms: MANIFEST_CIRCUIT_POLICY.maximum_cooldown_ms,
        },
      };
    }
    const service = this.coordinatorEnv.CONTROL_PLANE;
    let result: ManifestAdmissionResult;
    try {
      if (!service) throw new Error("CONTROL_PLANE binding is unavailable.");
      result = await service.checkManifestAdmission(request);
    } catch (error) {
      const classification = classifyDurableObjectFailure(error);
      log("reliability.slo", { ...reliabilitySloEvent({
        component: "coordinator",
        operation: "manifest_admission",
        outcome: "controlled_failure",
        classification,
      }) });
      result = {
        ok: false,
        status: "unavailable",
        http_status: 503,
        error: durableObjectUnavailableError(
          error,
          "TX_MANIFEST_UNAVAILABLE",
          "Manifest admission is unavailable before prepare.",
        ),
        circuit: {
          count_toward_open: true,
          failure_threshold: MANIFEST_CIRCUIT_POLICY.failure_threshold,
          failure_window_ms: MANIFEST_CIRCUIT_POLICY.failure_window_ms,
          maximum_cooldown_ms: MANIFEST_CIRCUIT_POLICY.maximum_cooldown_ms,
        },
      };
    }
    if (result.ok) this.recordAdmissionSuccess();
    else if (result.error.retryable || result.error.overloaded) this.recordAdmissionFailure(now, attempt.state);
    else this.recordAdmissionNeutral();
    return result;
  }

  private async ensureAlarmScheduled(atLeastByMs: number): Promise<void> {
    const existing = await this.ctx.storage.getAlarm();
    if (existing === null || existing > atLeastByMs) await this.ctx.storage.setAlarm(atLeastByMs);
  }

  private queueRecovery(txId: string, action: RecoveryAction, at: string, attemptCount = 0): void {
    this.sql.exec(
      `INSERT INTO recovery_queue (tx_id, action, next_attempt_at, attempt_count)
       VALUES (?, ?, ?, ?)
       ON CONFLICT(tx_id) DO UPDATE SET action = excluded.action,
         next_attempt_at = excluded.next_attempt_at, attempt_count = excluded.attempt_count`,
      txId,
      action,
      at,
      attemptCount,
    );
  }

  private transition(txId: string, expected: readonly TransactionState[], next: TransactionState, extra?: () => void): TxRow {
    this.ctx.storage.transactionSync(() => {
      const current = this.loadTx(txId);
      if (!current) throw new Error(`Missing transaction ${txId}.`);
      const currentState = this.stateOf(current);
      if (!expected.includes(currentState)) throw new CoordinatorCasLost(currentState);
      assertTransactionTransition(currentState, next);
      this.sql.exec("UPDATE transactions SET status = ?, updated_at = ? WHERE tx_id = ?", next, new Date().toISOString(), txId);
      extra?.();
    });
    const updated = this.loadTx(txId);
    if (!updated) throw new Error(`Missing transaction ${txId} after transition.`);
    return updated;
  }

  private redoParticipants(participants: BeginParticipant[], epoch: number): RedoParticipantV1[] {
    return [...participants]
      .sort((a, b) => a.shardId.localeCompare(b.shardId))
      .map((participant) => ({
        participant_id: participant.shardId,
        epoch,
        intents: participant.intents.map((intent, intentSeq) => ({
          intent_seq: intentSeq,
          sql: intent.sql,
          params: intent.params ?? [],
          tenant_id: intent.tenantId,
          table_name: intent.table,
          partition_key: intent.partitionKey,
          vbucket: intent.vbucket ?? null,
          operation: intent.op ?? null,
          mirror_target_participant_id: intent.mirrorTargetShardId ?? null,
        })),
      }));
  }

  private enterMutation(): void {
    this.activeMutations += 1;
  }

  private leaveMutation(): void {
    this.activeMutations -= 1;
    if (this.activeMutations === 0) {
      for (const resolve of this.mutationDrainWaiters.splice(0)) resolve();
    }
  }

  private async awaitMutationDrain(): Promise<void> {
    if (this.activeMutations === 0) return;
    await new Promise<void>((resolve) => this.mutationDrainWaiters.push(resolve));
  }

  private async runRecoveryExclusive(txId: string, status: string, work: () => Promise<Response>): Promise<Response> {
    if (this.recoveryInFlight.has(txId)) return json({ ok: true, txId, status }, 202);
    this.recoveryInFlight.add(txId);
    try {
      return await work();
    } finally {
      this.recoveryInFlight.delete(txId);
    }
  }

  private deploymentFleetId(): string {
    return this.coordinatorEnv.DEPLOYMENT_FLEET_ID || "default";
  }

  private restoreCoordinatorStub(fleetId = this.deploymentFleetId()): RestoreCoordinatorStub | null {
    const namespace = this.restoreCoordinatorOverride ?? this.coordinatorEnv.RESTORE_COORDINATOR;
    return namespace ? namespace.getByName(`fleet:${fleetId}`) : null;
  }

  private async restoreGateSnapshot(
    fleetId = this.deploymentFleetId(),
    claim?: { restoreId: string; generation: number },
  ): Promise<RestoreGateSnapshot | null> {
    const stub = this.restoreCoordinatorStub(fleetId);
    if (!stub) return null;
    const response = await stub.fetch("https://restore.internal/gate", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        fleet_id: fleetId,
        participant_kind: "coordinator",
        ...(claim ? { restore_id: claim.restoreId, generation: claim.generation } : {}),
      }),
    });
    if (!response.ok) throw new RestoreGateDenied(`restore gate responded ${response.status}`);
    const body = await response.json() as {
      ok?: unknown;
      active?: unknown;
      allowed?: unknown;
      restore_id?: unknown;
      generation?: unknown;
      phase?: unknown;
    };
    if (
      body.ok !== true
      || typeof body.active !== "boolean"
      || typeof body.allowed !== "boolean"
      || typeof body.generation !== "number"
      || !Number.isSafeInteger(body.generation)
      || (body.restore_id !== null && typeof body.restore_id !== "string")
    ) {
      throw new RestoreGateDenied("restore gate returned a malformed snapshot");
    }
    return {
      ok: true,
      state: body.active ? "fenced" : "open",
      fleetId,
      restoreId: body.restore_id as string | null,
      generation: body.generation,
      ...(typeof body.phase === "string" ? { phase: body.phase } : {}),
    };
  }

  private async assertRestoreGateOpen(fleetId = this.deploymentFleetId()): Promise<void> {
    try {
      const gate = await this.restoreGateSnapshot(fleetId);
      if (gate && gate.state !== "open") throw new RestoreGateDenied("fleet restore is in progress");
    } catch (error) {
      if (error instanceof RestoreGateDenied) throw error;
      throw new RestoreGateDenied(error instanceof Error ? error.message : String(error));
    }
  }

  private restoreGateResponse(error: unknown): Response {
    log("coordinator.restore_gate_denied", { message: error instanceof Error ? error.message : String(error) });
    return json({ error: { code: "RESTORE_GATE_UNAVAILABLE", message: "The external fleet restore gate denied coordinator mutation.", retryable: true } }, 503);
  }

  private async registerPhysicalCoordinator(
    fleetId: string,
    coordinatorId: string,
    txId: string,
    existingCreatedAt?: string,
  ): Promise<CoordinatorRegistrationResult> {
    const stub = this.restoreCoordinatorStub(fleetId);
    if (!stub) return { disposition: "registered" }; // expand-first compatibility until the binding is deployed
    let response: Response;
    try {
      response = await stub.fetch("https://restore.internal/register-coordinator", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          fleetId,
          coordinatorId,
          txId,
          ...(existingCreatedAt === undefined ? {} : { existing_created_at: existingCreatedAt }),
        }),
      });
    } catch (error) {
      throw new RestoreGateDenied(error instanceof Error ? error.message : String(error));
    }
    if (!response.ok) throw new RestoreGateDenied(`coordinator registry responded ${response.status}`);
    const body = await response.json().catch(() => null) as {
      ok?: unknown;
      registered_at?: unknown;
      disposition?: unknown;
      restore_id?: unknown;
      generation?: unknown;
    } | null;
    if (body?.disposition === "discard_required") {
      if (
        typeof body.restore_id !== "string"
        || body.restore_id.length === 0
        || typeof body.generation !== "number"
        || !Number.isSafeInteger(body.generation)
        || body.generation < 0
      ) {
        throw new RestoreGateDenied("coordinator registry returned a malformed discard directive");
      }
      return { disposition: "discard_required", restoreId: body.restore_id, generation: body.generation };
    }
    if (!body || body.ok !== true || typeof body.registered_at !== "string") throw new RestoreGateDenied("coordinator registry returned malformed acknowledgement");
    // Registration is an external await. Re-read the gate so a restore that
    // started concurrently cannot race admission after inventory capture.
    await this.assertRestoreGateOpen(fleetId);
    return { disposition: "registered" };
  }

  private redoParticipantsV2(row: TxRow): RedoParticipantV2[] {
    const bookmarks = new Map(
      Array.from(this.sql.exec<{ shard_id: string; prepare_bookmark: string | null }>(
        "SELECT shard_id, prepare_bookmark FROM transaction_participants WHERE tx_id = ? ORDER BY shard_id ASC",
        row.tx_id,
      )).map((participant) => [participant.shard_id, participant.prepare_bookmark]),
    );
    return this.redoParticipants(this.participants(row), row.epoch).map((participant) => {
      const prepareBookmark = bookmarks.get(participant.participant_id);
      if (!prepareBookmark) {
        throw new TransactionContractViolation(
          transactionError("TX_ENVELOPE_INVALID", `Participant ${participant.participant_id} is missing its exact post-prepare bookmark.`),
        );
      }
      return { ...participant, prepare_bookmark: prepareBookmark };
    });
  }

  private async envelopeFor(
    txId: string,
    fleetId: string,
    coordinatorId: string,
    participants: BeginParticipant[],
    epoch: number,
    commitDecidedAt: string,
  ): Promise<RedoEnvelopeV1> {
    const redoParticipants = this.redoParticipants(participants, epoch);
    const envelope: RedoEnvelopeV1 = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: REDO_ENVELOPE_V1_FORMAT_VERSION,
      tx_id: txId,
      fleet_id: fleetId,
      coordinator_id: coordinatorId,
      decision: "commit",
      decision_epoch: epoch,
      commit_decided_at: commitDecidedAt,
      retention_deadline: retentionDeadline(commitDecidedAt),
      operation_hash: await hashParticipantOperations(redoParticipants),
      participants: redoParticipants,
    };
    await validateRedoEnvelope(envelope);
    return envelope;
  }

  private isV2(row: TxRow): boolean {
    return row.state_model_version === 2;
  }

  private async routeAssignmentRequest(
    txId: string,
    fleetId: string,
    coordinatorId: string,
    operationHash: string,
    decisionEpoch: number,
  ): Promise<ManifestRouteAssignmentRequestV1> {
    return {
      draft: {
        fleet_id: fleetId,
        tx_id: txId,
        coordinator_id: coordinatorId,
        operation_hash: operationHash,
        decision_epoch: decisionEpoch,
      },
      idempotency_key: await hashCanonicalJson([
        "coordinator-manifest-route",
        fleetId,
        txId,
        coordinatorId,
        operationHash,
        decisionEpoch,
      ]),
    };
  }

  private async validateAssignedReservation(
    request: ManifestRouteAssignmentRequestV1,
    result: Extract<ManifestRouteAssignmentResult, { ok: true }>,
  ): Promise<void> {
    validateManifestReservation(result.reservation);
    const expectedHash = await hashManifestReservation(result.reservation);
    if (
      expectedHash !== result.reservation_hash
      || result.reservation.fleet_id !== request.draft.fleet_id
      || result.reservation.tx_id !== request.draft.tx_id
      || result.reservation.coordinator_id !== request.draft.coordinator_id
      || result.reservation.operation_hash !== request.draft.operation_hash
      || result.reservation.decision_epoch !== request.draft.decision_epoch
    ) {
      throw new TransactionContractViolation(
        transactionError("MANIFEST_RESERVATION_CONFLICT", "Assigned manifest reservation does not match the immutable transaction draft."),
      );
    }
  }

  private async reservationFor(row: TxRow): Promise<{ reservation: ManifestReservationV1; reservationHash: string }> {
    if (!row.manifest_reservation_json || !row.manifest_reservation_hash) {
      throw new TransactionContractViolation(
        transactionError("TX_DECISION_UNAVAILABLE", "Model-2 transaction is missing its frozen manifest reservation."),
      );
    }
    const reservation = JSON.parse(row.manifest_reservation_json) as unknown;
    validateManifestReservation(reservation);
    const reservationHash = await hashManifestReservation(reservation);
    if (
      reservationHash !== row.manifest_reservation_hash
      || reservation.tx_id !== row.tx_id
      || reservation.fleet_id !== row.fleet_id
      || reservation.coordinator_id !== (row.coordinator_id || row.tx_id)
      || reservation.operation_hash !== row.operation_hash
      || reservation.decision_epoch !== row.epoch
    ) {
      throw new TransactionContractViolation(
        transactionError("MANIFEST_RESERVATION_CONFLICT", "Stored manifest reservation conflicts with the durable coordinator identity."),
      );
    }
    return { reservation, reservationHash };
  }

  private async reconcileReservation(row: TxRow): Promise<Response> {
    await this.assertRestoreGateOpen(row.fleet_id);
    if (this.stateOf(row) !== "manifest_reserving") return this.resume(row);
    const { reservation, reservationHash } = await this.reservationFor(row);
    const request: ManifestReserveRequestV1 = { reservation, reservation_hash: reservationHash };
    let result: ManifestReserveResult | null = null;
    try {
      result = this.coordinatorEnv.CONTROL_PLANE
        ? await this.coordinatorEnv.CONTROL_PLANE.reserveManifest(request)
        : null;
    } catch {
      result = null;
    }

    const latest = this.loadTx(row.tx_id);
    if (!latest) throw new Error(`Missing transaction ${row.tx_id}.`);
    if (this.stateOf(latest) !== "manifest_reserving") return this.resume(latest);
    row = latest;

    if (!result || (!result.ok && result.status === "unavailable")) {
      await this.reschedule(row.tx_id, "reserve", ["manifest_reserving"]);
      return json({ ok: true, txId: row.tx_id, status: "manifest_reserving" }, 202);
    }
    if (result.ok) {
      if (result.reservation_hash !== reservationHash) {
        throw new TransactionContractViolation(
          transactionError("MANIFEST_RESERVATION_CONFLICT", "Manifest reserve acknowledgement returned a different reservation hash."),
        );
      }
      try {
        this.transition(row.tx_id, ["manifest_reserving"], "preparing", () => {
          this.sql.exec("DELETE FROM recovery_queue WHERE tx_id = ?", row.tx_id);
        });
      } catch (error) {
        if (error instanceof CoordinatorCasLost) {
          const concurrent = this.loadTx(row.tx_id);
          if (concurrent) return this.resume(concurrent);
        }
        throw error;
      }
      const preparing = this.loadTx(row.tx_id);
      if (!preparing) throw new Error(`Missing transaction ${row.tx_id} after manifest reservation.`);
      return this.prepare(preparing);
    }
    if (result.status === "rejected_absent" && !result.bucket_row_may_exist) {
      this.transition(row.tx_id, ["manifest_reserving"], "aborted", () => {
        this.sql.exec("UPDATE transactions SET decision = 'abort', last_error = ?, result_json = ? WHERE tx_id = ?", JSON.stringify(result.error), JSON.stringify({ status: "aborted" }), row.tx_id);
        this.sql.exec("DELETE FROM recovery_queue WHERE tx_id = ?", row.tx_id);
      });
      return protocolResponse(transactionError("TX_ABORTED", "Manifest reservation was rejected before participant prepare."));
    }

    const aborting = await this.persistAbortDecision(row);
    return this.reconcileAbort(aborting);
  }

  private async persistAbortDecision(row: TxRow, legacyPredecisionAbort = false): Promise<TxRow> {
    await this.assertRestoreGateOpen(row.fleet_id);
    await this.ensureAlarmScheduled(Date.now());
    const now = new Date().toISOString();
    let cancelRequestJson: string | null = null;
    if (this.isV2(row)) {
      const { reservation, reservationHash } = await this.reservationFor(row);
      const intent: ManifestCancelIntentV1 = {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        format_version: MANIFEST_TERMINAL_INTENT_FORMAT_VERSION,
        tx_id: row.tx_id,
        reservation_hash: reservationHash,
        operation_hash: row.operation_hash,
        decision_epoch: row.epoch,
        idempotency_key: await hashCanonicalJson(["coordinator-manifest-cancel", reservationHash]),
      };
      validateManifestCancelIntent(intent);
      cancelRequestJson = canonicalJson({ reservation, reservation_hash: reservationHash, intent });
    }
    try {
      const expected: TransactionState[] = this.isV2(row) ? ["manifest_reserving", "preparing", "prepared"] : ["preparing", "prepared"];
      return this.transition(row.tx_id, expected, "abort_decided", () => {
        this.sql.exec(
          "UPDATE transactions SET decision = 'abort', manifest_cancel_request_json = COALESCE(?, manifest_cancel_request_json) WHERE tx_id = ?",
          cancelRequestJson,
          row.tx_id,
        );
        this.queueRecovery(row.tx_id, legacyPredecisionAbort ? "legacy_abort" : "abort", now);
      });
    } catch (error) {
      if (error instanceof CoordinatorCasLost) {
        const latest = this.loadTx(row.tx_id);
        if (latest && ["abort_decided", "aborting", "aborted_pending_manifest_cancel", "aborted"].includes(this.stateOf(latest))) return latest;
      }
      throw error;
    }
  }

  private async reconcileAbort(row: TxRow, knownLegacyPredecisionAbort?: boolean): Promise<Response> {
    await this.assertRestoreGateOpen(row.fleet_id);
    let current = row;
    const state = this.stateOf(current);
    if (state === "abort_decided") {
      try {
        current = this.transition(current.tx_id, ["abort_decided"], "aborting");
      } catch (error) {
        if (!(error instanceof CoordinatorCasLost)) throw error;
        const latest = this.loadTx(current.tx_id);
        if (!latest) throw new Error(`Missing transaction ${current.tx_id} during abort recovery.`);
        return this.resume(latest);
      }
    }
    else if (state === "aborted") return protocolResponse(transactionError("TX_ABORTED", "Transaction was aborted."));
    else if (state === "aborted_pending_manifest_cancel") return this.reconcileCancel(current);
    else if (state !== "aborting") return protocolResponse(transactionError("TX_INVALID_TRANSITION", `Cannot reconcile abort from ${state}.`));

    const legacyPredecisionAbort = knownLegacyPredecisionAbort ?? await this.coordinatorIdentity(current) === "legacy";
    const messageRow = legacyPredecisionAbort
      ? {
          ...current,
          protocol_version: CURRENT_PROTOCOL_VERSION,
          epoch: 1,
          operation_hash: await hashParticipantOperations(this.redoParticipants(this.participants(current), 1)),
        }
      : current;
    const outcomes = await Promise.allSettled(
      this.participants(current).map((participant) => this.callShard(messageRow, participant, "abort", { legacyPredecisionAbort })),
    );
    const allAcknowledged = outcomes.every((outcome) => outcome.status === "fulfilled" && outcome.value.ok);
    if (allAcknowledged) {
      if (this.isV2(current)) {
        try {
          this.transition(current.tx_id, ["aborting"], "aborted_pending_manifest_cancel", () => {
            this.queueRecovery(current.tx_id, "cancel", new Date().toISOString());
          });
        } catch (error) {
          if (!(error instanceof CoordinatorCasLost)) throw error;
          const latest = this.loadTx(current.tx_id);
          if (!latest) throw new Error(`Missing transaction ${current.tx_id} during manifest cancellation recovery.`);
          return this.resume(latest);
        }
        const pending = this.loadTx(current.tx_id);
        if (!pending) throw new Error(`Missing transaction ${current.tx_id} before manifest cancellation.`);
        return this.reconcileCancel(pending);
      }
      this.transition(current.tx_id, ["aborting"], "aborted", () => {
        this.sql.exec("DELETE FROM recovery_queue WHERE tx_id = ?", current.tx_id);
        this.sql.exec("UPDATE transactions SET result_json = ? WHERE tx_id = ?", JSON.stringify({ status: "aborted" }), current.tx_id);
      });
    } else {
      await this.reschedule(current.tx_id, legacyPredecisionAbort ? "legacy_abort" : "abort");
    }
    return protocolResponse(transactionError("TX_ABORTED", "Transaction was aborted."));
  }

  private async cancelRequestFor(row: TxRow): Promise<ManifestCancelRequestV1> {
    if (!row.manifest_cancel_request_json) {
      throw new TransactionContractViolation(
        transactionError("TX_DECISION_UNAVAILABLE", "Aborted model-2 transaction is missing its immutable cancellation request."),
      );
    }
    const request = JSON.parse(row.manifest_cancel_request_json) as ManifestCancelRequestV1;
    validateManifestReservation(request.reservation);
    validateManifestCancelIntent(request.intent);
    const reservationHash = await hashManifestReservation(request.reservation);
    if (
      reservationHash !== request.reservation_hash
      || request.intent.reservation_hash !== reservationHash
      || request.intent.tx_id !== row.tx_id
      || request.intent.operation_hash !== row.operation_hash
      || request.intent.decision_epoch !== row.epoch
    ) {
      throw new TransactionContractViolation(
        transactionError("MANIFEST_TERMINAL_CONFLICT", "Stored cancellation request conflicts with the durable transaction identity."),
      );
    }
    return request;
  }

  private async reconcileCancel(row: TxRow): Promise<Response> {
    return this.runRecoveryExclusive(row.tx_id, "aborted_pending_manifest_cancel", () => this.reconcileCancelOnce(row));
  }

  private async reconcileCancelOnce(row: TxRow): Promise<Response> {
    await this.assertRestoreGateOpen(row.fleet_id);
    if (this.stateOf(row) === "aborted") return protocolResponse(transactionError("TX_ABORTED", "Transaction was aborted."));
    if (this.stateOf(row) !== "aborted_pending_manifest_cancel") return this.resume(row);
    const queued = this.one<{ action: string }>("SELECT action FROM recovery_queue WHERE tx_id = ?", row.tx_id);
    if (!queued && row.last_error) {
      try {
        const parked = JSON.parse(row.last_error) as { code?: string };
        if (parked.code === "MANIFEST_QUARANTINED" || parked.code === "MANIFEST_RESERVATION_CONFLICT") {
          return json({ ok: true, txId: row.tx_id, status: "aborted_pending_manifest_cancel" }, 202);
        }
      } catch {
        // A malformed predecessor diagnostic is not proof of an audited park;
        // continue with durable cancellation recovery instead of suppressing it.
      }
    }
    const request = await this.cancelRequestFor(row);
    const service = this.coordinatorEnv.CONTROL_PLANE;

    // A force-abort may race an ambiguous reserve acknowledgement. Replaying
    // the identical reservation first makes cancellation well-defined even if
    // the original reserve failed before the bucket row was created.
    let reserve: ManifestReserveResult | null = null;
    try {
      reserve = service ? await service.reserveManifest({ reservation: request.reservation, reservation_hash: request.reservation_hash }) : null;
    } catch {
      reserve = null;
    }
    let latest = this.loadTx(row.tx_id);
    if (!latest) throw new Error(`Missing transaction ${row.tx_id}.`);
    if (this.stateOf(latest) !== "aborted_pending_manifest_cancel") return this.resume(latest);
    row = latest;
    if (!reserve || (!reserve.ok && reserve.status === "unavailable")) {
      await this.reschedule(row.tx_id, "cancel", ["aborted_pending_manifest_cancel"]);
      return json({ ok: true, txId: row.tx_id, status: "aborted_pending_manifest_cancel" }, 202);
    }
    if (!reserve.ok && reserve.status === "quarantined") {
      this.ctx.storage.transactionSync(() => {
        this.sql.exec("UPDATE transactions SET last_error = ? WHERE tx_id = ?", JSON.stringify(reserve.error), row.tx_id);
        this.sql.exec("DELETE FROM recovery_queue WHERE tx_id = ?", row.tx_id);
      });
      return json({ ok: true, txId: row.tx_id, status: "aborted_pending_manifest_cancel" }, 202);
    }
    if (!reserve.ok && reserve.status === "rejected_absent" && !reserve.bucket_row_may_exist) {
      this.transition(row.tx_id, ["aborted_pending_manifest_cancel"], "aborted", () => {
        this.sql.exec("DELETE FROM recovery_queue WHERE tx_id = ?", row.tx_id);
        this.sql.exec("UPDATE transactions SET result_json = ? WHERE tx_id = ?", JSON.stringify({ status: "aborted" }), row.tx_id);
      });
      return protocolResponse(transactionError("TX_ABORTED", "Transaction was aborted."));
    }

    let result: ManifestCancelResult | null = null;
    try {
      result = service ? await service.cancelManifest(request) : null;
    } catch {
      result = null;
    }
    latest = this.loadTx(row.tx_id);
    if (!latest) throw new Error(`Missing transaction ${row.tx_id}.`);
    if (this.stateOf(latest) !== "aborted_pending_manifest_cancel") return this.resume(latest);
    row = latest;
    if (!result || (!result.ok && result.status === "unavailable")) {
      await this.reschedule(row.tx_id, "cancel", ["aborted_pending_manifest_cancel"]);
      return json({ ok: true, txId: row.tx_id, status: "aborted_pending_manifest_cancel" }, 202);
    }
    if (!result.ok && result.status === "quarantined_pending_resolution") {
      this.ctx.storage.transactionSync(() => {
        this.sql.exec("UPDATE transactions SET last_error = ? WHERE tx_id = ?", JSON.stringify(result.error), row.tx_id);
        this.sql.exec("DELETE FROM recovery_queue WHERE tx_id = ?", row.tx_id);
      });
      return json({ ok: true, txId: row.tx_id, status: "aborted_pending_manifest_cancel" }, 202);
    }
    if (!result.ok) {
      this.transition(row.tx_id, ["aborted_pending_manifest_cancel"], "quarantined", () => {
        this.sql.exec("UPDATE transactions SET decision = 'quarantined', last_error = ? WHERE tx_id = ?", JSON.stringify(result.error), row.tx_id);
        this.sql.exec("DELETE FROM recovery_queue WHERE tx_id = ?", row.tx_id);
      });
      return protocolResponse(result.error);
    }
    this.transition(row.tx_id, ["aborted_pending_manifest_cancel"], "aborted", () => {
      this.sql.exec("DELETE FROM recovery_queue WHERE tx_id = ?", row.tx_id);
      this.sql.exec("UPDATE transactions SET result_json = ? WHERE tx_id = ?", JSON.stringify({ status: "aborted" }), row.tx_id);
    });
    return protocolResponse(transactionError("TX_ABORTED", "Transaction was aborted."));
  }

  private async persistCommitDecision(row: TxRow): Promise<TxRow> {
    await this.assertRestoreGateOpen(row.fleet_id);
    if (this.isV2(row)) return this.persistCommitDeciding(row);
    await this.ensureAlarmScheduled(Date.now());
    const commitDecidedAt = new Date().toISOString();
    const envelope = await this.envelopeFor(
      row.tx_id,
      row.fleet_id,
      row.coordinator_id || row.tx_id,
      this.participants(row),
      row.epoch,
      commitDecidedAt,
    );
    const registration = await createManifestRegistration(envelope);
    const now = new Date().toISOString();
    try {
      return this.transition(row.tx_id, ["prepared"], "commit_decided", () => {
        this.sql.exec(
          `UPDATE transactions SET decision = 'commit', redo_envelope_json = ?, manifest_registration_json = ? WHERE tx_id = ?`,
          JSON.stringify(envelope),
          JSON.stringify(registration),
          row.tx_id,
        );
        this.queueRecovery(row.tx_id, "manifest", now);
      });
    } catch (error) {
      if (error instanceof CoordinatorCasLost) {
        const latest = this.loadTx(row.tx_id);
        if (latest && isCommitDecidedOrLater(this.stateOf(latest))) return latest;
      }
      throw error;
    }
  }

  private async persistCommitDeciding(row: TxRow): Promise<TxRow> {
    await this.assertRestoreGateOpen(row.fleet_id);
    await this.ensureAlarmScheduled(Date.now());
    const { reservation, reservationHash } = await this.reservationFor(row);
    const checkpointCertified = !!(this.restoreCoordinatorOverride ?? this.coordinatorEnv.RESTORE_COORDINATOR);
    const redoEnvelopeIntent = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: checkpointCertified ? CURRENT_REDO_ENVELOPE_FORMAT_VERSION : REDO_ENVELOPE_V1_FORMAT_VERSION,
      tx_id: row.tx_id,
      fleet_id: row.fleet_id,
      coordinator_id: row.coordinator_id || row.tx_id,
      decision: "commit" as const,
      decision_epoch: row.epoch,
      operation_hash: row.operation_hash,
      participants: checkpointCertified ? this.redoParticipantsV2(row) : this.redoParticipants(this.participants(row), row.epoch),
    };
    if (checkpointCertified) {
      const validationDecisionTime = new Date().toISOString();
      validateWritableRedoEnvelopeStructure({
        ...redoEnvelopeIntent,
        commit_decided_at: validationDecisionTime,
        retention_deadline: retentionDeadline(validationDecisionTime),
      });
    }
    const computedOperationHash = await hashParticipantOperations(redoEnvelopeIntent.participants);
    if (computedOperationHash !== row.operation_hash) {
      throw new TransactionContractViolation(
        transactionError("TX_ENVELOPE_HASH_MISMATCH", "Stored transaction participants do not match the durable operation hash."),
      );
    }
    const redoEnvelopeHash = await hashCanonicalJson(redoEnvelopeIntent);
    const intent: ManifestFinalizeIntentV1 = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_TERMINAL_INTENT_FORMAT_VERSION,
      tx_id: row.tx_id,
      reservation_hash: reservationHash,
      redo_envelope_hash: redoEnvelopeHash,
      operation_hash: row.operation_hash,
      decision_epoch: row.epoch,
      idempotency_key: await hashCanonicalJson(["coordinator-manifest-finalize", reservationHash, redoEnvelopeHash]),
    };
    validateManifestFinalizeIntent(intent);
    const request: ManifestFinalizeRequestV1 = { reservation, reservation_hash: reservationHash, intent };
    const now = new Date().toISOString();
    try {
      return this.transition(row.tx_id, ["prepared"], "commit_deciding", () => {
        this.sql.exec(
          `UPDATE transactions
              SET decision = 'commit', redo_envelope_intent_json = ?, manifest_finalize_request_json = ?
            WHERE tx_id = ?`,
          canonicalJson(redoEnvelopeIntent),
          canonicalJson(request),
          row.tx_id,
        );
        this.queueRecovery(row.tx_id, "finalize", now);
      });
    } catch (error) {
      if (error instanceof CoordinatorCasLost) {
        const latest = this.loadTx(row.tx_id);
        if (latest && isCommitDecidedOrLater(this.stateOf(latest))) return latest;
      }
      throw error;
    }
  }

  private async finalizeRequestFor(row: TxRow): Promise<ManifestFinalizeRequestV1> {
    if (!row.manifest_finalize_request_json || !row.redo_envelope_intent_json) {
      throw new TransactionContractViolation(
        transactionError("TX_DECISION_UNAVAILABLE", "Commit-deciding transaction is missing immutable finalization recovery data."),
      );
    }
    const request = JSON.parse(row.manifest_finalize_request_json) as ManifestFinalizeRequestV1;
    validateManifestReservation(request.reservation);
    validateManifestFinalizeIntent(request.intent);
    const reservationHash = await hashManifestReservation(request.reservation);
    const redoEnvelopeHash = await hashCanonicalJson(JSON.parse(row.redo_envelope_intent_json));
    if (
      reservationHash !== request.reservation_hash
      || request.intent.reservation_hash !== reservationHash
      || request.intent.redo_envelope_hash !== redoEnvelopeHash
      || request.intent.tx_id !== row.tx_id
      || request.intent.operation_hash !== row.operation_hash
      || request.intent.decision_epoch !== row.epoch
    ) {
      throw new TransactionContractViolation(
        transactionError("MANIFEST_TERMINAL_CONFLICT", "Stored finalization request conflicts with the durable transaction identity."),
      );
    }
    return request;
  }

  private async reconcileFinalize(row: TxRow): Promise<Response> {
    await this.assertRestoreGateOpen(row.fleet_id);
    const state = this.stateOf(row);
    if (state !== "commit_deciding" && state !== "commit_pending_manifest") return this.resume(row);
    const request = await this.finalizeRequestFor(row);
    let result: ManifestFinalizeResult | null = null;
    try {
      result = this.coordinatorEnv.CONTROL_PLANE
        ? await this.coordinatorEnv.CONTROL_PLANE.finalizeManifest(request)
        : null;
    } catch {
      result = null;
    }

    const latest = this.loadTx(row.tx_id);
    if (!latest) throw new Error(`Missing transaction ${row.tx_id}.`);
    if (this.stateOf(latest) !== state) return this.resume(latest);
    row = latest;
    if (!result || (!result.ok && result.status === "unavailable")) {
      try {
        if (state === "commit_deciding") this.transition(row.tx_id, ["commit_deciding"], "commit_pending_manifest");
      } catch (error) {
        if (error instanceof CoordinatorCasLost) {
          const concurrent = this.loadTx(row.tx_id);
          if (concurrent) return this.resume(concurrent);
        }
        throw error;
      }
      await this.reschedule(row.tx_id, "finalize", ["commit_deciding", "commit_pending_manifest"]);
      return json({ ok: true, txId: row.tx_id, status: "commit_pending_manifest" }, 202);
    }
    if (!result.ok) {
      this.transition(row.tx_id, [state], "quarantined", () => {
        this.sql.exec("UPDATE transactions SET decision = 'quarantined', last_error = ? WHERE tx_id = ?", JSON.stringify(result.error), row.tx_id);
        this.sql.exec("DELETE FROM recovery_queue WHERE tx_id = ?", row.tx_id);
      });
      return protocolResponse(result.error);
    }

    validateManifestRecordV2(result.record);
    const expectedRecordHash = await hashManifestRecordV2(result.record);
    if (
      expectedRecordHash !== result.record_hash
      || result.record.tx_id !== row.tx_id
      || result.record.fleet_id !== row.fleet_id
      || result.record.coordinator_id !== (row.coordinator_id || row.tx_id)
      || result.record.operation_hash !== row.operation_hash
      || result.record.decision_epoch !== row.epoch
      || result.record.reservation_hash !== request.reservation_hash
      || result.record.envelope_hash !== request.intent.redo_envelope_hash
    ) {
      throw new TransactionContractViolation(
        transactionError("MANIFEST_TERMINAL_CONFLICT", "Finalized manifest record conflicts with the durable coordinator decision."),
      );
    }
    const storedEnvelopeIntent = JSON.parse(row.redo_envelope_intent_json!) as Record<string, unknown>;
    const completedEnvelope = (
      "commit_decided_at" in storedEnvelopeIntent && "retention_deadline" in storedEnvelopeIntent
        ? storedEnvelopeIntent
        : {
            ...storedEnvelopeIntent,
            commit_decided_at: result.record.commit_decided_at,
            retention_deadline: result.record.retention_deadline,
          }
    ) as unknown as ReadableRedoEnvelope;
    await validateRedoEnvelope(completedEnvelope);
    try {
      this.transition(row.tx_id, [state], "manifest_registered", () => {
        this.sql.exec(
          `UPDATE transactions
              SET redo_envelope_json = ?, manifest_record_json = ?, manifest_record_hash = ?,
                  commit_decided_at_ms = ?, decision_sequence = ?
            WHERE tx_id = ?`,
          canonicalJson(completedEnvelope),
          canonicalJson(result.record),
          result.record_hash,
          result.record.commit_decided_at_ms,
          result.record.decision_sequence,
          row.tx_id,
        );
        this.queueRecovery(row.tx_id, "commit", new Date().toISOString());
      });
    } catch (error) {
      if (error instanceof CoordinatorCasLost) {
        const concurrent = this.loadTx(row.tx_id);
        if (concurrent) return this.resume(concurrent);
      }
      throw error;
    }
    const registered = this.loadTx(row.tx_id);
    if (!registered) throw new Error(`Missing transaction ${row.tx_id} after manifest finalization.`);
    return this.reconcileCommit(registered);
  }

  private async beginV2Bridge(row: TxRow, expectedState: "commit_decided" | "commit_pending_manifest"): Promise<Response> {
    const assignmentRequest = await this.routeAssignmentRequest(
      row.tx_id,
      row.fleet_id,
      row.coordinator_id || row.tx_id,
      row.operation_hash,
      row.epoch,
    );
    let assignment: ManifestRouteAssignmentResult | null = null;
    try {
      assignment = this.coordinatorEnv.CONTROL_PLANE
        ? await this.coordinatorEnv.CONTROL_PLANE.assignManifestRoute(assignmentRequest)
        : null;
    } catch {
      assignment = null;
    }
    const latest = this.loadTx(row.tx_id);
    if (!latest) throw new Error(`Missing transaction ${row.tx_id}.`);
    if (this.stateOf(latest) !== expectedState || latest.state_model_version !== 1) return this.resume(latest);
    row = latest;
    if (!assignment || (!assignment.ok && assignment.status === "unavailable")) {
      await this.reschedule(row.tx_id, "manifest", [expectedState]);
      return json({ ok: true, txId: row.tx_id, status: "commit_pending_manifest" }, 202);
    }
    if (!assignment.ok) {
      this.transition(row.tx_id, [expectedState], "quarantined", () => {
        this.sql.exec("UPDATE transactions SET decision = 'quarantined', last_error = ? WHERE tx_id = ?", JSON.stringify(assignment.error), row.tx_id);
        this.sql.exec("DELETE FROM recovery_queue WHERE tx_id = ?", row.tx_id);
      });
      return protocolResponse(assignment.error);
    }
    await this.validateAssignedReservation(assignmentRequest, assignment);
    assertTransactionTransition(expectedState, "commit_pending_manifest");
    try {
      this.ctx.storage.transactionSync(() => {
        const current = this.loadTx(row.tx_id);
        if (!current || this.stateOf(current) !== expectedState || current.state_model_version !== 1) {
          throw new CoordinatorCasLost(current ? this.stateOf(current) : "quarantined");
        }
        this.sql.exec(
          `UPDATE transactions
              SET state_model_version = 2, status = 'commit_pending_manifest',
                  manifest_route_assignment_request_json = ?, manifest_reservation_json = ?,
                  manifest_reservation_hash = ?, updated_at = ?
            WHERE tx_id = ?`,
          canonicalJson(assignmentRequest),
          canonicalJson(assignment.reservation),
          assignment.reservation_hash,
          new Date().toISOString(),
          row.tx_id,
        );
        this.queueRecovery(row.tx_id, "reserve", new Date().toISOString());
      });
    } catch (error) {
      if (error instanceof CoordinatorCasLost) {
        const concurrent = this.loadTx(row.tx_id);
        if (concurrent) return this.resume(concurrent);
      }
      throw error;
    }
    const bridged = this.loadTx(row.tx_id);
    if (!bridged) throw new Error(`Missing transaction ${row.tx_id} after V1-to-V2 bridge assignment.`);
    return this.reconcileV2BridgeReservation(bridged);
  }

  private async reconcileV2BridgeReservation(row: TxRow): Promise<Response> {
    const state = this.stateOf(row);
    if (row.state_model_version !== 2 || state !== "commit_pending_manifest") {
      return protocolResponse(transactionError(
        "TX_VERSION_UNSUPPORTED",
        `State ${state} cannot enter the V1-to-V2 bridge.`,
      ));
    }
    if (row.manifest_finalize_request_json) return this.reconcileFinalize(row);
    if (!row.redo_envelope_json || !row.manifest_registration_json) {
      throw new TransactionContractViolation(
        transactionError("TX_DECISION_UNAVAILABLE", "V1-to-V2 bridge is missing its immutable predecessor decision envelope."),
      );
    }
    const predecessorEnvelope = JSON.parse(row.redo_envelope_json) as RedoEnvelopeV1;
    const predecessorRegistration = JSON.parse(row.manifest_registration_json) as ManifestRegistrationV1;
    await validateRedoEnvelope(predecessorEnvelope);
    await validateManifestRegistration(predecessorRegistration, predecessorEnvelope);
    if (
      predecessorEnvelope.tx_id !== row.tx_id
      || predecessorEnvelope.operation_hash !== row.operation_hash
      || predecessorRegistration.record.tx_id !== row.tx_id
      || predecessorRegistration.record.envelope_hash !== await hashCanonicalJson(predecessorEnvelope)
    ) {
      throw new TransactionContractViolation(
        transactionError("TX_ENVELOPE_HASH_MISMATCH", "V1-to-V2 bridge predecessor artifacts conflict with the durable coordinator identity."),
      );
    }
    const { reservation, reservationHash } = await this.reservationFor(row);
    let reserve: ManifestReserveResult | null = null;
    try {
      reserve = this.coordinatorEnv.CONTROL_PLANE
        ? await this.coordinatorEnv.CONTROL_PLANE.reserveManifest({ reservation, reservation_hash: reservationHash })
        : null;
    } catch {
      reserve = null;
    }
    let latest = this.loadTx(row.tx_id);
    if (!latest) throw new Error(`Missing transaction ${row.tx_id}.`);
    if (this.stateOf(latest) !== "commit_pending_manifest" || latest.state_model_version !== 2) return this.resume(latest);
    row = latest;
    if (!reserve || (!reserve.ok && reserve.status === "unavailable")) {
      await this.reschedule(row.tx_id, "reserve", ["commit_pending_manifest"]);
      return json({ ok: true, txId: row.tx_id, status: "commit_pending_manifest" }, 202);
    }
    if (!reserve.ok) {
      this.transition(row.tx_id, ["commit_pending_manifest"], "quarantined", () => {
        this.sql.exec("UPDATE transactions SET decision = 'quarantined', last_error = ? WHERE tx_id = ?", JSON.stringify(reserve.error), row.tx_id);
        this.sql.exec("DELETE FROM recovery_queue WHERE tx_id = ?", row.tx_id);
      });
      return protocolResponse(reserve.error);
    }
    if (reserve.reservation_hash !== reservationHash) {
      throw new TransactionContractViolation(
        transactionError("MANIFEST_RESERVATION_CONFLICT", "V1-to-V2 bridge reserve acknowledgement returned a different reservation hash."),
      );
    }
    const redoEnvelopeHash = await hashCanonicalJson(predecessorEnvelope);
    const intent: ManifestFinalizeIntentV1 = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_TERMINAL_INTENT_FORMAT_VERSION,
      tx_id: row.tx_id,
      reservation_hash: reservationHash,
      redo_envelope_hash: redoEnvelopeHash,
      operation_hash: row.operation_hash,
      decision_epoch: row.epoch,
      idempotency_key: await hashCanonicalJson(["coordinator-v1-v2-bridge-finalize", reservationHash, redoEnvelopeHash]),
    };
    validateManifestFinalizeIntent(intent);
    const finalizeRequest: ManifestFinalizeRequestV1 = { reservation, reservation_hash: reservationHash, intent };
    try {
      this.ctx.storage.transactionSync(() => {
        const current = this.loadTx(row.tx_id);
        if (!current || this.stateOf(current) !== "commit_pending_manifest" || current.state_model_version !== 2) {
          throw new CoordinatorCasLost(current ? this.stateOf(current) : "quarantined");
        }
        this.sql.exec(
          `UPDATE transactions
              SET redo_envelope_intent_json = ?, manifest_finalize_request_json = ?, updated_at = ?
            WHERE tx_id = ?`,
          canonicalJson(predecessorEnvelope),
          canonicalJson(finalizeRequest),
          new Date().toISOString(),
          row.tx_id,
        );
        this.queueRecovery(row.tx_id, "finalize", new Date().toISOString());
      });
    } catch (error) {
      if (error instanceof CoordinatorCasLost) {
        const concurrent = this.loadTx(row.tx_id);
        if (concurrent) return this.resume(concurrent);
      }
      throw error;
    }
    latest = this.loadTx(row.tx_id);
    if (!latest) throw new Error(`Missing transaction ${row.tx_id} after V1-to-V2 bridge reservation.`);
    return this.reconcileFinalize(latest);
  }

  private async reconcileManifest(row: TxRow): Promise<Response> {
    await this.assertRestoreGateOpen(row.fleet_id);
    if (this.isV2(row)) return this.reconcileFinalize(row);
    const state = this.stateOf(row);
    if (state !== "commit_decided" && state !== "commit_pending_manifest") {
      return this.resume(row);
    }
    if (!row.manifest_registration_json || !row.redo_envelope_json) {
      return protocolResponse(transactionError("TX_DECISION_UNAVAILABLE", "Committed decision is missing immutable recovery data."));
    }
    const registration = JSON.parse(row.manifest_registration_json) as ManifestRegistrationV1;
    const envelope = JSON.parse(row.redo_envelope_json) as RedoEnvelopeV1;
    await validateRedoEnvelope(envelope);

    const service = this.coordinatorEnv.CONTROL_PLANE;
    let result: ManifestServiceRegisterResult | null = null;
    if (service) {
      try {
        result = await service.registerManifest(registration);
      } catch {
        result = null;
      }
    }

    // registerManifest is an external await. An identical /begin retry can
    // advance this coordinator while the RPC is in flight, so never apply the
    // stale branch below to the state observed before that await.
    const latestAfterRegistration = this.loadTx(row.tx_id);
    if (!latestAfterRegistration) throw new Error(`Missing transaction ${row.tx_id}.`);
    if (this.stateOf(latestAfterRegistration) !== state) return this.resume(latestAfterRegistration);
    row = latestAfterRegistration;

    if (
      !result
      || (!result.ok && (result.status === "commit_pending_manifest" || result.status === "unavailable"))
    ) {
      try {
        if (state === "commit_decided") this.transition(row.tx_id, ["commit_decided"], "commit_pending_manifest");
      } catch (error) {
        if (error instanceof CoordinatorCasLost) {
          const latest = this.loadTx(row.tx_id);
          if (latest) return this.resume(latest);
        }
        throw error;
      }
      const scheduled = await this.reschedule(
        row.tx_id,
        "manifest",
        ["commit_decided", "commit_pending_manifest"],
      );
      if (!scheduled) {
        const latest = this.loadTx(row.tx_id);
        if (latest) return this.resume(latest);
      }
      return json({ ok: true, txId: row.tx_id, status: "commit_pending_manifest" }, 202);
    }
    if (!result.ok && result.error.code === "V1_CLOSED") {
      return this.beginV2Bridge(row, state);
    }
    if (!result.ok) {
      // Every remaining result is a deterministic rejection (including a
      // manifest conflict). An irreversible commit decision must not retain a
      // due recovery row with no future alarm, so converge to an inspectable
      // quarantine and remove the now-nonretryable work atomically.
      try {
        this.transition(row.tx_id, [state], "quarantined", () => {
          this.sql.exec("UPDATE transactions SET decision = 'quarantined', last_error = ? WHERE tx_id = ?", JSON.stringify(result.error), row.tx_id);
          this.sql.exec("DELETE FROM recovery_queue WHERE tx_id = ?", row.tx_id);
        });
      } catch (error) {
        if (error instanceof CoordinatorCasLost) {
          const latest = this.loadTx(row.tx_id);
          if (latest) return this.resume(latest);
        }
        throw error;
      }
      return protocolResponse(result.error);
    }

    try {
      this.transition(row.tx_id, [state], "manifest_registered", () => {
        this.queueRecovery(row.tx_id, "commit", new Date().toISOString());
      });
    } catch (error) {
      if (error instanceof CoordinatorCasLost) {
        const latest = this.loadTx(row.tx_id);
        if (latest) return this.resume(latest);
      }
      throw error;
    }
    const registered = this.loadTx(row.tx_id);
    if (!registered) throw new Error(`Missing transaction ${row.tx_id}.`);
    return this.reconcileCommit(registered);
  }

  private async reconcileCommit(row: TxRow): Promise<Response> {
    return this.runRecoveryExclusive(row.tx_id, "committed_pending_ack", () => this.reconcileCommitOnce(row));
  }

  private async reconcileCommitOnce(row: TxRow): Promise<Response> {
    await this.assertRestoreGateOpen(row.fleet_id);
    let current = row;
    const state = this.stateOf(current);
    if (state === "manifest_registered") {
      try {
        current = this.transition(current.tx_id, ["manifest_registered"], "committing");
      } catch (error) {
        if (error instanceof CoordinatorCasLost) {
          const latest = this.loadTx(current.tx_id);
          if (latest) return this.reconcileCommit(latest);
        }
        throw error;
      }
    }
    else if (state === "committed") return json({ ok: true, txId: current.tx_id, status: "committed" });
    else if (state !== "committing" && state !== "committed_pending_ack") return this.resume(current);

    const participants = this.participants(current);
    const outcomes = await Promise.allSettled(participants.map((participant) => this.callShard(current, participant, "commit")));
    const acknowledgments = outcomes.map((outcome, index) => ({
      shardId: participants[index].shardId,
      ok: outcome.status === "fulfilled" && outcome.value.ok,
    }));
    const allAcknowledged = acknowledgments.every((acknowledgment) => acknowledgment.ok);
    const nextState: TransactionState = allAcknowledged ? "committed" : "committed_pending_ack";
    if (allAcknowledged) await this.ensureAlarmScheduled(Date.now());

    // Participant RPCs (and getAlarm/setAlarm above) yield the input gate. A
    // concurrent identical retry may have already recorded the same or a
    // later outcome; reload before mutating so a stale response converges.
    const latestAfterAcknowledgments = this.loadTx(current.tx_id);
    if (!latestAfterAcknowledgments) throw new Error(`Missing transaction ${current.tx_id}.`);
    const latestState = this.stateOf(latestAfterAcknowledgments);
    if (latestState === "committed") {
      return json({ ok: true, txId: current.tx_id, status: "committed" });
    }
    if (latestState !== "committing" && latestState !== "committed_pending_ack") {
      return this.resume(latestAfterAcknowledgments);
    }
    current = latestAfterAcknowledgments;

    const recordAcknowledgments = (queueCommitRecovery = true) => {
      const now = new Date().toISOString();
      for (const acknowledgment of acknowledgments) {
        if (acknowledgment.ok) {
          this.sql.exec(
            "UPDATE transaction_participants SET phase_status = 'committed', updated_at = ? WHERE tx_id = ? AND shard_id = ?",
            now,
            current.tx_id,
            acknowledgment.shardId,
          );
        }
      }
      if (allAcknowledged) {
        // Retention release is its own durable forward-work item. The alarm
        // executes it only after every participant has acknowledged and the
        // coordinator state is terminal committed.
        this.queueRecovery(current.tx_id, "release", now);
        this.sql.exec("UPDATE transactions SET result_json = ? WHERE tx_id = ?", JSON.stringify({ status: "committed" }), current.tx_id);
      } else if (queueCommitRecovery) {
        this.queueRecovery(current.tx_id, "commit", now);
      }
    };
    try {
      if (latestState === nextState) {
        // A concurrent retry already selected committed_pending_ack. Preserve
        // any additional participant acknowledgements without attempting an
        // invalid self-transition.
        this.ctx.storage.transactionSync(() => {
          const latest = this.loadTx(current.tx_id);
          if (!latest || this.stateOf(latest) !== latestState) {
            throw new CoordinatorCasLost(latest ? this.stateOf(latest) : "quarantined");
          }
          this.sql.exec("UPDATE transactions SET updated_at = ? WHERE tx_id = ?", new Date().toISOString(), current.tx_id);
          // The recovery row already exists in this same-state retry. Do not
          // recreate it with attempt_count=0; reschedule() below must advance
          // the durable counter monotonically.
          recordAcknowledgments(false);
        });
      } else {
        this.transition(current.tx_id, ["committing", "committed_pending_ack"], nextState, recordAcknowledgments);
      }
    } catch (error) {
      if (error instanceof CoordinatorCasLost) {
        const latest = this.loadTx(current.tx_id);
        if (latest) return this.resume(latest);
      }
      throw error;
    }
    if (!allAcknowledged) {
      const scheduled = await this.reschedule(
        current.tx_id,
        "commit",
        ["committing", "committed_pending_ack"],
      );
      if (!scheduled) {
        const latest = this.loadTx(current.tx_id);
        if (latest) return this.resume(latest);
      }
      return json({ ok: true, txId: current.tx_id, status: "committed_pending_ack" }, 202);
    }
    return json({ ok: true, txId: current.tx_id, status: "committed" });
  }

  private async reconcileManifestRelease(row: TxRow): Promise<void> {
    const existing = this.releaseRecoveryInFlight.get(row.tx_id);
    if (existing) return existing;
    const work = this.reconcileManifestReleaseOnce(row);
    this.releaseRecoveryInFlight.set(row.tx_id, work);
    try {
      await work;
    } finally {
      if (this.releaseRecoveryInFlight.get(row.tx_id) === work) this.releaseRecoveryInFlight.delete(row.tx_id);
    }
  }

  private async reconcileManifestReleaseOnce(row: TxRow): Promise<void> {
    await this.assertRestoreGateOpen(row.fleet_id);
    if (this.stateOf(row) !== "committed") {
      throw new TransactionContractViolation(
        transactionError("TX_INVALID_TRANSITION", "Manifest retention can be released only after terminal commit."),
      );
    }
    if (this.isV2(row)) {
      const { reservation, reservationHash } = await this.reservationFor(row);
      if (!row.manifest_record_json || !row.manifest_record_hash) {
        throw new TransactionContractViolation(
          transactionError("TX_DECISION_UNAVAILABLE", "Committed model-2 transaction is missing its finalized manifest record."),
        );
      }
      const record = JSON.parse(row.manifest_record_json) as ManifestRecordV2;
      validateManifestRecordV2(record);
      if (await hashManifestRecordV2(record) !== row.manifest_record_hash) {
        throw new TransactionContractViolation(
          transactionError("MANIFEST_TERMINAL_CONFLICT", "Stored model-2 manifest record hash is invalid."),
        );
      }
      let result: ManifestReleaseResult | null = null;
      try {
        result = this.coordinatorEnv.CONTROL_PLANE
          ? await this.coordinatorEnv.CONTROL_PLANE.releaseManifestV2({
              reservation,
              reservation_hash: reservationHash,
              record_hash: row.manifest_record_hash,
              retention_deadline_ms: new Date(record.retention_deadline).getTime(),
            })
          : null;
      } catch {
        result = null;
      }
      if (result?.ok) {
        this.sql.exec("DELETE FROM recovery_queue WHERE tx_id = ?", row.tx_id);
        return;
      }
      if (result && result.status === "quarantined") {
        this.sql.exec("UPDATE transactions SET last_error = ? WHERE tx_id = ?", JSON.stringify(result.error), row.tx_id);
      }
      await this.reschedule(row.tx_id, "release");
      return;
    }
    if (!row.manifest_registration_json) {
      // Expand-first legacy rows never registered a v1 manifest.
      this.sql.exec("DELETE FROM recovery_queue WHERE tx_id = ?", row.tx_id);
      return;
    }
    const registration = JSON.parse(row.manifest_registration_json) as ManifestRegistrationV1;
    const request: ManifestReleaseRequest = {
      fleet_id: registration.record.fleet_id,
      tx_id: registration.record.tx_id,
      commit_decided_at: registration.record.commit_decided_at,
      record_hash: registration.record_hash,
    };
    let result: ManifestReleaseResult | null = null;
    try {
      result = this.coordinatorEnv.CONTROL_PLANE
        ? await this.coordinatorEnv.CONTROL_PLANE.releaseManifestRetention(request)
        : null;
    } catch {
      result = null;
    }
    if (result?.ok) {
      this.sql.exec("DELETE FROM recovery_queue WHERE tx_id = ?", row.tx_id);
      return;
    }
    if (result && result.status === "quarantined") {
      this.sql.exec("UPDATE transactions SET last_error = ? WHERE tx_id = ?", JSON.stringify(result.error), row.tx_id);
    }
    await this.reschedule(row.tx_id, "release");
  }

  private async reschedule(
    txId: string,
    action: RecoveryAction,
    onlyWhile?: readonly TransactionState[],
  ): Promise<boolean> {
    let due = Date.now();
    const scheduled = this.ctx.storage.transactionSync(() => {
      if (onlyWhile) {
        const current = this.loadTx(txId);
        if (!current || !onlyWhile.includes(this.stateOf(current))) return false;
      }
      // Read the attempt from durable state instead of trusting a caller's
      // in-memory snapshot. Alarm dispatch may pass through resume() and
      // several reconciliation methods, but every failed attempt still
      // monotonically advances the persisted backoff counter.
      const queued = this.one<{ attempt_count: number }>(
        "SELECT attempt_count FROM recovery_queue WHERE tx_id = ?",
        txId,
      );
      const priorAttempt = queued?.attempt_count ?? 0;
      const attempt = priorAttempt + 1;
      const delay = Math.min(RECOVERY_MAX_DELAY_MS, RECOVERY_BASE_DELAY_MS * 2 ** Math.min(priorAttempt, 10));
      due = Date.now() + delay;
      this.queueRecovery(txId, action, new Date(due).toISOString(), attempt);
      return true;
    });
    if (!scheduled) return false;
    await this.ctx.storage.setAlarm(due);
    return true;
  }

  private async adoptLegacyPredecision(row: TxRow): Promise<Response> {
    const participants = this.participants(row);
    if (participants.length === 0 || participants.some((participant) => participant.intents.length === 0)) {
      return protocolResponse(transactionError("TX_ENVELOPE_INVALID", "Legacy prepared transaction is missing its complete participant intent set."));
    }
    const epoch = 1;
    const operationHash = await hashParticipantOperations(this.redoParticipants(participants, epoch));
    if (await this.coordinatorIdentity(row) !== "legacy") {
      return protocolResponse(transactionError("TX_ID_REQUEST_MISMATCH", "Prepared coordinator identity is neither current nor a verified predecessor hash."));
    }
    const predecessorHash = row.operation_hash;
    const provisional: TxRow = {
      ...row,
      protocol_version: CURRENT_PROTOCOL_VERSION,
      state_model_version: LEGACY_ADOPTION_STATE_MODEL_VERSION,
      epoch,
      operation_hash: operationHash,
    };

    const admission = await this.checkManifestAdmission({
      fleet_id: row.fleet_id,
      tx_id: row.tx_id,
      commit_decided_at: new Date().toISOString(),
    });
    if (!admission.ok) return protocolResponse(admission.error);

    // A legacy prepared coordinator must replay the exact prepare first. The
    // participant adopts the v1 identity only after comparing every stored
    // intent, guard, and routing identity; commit is never used for adoption.
    const outcomes = await Promise.all(
      participants.map(async (participant) => {
        try {
          const response = await this.callShard(provisional, participant, "prepare");
          return { participant, ok: response.ok, body: await response.json().catch(() => ({})) };
        } catch (error) {
          return { participant, ok: false, body: { error: error instanceof Error ? error.message : String(error) } };
        }
      }),
    );
    const failed = outcomes.find((outcome) => !outcome.ok);
    if (failed) {
      const aborting = await this.persistAbortDecision(row, true);
      await this.reconcileAbort(aborting, true);
      return json({
        error: {
          ...transactionError("TX_ABORTED", `Legacy prepare adoption was rejected by participant ${failed.participant.shardId}; abort is durable.`),
          participant: failed.participant.shardId,
          details: failed.body,
        },
      }, 409);
    }

    try {
      this.ctx.storage.transactionSync(() => {
        const current = this.loadTx(row.tx_id);
        if (
          !current
          || !["preparing", "prepared"].includes(this.stateOf(current))
          || current.operation_hash !== predecessorHash
        ) {
          throw new CoordinatorCasLost(current ? this.stateOf(current) : "quarantined");
        }
        const now = new Date().toISOString();
        this.sql.exec(
          `UPDATE transactions
            SET status = 'prepared', operation_hash = ?, protocol_version = ?, state_model_version = ?, epoch = ?, updated_at = ?
           WHERE tx_id = ?`,
          operationHash,
          CURRENT_PROTOCOL_VERSION,
          LEGACY_ADOPTION_STATE_MODEL_VERSION,
          epoch,
          now,
          row.tx_id,
        );
        for (const participant of participants) {
          this.sql.exec(
            `INSERT INTO transaction_participants (tx_id, shard_id, phase_status, updated_at, epoch, operation_hash)
             VALUES (?, ?, 'prepared', ?, ?, ?)
             ON CONFLICT(tx_id, shard_id) DO UPDATE SET phase_status = 'prepared', updated_at = excluded.updated_at,
               epoch = excluded.epoch, operation_hash = excluded.operation_hash`,
            row.tx_id,
            participant.shardId,
            now,
            epoch,
            operationHash,
          );
        }
      });
    } catch (error) {
      if (!(error instanceof CoordinatorCasLost)) throw error;
    }
    const adopted = this.loadTx(row.tx_id);
    if (!adopted) throw new Error(`Missing transaction ${row.tx_id} after legacy adoption.`);
    return this.resume(adopted);
  }

  private async resume(row: TxRow): Promise<Response> {
    if (this.restoreDiscard()) return this.discardedByRestoreResponse();
    const state = this.stateOf(row);
    switch (state) {
      case "new":
        return protocolResponse(transactionError("TX_INVALID_TRANSITION", "A durable transaction row cannot remain new."));
      case "manifest_reserving":
        return this.reconcileReservation(row);
      case "commit_deciding":
        return this.reconcileFinalize(row);
      case "aborted_pending_manifest_cancel":
        return this.reconcileCancel(row);
      case "preparing":
        if (await this.coordinatorIdentity(row) === "legacy") return this.adoptLegacyPredecision(row);
        return this.prepare(row);
      case "prepared": {
        const identity = await this.coordinatorIdentity(row);
        if (identity === "legacy") return this.adoptLegacyPredecision(row);
        if (identity === "invalid") {
          return protocolResponse(transactionError("TX_ID_REQUEST_MISMATCH", "Prepared coordinator hash does not match current or verified predecessor content."));
        }
        return this.reconcileManifest(await this.persistCommitDecision(row));
      }
      case "abort_decided":
      case "aborting":
        return this.reconcileAbort(row);
      case "aborted":
        return protocolResponse(transactionError("TX_ABORTED", "Transaction was aborted."));
      case "commit_decided":
      case "commit_pending_manifest":
        return this.isV2(row)
          ? (row.manifest_finalize_request_json ? this.reconcileFinalize(row) : this.reconcileV2BridgeReservation(row))
          : this.reconcileManifest(row);
      case "manifest_registered":
      case "committing":
      case "committed_pending_ack":
        return this.reconcileCommit(row);
      case "committed":
        return json({ ok: true, txId: row.tx_id, status: "committed" });
      case "quarantined":
        return protocolResponse(transactionError("TX_QUARANTINED", "Transaction is quarantined for manual inspection."));
    }
  }

  private async prepare(row: TxRow): Promise<Response> {
    const participants = this.participants(row);
    const checkpointCertificationRequired = !!this.coordinatorEnv.RESTORE_COORDINATOR;
    const outcomes = await Promise.all(
      participants.map(async (participant) => {
        try {
          const response = await this.callShard(row, participant, "prepare");
          const body = await response.json().catch(() => ({})) as { prepareBookmark?: unknown; prepareCheckpointExact?: unknown };
          const checkpointOk = !checkpointCertificationRequired
            || (typeof body.prepareBookmark === "string" && body.prepareBookmark.length > 0 && body.prepareCheckpointExact === true);
          return { participant, ok: response.ok && checkpointOk, body, prepareBookmark: checkpointOk && typeof body.prepareBookmark === "string" ? body.prepareBookmark : null };
        } catch (error) {
          return { participant, ok: false, body: { error: error instanceof Error ? error.message : String(error) }, prepareBookmark: null };
        }
      }),
    );
    const failed = outcomes.find((outcome) => !outcome.ok);
    if (failed) {
      const latest = this.loadTx(row.tx_id);
      if (!latest) throw new Error(`Missing transaction ${row.tx_id}.`);
      const latestState = this.stateOf(latest);
      if (isCommitDecidedOrLater(latestState)) return this.resume(latest);
      const aborting = await this.persistAbortDecision(latest);
      await this.reconcileAbort(aborting);
      return json({
        error: {
          ...transactionError("TX_ABORTED", `Prepare failed on participant ${failed.participant.shardId}; abort is durable.`),
          participant: failed.participant.shardId,
          cause: failed.body,
          // Preserve the original participant response for existing gateway
          // consumers while exposing the normalized protocol error above.
          details: failed.body,
        },
      }, 409);
    }

    try {
      this.transition(row.tx_id, ["preparing"], "prepared", () => {
        const now = new Date().toISOString();
        for (const outcome of outcomes) {
          this.sql.exec(
            `UPDATE transaction_participants
                SET phase_status = 'prepared', prepare_bookmark = ?, updated_at = ?
              WHERE tx_id = ? AND shard_id = ?`,
            outcome.prepareBookmark,
            now,
            row.tx_id,
            outcome.participant.shardId,
          );
        }
      });
    } catch (error) {
      if (!(error instanceof CoordinatorCasLost)) throw error;
    }
    const latest = this.loadTx(row.tx_id);
    if (!latest) throw new Error(`Missing transaction ${row.tx_id}.`);
    return this.resume(latest);
  }

  async alarm(): Promise<void> {
    // Recovery can cross the commit decision and mutate every participant.
    // Register the durable row before any recovery mutation so coordinators
    // created before inventory rollout cannot escape a concurrent restore.
    // A closed or unavailable external gate must otherwise make the alarm a
    // no-op; throwing preserves work for the platform's alarm retry.
    await this.awaitMutationDrain();
    this.enterMutation();
    try {
    this.ensureSchema();
    if (this.restoreDiscard()) return;
    const work = this.one<{ tx_id: string; action: string; next_attempt_at: string; attempt_count: number }>(
      "SELECT tx_id, action, next_attempt_at, attempt_count FROM recovery_queue ORDER BY next_attempt_at LIMIT 1",
    );
    if (!work) return;
    const row = this.loadTx(work.tx_id);
    if (!row) {
      await this.assertRestoreGateOpen();
      this.sql.exec("DELETE FROM recovery_queue WHERE tx_id = ?", work.tx_id);
      return;
    }
    const registration = await this.registerPhysicalCoordinator(
      row.fleet_id || this.deploymentFleetId(),
      row.coordinator_id || row.tx_id,
      row.tx_id,
      row.created_at,
    );
    if (registration.disposition === "discard_required") {
      const discarded = this.persistRestoreDiscard(registration.restoreId, registration.generation);
      if (!discarded.ok) throw new RestoreGateDenied("coordinator registry discard directive conflicts with local restore state");
      return;
    }
    await this.assertRestoreGateOpen(row.fleet_id || this.deploymentFleetId());
    const due = new Date(work.next_attempt_at).getTime();
    if (due > Date.now()) {
      await this.ctx.storage.setAlarm(due);
      return;
    }
    try {
      // Expand-first compatibility for recovery work written by the previous
      // coordinator schema. Those rows used slash-prefixed shard routes and
      // predate epoch/hash messages, so replay them through the participant's
      // explicit legacy adapter instead of interpreting their terminal state
      // using the v1 state machine.
      if (work.action === "/commit" || work.action === "/abort") {
        const allAcknowledged = await this.reconcileLegacyRecovery(row, work.action);
        if (!allAcknowledged) {
          await this.reschedule(work.tx_id, work.action === "/commit" ? "commit" : "abort");
        }
      } else if (work.action === "release") {
        await this.reconcileManifestRelease(row);
      } else {
        await this.resume(row);
      }
    } catch (error) {
      log("coordinator.recovery_retry_failed", {
        txId: work.tx_id,
        action: work.action,
        attemptCount: work.attempt_count + 1,
        error: error instanceof Error ? error.message : String(error),
      });
      const normalizedAction: RecoveryAction = work.action === "/commit" ? "commit"
        : work.action === "/abort" ? "abort"
        : work.action === "reserve" ? "reserve"
        : work.action === "finalize" ? "finalize"
        : work.action === "cancel" ? "cancel"
        : work.action === "abort" ? "abort"
        : work.action === "legacy_abort" ? "legacy_abort"
        : work.action === "commit" ? "commit"
        : work.action === "release" ? "release"
        : "manifest";
      await this.reschedule(work.tx_id, normalizedAction);
    }
    } finally {
      this.leaveMutation();
    }
  }

  private coordinatorRestoreState(): {
    restore_id: string;
    generation: number;
    phase: string;
    target_bookmark: string | null;
    undo_bookmark: string | null;
  } | null {
    return this.one("SELECT restore_id, generation, phase, target_bookmark, undo_bookmark FROM coordinator_restore_state WHERE singleton = 1");
  }

  private writeCoordinatorRestoreState(
    restoreId: string,
    generation: number,
    phase: string,
    targetBookmark: string | null,
    undoBookmark: string | null,
  ): void {
    this.sql.exec(
      `INSERT OR REPLACE INTO coordinator_restore_state
        (singleton, restore_id, generation, phase, target_bookmark, undo_bookmark, updated_at_ms)
       VALUES (1, ?, ?, ?, ?, ?, ?)`,
      restoreId,
      generation,
      phase,
      targetBookmark,
      undoBookmark,
      Date.now(),
    );
  }

  private async matchingRestoreFence(
    restoreId: unknown,
    generation: unknown,
  ): Promise<{ gate: RestoreGateSnapshot } | { response: Response }> {
    if (typeof restoreId !== "string" || !restoreId || !Number.isSafeInteger(generation) || (generation as number) < 1) {
      return { response: json({ error: "Missing/invalid restoreId or generation." }, 400) };
    }
    try {
      const gate = await this.restoreGateSnapshot(this.deploymentFleetId(), { restoreId, generation: generation as number });
      if (!gate) return { response: json({ error: { code: "RESTORE_GATE_UNAVAILABLE", message: "RESTORE_COORDINATOR is not configured." } }, 503) };
      if (gate.state !== "fenced" || gate.restoreId !== restoreId || gate.generation !== generation) {
        return { response: json({ error: { code: "RESTORE_FENCE_MISMATCH", message: "External restore authority does not hold the requested fence." } }, 409) };
      }
      return { gate };
    } catch (error) {
      return { response: this.restoreGateResponse(error) };
    }
  }

  private async handleRestoreFence(request: Request): Promise<Response> {
    const body = await request.json() as { restore_id?: string; restoreId?: string; generation?: number; action?: string };
    const restoreId = body.restore_id ?? body.restoreId;
    const match = await this.matchingRestoreFence(restoreId, body.generation);
    if ("response" in match) return match.response;
    const existing = this.coordinatorRestoreState();
    if (existing && (existing.restore_id !== restoreId || existing.generation !== body.generation)) {
      return json({ error: { code: "RESTORE_FENCE_CONFLICT", message: "A different restore generation is already recorded locally." } }, 409);
    }
    if (body.action === "release") {
      if (existing && !["fenced", "install"].includes(existing.phase)) {
        return json({ error: { code: "RESTORE_RELEASE_REQUIRES_PITR_RELEASE", message: "PITR staging has begun; use /pitr-release after verification." } }, 409);
      }
      this.sql.exec("DELETE FROM coordinator_restore_state WHERE singleton = 1");
      return json({ ok: true, restore_id: restoreId, generation: body.generation, released: true });
    }
    this.writeCoordinatorRestoreState(restoreId!, body.generation!, body.action || "fenced", existing?.target_bookmark ?? null, existing?.undo_bookmark ?? null);
    return json({ ok: true, restore_id: restoreId, generation: body.generation, externally_fenced: true });
  }

  private async handlePitrPreview(request: Request): Promise<Response> {
    const body = await request.json() as { cutoff?: string | number; restore_id?: string; restoreId?: string; generation?: number };
    const restoreId = body.restore_id ?? body.restoreId;
    if (body.generation !== undefined) {
      const match = await this.matchingRestoreFence(restoreId, body.generation);
      if ("response" in match) return match.response;
    } else {
      await this.assertRestoreGateOpen();
    }
    const cutoffMs = typeof body.cutoff === "number" ? body.cutoff : Date.parse(body.cutoff ?? "");
    if (!Number.isFinite(cutoffMs)) return json({ error: "cutoff must be an ISO timestamp or epoch milliseconds." }, 400);
    const preview = { bookmark: await this.pitrPort.getCurrentBookmark() };
    // Provider time lookup is deliberately not called. It is approximate and
    // contributes no evidence to exact checkpoint selection.
    let target = this.one<{ checkpoint_at_ms: number; bookmark: string }>(
      `SELECT checkpoint_at_ms, bookmark FROM coordinator_restore_checkpoints
        WHERE checkpoint_at_ms <= ? ORDER BY checkpoint_at_ms DESC LIMIT 1`,
      cutoffMs,
    );
    const meta = this.one<{
      coverage_start_ms: number | null;
      initial_empty_bookmark: string | null;
      initial_empty_at_ms: number | null;
    }>(
      `SELECT coverage_start_ms, initial_empty_bookmark, initial_empty_at_ms
         FROM coordinator_restore_checkpoint_meta WHERE singleton = 1`,
    );
    if (!meta?.coverage_start_ms) {
      return json({ error: { code: "RESTORE_COVERAGE_MISSING", message: "Coordinator exact-checkpoint coverage is unavailable." } }, 409);
    }
    let emptyAtCutoff = false;
    if (!target && meta.initial_empty_bookmark && meta.initial_empty_at_ms !== null) {
      const firstTx = this.one<{ created_at: string }>("SELECT created_at FROM transactions ORDER BY created_at ASC LIMIT 1");
      const firstTxAt = firstTx ? Date.parse(firstTx.created_at) : Number.POSITIVE_INFINITY;
      if (meta.initial_empty_at_ms > cutoffMs && firstTxAt > cutoffMs) {
        target = { checkpoint_at_ms: meta.initial_empty_at_ms, bookmark: meta.initial_empty_bookmark };
        emptyAtCutoff = true;
      }
    }
    if (!target) {
      return json({ error: { code: "RESTORE_BOOKMARK_MISSING", message: "No certified exact coordinator checkpoint exists at or before cutoff." } }, 409);
    }
    return json({
      ok: true,
      restore_id: restoreId ?? null,
      generation: body.generation,
      target_bookmark: target.bookmark,
      preview_bookmark: preview.bookmark,
      checkpoint_at: new Date(target.checkpoint_at_ms).toISOString(),
      coverage_start: new Date(meta.coverage_start_ms).toISOString(),
      exact: true,
      empty_at_cutoff: emptyAtCutoff,
    });
  }

  private async stageCoordinatorBookmark(
    body: { restore_id?: string; restoreId?: string; generation?: number; target_bookmark?: string; targetBookmark?: string },
    phase: "staging" | "undo-staging",
  ): Promise<Response> {
    const restoreId = body.restore_id ?? body.restoreId;
    const targetBookmark = body.target_bookmark ?? body.targetBookmark;
    const match = await this.matchingRestoreFence(restoreId, body.generation);
    if ("response" in match) return match.response;
    if (typeof targetBookmark !== "string" || !targetBookmark) return json({ error: "Missing target_bookmark." }, 400);
    const existing = this.coordinatorRestoreState();
    if (existing && (existing.restore_id !== restoreId || existing.generation !== body.generation)) {
      return json({ error: { code: "RESTORE_FENCE_CONFLICT", message: "A different restore generation is already recorded locally." } }, 409);
    }
    this.writeCoordinatorRestoreState(restoreId!, body.generation!, phase, targetBookmark, existing?.undo_bookmark ?? null);
    const undoBookmark = await this.pitrPort.stageRestoreBookmark(targetBookmark);
    const revalidated = await this.matchingRestoreFence(restoreId, body.generation);
    if ("response" in revalidated) return revalidated.response;
    this.writeCoordinatorRestoreState(restoreId!, body.generation!, phase === "staging" ? "staged" : "undo-staged", targetBookmark, undoBookmark);
    return json({ ok: true, restore_id: restoreId, generation: body.generation, target_bookmark: targetBookmark, undo_bookmark: undoBookmark });
  }

  private async handlePitrStage(request: Request): Promise<Response> {
    return this.stageCoordinatorBookmark(await request.json(), "staging");
  }

  private async handlePitrUndo(request: Request): Promise<Response> {
    const body = await request.json() as { restore_id?: string; restoreId?: string; generation?: number; target_bookmark?: string; targetBookmark?: string; mode?: "undo" };
    if (!body.target_bookmark && !body.targetBookmark) body.target_bookmark = this.coordinatorRestoreState()?.undo_bookmark ?? undefined;
    return this.stageCoordinatorBookmark(body, "undo-staging");
  }

  private async handlePitrApply(request: Request): Promise<Response> {
    const body = await request.json() as { restore_id?: string; restoreId?: string; generation?: number };
    const restoreId = body.restore_id ?? body.restoreId;
    const match = await this.matchingRestoreFence(restoreId, body.generation);
    if ("response" in match) return match.response;
    const state = this.coordinatorRestoreState();
    if (!state || state.restore_id !== restoreId || state.generation !== body.generation || !["staged", "undo-staged"].includes(state.phase)) {
      return json({ error: { code: "RESTORE_NOT_STAGED", message: "No staged bookmark exists for this restore generation." } }, 409);
    }
    this.writeCoordinatorRestoreState(state.restore_id, state.generation, "applying", state.target_bookmark, state.undo_bookmark);
    this.pitrPort.abort();
    return json({ ok: true, restarting: true, restore_id: restoreId, generation: body.generation }, 202);
  }

  private async handlePitrVerify(request: Request): Promise<Response> {
    const body = await request.json() as { restore_id?: string; restoreId?: string; generation?: number; target_bookmark?: string; targetBookmark?: string; mode?: "undo" };
    const restoreId = body.restore_id ?? body.restoreId;
    const targetBookmark = body.target_bookmark ?? body.targetBookmark;
    const match = await this.matchingRestoreFence(restoreId, body.generation);
    if ("response" in match) return match.response;
    if (typeof targetBookmark !== "string" || !targetBookmark) return json({ error: "Missing target_bookmark." }, 400);
    const state = this.coordinatorRestoreState();
    if (state) {
      if (state.restore_id !== restoreId || state.generation !== body.generation) {
        return json({ error: { code: "RESTORE_FENCE_CONFLICT", message: "Local restore probe belongs to a different restore generation." } }, 409);
      }
      if (body.mode === "undo" && state.phase === "staging") {
        const currentBookmark = await this.pitrPort.getCurrentBookmark();
        if (!currentBookmark) return json({ error: { code: "RESTORE_VERIFY_FAILED", message: "Provider current bookmark is unavailable." } }, 409);
        return json({
          ok: true,
          verified: true,
          mode: "undo",
          restore_id: restoreId,
          generation: body.generation,
          target_bookmark: targetBookmark,
          preview_bookmark: currentBookmark,
        });
      }
      return json({
        ok: true,
        verified: false,
        pending: true,
        restore_id: restoreId,
        generation: body.generation,
        phase: state.phase,
      }, 202);
    }
    if (body.mode === "undo") {
      return json({ ok: true, verified: false, pending: true, mode: "undo", restore_id: restoreId, generation: body.generation }, 202);
    }
    const currentBookmark = await this.pitrPort.getCurrentBookmark();
    if (!currentBookmark) return json({ error: { code: "RESTORE_VERIFY_FAILED", message: "Provider current bookmark is unavailable." } }, 409);
    return json({
      ok: true,
      verified: true,
      restore_id: restoreId,
      generation: body.generation,
      target_bookmark: targetBookmark,
      preview_bookmark: currentBookmark,
      bookmark_equality_assumed: false,
      certification: "provider-next-session-plus-external-fence",
    });
  }

  private async handlePitrRelease(request: Request): Promise<Response> {
    const body = await request.json() as { restore_id?: string; restoreId?: string; generation?: number };
    const restoreId = body.restore_id ?? body.restoreId;
    const match = await this.matchingRestoreFence(restoreId, body.generation);
    if ("response" in match) return match.response;
    this.sql.exec("DELETE FROM coordinator_restore_state WHERE singleton = 1");
    return json({ ok: true, restore_id: restoreId, generation: body.generation, externally_fenced: true });
  }

  private async handleRestoreLossPage(request: Request): Promise<Response> {
    const body = await request.json() as {
      restore_id?: string;
      restoreId?: string;
      generation?: number;
      cutoff?: string | number;
      through?: string | number;
    };
    const restoreId = body.restore_id ?? body.restoreId;
    const match = await this.matchingRestoreFence(restoreId, body.generation);
    if ("response" in match) return match.response;
    await this.awaitMutationDrain();
    const closedThroughMs = Date.now();
    const cutoffMs = typeof body.cutoff === "number" ? body.cutoff : Date.parse(body.cutoff ?? "");
    const throughMs = typeof body.through === "number" ? body.through : Date.parse(body.through ?? "");
    if (!Number.isFinite(cutoffMs) || !Number.isFinite(throughMs) || throughMs < cutoffMs) {
      return json({ error: "cutoff/through must be a valid ordered time window." }, 400);
    }
    const row = this.one<{
      tx_id: string;
      operation_hash: string;
      decision: string;
      created_at: string;
      commit_decided_at_ms: number | null;
      redo_envelope_json: string | null;
    }>(
      `SELECT tx_id, operation_hash, decision, created_at, commit_decided_at_ms, redo_envelope_json
         FROM transactions LIMIT 1`,
    );
    const entries = [] as Array<{
      tx_id: string;
      operation_hash: string;
      decision: string;
      commit_decided_at: string;
      envelope_hash: string | null;
    }>;
    const aborted = row?.decision === "abort";
    // A transaction belongs to the restored snapshot only when it has a
    // durable commit decision at or before the cutoff. Every other non-aborted
    // coordinator must be quarantined, including work created before the
    // cutoff that was still preparing/prepared when the fence arrived.
    const requiresDiscard = !!row
      && !aborted
      && (row.commit_decided_at_ms === null || row.commit_decided_at_ms > cutoffMs);
    if (row && requiresDiscard) {
      let envelopeHash: string | null = null;
      if (row.redo_envelope_json) {
        const envelope: unknown = JSON.parse(row.redo_envelope_json);
        await validateRedoEnvelope(envelope);
        validateRedoEnvelopeStructure(envelope);
        envelopeHash = await hashRedoEnvelope(envelope);
      }
      entries.push({
        tx_id: row.tx_id,
        operation_hash: row.operation_hash,
        decision: row.decision,
        commit_decided_at: row.commit_decided_at_ms === null ? row.created_at : new Date(row.commit_decided_at_ms).toISOString(),
        envelope_hash: envelopeHash,
      });
    }
    return json({
      ok: true,
      restore_id: restoreId,
      generation: body.generation,
      closed_through: new Date(closedThroughMs).toISOString(),
      requires_discard: requiresDiscard,
      entries,
      complete: true,
    });
  }

  private restoreDiscard(): { restore_id: string; generation: number; discarded_at_ms: number } | null {
    return this.one("SELECT restore_id, generation, discarded_at_ms FROM coordinator_restore_discard WHERE singleton = 1");
  }

  private persistRestoreDiscard(
    restoreId: string,
    generation: number,
  ): { ok: true; discardedAtMs: number } | { ok: false } {
    const existing = this.restoreDiscard();
    if (existing && (existing.restore_id !== restoreId || existing.generation !== generation)) return { ok: false };
    const discardedAtMs = existing?.discarded_at_ms ?? Date.now();
    this.ctx.storage.transactionSync(() => {
      this.sql.exec(
        `INSERT OR IGNORE INTO coordinator_restore_discard (singleton, restore_id, generation, discarded_at_ms)
         VALUES (1, ?, ?, ?)`,
        restoreId,
        generation,
        discardedAtMs,
      );
      this.sql.exec("DELETE FROM recovery_queue");
      this.sql.exec(
        `UPDATE transactions
            SET status = 'quarantined', decision = 'quarantined',
                last_error = ?, updated_at = ?`,
        JSON.stringify({ code: "TX_DISCARDED_BY_RESTORE", restore_id: restoreId, generation }),
        new Date(discardedAtMs).toISOString(),
      );
    });
    return { ok: true, discardedAtMs };
  }

  private discardedByRestoreResponse(discard = this.restoreDiscard()): Response {
    return json({
      error: {
        code: "TX_DISCARDED_BY_RESTORE",
        message: "This post-cutoff coordinator was durably discarded by fleet restore.",
        restore_id: discard?.restore_id ?? null,
        generation: discard?.generation ?? null,
      },
    }, 409);
  }

  private async handleRestoreDiscard(request: Request): Promise<Response> {
    const body = await request.json() as { restore_id?: string; restoreId?: string; generation?: number };
    const restoreId = body.restore_id ?? body.restoreId;
    const match = await this.matchingRestoreFence(restoreId, body.generation);
    if ("response" in match) return match.response;
    await this.awaitMutationDrain();
    const discarded = this.persistRestoreDiscard(restoreId!, body.generation!);
    if (!discarded.ok) {
      return json({ error: { code: "RESTORE_DISCARD_CONFLICT", message: "Coordinator was already discarded by a different restore generation." } }, 409);
    }
    return json({
      ok: true,
      discarded_by_restore: true,
      restore_id: restoreId,
      generation: body.generation,
      discarded_at: new Date(discarded.discardedAtMs).toISOString(),
    });
  }

  private async handleRedoEnvelope(request: Request): Promise<Response> {
    const body = await request.json() as { txId?: string; envelopeHash?: string; tx_id?: string; envelope_hash?: string };
    const txId = body.tx_id ?? body.txId;
    const envelopeHash = body.envelope_hash ?? body.envelopeHash;
    if (!txId || !envelopeHash) return json({ error: "Missing tx_id or envelope_hash." }, 400);
    const row = this.loadTx(txId);
    if (!row?.redo_envelope_json) return json({ found: false }, 404);
    const envelope = JSON.parse(row.redo_envelope_json) as ReadableRedoEnvelope;
    await validateRedoEnvelope(envelope);
    const actualHash = await hashRedoEnvelope(envelope);
    if (actualHash !== envelopeHash) {
      return json({ error: { code: "TX_ENVELOPE_HASH_MISMATCH", message: "Requested content address does not match the durable validated envelope." } }, 409);
    }
    return json({ ok: true, envelope });
  }

  async fetch(request: Request): Promise<Response> {
    try {
      return await this.handle(request);
    } catch (error) {
      const typed = contractResponse(error);
      if (typed) return typed;
      if (error instanceof RestoreGateDenied) return this.restoreGateResponse(error);
      log("coordinator.unhandled_error", { path: new URL(request.url).pathname, message: error instanceof Error ? error.message : String(error) });
      return json({ error: "Internal error." }, 500);
    }
  }

  private async handle(request: Request): Promise<Response> {
    if (request.method.toUpperCase() !== "POST") return json({ error: "Only POST allowed for coordinator endpoints." }, 405);
    const path = new URL(request.url).pathname;
    if (COORDINATOR_GATE_MUTATING_PATHS.has(path)) this.enterMutation();
    try {
    if (COORDINATOR_GATE_MUTATING_PATHS.has(path)) await this.assertRestoreGateOpen();
    this.ensureSchema();
    if (COORDINATOR_GATE_MUTATING_PATHS.has(path) && this.restoreDiscard()) return this.discardedByRestoreResponse();
    const handler = this.routes[path];
    return handler ? handler(request) : json({ error: `Unknown coordinator route: ${path}` }, 404);
    } finally {
      if (COORDINATOR_GATE_MUTATING_PATHS.has(path)) this.leaveMutation();
    }
  }

  private async handleTxStatus(request: Request): Promise<Response> {
    const body = (await request.json()) as { txId?: string };
    if (!body.txId) return json({ error: "Missing txId" }, 400);
    const discard = this.restoreDiscard();
    if (discard) {
      return json({
        found: true,
        status: "quarantined",
        decision: "quarantined",
        discarded_by_restore: true,
        restore_id: discard.restore_id,
        generation: discard.generation,
        discarded_at: new Date(discard.discarded_at_ms).toISOString(),
      });
    }
    const row = this.loadTx(body.txId);
    if (!row) return json({ found: false });
    const state = this.stateOf(row);
    const quarantineCandidates: Array<{ kind: "record" | "finalize_intent" | "cancel_intent"; hash: string }> = [];
    if (row.manifest_record_hash) quarantineCandidates.push({ kind: "record", hash: row.manifest_record_hash });
    if (row.manifest_finalize_request_json) {
      const finalize = JSON.parse(row.manifest_finalize_request_json) as ManifestFinalizeRequestV1;
      quarantineCandidates.push({ kind: "finalize_intent", hash: await hashManifestFinalizeIntent(finalize.intent) });
    }
    if (row.manifest_cancel_request_json) {
      const cancel = JSON.parse(row.manifest_cancel_request_json) as ManifestCancelRequestV1;
      quarantineCandidates.push({ kind: "cancel_intent", hash: await hashCanonicalJson(cancel.intent) });
    }
    return json({
      found: true,
      status: state,
      decision: row.decision,
      epoch: row.epoch,
      operationHash: row.operation_hash,
      commitAuthorized: ["manifest_registered", "committing", "committed_pending_ack", "committed"].includes(state),
      ...(["quarantined", "aborted_pending_manifest_cancel"].includes(state) ? { quarantineCandidates } : {}),
    });
  }

  private async handleBegin(request: Request): Promise<Response> {
    const body = (await request.json()) as BeginPayload;
    if (!body.txId || !Array.isArray(body.participants) || body.participants.length === 0) {
      return json({ error: { code: "MISSING_FIELDS", message: "Missing txId or participants." } }, 400);
    }
    const participants = [...body.participants].sort((a, b) => a.shardId.localeCompare(b.shardId));
    const fleetId = body.fleetId || "default";
    const coordinatorId = body.coordinatorId || body.txId;
    if (this.coordinatorEnv.RESTORE_COORDINATOR && fleetId !== this.deploymentFleetId()) {
      return json({
        error: {
          code: "RESTORE_FLEET_MISMATCH",
          message: `Transaction fleet ${fleetId} does not match deployment PITR domain ${this.deploymentFleetId()}.`,
        },
      }, 409);
    }
    const existing = this.loadTx(body.txId);
    // The external inventory is outside this coordinator's PITR domain. Its
    // atomic registration acknowledgement is required before any local
    // transaction row can be admitted or resumed. Existing rows carry their
    // original creation time so the inventory can detect pre-rollout work.
    const registration = await this.registerPhysicalCoordinator(
      existing?.fleet_id || fleetId,
      existing?.coordinator_id || coordinatorId,
      body.txId,
      existing?.created_at,
    );
    if (registration.disposition === "discard_required") {
      if (!existing) throw new RestoreGateDenied("coordinator registry returned a discard directive for a new transaction");
      const discarded = this.persistRestoreDiscard(registration.restoreId, registration.generation);
      if (!discarded.ok) {
        return json({ error: { code: "RESTORE_DISCARD_CONFLICT", message: "Coordinator was already discarded by a different restore generation." } }, 409);
      }
      return this.discardedByRestoreResponse();
    }
    if (existing && await this.coordinatorIdentity(existing) === "legacy") {
      const state = this.stateOf(existing);
      const exactRetry = canonicalJson(this.normalizedParticipants(this.participants(existing))) === canonicalJson(participants);
      if (!exactRetry) {
        return protocolResponse(transactionError("TX_ID_REQUEST_MISMATCH", "This txId predates immutable request matching and cannot be retried with different content."));
      }
      if (["preparing", "prepared", "committed", "aborted"].includes(state)) return this.resume(existing);
      return protocolResponse(transactionError("TX_ID_REQUEST_MISMATCH", `Predecessor transaction state ${state} cannot be safely adopted.`));
    }
    const preflightAt = new Date().toISOString();
    const preflight = await this.envelopeFor(body.txId, fleetId, coordinatorId, participants, 1, preflightAt);
    validateRedoEnvelopeStructure(preflight); // byte rejection is complete before admission/prepare
    const operationHash = preflight.operation_hash;

    if (existing) {
      if (await this.coordinatorIdentity(existing) === "invalid") {
        return protocolResponse(transactionError("TX_ID_REQUEST_MISMATCH", "Stored coordinator hash does not match current or verified predecessor content."));
      }
      if (existing.operation_hash !== operationHash) {
        return protocolResponse(transactionError("TX_ID_REQUEST_MISMATCH", "This txId was already used with different immutable content."));
      }
      return this.resume(existing);
    }

    const assignmentRequest = await this.routeAssignmentRequest(body.txId, fleetId, coordinatorId, operationHash, 1);
    const admissionNow = Date.now();
    const admissionAttempt = this.beginAdmissionAttempt(admissionNow);
    if (admissionAttempt.state === "open") {
      log("reliability.slo", { ...reliabilitySloEvent({
        component: "coordinator",
        operation: "manifest_route_assignment",
        outcome: "controlled_failure",
        classification: { overloaded: false, retryable: true, retry_after_ms: admissionAttempt.retry_after_ms },
      }) });
      return protocolResponse(transactionError(
        "TX_MANIFEST_UNAVAILABLE",
        "Manifest admission circuit is open; retry later.",
        undefined,
        { overloaded: false, retryable: true, retry_after_ms: admissionAttempt.retry_after_ms },
      ));
    }
    let assignment: ManifestRouteAssignmentResult | null = null;
    let assignmentError: TransactionProtocolError | null = null;
    try {
      assignment = this.coordinatorEnv.CONTROL_PLANE
        ? await this.coordinatorEnv.CONTROL_PLANE.assignManifestRoute(assignmentRequest)
        : null;
    } catch (error) {
      const classification = classifyDurableObjectFailure(error);
      assignmentError = durableObjectUnavailableError(
        error,
        "TX_MANIFEST_UNAVAILABLE",
        "Manifest route assignment is temporarily unavailable before reservation.",
      );
      log("reliability.slo", { ...reliabilitySloEvent({
        component: "coordinator",
        operation: "manifest_route_assignment",
        outcome: "controlled_failure",
        classification,
      }) });
      assignment = null;
    }
    if (!assignment) {
      this.recordAdmissionFailure(admissionNow, admissionAttempt.state);
      return protocolResponse(assignmentError ?? transactionError(
        "TX_MANIFEST_UNAVAILABLE",
        "Manifest route assignment is temporarily unavailable before reservation.",
      ));
    }
    if (!assignment.ok) {
      if (assignment.error.retryable || assignment.error.overloaded) this.recordAdmissionFailure(admissionNow, admissionAttempt.state);
      else this.recordAdmissionNeutral();
      if (assignment.error.overloaded) {
        const classification = classifyDurableObjectFailure(assignment.error);
        const overloadError = durableObjectUnavailableError(
          assignment.error,
          "TX_MANIFEST_UNAVAILABLE",
          "Manifest route assignment is temporarily unavailable before reservation.",
        );
        log("reliability.slo", { ...reliabilitySloEvent({
          component: "coordinator",
          operation: "manifest_route_assignment",
          outcome: "controlled_failure",
          classification,
        }) });
        return protocolResponse(overloadError);
      }
      if (assignment.status === "unavailable") {
        return protocolResponse(durableObjectUnavailableError(
          assignment.error,
          "TX_MANIFEST_UNAVAILABLE",
          "Manifest route assignment is temporarily unavailable before reservation.",
        ));
      }
      return protocolResponse(assignment.error);
    }
    this.recordAdmissionSuccess();
    await this.validateAssignedReservation(assignmentRequest, assignment);
    await this.ensureAlarmScheduled(Date.now());

    const now = new Date().toISOString();
    try {
      this.ctx.storage.transactionSync(() => {
        this.sql.exec(
        `INSERT INTO transactions
          (tx_id, status, participant_shards_json, operation_json, operation_hash, created_at, updated_at,
           protocol_version, state_model_version, epoch, decision, fleet_id, coordinator_id,
           manifest_route_assignment_request_json, manifest_reservation_json, manifest_reservation_hash)
         VALUES (?, 'manifest_reserving', ?, ?, ?, ?, ?, ?, ?, 1, 'undecided', ?, ?, ?, ?, ?)`,
        body.txId,
        JSON.stringify(participants.map((participant) => participant.shardId)),
        JSON.stringify(participants),
        operationHash,
        now,
        now,
        CURRENT_PROTOCOL_VERSION,
        COORDINATOR_WRITE_STATE_MODEL_VERSION,
        fleetId,
        coordinatorId,
        canonicalJson(assignmentRequest),
        canonicalJson(assignment.reservation),
        assignment.reservation_hash,
      );
        for (const participant of participants) {
          this.sql.exec(
          `INSERT INTO transaction_participants
            (tx_id, shard_id, phase_status, updated_at, epoch, operation_hash)
           VALUES (?, ?, 'pending', ?, 1, ?)`,
          body.txId,
          participant.shardId,
          now,
          operationHash,
          );
        }
        this.queueRecovery(body.txId, "reserve", now);
      });
    } catch (error) {
      const concurrent = this.loadTx(body.txId);
      if (concurrent && concurrent.operation_hash === operationHash) return this.resume(concurrent);
      throw error;
    }
    const row = this.loadTx(body.txId);
    if (!row) throw new Error(`Failed to persist transaction ${body.txId}.`);
    return this.reconcileReservation(row);
  }

  private async handleForceAbort(request: Request): Promise<Response> {
    const body = (await request.json()) as { txId?: string };
    if (!body.txId) return json({ error: "Missing txId" }, 400);
    const row = this.loadTx(body.txId);
    if (!row) return json({ error: "Transaction not found." }, 404);
    const state = this.stateOf(row);
    if (isCommitDecidedOrLater(state)) {
      return protocolResponse(transactionError("TX_COMMIT_ALREADY_DECIDED", "Cannot force-abort after commit is durable."));
    }
    if (state === "quarantined") return protocolResponse(transactionError("TX_QUARANTINED", "Transaction is quarantined."));
    if (state === "aborted") return json({ ok: true, txId: body.txId, status: "aborted" });
    if (state === "aborted_pending_manifest_cancel") return this.reconcileCancel(row);
    const identity = await this.coordinatorIdentity(row);
    if (identity === "invalid") {
      return protocolResponse(transactionError("TX_ID_REQUEST_MISMATCH", "Cannot force-abort a coordinator whose stored hash is unverifiable."));
    }
    const legacyPredecisionAbort = identity === "legacy";
    const aborting = state === "abort_decided" || state === "aborting"
      ? row
      : await this.persistAbortDecision(row, legacyPredecisionAbort);
    const response = await this.reconcileAbort(aborting, legacyPredecisionAbort);
    if (response.status === 409) return json({ ok: true, txId: body.txId, status: "aborted" });
    return response;
  }

  private async handleResolveManifestQuarantine(request: Request): Promise<Response> {
    const body = (await request.json()) as {
      txId?: string;
      resolution?: "FINALIZED" | "CANCELLED";
      selectedHash?: string;
      evidenceHash?: string;
      actor?: string;
      reason?: string;
      idempotencyKey?: string;
    };
    if (
      !body.txId || (body.resolution !== "FINALIZED" && body.resolution !== "CANCELLED")
      || !body.selectedHash || !body.evidenceHash || !body.actor || !body.reason || !body.idempotencyKey
    ) return json({ error: "Missing or invalid quarantine-resolution fields." }, 400);
    const row = this.loadTx(body.txId);
    if (!row) return json({ error: "Transaction not found." }, 404);
    if (!this.isV2(row)) {
      return protocolResponse(transactionError("TX_VERSION_UNSUPPORTED", "Quarantine resolution requires a V2 reservation."));
    }
    const { reservation, reservationHash } = await this.reservationFor(row);
    let terminalIntent: ManifestFinalizeIntentV1 | ManifestCancelIntentV1;
    let authorizationSourceHash: string;
    if (body.resolution === "FINALIZED") {
      if (!row.manifest_finalize_request_json) {
        return protocolResponse(transactionError("TX_DECISION_UNAVAILABLE", "Coordinator has no durable finalize authorization."));
      }
      terminalIntent = (JSON.parse(row.manifest_finalize_request_json) as ManifestFinalizeRequestV1).intent;
      validateManifestFinalizeIntent(terminalIntent);
      authorizationSourceHash = await hashCanonicalJson(terminalIntent);
    } else {
      if (!row.manifest_cancel_request_json) {
        return protocolResponse(transactionError("TX_DECISION_UNAVAILABLE", "Coordinator has no durable cancel authorization."));
      }
      terminalIntent = (JSON.parse(row.manifest_cancel_request_json) as ManifestCancelRequestV1).intent;
      validateManifestCancelIntent(terminalIntent);
      authorizationSourceHash = await hashCanonicalJson(terminalIntent);
    }
    const coordinatorState = {
      tx_id: row.tx_id,
      coordinator_id: row.coordinator_id || row.tx_id,
      state: this.stateOf(row),
      decision: row.decision,
      epoch: row.epoch,
      operation_hash: row.operation_hash,
      reservation_hash: reservationHash,
      authorization_source_hash: authorizationSourceHash,
    } as const;
    const resolutionRequest: ManifestQuarantineResolutionRequestV1 = {
      reservation,
      reservation_hash: reservationHash,
      resolution: body.resolution,
      selected_hash: body.selectedHash,
      evidence_hash: body.evidenceHash,
      actor: body.actor,
      reason: body.reason,
      terminal_intent: terminalIntent,
      coordinator_state: coordinatorState,
      coordinator_state_hash: await hashCanonicalJson(coordinatorState),
      idempotency_key: body.idempotencyKey,
    };
    let result: ManifestQuarantineResolutionResult | null = null;
    try {
      result = this.coordinatorEnv.CONTROL_PLANE?.resolveManifestQuarantine
        ? await this.coordinatorEnv.CONTROL_PLANE.resolveManifestQuarantine(resolutionRequest)
        : null;
    } catch {
      result = null;
    }
    if (result === null) {
      return protocolResponse(transactionError("TX_MANIFEST_UNAVAILABLE", "Quarantine resolution service is unavailable."));
    }
    if (!result.ok) return protocolResponse(result.error);
    if (result.resolution === "FINALIZED") {
      if (result.record === undefined || result.record_hash === undefined) {
        return protocolResponse(transactionError("TX_DECISION_UNAVAILABLE", "Resolved finalize omitted the canonical record."));
      }
      const resolvedRecord = result.record;
      validateManifestRecordV2(resolvedRecord);
      if (
        await hashManifestRecordV2(resolvedRecord) !== result.record_hash
        || resolvedRecord.tx_id !== row.tx_id
        || resolvedRecord.reservation_hash !== reservationHash
        || resolvedRecord.envelope_hash !== (terminalIntent as ManifestFinalizeIntentV1).redo_envelope_hash
      ) return protocolResponse(transactionError("MANIFEST_TERMINAL_CONFLICT", "Resolved record conflicts with coordinator identity."));
      if (!row.redo_envelope_intent_json) {
        return protocolResponse(transactionError("TX_DECISION_UNAVAILABLE", "Resolved finalize has no durable redo-envelope intent."));
      }
      const storedEnvelopeIntent = JSON.parse(row.redo_envelope_intent_json) as Record<string, unknown>;
      const completedEnvelope = (
        "commit_decided_at" in storedEnvelopeIntent && "retention_deadline" in storedEnvelopeIntent
          ? storedEnvelopeIntent
          : {
              ...storedEnvelopeIntent,
              commit_decided_at: resolvedRecord.commit_decided_at,
              retention_deadline: resolvedRecord.retention_deadline,
            }
      ) as unknown as ReadableRedoEnvelope;
      await validateRedoEnvelope(completedEnvelope);
      try {
        this.ctx.storage.transactionSync(() => {
          const current = this.loadTx(row.tx_id);
          if (
            !current
            || this.stateOf(current) !== coordinatorState.state
            || current.decision !== coordinatorState.decision
            || current.epoch !== coordinatorState.epoch
            || current.operation_hash !== coordinatorState.operation_hash
            || current.manifest_reservation_hash !== coordinatorState.reservation_hash
            || current.manifest_finalize_request_json !== row.manifest_finalize_request_json
          ) throw new CoordinatorCasLost(current ? this.stateOf(current) : "quarantined");
          const currentState = this.stateOf(current);
          const nextState: TransactionState = ["manifest_registered", "committing", "committed_pending_ack", "committed"].includes(currentState)
            ? currentState
            : "manifest_registered";
          assertTransactionTransition(currentState, nextState);
          this.sql.exec(
            `UPDATE transactions
                SET status = ?, decision = 'commit', redo_envelope_json = ?, manifest_record_json = ?,
                    manifest_record_hash = ?, commit_decided_at_ms = ?, decision_sequence = ?,
                    last_error = NULL, updated_at = ?
              WHERE tx_id = ?`,
            nextState,
            canonicalJson(completedEnvelope),
            canonicalJson(resolvedRecord),
            result.record_hash,
            resolvedRecord.commit_decided_at_ms,
            resolvedRecord.decision_sequence,
            new Date().toISOString(),
            row.tx_id,
          );
          if (nextState === "manifest_registered") this.queueRecovery(row.tx_id, "commit", new Date().toISOString());
        });
      } catch (error) {
        if (error instanceof CoordinatorCasLost) {
          return protocolResponse(transactionError("TX_INVALID_TRANSITION", `Coordinator state changed to ${error.state} during quarantine repair.`));
        }
        throw error;
      }
      const resolved = this.loadTx(row.tx_id);
      if (!resolved) throw new Error(`Missing resolved transaction ${row.tx_id}.`);
      return this.reconcileCommit(resolved);
    }
    let repairedAbortState: TransactionState;
    try {
      repairedAbortState = this.ctx.storage.transactionSync(() => {
        const current = this.loadTx(row.tx_id);
        if (
          !current
          || this.stateOf(current) !== coordinatorState.state
          || current.decision !== coordinatorState.decision
          || current.epoch !== coordinatorState.epoch
          || current.operation_hash !== coordinatorState.operation_hash
          || current.manifest_reservation_hash !== coordinatorState.reservation_hash
          || current.manifest_cancel_request_json !== row.manifest_cancel_request_json
        ) throw new CoordinatorCasLost(current ? this.stateOf(current) : "quarantined");
        const currentState = this.stateOf(current);
        const nextState: TransactionState = ["abort_decided", "aborting"].includes(currentState) ? currentState : "aborted";
        assertTransactionTransition(currentState, nextState);
        this.sql.exec(
          `UPDATE transactions
              SET status = ?, decision = 'abort', result_json = ?, last_error = NULL, updated_at = ?
            WHERE tx_id = ?`,
          nextState,
          nextState === "aborted" ? JSON.stringify({ status: "aborted" }) : current.result_json,
          new Date().toISOString(),
          row.tx_id,
        );
        if (nextState === "aborted") this.sql.exec("DELETE FROM recovery_queue WHERE tx_id = ?", row.tx_id);
        else this.queueRecovery(row.tx_id, "abort", new Date().toISOString());
        return nextState;
      });
    } catch (error) {
      if (error instanceof CoordinatorCasLost) {
        return protocolResponse(transactionError("TX_INVALID_TRANSITION", `Coordinator state changed to ${error.state} during quarantine repair.`));
      }
      throw error;
    }
    if (repairedAbortState !== "aborted") {
      const repaired = this.loadTx(row.tx_id);
      if (!repaired) throw new Error(`Missing repaired transaction ${row.tx_id}.`);
      const response = await this.reconcileAbort(repaired);
      if (response.status !== 409) return response;
    }
    return json({ ok: true, txId: row.tx_id, status: "aborted", resolutionAttestationHash: result.resolution_attestation_hash });
  }

  private async handleStats(): Promise<Response> {
    const row = this.one<{ status: string }>("SELECT status FROM transactions LIMIT 1");
    const recovery = this.one<{ attempt_count: number; action: string }>("SELECT attempt_count, action FROM recovery_queue LIMIT 1");
    return json({ ok: true, status: row?.status ?? null, recovery: recovery ? { action: recovery.action, attemptCount: recovery.attempt_count } : null });
  }
}
