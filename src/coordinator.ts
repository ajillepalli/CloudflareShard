import { DurableObject } from "cloudflare:workers";
import {
  COORDINATOR_RETENTION_DAYS,
  CURRENT_PROTOCOL_VERSION,
  MANIFEST_TERMINAL_INTENT_FORMAT_VERSION,
  REDO_ENVELOPE_FORMAT_VERSION,
  TransactionContractViolation,
  assertReadableTransactionStateModelVersion,
  assertReadableProtocolVersion,
  assertTransactionTransition,
  canonicalJson,
  createManifestRegistration,
  hashCanonicalJson,
  hashManifestRecordV2,
  hashManifestReservation,
  hashParticipantOperations,
  isCommitDecidedOrLater,
  isTransactionState,
  sha256Hex,
  transactionError,
  validateRedoEnvelope,
  validateRedoEnvelopeStructure,
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
  type RedoParticipantV1,
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

type CoordinatorEnv = Omit<Cloudflare.Env, "CONTROL_PLANE"> & {
  CONTROL_PLANE?: TransactionManifestService;
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

class CoordinatorCasLost extends Error {
  constructor(readonly state: TransactionState) {
    super(`Coordinator state changed to ${state}.`);
  }
}

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

  constructor(ctx: DurableObjectState, env: CoordinatorEnv) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.coordinatorEnv = env;
    this.routes = {
      "/tx-status": this.handleTxStatus.bind(this),
      "/begin": this.handleBegin.bind(this),
      "/force-abort": this.handleForceAbort.bind(this),
      "/resolve-manifest-quarantine": this.handleResolveManifestQuarantine.bind(this),
      "/stats": this.handleStats.bind(this),
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
  }

  private one<T extends object>(statement: string, ...params: unknown[]): T | null {
    for (const row of this.sql.exec(statement, ...params)) return row as T;
    return null;
  }

  private loadTx(txId: string): TxRow | null {
    return this.one<TxRow>(
      `SELECT tx_id, status, participant_shards_json, operation_json, operation_hash,
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

  private beginAdmissionAttempt(now: number): "closed" | "probe" | "open" {
    return this.ctx.storage.transactionSync(() => {
      const circuit = this.one<{
        open_until_ms: number;
        half_open_probe: number;
        half_open_probe_until_ms: number;
      }>("SELECT open_until_ms, half_open_probe, half_open_probe_until_ms FROM manifest_admission_circuit WHERE singleton = 1");
      if (!circuit) throw new Error("Manifest admission circuit row is missing.");
      if (circuit.open_until_ms > now) return "open";
      if (circuit.open_until_ms > 0) {
        if (circuit.half_open_probe === 1 && circuit.half_open_probe_until_ms > now) return "open";
        this.sql.exec(
          "UPDATE manifest_admission_circuit SET half_open_probe = 1, half_open_probe_until_ms = ? WHERE singleton = 1",
          now + MANIFEST_CIRCUIT_POLICY.failure_window_ms,
        );
        return "probe";
      }
      return "closed";
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
    if (attempt === "open") {
      return {
        ok: false,
        status: "unavailable",
        http_status: 503,
        error: transactionError("TX_MANIFEST_UNAVAILABLE", "Manifest admission circuit is open; retry later."),
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
    } catch {
      result = {
        ok: false,
        status: "unavailable",
        http_status: 503,
        error: transactionError("TX_MANIFEST_UNAVAILABLE", "Manifest admission is unavailable before prepare."),
        circuit: {
          count_toward_open: true,
          failure_threshold: MANIFEST_CIRCUIT_POLICY.failure_threshold,
          failure_window_ms: MANIFEST_CIRCUIT_POLICY.failure_window_ms,
          maximum_cooldown_ms: MANIFEST_CIRCUIT_POLICY.maximum_cooldown_ms,
        },
      };
    }
    if (result.ok) this.recordAdmissionSuccess();
    else this.recordAdmissionFailure(now, attempt);
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
      format_version: REDO_ENVELOPE_FORMAT_VERSION,
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
        if (latest && ["abort_decided", "aborting", "aborted"].includes(this.stateOf(latest))) return latest;
      }
      throw error;
    }
  }

  private async reconcileAbort(row: TxRow, knownLegacyPredecisionAbort?: boolean): Promise<Response> {
    let current = row;
    const state = this.stateOf(current);
    if (state === "abort_decided") current = this.transition(current.tx_id, ["abort_decided"], "aborting");
    else if (state === "aborted") return protocolResponse(transactionError("TX_ABORTED", "Transaction was aborted."));
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
        this.transition(current.tx_id, ["aborting"], "aborted_pending_manifest_cancel", () => {
          this.queueRecovery(current.tx_id, "cancel", new Date().toISOString());
        });
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
    if (this.stateOf(row) === "aborted") return protocolResponse(transactionError("TX_ABORTED", "Transaction was aborted."));
    if (this.stateOf(row) !== "aborted_pending_manifest_cancel") return this.resume(row);
    const queued = this.one<{ action: string }>("SELECT action FROM recovery_queue WHERE tx_id = ?", row.tx_id);
    if (!queued && row.last_error) {
      try {
        const parked = JSON.parse(row.last_error) as { code?: string };
        if (parked.code === "MANIFEST_QUARANTINED") {
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
    await this.ensureAlarmScheduled(Date.now());
    const { reservation, reservationHash } = await this.reservationFor(row);
    const redoEnvelopeIntent = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: REDO_ENVELOPE_FORMAT_VERSION,
      tx_id: row.tx_id,
      fleet_id: row.fleet_id,
      coordinator_id: row.coordinator_id || row.tx_id,
      decision: "commit" as const,
      decision_epoch: row.epoch,
      operation_hash: row.operation_hash,
      participants: this.redoParticipants(this.participants(row), row.epoch),
    };
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
    try {
      this.transition(row.tx_id, [state], "manifest_registered", () => {
        this.sql.exec(
          `UPDATE transactions
              SET manifest_record_json = ?, manifest_record_hash = ?, commit_decided_at_ms = ?, decision_sequence = ?
            WHERE tx_id = ?`,
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
    if (row.state_model_version !== 2 || state !== "commit_pending_manifest") return this.resume(row);
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
    const outcomes = await Promise.all(
      participants.map(async (participant) => {
        try {
          const response = await this.callShard(row, participant, "prepare");
          return { participant, ok: response.ok, body: await response.json().catch(() => ({})) };
        } catch (error) {
          return { participant, ok: false, body: { error: error instanceof Error ? error.message : String(error) } };
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
        this.sql.exec("UPDATE transaction_participants SET phase_status = 'prepared', updated_at = ? WHERE tx_id = ?", now, row.tx_id);
      });
    } catch (error) {
      if (!(error instanceof CoordinatorCasLost)) throw error;
    }
    const latest = this.loadTx(row.tx_id);
    if (!latest) throw new Error(`Missing transaction ${row.tx_id}.`);
    return this.resume(latest);
  }

  async alarm(): Promise<void> {
    this.ensureSchema();
    const work = this.one<{ tx_id: string; action: string; next_attempt_at: string; attempt_count: number }>(
      "SELECT tx_id, action, next_attempt_at, attempt_count FROM recovery_queue ORDER BY next_attempt_at LIMIT 1",
    );
    if (!work) return;
    const due = new Date(work.next_attempt_at).getTime();
    if (due > Date.now()) {
      await this.ctx.storage.setAlarm(due);
      return;
    }
    const row = this.loadTx(work.tx_id);
    if (!row) {
      this.sql.exec("DELETE FROM recovery_queue WHERE tx_id = ?", work.tx_id);
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
  }

  async fetch(request: Request): Promise<Response> {
    try {
      return await this.handle(request);
    } catch (error) {
      const typed = contractResponse(error);
      if (typed) return typed;
      log("coordinator.unhandled_error", { path: new URL(request.url).pathname, message: error instanceof Error ? error.message : String(error) });
      return json({ error: "Internal error." }, 500);
    }
  }

  private async handle(request: Request): Promise<Response> {
    this.ensureSchema();
    if (request.method.toUpperCase() !== "POST") return json({ error: "Only POST allowed for coordinator endpoints." }, 405);
    const path = new URL(request.url).pathname;
    const handler = this.routes[path];
    return handler ? handler(request) : json({ error: `Unknown coordinator route: ${path}` }, 404);
  }

  private async handleTxStatus(request: Request): Promise<Response> {
    const body = (await request.json()) as { txId?: string };
    if (!body.txId) return json({ error: "Missing txId" }, 400);
    const row = this.loadTx(body.txId);
    if (!row) return json({ found: false });
    const state = this.stateOf(row);
    return json({
      found: true,
      status: state,
      decision: row.decision,
      epoch: row.epoch,
      operationHash: row.operation_hash,
      commitAuthorized: isCommitDecidedOrLater(state),
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
    const existing = this.loadTx(body.txId);
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
    let assignment: ManifestRouteAssignmentResult | null = null;
    try {
      assignment = this.coordinatorEnv.CONTROL_PLANE
        ? await this.coordinatorEnv.CONTROL_PLANE.assignManifestRoute(assignmentRequest)
        : null;
    } catch {
      assignment = null;
    }
    if (!assignment) {
      return protocolResponse(transactionError("TX_MANIFEST_UNAVAILABLE", "Manifest route assignment is temporarily unavailable before reservation."));
    }
    if (!assignment.ok) return protocolResponse(assignment.error);
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
      ) return protocolResponse(transactionError("MANIFEST_TERMINAL_CONFLICT", "Resolved record conflicts with coordinator identity."));
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
        this.sql.exec(
          `UPDATE transactions
              SET status = 'manifest_registered', decision = 'commit', manifest_record_json = ?,
                  manifest_record_hash = ?, commit_decided_at_ms = ?, decision_sequence = ?,
                  last_error = NULL, updated_at = ?
            WHERE tx_id = ?`,
          canonicalJson(resolvedRecord),
          result.record_hash,
          resolvedRecord.commit_decided_at_ms,
          resolvedRecord.decision_sequence,
          new Date().toISOString(),
          row.tx_id,
        );
        this.queueRecovery(row.tx_id, "commit", new Date().toISOString());
      });
      const resolved = this.loadTx(row.tx_id);
      if (!resolved) throw new Error(`Missing resolved transaction ${row.tx_id}.`);
      return this.reconcileCommit(resolved);
    }
    this.ctx.storage.transactionSync(() => {
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
      this.sql.exec(
        `UPDATE transactions
            SET status = 'aborted', decision = 'abort', result_json = ?, last_error = NULL, updated_at = ?
          WHERE tx_id = ?`,
        JSON.stringify({ status: "aborted" }),
        new Date().toISOString(),
        row.tx_id,
      );
      this.sql.exec("DELETE FROM recovery_queue WHERE tx_id = ?", row.tx_id);
    });
    return json({ ok: true, txId: row.tx_id, status: "aborted", resolutionAttestationHash: result.resolution_attestation_hash });
  }

  private async handleStats(): Promise<Response> {
    const row = this.one<{ status: string }>("SELECT status FROM transactions LIMIT 1");
    const recovery = this.one<{ attempt_count: number; action: string }>("SELECT attempt_count, action FROM recovery_queue LIMIT 1");
    return json({ ok: true, status: row?.status ?? null, recovery: recovery ? { action: recovery.action, attemptCount: recovery.attempt_count } : null });
  }
}
