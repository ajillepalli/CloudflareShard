import { DurableObject } from "cloudflare:workers";
import {
  COORDINATOR_RETENTION_DAYS,
  CURRENT_PROTOCOL_VERSION,
  MANIFEST_RECORD_V2_FORMAT_VERSION,
  MANIFEST_CURSOR_FORMAT_VERSION,
  MANIFEST_SEAL_FORMAT_VERSION,
  TransactionContractViolation,
  canonicalJson,
  hashCanonicalJson,
  hashManifestFinalizeIntent,
  hashManifestRecordV2,
  hashManifestReservation,
  hashManifestRequest,
  assertManifestCursorMatchesRequest,
  validateManifestLocalPageRequest,
  validateManifestSealRequest,
  validateManifestCancelIntent,
  validateManifestFinalizeIntent,
  validateManifestReservation,
  type ManifestCancelIntentV1,
  type ManifestFinalizeIntentV1,
  type ManifestLocalPageRequestV1,
  type ManifestRecordV1,
  type ManifestRecordV2,
  type ManifestReservationV1,
  type ManifestSealReceiptV1,
  type ManifestSealRequestV1,
} from "../../../packages/contracts/src/index.js";
import {
  HELD_RETENTION_RECHECK_MS,
  manifestError,
  toManifestRpcError,
  type ManifestLookupResult,
  type ManifestCancelResult,
  type ManifestFinalizeResult,
  type ManifestRegisterResult,
  type ManifestReleaseResult,
  type ManifestReserveResult,
  type ManifestLocalPageResult,
  type ManifestQuarantineResolutionRequestV1,
  type ManifestQuarantineResolutionResult,
  type ManifestSealResult,
} from "./manifest-types.js";
import { manifestRegistrationTxId, validatedManifestRegistration } from "./service.js";

export const LIFECYCLE_FAILURE_RETRY_MS = 5 * 60 * 1000;
export const MANIFEST_CURSOR_LEASE_MS = 15 * 60 * 1000;
const DAY_MS = 24 * 60 * 60 * 1000;
const ZERO_HASH = "0".repeat(64);

interface LifecycleAlarmScheduler {
  setAlarm(timestampMs: number): Promise<void>;
  deleteAlarm(): Promise<void>;
}

interface LifecycleSweepSchedule {
  readonly next_alarm_ms: number | null;
  readonly deleted: number;
  readonly held_expired: number;
}

/**
 * Runs one idempotent lifecycle sweep. If either the sweep or its normal
 * scheduling step fails, explicitly install a later alarm. Returning only
 * after that durable fallback exists avoids relying on the platform's finite
 * automatic retry series; a fallback scheduling failure is rethrown so those
 * platform retries are still available.
 */
export async function executeLifecycleAlarm(
  scheduler: LifecycleAlarmScheduler,
  sweep: () => LifecycleSweepSchedule | Promise<LifecycleSweepSchedule>,
  nowMs: number,
): Promise<void> {
  try {
    const schedule = await sweep();
    if (schedule.next_alarm_ms === null) await scheduler.deleteAlarm();
    else await scheduler.setAlarm(schedule.next_alarm_ms);
    log("info", "journal_manifest.lifecycle_sweep", {
      deleted: schedule.deleted,
      held_expired: schedule.held_expired,
      next_alarm_ms: schedule.next_alarm_ms,
    });
  } catch (error) {
    const fallbackAlarmMs = nowMs + LIFECYCLE_FAILURE_RETRY_MS;
    try {
      await scheduler.setAlarm(fallbackAlarmMs);
    } catch {
      // Let the runtime's automatic alarm retries cover a storage outage that
      // also prevented installation of our explicit fallback.
      throw error;
    }
    log("error", "journal_manifest.lifecycle_sweep_failed", {
      error: error instanceof Error ? error.message : String(error),
      fallback_alarm_ms: fallbackAlarmMs,
    });
  }
}

interface ManifestRow {
  readonly [key: string]: SqlStorageValue;
  protocol_version: number;
  format_version: number;
  fleet_id: string;
  utc_day: string;
  partition: number;
  partition_count: number;
  routing_key: string;
  tx_id: string;
  coordinator_id: string;
  commit_decided_at: string;
  decision_epoch: number;
  envelope_hash: string;
  retention_deadline: string;
  retention_deadline_ms: number;
  record_hash: string;
  lifecycle_released: number;
  quarantined: number;
}

interface PartitionMetadataRow {
  readonly [key: string]: SqlStorageValue;
  fleet_id: string;
  utc_day: string;
  partition: number;
  partition_count: number;
  routing_key: string;
}

interface ReservationRow {
  readonly [key: string]: SqlStorageValue;
  tx_id: string;
  reservation_hash: string;
  reservation_json: string;
  state: "RESERVED" | "FINALIZED" | "CANCELLED";
  terminal_intent_hash: string | null;
  terminal_intent_json: string | null;
  record_hash: string | null;
  record_json: string | null;
  commit_decided_at_ms: number | null;
  decision_sequence: number | null;
  quarantine_state: "NONE" | "UNRESOLVED" | "RESOLVED";
}

interface BucketStateRow {
  readonly [key: string]: SqlStorageValue;
  partition_config_hash: string | null;
  decision_floor_ms: number;
  sealed_through_ms: number;
  last_assigned_ms: number;
  next_decision_sequence: number;
  records_deleted_through_ms: number;
  retention_epoch: number;
  legacy_certificate_hash: string | null;
  legacy_certificate_json: string | null;
  legacy_closed: number;
}

interface SealGenerationRow {
  readonly [key: string]: SqlStorageValue;
  generation: number;
  idempotency_key: string;
  cutoff_ms: number;
  mode: "ADVANCE" | "EXACT_PREFIX";
  status: "DRAINING" | "COMPLETE" | "QUARANTINED";
  cursor_decided_at_ms: number | null;
  cursor_decision_sequence: number | null;
  cursor_tx_id: string | null;
  digest_count: number;
  digest_root: string;
  receipt_hash: string | null;
  receipt_json: string | null;
}

type JournalManifestEnv = Record<string, never>;

function log(level: "info" | "warn" | "error", event: string, fields: Readonly<Record<string, unknown>>): void {
  const entry = JSON.stringify({ level, event, timestamp: new Date().toISOString(), ...fields });
  if (level === "error") console.error(entry);
  else if (level === "warn") console.warn(entry);
  else console.log(entry);
}

function rejected(error: unknown): ManifestRegisterResult {
  const protocolError =
    error instanceof TransactionContractViolation
      ? toManifestRpcError(error.protocolError)
      : manifestError("TX_MANIFEST_UNAVAILABLE", "Journal manifest registration failed before persistence.");
  return {
    ok: false,
    status: protocolError.code === "TX_MANIFEST_UNAVAILABLE" ? "unavailable" : "rejected",
    http_status: protocolError.http_status,
    error: protocolError,
  };
}

function manifestRecordFromRow(row: ManifestRow): ManifestRecordV1 {
  return {
    protocol_version: 1,
    format_version: 1,
    fleet_id: row.fleet_id,
    utc_day: row.utc_day,
    partition: row.partition,
    partition_count: 16,
    routing_key: row.routing_key,
    tx_id: row.tx_id,
    coordinator_id: row.coordinator_id,
    commit_decided_at: row.commit_decided_at,
    decision_epoch: row.decision_epoch,
    envelope_hash: row.envelope_hash,
    retention_deadline: row.retention_deadline,
  };
}

export class JournalManifestDO extends DurableObject<JournalManifestEnv> {
  constructor(ctx: DurableObjectState, env: JournalManifestEnv) {
    super(ctx, env);
    void this.ctx.blockConcurrencyWhile(async () => {
      this.migrate();
    });
  }

  private migrate(): void {
    this.ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS _sql_schema_migrations (
        id INTEGER PRIMARY KEY,
        applied_at TEXT NOT NULL
      )
    `);
    const current = this.ctx.storage.sql
      .exec<{ version: number }>("SELECT COALESCE(MAX(id), 0) AS version FROM _sql_schema_migrations")
      .one().version;
    if (current < 1) this.ctx.storage.transactionSync(() => {
      this.ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS partition_metadata (
          id INTEGER PRIMARY KEY CHECK (id = 1),
          fleet_id TEXT NOT NULL,
          utc_day TEXT NOT NULL,
          partition INTEGER NOT NULL,
          partition_count INTEGER NOT NULL,
          routing_key TEXT NOT NULL
        )
      `);
      this.ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS manifest_records (
          tx_id TEXT PRIMARY KEY,
          protocol_version INTEGER NOT NULL,
          format_version INTEGER NOT NULL,
          fleet_id TEXT NOT NULL,
          utc_day TEXT NOT NULL,
          partition INTEGER NOT NULL,
          partition_count INTEGER NOT NULL,
          routing_key TEXT NOT NULL,
          coordinator_id TEXT NOT NULL,
          commit_decided_at TEXT NOT NULL,
          decision_epoch INTEGER NOT NULL,
          envelope_hash TEXT NOT NULL,
          retention_deadline TEXT NOT NULL,
          retention_deadline_ms INTEGER NOT NULL,
          record_hash TEXT NOT NULL,
          record_json TEXT NOT NULL,
          lifecycle_released INTEGER NOT NULL DEFAULT 0,
          quarantined INTEGER NOT NULL DEFAULT 0,
          created_at_ms INTEGER NOT NULL,
          updated_at_ms INTEGER NOT NULL
        )
      `);
      this.ctx.storage.sql.exec(
        "CREATE INDEX IF NOT EXISTS idx_manifest_lifecycle ON manifest_records (quarantined, lifecycle_released, retention_deadline_ms)",
      );
      this.ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS manifest_conflicts (
          tx_id TEXT NOT NULL,
          original_record_hash TEXT NOT NULL,
          conflicting_record_hash TEXT NOT NULL,
          conflicting_record_json TEXT NOT NULL,
          observed_at_ms INTEGER NOT NULL,
          PRIMARY KEY (tx_id, conflicting_record_hash)
        )
      `);
      this.ctx.storage.sql.exec(
        "INSERT INTO _sql_schema_migrations (id, applied_at) VALUES (1, ?)",
        new Date().toISOString(),
      );
    });

    if (current < 2) this.ctx.storage.transactionSync(() => {
      this.ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS manifest_bucket_state (
          id INTEGER PRIMARY KEY CHECK (id = 1),
          partition_config_hash TEXT,
          decision_floor_ms INTEGER NOT NULL DEFAULT 0,
          sealed_through_ms INTEGER NOT NULL DEFAULT 0,
          last_assigned_ms INTEGER NOT NULL DEFAULT 0,
          next_decision_sequence INTEGER NOT NULL DEFAULT 1,
          next_seal_generation INTEGER NOT NULL DEFAULT 1,
          records_deleted_through_ms INTEGER NOT NULL DEFAULT 0,
          retention_epoch INTEGER NOT NULL DEFAULT 0,
          retention_evidence_root TEXT NOT NULL DEFAULT '',
          legacy_certificate_hash TEXT,
          legacy_certificate_json TEXT,
          legacy_closed INTEGER NOT NULL DEFAULT 0,
          updated_at_ms INTEGER NOT NULL
        )
      `);
      this.ctx.storage.sql.exec(
        `INSERT OR IGNORE INTO manifest_bucket_state
          (id, decision_floor_ms, sealed_through_ms, last_assigned_ms,
           next_decision_sequence, next_seal_generation,
           records_deleted_through_ms, retention_epoch,
           retention_evidence_root, legacy_closed, updated_at_ms)
         VALUES (1, 0, 0, 0, 1, 1, 0, 0, '', 0, ?)`,
        Date.now(),
      );
      this.ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS manifest_reservations (
          tx_id TEXT PRIMARY KEY,
          protocol_version INTEGER NOT NULL,
          format_version INTEGER NOT NULL,
          fleet_id TEXT NOT NULL,
          reservation_utc_day TEXT NOT NULL,
          partition INTEGER NOT NULL,
          partition_count INTEGER NOT NULL,
          routing_key TEXT NOT NULL,
          partition_config_hash TEXT NOT NULL,
          coordinator_id TEXT NOT NULL,
          operation_hash TEXT NOT NULL,
          decision_epoch INTEGER NOT NULL,
          reserved_at TEXT NOT NULL,
          reserved_at_ms INTEGER NOT NULL,
          reservation_hash TEXT NOT NULL,
          reservation_json TEXT NOT NULL,
          state TEXT NOT NULL CHECK (state IN ('RESERVED', 'FINALIZED', 'CANCELLED')),
          terminal_intent_hash TEXT,
          terminal_intent_json TEXT,
          record_hash TEXT,
          record_json TEXT,
          commit_decided_at_ms INTEGER,
          decision_sequence INTEGER,
          retention_deadline_ms INTEGER,
          lifecycle_released INTEGER NOT NULL DEFAULT 0,
          quarantine_state TEXT NOT NULL DEFAULT 'NONE'
            CHECK (quarantine_state IN ('NONE', 'UNRESOLVED', 'RESOLVED')),
          conflict_root TEXT NOT NULL DEFAULT '',
          resolution_attestation_hash TEXT,
          created_at_ms INTEGER NOT NULL,
          updated_at_ms INTEGER NOT NULL
        )
      `);
      this.ctx.storage.sql.exec(
        `CREATE INDEX IF NOT EXISTS idx_manifest_reservation_members
           ON manifest_reservations
             (state, commit_decided_at_ms, decision_sequence, tx_id)`,
      );
      this.ctx.storage.sql.exec(
        `CREATE INDEX IF NOT EXISTS idx_manifest_reservation_lifecycle
           ON manifest_reservations
             (quarantine_state, lifecycle_released, retention_deadline_ms)`,
      );
      this.ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS manifest_reservation_conflicts (
          tx_id TEXT NOT NULL,
          candidate_hash TEXT NOT NULL,
          candidate_json TEXT NOT NULL,
          transition_kind TEXT NOT NULL,
          observed_at_ms INTEGER NOT NULL,
          PRIMARY KEY (tx_id, candidate_hash)
        )
      `);
      this.ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS manifest_seal_generations (
          generation INTEGER PRIMARY KEY,
          idempotency_key TEXT NOT NULL UNIQUE,
          cutoff_ms INTEGER NOT NULL,
          mode TEXT NOT NULL CHECK (mode IN ('ADVANCE', 'EXACT_PREFIX')),
          status TEXT NOT NULL CHECK (status IN ('DRAINING', 'COMPLETE', 'QUARANTINED')),
          cursor_decided_at_ms INTEGER,
          cursor_decision_sequence INTEGER,
          cursor_tx_id TEXT,
          digest_count INTEGER NOT NULL DEFAULT 0,
          digest_root TEXT NOT NULL DEFAULT '',
          receipt_hash TEXT,
          receipt_json TEXT,
          created_at_ms INTEGER NOT NULL,
          updated_at_ms INTEGER NOT NULL
        )
      `);
      this.ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS manifest_seal_digest_entries (
          generation INTEGER NOT NULL,
          commit_decided_at_ms INTEGER NOT NULL,
          decision_sequence INTEGER NOT NULL,
          tx_id TEXT NOT NULL,
          entry_hash TEXT NOT NULL,
          PRIMARY KEY (generation, commit_decided_at_ms, decision_sequence, tx_id)
        )
      `);
      this.ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS manifest_alarm_schedule (
          purpose TEXT PRIMARY KEY,
          fire_at_ms INTEGER NOT NULL,
          generation INTEGER NOT NULL DEFAULT 0,
          payload_hash TEXT NOT NULL DEFAULT ''
        )
      `);
      this.ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS manifest_page_cursors (
          cursor_json TEXT PRIMARY KEY,
          request_hash TEXT NOT NULL,
          created_at_ms INTEGER NOT NULL,
          lease_expires_at_ms INTEGER NOT NULL
        )
      `);
      this.ctx.storage.sql.exec(
        `INSERT OR IGNORE INTO manifest_alarm_schedule (purpose, fire_at_ms, generation, payload_hash)
         SELECT 'retention', MIN(retention_deadline_ms), 0, ''
           FROM manifest_records
          HAVING COUNT(*) > 0`,
      );
      this.ctx.storage.sql.exec(
        "INSERT INTO _sql_schema_migrations (id, applied_at) VALUES (2, ?)",
        new Date().toISOString(),
      );
    });
    if (current < 3) this.ctx.storage.transactionSync(() => {
      this.ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS manifest_quarantine_resolutions (
          idempotency_key TEXT PRIMARY KEY,
          tx_id TEXT NOT NULL,
          request_hash TEXT NOT NULL,
          resolution TEXT NOT NULL CHECK (resolution IN ('FINALIZED', 'CANCELLED')),
          selected_hash TEXT NOT NULL,
          evidence_hash TEXT NOT NULL,
          coordinator_state_hash TEXT NOT NULL,
          prior_conflict_root TEXT NOT NULL,
          attestation_hash TEXT NOT NULL UNIQUE,
          attestation_json TEXT NOT NULL,
          created_at_ms INTEGER NOT NULL
        )
      `);
      this.ctx.storage.sql.exec(
        "INSERT INTO _sql_schema_migrations (id, applied_at) VALUES (3, ?)",
        new Date().toISOString(),
      );
    });
    if (current < 4) this.ctx.storage.transactionSync(() => {
      const cursorColumns = this.ctx.storage.sql
        .exec<{ readonly [key: string]: SqlStorageValue; name: string }>("PRAGMA table_info(manifest_page_cursors)")
        .toArray();
      if (!cursorColumns.some((column) => column.name === "lease_expires_at_ms")) {
        this.ctx.storage.sql.exec("ALTER TABLE manifest_page_cursors ADD COLUMN lease_expires_at_ms INTEGER NOT NULL DEFAULT 0");
      }
      this.ctx.storage.sql.exec(
        "INSERT INTO _sql_schema_migrations (id, applied_at) VALUES (4, ?)",
        new Date().toISOString(),
      );
    });
  }

  private partitionMatches(record: ManifestRecordV1, metadata: PartitionMetadataRow): boolean {
    return (
      record.fleet_id === metadata.fleet_id &&
      record.utc_day === metadata.utc_day &&
      record.partition === metadata.partition &&
      record.partition_count === metadata.partition_count &&
      record.routing_key === metadata.routing_key
    );
  }

  private async rearmPhysicalAlarm(): Promise<void> {
    const next = this.ctx.storage.sql
      .exec<{ fire_at_ms: number | null }>("SELECT MIN(fire_at_ms) AS fire_at_ms FROM manifest_alarm_schedule")
      .one().fire_at_ms;
    if (next === null) await this.ctx.storage.deleteAlarm();
    else await this.ctx.storage.setAlarm(next);
  }

  private async scheduleAlarmPurpose(
    purpose: string,
    timestampMs: number,
    generation = 0,
    payloadHash = "",
  ): Promise<void> {
    this.ctx.storage.transactionSync(() => {
      this.ctx.storage.sql.exec(
        `INSERT INTO manifest_alarm_schedule (purpose, fire_at_ms, generation, payload_hash)
         VALUES (?, ?, ?, ?)
         ON CONFLICT(purpose) DO UPDATE SET
           fire_at_ms = MIN(manifest_alarm_schedule.fire_at_ms, excluded.fire_at_ms),
           generation = excluded.generation,
           payload_hash = excluded.payload_hash`,
        purpose,
        timestampMs,
        generation,
        payloadHash,
      );
    });
    await this.rearmPhysicalAlarm();
  }

  private async clearAlarmPurpose(purpose: string): Promise<void> {
    this.ctx.storage.sql.exec("DELETE FROM manifest_alarm_schedule WHERE purpose = ?", purpose);
    await this.rearmPhysicalAlarm();
  }

  private async ensureAlarmAtOrBefore(timestampMs: number): Promise<void> {
    await this.scheduleAlarmPurpose("retention", timestampMs);
  }

  async admission(): Promise<{ readonly ok: true; readonly status: "ready" }> {
    // A successful RPC proves the target partition is currently reachable.
    // The calling CoordinatorDO owns the durable 3/30s circuit because only it
    // can observe binding failures that prevent this method from running.
    return { ok: true, status: "ready" };
  }

  private partitionMatchesReservation(
    reservation: ManifestReservationV1,
    metadata: PartitionMetadataRow,
  ): boolean {
    return (
      reservation.fleet_id === metadata.fleet_id
      && reservation.reservation_utc_day === metadata.utc_day
      && reservation.partition === metadata.partition
      && reservation.partition_count === metadata.partition_count
      && reservation.routing_key === metadata.routing_key
    );
  }

  /** Permanently fence V1 insertion before certifying the local legacy ledger.
   * Hashing is outside SQLite, but a crash leaves the fence closed and retry
   * deterministically completes the certificate before V2 admission. */
  private async ensureLegacyCertificate(): Promise<string> {
    const existing = this.ctx.storage.sql
      .exec<{ legacy_certificate_hash: string | null }>(
        "SELECT legacy_certificate_hash FROM manifest_bucket_state WHERE id = 1",
      )
      .one().legacy_certificate_hash;
    if (existing !== null) return existing;

    const legacyRows = this.ctx.storage.transactionSync(() => {
      this.ctx.storage.sql.exec(
        "UPDATE manifest_bucket_state SET legacy_closed = 1, updated_at_ms = ? WHERE id = 1",
        Date.now(),
      );
      return this.ctx.storage.sql
        .exec<{ tx_id: string; record_hash: string }>(
          "SELECT tx_id, record_hash FROM manifest_records ORDER BY tx_id",
        )
        .toArray();
    });
    const certificate = {
      schema_version: 1,
      status: legacyRows.length === 0 ? "NO_LEGACY" : "IMPORTED",
      record_count: legacyRows.length,
      records_root: await hashCanonicalJson(legacyRows),
    } as const;
    const certificateHash = await hashCanonicalJson(certificate);
    return this.ctx.storage.transactionSync(() => {
      const current = this.ctx.storage.sql
        .exec<{ legacy_certificate_hash: string | null }>(
          "SELECT legacy_certificate_hash FROM manifest_bucket_state WHERE id = 1",
        )
        .one().legacy_certificate_hash;
      if (current !== null) return current;
      this.ctx.storage.sql.exec(
        `UPDATE manifest_bucket_state
            SET legacy_certificate_hash = ?, legacy_certificate_json = ?, updated_at_ms = ?
          WHERE id = 1`,
        certificateHash,
        canonicalJson(certificate),
        Date.now(),
      );
      return certificateHash;
    });
  }

  async reserve(
    input: unknown,
    claimedReservationHash: string,
    requiredDecisionFloorMs: number,
  ): Promise<ManifestReserveResult> {
    try {
      validateManifestReservation(input);
      const reservation = input;
      const reservationHash = await hashManifestReservation(reservation);
      if (reservationHash !== claimedReservationHash) {
        return {
          ok: false,
          status: "rejected_absent",
          bucket_row_may_exist: false,
          http_status: 409,
          error: manifestError("MANIFEST_RESERVATION_CONFLICT", "reservation_hash does not match the frozen reservation."),
        };
      }
      if (!Number.isSafeInteger(requiredDecisionFloorMs) || requiredDecisionFloorMs < 0) {
        return {
          ok: false,
          status: "rejected_absent",
          bucket_row_may_exist: false,
          http_status: 400,
          error: manifestError("MANIFEST_INVALID_REQUEST", "required_decision_floor_ms must be a non-negative safe integer."),
        };
      }
      const legacyCertificateHash = await this.ensureLegacyCertificate();
      const now = Date.now();
      return this.ctx.storage.transactionSync<ManifestReserveResult>(() => {
        const metadata = this.ctx.storage.sql
          .exec<PartitionMetadataRow>(
            "SELECT fleet_id, utc_day, partition, partition_count, routing_key FROM partition_metadata WHERE id = 1",
          )
          .toArray()[0];
        if (metadata === undefined) {
          this.ctx.storage.sql.exec(
            `INSERT INTO partition_metadata
              (id, fleet_id, utc_day, partition, partition_count, routing_key)
             VALUES (1, ?, ?, ?, ?, ?)`,
            reservation.fleet_id,
            reservation.reservation_utc_day,
            reservation.partition,
            reservation.partition_count,
            reservation.routing_key,
          );
        } else if (!this.partitionMatchesReservation(reservation, metadata)) {
          return {
            ok: false,
            status: "rejected_absent",
            bucket_row_may_exist: false,
            http_status: 409,
            error: manifestError("MANIFEST_RESERVATION_CONFLICT", "Reservation was routed to a different manifest bucket."),
          };
        }

        const bucket = this.ctx.storage.sql
          .exec<BucketStateRow>("SELECT * FROM manifest_bucket_state WHERE id = 1")
          .one();
        if (bucket.partition_config_hash !== null && bucket.partition_config_hash !== reservation.partition_config_hash) {
          return {
            ok: false,
            status: "rejected_absent",
            bucket_row_may_exist: false,
            http_status: 409,
            error: manifestError("MANIFEST_RESERVATION_CONFLICT", "Reservation partition configuration conflicts with this bucket."),
          };
        }
        this.ctx.storage.sql.exec(
          `UPDATE manifest_bucket_state
              SET partition_config_hash = COALESCE(partition_config_hash, ?),
                  decision_floor_ms = MAX(decision_floor_ms, ?), updated_at_ms = ?
            WHERE id = 1`,
          reservation.partition_config_hash,
          requiredDecisionFloorMs,
          now,
        );

        const existingReservation = this.ctx.storage.sql
          .exec<ReservationRow>("SELECT * FROM manifest_reservations WHERE tx_id = ?", reservation.tx_id)
          .toArray()[0];
        if (existingReservation !== undefined) {
          if (existingReservation.reservation_hash === reservationHash && existingReservation.quarantine_state !== "UNRESOLVED") {
            return {
              ok: true,
              status: "already_reserved",
              reservation_hash: reservationHash,
              required_decision_floor_ms: Math.max(bucket.decision_floor_ms, requiredDecisionFloorMs),
              local_legacy_certificate_hash: legacyCertificateHash,
            };
          }
          this.ctx.storage.sql.exec(
            `UPDATE manifest_reservations
                SET quarantine_state = 'UNRESOLVED', conflict_root = ?, updated_at_ms = ?
              WHERE tx_id = ?`,
            reservationHash,
            now,
            reservation.tx_id,
          );
          this.ctx.storage.sql.exec(
            `INSERT OR IGNORE INTO manifest_reservation_conflicts
              (tx_id, candidate_hash, candidate_json, transition_kind, observed_at_ms)
             VALUES (?, ?, ?, 'RESERVE', ?)`,
            reservation.tx_id,
            reservationHash,
            canonicalJson(reservation),
            now,
          );
          return {
            ok: false,
            status: "quarantined",
            bucket_row_may_exist: true,
            http_status: 409,
            error: manifestError("MANIFEST_RESERVATION_CONFLICT", "Reservation identity has conflicting immutable content."),
          };
        }

        this.ctx.storage.sql.exec(
          `INSERT INTO manifest_reservations (
            tx_id, protocol_version, format_version, fleet_id,
            reservation_utc_day, partition, partition_count, routing_key,
            partition_config_hash, coordinator_id, operation_hash,
            decision_epoch, reserved_at, reserved_at_ms, reservation_hash,
            reservation_json, state, created_at_ms, updated_at_ms
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'RESERVED', ?, ?)`,
          reservation.tx_id,
          reservation.protocol_version,
          reservation.format_version,
          reservation.fleet_id,
          reservation.reservation_utc_day,
          reservation.partition,
          reservation.partition_count,
          reservation.routing_key,
          reservation.partition_config_hash,
          reservation.coordinator_id,
          reservation.operation_hash,
          reservation.decision_epoch,
          reservation.reserved_at,
          new Date(reservation.reserved_at).getTime(),
          reservationHash,
          canonicalJson(reservation),
          now,
          now,
        );
        return {
          ok: true,
          status: "reserved",
          reservation_hash: reservationHash,
          required_decision_floor_ms: Math.max(bucket.decision_floor_ms, requiredDecisionFloorMs),
          local_legacy_certificate_hash: legacyCertificateHash,
        };
      });
    } catch (error) {
      log("error", "journal_manifest.reserve_failed", {
        error: error instanceof Error ? error.message : String(error),
      });
      const protocolError = error instanceof TransactionContractViolation
        ? toManifestRpcError(error.protocolError)
        : manifestError("LEGACY_CERTIFICATION_UNAVAILABLE", "Manifest reservation is temporarily unavailable.");
      return {
        ok: false,
        status: protocolError.retryable ? "unavailable" : "rejected_absent",
        bucket_row_may_exist: false,
        http_status: protocolError.http_status,
        error: protocolError,
      };
    }
  }

  async finalize(input: unknown): Promise<ManifestFinalizeResult> {
    try {
      validateManifestFinalizeIntent(input);
      const intent: ManifestFinalizeIntentV1 = input;
      const intentHash = await hashManifestFinalizeIntent(intent);
      const now = Date.now();
      type Assignment =
        | { readonly kind: "assigned"; readonly reservation: ManifestReservationV1; readonly decided_at_ms: number; readonly sequence: number }
        | { readonly kind: "complete"; readonly record: ManifestRecordV2; readonly record_hash: string }
        | { readonly kind: "error"; readonly result: ManifestFinalizeResult };
      const assignment = this.ctx.storage.transactionSync<Assignment>(() => {
        const row = this.ctx.storage.sql
          .exec<ReservationRow>("SELECT * FROM manifest_reservations WHERE tx_id = ?", intent.tx_id)
          .toArray()[0];
        if (row === undefined || row.reservation_hash !== intent.reservation_hash) {
          return {
            kind: "error",
            result: {
              ok: false,
              status: "conflict",
              http_status: 409,
              error: manifestError("MANIFEST_TERMINAL_CONFLICT", "Finalize does not identify a durable reservation."),
            },
          };
        }
        if (row.quarantine_state === "UNRESOLVED") {
          return {
            kind: "error",
            result: {
              ok: false,
              status: "quarantined",
              http_status: 409,
              error: manifestError("MANIFEST_QUARANTINED", "Reservation is quarantined pending audited resolution."),
            },
          };
        }
        if (row.state === "CANCELLED" || (row.terminal_intent_hash !== null && row.terminal_intent_hash !== intentHash)) {
          this.ctx.storage.sql.exec(
            `UPDATE manifest_reservations
                SET quarantine_state = 'UNRESOLVED', conflict_root = ?, updated_at_ms = ?
              WHERE tx_id = ?`,
            intentHash,
            now,
            intent.tx_id,
          );
          this.ctx.storage.sql.exec(
            `INSERT OR IGNORE INTO manifest_reservation_conflicts
              (tx_id, candidate_hash, candidate_json, transition_kind, observed_at_ms)
             VALUES (?, ?, ?, 'FINALIZE', ?)`,
            intent.tx_id,
            intentHash,
            canonicalJson(intent),
            now,
          );
          return {
            kind: "error",
            result: {
              ok: false,
              status: "conflict",
              http_status: 409,
              error: manifestError("MANIFEST_TERMINAL_CONFLICT", "Finalize conflicts with the durable terminal transition."),
            },
          };
        }
        if (row.state === "FINALIZED" && row.record_json !== null && row.record_hash !== null) {
          return {
            kind: "complete",
            record: JSON.parse(row.record_json) as ManifestRecordV2,
            record_hash: row.record_hash,
          };
        }
        const reservation = JSON.parse(row.reservation_json) as ManifestReservationV1;
        if (
          reservation.operation_hash !== intent.operation_hash
          || reservation.decision_epoch !== intent.decision_epoch
        ) {
          return {
            kind: "error",
            result: {
              ok: false,
              status: "conflict",
              http_status: 409,
              error: manifestError("MANIFEST_TERMINAL_CONFLICT", "Finalize identity conflicts with the frozen reservation."),
            },
          };
        }
        if (row.state === "FINALIZED" && row.commit_decided_at_ms !== null && row.decision_sequence !== null) {
          return {
            kind: "assigned",
            reservation,
            decided_at_ms: row.commit_decided_at_ms,
            sequence: row.decision_sequence,
          };
        }
        const bucket = this.ctx.storage.sql
          .exec<BucketStateRow>("SELECT * FROM manifest_bucket_state WHERE id = 1")
          .one();
        const decidedAtMs = Math.max(now, bucket.last_assigned_ms + 1, bucket.decision_floor_ms + 1);
        const sequence = bucket.next_decision_sequence;
        this.ctx.storage.sql.exec(
          `UPDATE manifest_bucket_state
              SET last_assigned_ms = ?, next_decision_sequence = ?, updated_at_ms = ?
            WHERE id = 1`,
          decidedAtMs,
          sequence + 1,
          now,
        );
        // FINALIZED is visible immediately at the race boundary. Until the
        // canonical record hash is filled, sealing observes the incomplete
        // row and fails closed rather than publishing around it.
        this.ctx.storage.sql.exec(
          `UPDATE manifest_reservations
              SET state = 'FINALIZED', terminal_intent_hash = ?, terminal_intent_json = ?,
                  commit_decided_at_ms = ?, decision_sequence = ?, updated_at_ms = ?
            WHERE tx_id = ?`,
          intentHash,
          canonicalJson(intent),
          decidedAtMs,
          sequence,
          now,
          intent.tx_id,
        );
        return { kind: "assigned", reservation, decided_at_ms: decidedAtMs, sequence };
      });

      if (assignment.kind === "error") return assignment.result;
      if (assignment.kind === "complete") {
        return { ok: true, status: "already_finalized", record: assignment.record, record_hash: assignment.record_hash };
      }
      const commitDecidedAt = new Date(assignment.decided_at_ms).toISOString();
      const retentionDeadlineMs = assignment.decided_at_ms + COORDINATOR_RETENTION_DAYS * DAY_MS;
      await this.ensureAlarmAtOrBefore(retentionDeadlineMs);
      const reservation = assignment.reservation;
      const record: ManifestRecordV2 = {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        format_version: MANIFEST_RECORD_V2_FORMAT_VERSION,
        fleet_id: reservation.fleet_id,
        reservation_utc_day: reservation.reservation_utc_day,
        partition: reservation.partition,
        partition_count: reservation.partition_count,
        routing_key: reservation.routing_key,
        partition_config_hash: reservation.partition_config_hash,
        tx_id: reservation.tx_id,
        coordinator_id: reservation.coordinator_id,
        operation_hash: reservation.operation_hash,
        decision_epoch: reservation.decision_epoch,
        reserved_at: reservation.reserved_at,
        reservation_hash: intent.reservation_hash,
        envelope_hash: intent.redo_envelope_hash,
        commit_decided_at: commitDecidedAt,
        commit_decided_at_ms: assignment.decided_at_ms,
        decision_sequence: assignment.sequence,
        retention_deadline: new Date(retentionDeadlineMs).toISOString(),
      };
      const recordHash = await hashManifestRecordV2(record);
      const persisted = this.ctx.storage.transactionSync(() => {
        const row = this.ctx.storage.sql
          .exec<ReservationRow>("SELECT * FROM manifest_reservations WHERE tx_id = ?", intent.tx_id)
          .one();
        if (
          row.state !== "FINALIZED"
          || row.terminal_intent_hash !== intentHash
          || row.commit_decided_at_ms !== assignment.decided_at_ms
          || row.decision_sequence !== assignment.sequence
          || row.quarantine_state === "UNRESOLVED"
        ) return false;
        if (row.record_hash !== null) return row.record_hash === recordHash;
        this.ctx.storage.sql.exec(
          `UPDATE manifest_reservations
              SET record_hash = ?, record_json = ?, retention_deadline_ms = ?, updated_at_ms = ?
            WHERE tx_id = ?`,
          recordHash,
          canonicalJson(record),
          retentionDeadlineMs,
          Date.now(),
          intent.tx_id,
        );
        return true;
      });
      if (!persisted) {
        return {
          ok: false,
          status: "quarantined",
          http_status: 409,
          error: manifestError("MANIFEST_QUARANTINED", "Finalize evidence changed before canonical record publication."),
        };
      }
      return { ok: true, status: "finalized", record, record_hash: recordHash };
    } catch (error) {
      const protocolError = error instanceof TransactionContractViolation
        ? toManifestRpcError(error.protocolError)
        : manifestError("MANIFEST_TEMPORARILY_UNAVAILABLE", "Manifest finalization is temporarily unavailable.");
      return {
        ok: false,
        status: protocolError.retryable ? "unavailable" : "conflict",
        http_status: protocolError.http_status,
        error: protocolError,
      };
    }
  }

  async cancel(input: unknown): Promise<ManifestCancelResult> {
    try {
      validateManifestCancelIntent(input);
      const intent: ManifestCancelIntentV1 = input;
      const intentHash = await hashCanonicalJson(intent);
      const now = Date.now();
      return this.ctx.storage.transactionSync<ManifestCancelResult>(() => {
        const row = this.ctx.storage.sql
          .exec<ReservationRow>("SELECT * FROM manifest_reservations WHERE tx_id = ?", intent.tx_id)
          .toArray()[0];
        if (row === undefined || row.reservation_hash !== intent.reservation_hash) {
          return {
            ok: false,
            status: "conflict",
            http_status: 409,
            error: manifestError("MANIFEST_TERMINAL_CONFLICT", "Cancel does not identify a durable reservation."),
          };
        }
        if (row.quarantine_state === "UNRESOLVED") {
          return {
            ok: false,
            status: "quarantined_pending_resolution",
            http_status: 409,
            error: manifestError("MANIFEST_QUARANTINED", "Cancellation is parked pending audited quarantine resolution."),
          };
        }
        if (row.state === "CANCELLED" && row.terminal_intent_hash === intentHash) {
          return { ok: true, status: "already_cancelled" };
        }
        if (row.state === "FINALIZED" || (row.terminal_intent_hash !== null && row.terminal_intent_hash !== intentHash)) {
          this.ctx.storage.sql.exec(
            `UPDATE manifest_reservations
                SET quarantine_state = 'UNRESOLVED', conflict_root = ?, updated_at_ms = ?
              WHERE tx_id = ?`,
            intentHash,
            now,
            intent.tx_id,
          );
          this.ctx.storage.sql.exec(
            `INSERT OR IGNORE INTO manifest_reservation_conflicts
              (tx_id, candidate_hash, candidate_json, transition_kind, observed_at_ms)
             VALUES (?, ?, ?, 'CANCEL', ?)`,
            intent.tx_id,
            intentHash,
            canonicalJson(intent),
            now,
          );
          return {
            ok: false,
            status: "conflict",
            http_status: 409,
            error: manifestError("MANIFEST_TERMINAL_CONFLICT", "Cancel conflicts with the durable terminal transition."),
          };
        }
        const reservation = JSON.parse(row.reservation_json) as ManifestReservationV1;
        if (reservation.operation_hash !== intent.operation_hash || reservation.decision_epoch !== intent.decision_epoch) {
          return {
            ok: false,
            status: "conflict",
            http_status: 409,
            error: manifestError("MANIFEST_TERMINAL_CONFLICT", "Cancel identity conflicts with the frozen reservation."),
          };
        }
        this.ctx.storage.sql.exec(
          `UPDATE manifest_reservations
              SET state = 'CANCELLED', terminal_intent_hash = ?, terminal_intent_json = ?, updated_at_ms = ?
            WHERE tx_id = ?`,
          intentHash,
          canonicalJson(intent),
          now,
          intent.tx_id,
        );
        return { ok: true, status: "cancelled" };
      });
    } catch (error) {
      const protocolError = error instanceof TransactionContractViolation
        ? toManifestRpcError(error.protocolError)
        : manifestError("MANIFEST_TEMPORARILY_UNAVAILABLE", "Manifest cancellation is temporarily unavailable.");
      return {
        ok: false,
        status: protocolError.retryable ? "unavailable" : "conflict",
        http_status: protocolError.http_status,
        error: protocolError,
      };
    }
  }

  async resolveQuarantine(input: ManifestQuarantineResolutionRequestV1): Promise<ManifestQuarantineResolutionResult> {
    try {
      const requiredKeys = [
        "actor", "coordinator_state", "coordinator_state_hash", "evidence_hash", "idempotency_key",
        "reason", "reservation", "reservation_hash", "resolution", "selected_hash", "terminal_intent",
      ];
      if (
        input === null || typeof input !== "object" || Array.isArray(input)
        || Object.keys(input).sort().join(",") !== requiredKeys.sort().join(",")
      ) throw new TypeError("Quarantine resolution request has invalid fields.");
      validateManifestReservation(input.reservation);
      const reservationHash = await hashManifestReservation(input.reservation);
      const hashPattern = /^[a-f0-9]{64}$/;
      if (
        reservationHash !== input.reservation_hash
        || !hashPattern.test(input.selected_hash)
        || !hashPattern.test(input.evidence_hash)
        || !hashPattern.test(input.coordinator_state_hash)
        || typeof input.actor !== "string" || input.actor.trim().length === 0
        || typeof input.reason !== "string" || input.reason.trim().length === 0
        || typeof input.idempotency_key !== "string" || input.idempotency_key.length === 0
        || (input.resolution !== "FINALIZED" && input.resolution !== "CANCELLED")
      ) throw new TypeError("Quarantine resolution request is invalid.");
      if (await hashCanonicalJson(input.coordinator_state) !== input.coordinator_state_hash) {
        throw new TypeError("Coordinator state attestation hash does not match its content.");
      }
      const coordinator = input.coordinator_state;
      if (
        coordinator.tx_id !== input.reservation.tx_id
        || coordinator.coordinator_id !== input.reservation.coordinator_id
        || coordinator.epoch !== input.reservation.decision_epoch
        || coordinator.operation_hash !== input.reservation.operation_hash
        || coordinator.reservation_hash !== input.reservation_hash
      ) throw new TypeError("Coordinator state attestation conflicts with the reservation.");
      const commitAuthorized = new Set([
        "commit_deciding", "commit_pending_manifest", "manifest_registered", "committing",
        "committed_pending_ack", "committed", "quarantined",
      ]);
      const cancelAuthorized = new Set(["abort_decided", "aborting", "aborted_pending_manifest_cancel", "aborted"]);
      if (
        input.resolution === "FINALIZED"
          ? !commitAuthorized.has(coordinator.state) || !["commit", "quarantined"].includes(coordinator.decision)
          : !cancelAuthorized.has(coordinator.state) || coordinator.decision !== "abort"
      ) throw new TypeError("Coordinator state does not authorize the requested resolution.");
      if (input.resolution === "FINALIZED") validateManifestFinalizeIntent(input.terminal_intent);
      else validateManifestCancelIntent(input.terminal_intent);
      const terminalIntentHash = input.resolution === "FINALIZED"
        ? await hashManifestFinalizeIntent(input.terminal_intent as ManifestFinalizeIntentV1)
        : await hashCanonicalJson(input.terminal_intent);
      if (
        input.terminal_intent.tx_id !== input.reservation.tx_id
        || input.terminal_intent.reservation_hash !== input.reservation_hash
        || input.terminal_intent.operation_hash !== input.reservation.operation_hash
        || input.terminal_intent.decision_epoch !== input.reservation.decision_epoch
      ) throw new TypeError("Selected terminal intent conflicts with the reservation.");

      const row = this.ctx.storage.sql
        .exec<ReservationRow>("SELECT * FROM manifest_reservations WHERE tx_id = ?", input.reservation.tx_id)
        .toArray()[0];
      if (row === undefined || row.reservation_hash !== input.reservation_hash) {
        throw new TypeError("Quarantine resolution does not identify a durable reservation.");
      }
      const candidate = this.ctx.storage.sql
        .exec<{ candidate_hash: string; candidate_json: string; transition_kind: string }>(
          "SELECT candidate_hash, candidate_json, transition_kind FROM manifest_reservation_conflicts WHERE tx_id = ? AND candidate_hash = ?",
          row.tx_id,
          input.selected_hash,
        )
        .toArray()[0];
      if (coordinator.authorization_source_hash !== terminalIntentHash) {
        throw new TypeError("Coordinator authorization source is not the selected terminal evidence.");
      }
      if (row.state === "FINALIZED") {
        if (
          input.resolution !== "FINALIZED"
          || input.selected_hash !== row.record_hash
          || terminalIntentHash !== row.terminal_intent_hash
        ) {
          throw new TypeError("A finalized canonical record can only be confirmed.");
        }
      } else if (row.state === "CANCELLED") {
        if (
          input.resolution !== "CANCELLED"
          || input.selected_hash !== row.terminal_intent_hash
          || terminalIntentHash !== row.terminal_intent_hash
        ) {
          throw new TypeError("A cancelled canonical reservation can only be confirmed.");
        }
      } else if (
        input.selected_hash !== terminalIntentHash
        || (candidate !== undefined
          && candidate.transition_kind !== (input.resolution === "FINALIZED" ? "FINALIZE" : "CANCEL"))
      ) {
        throw new TypeError("Selected evidence does not authorize the requested terminal transition.");
      }

      const requestHash = await hashCanonicalJson(input);
      const priorConflictRoot = row.conflict_root || ZERO_HASH;
      const attestation = {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        tx_id: row.tx_id,
        resolution: input.resolution,
        selected_hash: input.selected_hash,
        evidence_hash: input.evidence_hash,
        actor: input.actor,
        reason: input.reason,
        coordinator_state_hash: input.coordinator_state_hash,
        prior_conflict_root: priorConflictRoot,
        idempotency_key: input.idempotency_key,
      } as const;
      const attestationHash = await hashCanonicalJson(attestation);
      const resolutionStatus = this.ctx.storage.transactionSync<"resolved" | "already_resolved">(() => {
        const existing = this.ctx.storage.sql
          .exec<{ request_hash: string; attestation_hash: string }>(
            "SELECT request_hash, attestation_hash FROM manifest_quarantine_resolutions WHERE idempotency_key = ?",
            input.idempotency_key,
          )
          .toArray()[0];
        if (existing !== undefined) {
          if (existing.request_hash !== requestHash || existing.attestation_hash !== attestationHash) {
            throw new TypeError("Quarantine resolution idempotency key conflicts with prior evidence.");
          }
          return "already_resolved";
        }
        const current = this.ctx.storage.sql.exec<ReservationRow>("SELECT * FROM manifest_reservations WHERE tx_id = ?", row.tx_id).one();
        if (current.quarantine_state !== "UNRESOLVED") {
          throw new TypeError("Reservation is not awaiting quarantine resolution.");
        }
        this.ctx.storage.sql.exec(
          `INSERT INTO manifest_quarantine_resolutions
            (idempotency_key, tx_id, request_hash, resolution, selected_hash, evidence_hash,
             coordinator_state_hash, prior_conflict_root, attestation_hash, attestation_json, created_at_ms)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          input.idempotency_key,
          row.tx_id,
          requestHash,
          input.resolution,
          input.selected_hash,
          input.evidence_hash,
          input.coordinator_state_hash,
          priorConflictRoot,
          attestationHash,
          canonicalJson(attestation),
          Date.now(),
        );
        this.ctx.storage.sql.exec(
          `UPDATE manifest_reservations
              SET quarantine_state = 'RESOLVED', resolution_attestation_hash = ?, updated_at_ms = ?
            WHERE tx_id = ?`,
          attestationHash,
          Date.now(),
          row.tx_id,
        );
        return "resolved";
      });

      let terminal = this.ctx.storage.sql.exec<ReservationRow>("SELECT * FROM manifest_reservations WHERE tx_id = ?", row.tx_id).one();
      if (terminal.state === "RESERVED") {
        if (input.resolution === "FINALIZED") {
          const finalized = await this.finalize(input.terminal_intent);
          if (!finalized.ok) return {
            ok: false,
            status: finalized.status === "unavailable" ? "unavailable" : "conflict",
            http_status: finalized.http_status,
            error: finalized.error,
          };
        } else {
          const cancelled = await this.cancel(input.terminal_intent);
          if (!cancelled.ok) return {
            ok: false,
            status: cancelled.status === "unavailable" ? "unavailable" : "conflict",
            http_status: cancelled.http_status,
            error: cancelled.error,
          };
        }
        terminal = this.ctx.storage.sql.exec<ReservationRow>("SELECT * FROM manifest_reservations WHERE tx_id = ?", row.tx_id).one();
      }
      const record = terminal.record_json === null ? undefined : JSON.parse(terminal.record_json) as ManifestRecordV2;
      return {
        ok: true,
        status: resolutionStatus,
        resolution: input.resolution,
        resolution_attestation_hash: attestationHash,
        ...(record === undefined || terminal.record_hash === null ? {} : { record, record_hash: terminal.record_hash }),
      };
    } catch (error) {
      log("error", "journal_manifest.quarantine_resolution_failed", {
        error: error instanceof Error ? error.message : String(error),
      });
      const protocolError = manifestError(
        error instanceof TypeError ? "MANIFEST_TERMINAL_CONFLICT" : "MANIFEST_TEMPORARILY_UNAVAILABLE",
        error instanceof Error ? error.message : "Quarantine resolution failed.",
      );
      return {
        ok: false,
        status: protocolError.retryable ? "unavailable" : "conflict",
        http_status: protocolError.http_status,
        error: protocolError,
      };
    }
  }

  async closeThrough(input: unknown): Promise<ManifestSealResult> {
    try {
      validateManifestSealRequest(input);
      const request: ManifestSealRequestV1 = input;
      const cutoffMs = new Date(request.cutoff).getTime();
      if (cutoffMs > Date.now()) {
        return {
          ok: false,
          status: "rejected",
          http_status: 409,
          error: manifestError("MANIFEST_FUTURE_CUTOFF", "A manifest bucket cannot seal a future cutoff."),
        };
      }
      await this.ensureLegacyCertificate();
      type Start =
        | { readonly kind: "resume"; readonly generation: number }
        | { readonly kind: "complete"; readonly generation: number; readonly receipt: ManifestSealReceiptV1 }
        | { readonly kind: "result"; readonly result: ManifestSealResult };
      const start = this.ctx.storage.transactionSync<Start>(() => {
        const metadata = this.ctx.storage.sql
          .exec<PartitionMetadataRow>(
            "SELECT fleet_id, utc_day, partition, partition_count, routing_key FROM partition_metadata WHERE id = 1",
          )
          .toArray()[0];
        if (
          metadata === undefined
          || request.fleet_id !== metadata.fleet_id
          || request.reservation_utc_day !== metadata.utc_day
          || request.partition !== metadata.partition
          || request.partition_count !== metadata.partition_count
          || request.routing_key !== metadata.routing_key
        ) {
          return {
            kind: "result",
            result: {
              ok: false,
              status: "rejected",
              http_status: 409,
              error: manifestError("MANIFEST_RESERVATION_CONFLICT", "Seal request does not identify this bucket."),
            },
          };
        }
        const bucket = this.ctx.storage.sql
          .exec<BucketStateRow>("SELECT * FROM manifest_bucket_state WHERE id = 1")
          .one();
        if (bucket.partition_config_hash !== request.partition_config_hash) {
          return {
            kind: "result",
            result: {
              ok: false,
              status: "rejected",
              http_status: 409,
              error: manifestError("MANIFEST_RESERVATION_CONFLICT", "Seal partition configuration is not pinned to this bucket."),
            },
          };
        }
        const replay = this.ctx.storage.sql
          .exec<SealGenerationRow>(
            "SELECT * FROM manifest_seal_generations WHERE idempotency_key = ?",
            request.idempotency_key,
          )
          .toArray()[0];
        if (replay !== undefined) {
          if (replay.cutoff_ms !== cutoffMs) {
            return {
              kind: "result",
              result: {
                ok: false,
                status: "rejected",
                http_status: 409,
                error: manifestError("MANIFEST_NONMONOTONIC_CUTOFF", "Seal idempotency key was used for another cutoff."),
              },
            };
          }
          if (replay.status === "COMPLETE" && replay.receipt_json !== null) {
            return { kind: "complete", generation: replay.generation, receipt: JSON.parse(replay.receipt_json) as ManifestSealReceiptV1 };
          }
          if (replay.status === "QUARANTINED") {
            const unresolved = this.ctx.storage.sql
              .exec<{ count: number }>(
                `SELECT COUNT(*) AS count FROM manifest_reservations
                  WHERE state = 'FINALIZED' AND commit_decided_at_ms <= ?
                    AND quarantine_state = 'UNRESOLVED'`,
                replay.cutoff_ms,
              )
              .one().count;
            if (unresolved === 0) {
              this.ctx.storage.sql.exec(
                `UPDATE manifest_seal_generations
                    SET status = 'DRAINING', cursor_decided_at_ms = NULL,
                        cursor_decision_sequence = NULL, cursor_tx_id = NULL,
                        digest_count = 0, digest_root = ?, receipt_hash = NULL,
                        receipt_json = NULL, updated_at_ms = ?
                  WHERE generation = ? AND status = 'QUARANTINED'`,
                ZERO_HASH,
                Date.now(),
                replay.generation,
              );
              this.ctx.storage.sql.exec(
                "DELETE FROM manifest_seal_digest_entries WHERE generation = ?",
                replay.generation,
              );
              return { kind: "resume", generation: replay.generation };
            }
            return {
              kind: "result",
              result: {
                ok: false,
                status: "quarantined",
                http_status: 409,
                error: manifestError("MANIFEST_QUARANTINED", "Seal generation is blocked by unresolved evidence."),
                generation: replay.generation,
                cutoff_ms: replay.cutoff_ms,
              },
            };
          }
          return { kind: "resume", generation: replay.generation };
        }
        const exact = this.ctx.storage.sql
          .exec<SealGenerationRow>(
            "SELECT * FROM manifest_seal_generations WHERE cutoff_ms = ? AND status = 'COMPLETE' ORDER BY generation DESC LIMIT 1",
            cutoffMs,
          )
          .toArray()[0];
        if (exact?.receipt_json !== null && exact?.receipt_json !== undefined) {
          return { kind: "complete", generation: exact.generation, receipt: JSON.parse(exact.receipt_json) as ManifestSealReceiptV1 };
        }
        const inProgress = this.ctx.storage.sql
          .exec<SealGenerationRow>(
            "SELECT * FROM manifest_seal_generations WHERE status = 'DRAINING' ORDER BY generation LIMIT 1",
          )
          .toArray()[0];
        if (inProgress !== undefined) {
          return {
            kind: "result",
            result: {
              ok: false,
              status: "seal_in_progress",
              http_status: 409,
              error: manifestError("MANIFEST_SEAL_IN_PROGRESS", "Another durable seal generation is in progress."),
              generation: inProgress.generation,
              idempotency_key: inProgress.idempotency_key,
              cutoff_ms: inProgress.cutoff_ms,
            },
          };
        }
        const generation = this.ctx.storage.sql
          .exec<{ next_seal_generation: number }>(
            "SELECT next_seal_generation FROM manifest_bucket_state WHERE id = 1",
          )
          .one().next_seal_generation;
        const mode = cutoffMs < bucket.sealed_through_ms ? "EXACT_PREFIX" : "ADVANCE";
        this.ctx.storage.sql.exec(
          `INSERT INTO manifest_seal_generations
            (generation, idempotency_key, cutoff_ms, mode, status,
             digest_count, digest_root, created_at_ms, updated_at_ms)
           VALUES (?, ?, ?, ?, 'DRAINING', 0, ?, ?, ?)`,
          generation,
          request.idempotency_key,
          cutoffMs,
          mode,
          ZERO_HASH,
          Date.now(),
          Date.now(),
        );
        this.ctx.storage.sql.exec(
          `UPDATE manifest_bucket_state
              SET next_seal_generation = ?, decision_floor_ms = MAX(decision_floor_ms, ?), updated_at_ms = ?
            WHERE id = 1`,
          generation + 1,
          cutoffMs,
          Date.now(),
        );
        return { kind: "resume", generation };
      });
      if (start.kind === "result") {
        if (!start.result.ok && start.result.status === "quarantined" && start.result.generation !== undefined) {
          await this.clearAlarmPurpose(`seal:${start.result.generation}`);
        }
        return start.result;
      }
      if (start.kind === "complete") {
        await this.clearAlarmPurpose(`seal:${start.generation}`);
        return { ok: true, status: "complete", generation: start.generation, cutoff_ms: cutoffMs, receipt: start.receipt };
      }
      return await this.resumeSeal(start.generation, request);
    } catch (error) {
      const protocolError = error instanceof TransactionContractViolation
        ? toManifestRpcError(error.protocolError)
        : manifestError("MANIFEST_TEMPORARILY_UNAVAILABLE", "Manifest sealing is temporarily unavailable.");
      return {
        ok: false,
        status: protocolError.retryable ? "unavailable" : "rejected",
        http_status: protocolError.http_status,
        error: protocolError,
      };
    }
  }

  async initializeForSeal(input: unknown, requiredDecisionFloorMs: number): Promise<{
    readonly ok: true;
    readonly local_legacy_certificate_hash: string;
    readonly decision_floor_ms: number;
  }> {
    validateManifestSealRequest(input);
    const request: ManifestSealRequestV1 = input;
    if (!Number.isSafeInteger(requiredDecisionFloorMs) || requiredDecisionFloorMs < 0) {
      throw new TransactionContractViolation(
        manifestError("MANIFEST_INVALID_REQUEST", "required_decision_floor_ms must be a non-negative safe integer."),
      );
    }
    const certificateHash = await this.ensureLegacyCertificate();
    const decisionFloor = this.ctx.storage.transactionSync(() => {
      const metadata = this.ctx.storage.sql
        .exec<PartitionMetadataRow>(
          "SELECT fleet_id, utc_day, partition, partition_count, routing_key FROM partition_metadata WHERE id = 1",
        )
        .toArray()[0];
      if (metadata === undefined) {
        this.ctx.storage.sql.exec(
          `INSERT INTO partition_metadata
            (id, fleet_id, utc_day, partition, partition_count, routing_key)
           VALUES (1, ?, ?, ?, ?, ?)`,
          request.fleet_id,
          request.reservation_utc_day,
          request.partition,
          request.partition_count,
          request.routing_key,
        );
      } else if (
        request.fleet_id !== metadata.fleet_id
        || request.reservation_utc_day !== metadata.utc_day
        || request.partition !== metadata.partition
        || request.partition_count !== metadata.partition_count
        || request.routing_key !== metadata.routing_key
      ) {
        throw new TransactionContractViolation(
          manifestError("MANIFEST_RESERVATION_CONFLICT", "Seal initialization conflicts with bucket identity."),
        );
      }
      const bucket = this.ctx.storage.sql
        .exec<BucketStateRow>("SELECT * FROM manifest_bucket_state WHERE id = 1")
        .one();
      if (bucket.partition_config_hash !== null && bucket.partition_config_hash !== request.partition_config_hash) {
        throw new TransactionContractViolation(
          manifestError("MANIFEST_RESERVATION_CONFLICT", "Seal initialization conflicts with partition configuration."),
        );
      }
      const nextFloor = Math.max(bucket.decision_floor_ms, requiredDecisionFloorMs);
      this.ctx.storage.sql.exec(
        `UPDATE manifest_bucket_state
            SET partition_config_hash = COALESCE(partition_config_hash, ?),
                decision_floor_ms = ?, updated_at_ms = ? WHERE id = 1`,
        request.partition_config_hash,
        nextFloor,
        Date.now(),
      );
      return nextFloor;
    });
    return { ok: true, local_legacy_certificate_hash: certificateHash, decision_floor_ms: decisionFloor };
  }

  private async resumeSeal(
    generation: number,
    request: ManifestSealRequestV1,
    pageSize = 128,
  ): Promise<ManifestSealResult> {
    const generationRow = this.ctx.storage.sql
      .exec<SealGenerationRow>("SELECT * FROM manifest_seal_generations WHERE generation = ?", generation)
      .one();
    if (generationRow.status === "COMPLETE" && generationRow.receipt_json !== null) {
      await this.clearAlarmPurpose(`seal:${generation}`);
      return {
        ok: true,
        status: "complete",
        generation,
        cutoff_ms: generationRow.cutoff_ms,
        receipt: JSON.parse(generationRow.receipt_json) as ManifestSealReceiptV1,
      };
    }
    if (generationRow.status === "QUARANTINED") {
      await this.clearAlarmPurpose(`seal:${generation}`);
      return {
        ok: false,
        status: "quarantined",
        http_status: 409,
        error: manifestError("MANIFEST_QUARANTINED", "Eligible manifest evidence is unresolved."),
        generation,
        cutoff_ms: generationRow.cutoff_ms,
      };
    }
    const unresolved = this.ctx.storage.sql
      .exec<{ count: number }>(
        `SELECT COUNT(*) AS count FROM manifest_reservations
          WHERE state = 'FINALIZED' AND commit_decided_at_ms <= ?
            AND quarantine_state = 'UNRESOLVED'`,
        generationRow.cutoff_ms,
      )
      .one().count;
    if (unresolved > 0) {
      this.ctx.storage.sql.exec(
        "UPDATE manifest_seal_generations SET status = 'QUARANTINED', updated_at_ms = ? WHERE generation = ? AND status = 'DRAINING'",
        Date.now(),
        generation,
      );
      await this.clearAlarmPurpose(`seal:${generation}`);
      return {
        ok: false,
        status: "quarantined",
        http_status: 409,
        error: manifestError("MANIFEST_QUARANTINED", "Eligible manifest evidence is unresolved."),
        generation,
        cutoff_ms: generationRow.cutoff_ms,
      };
    }
    const afterDecided = generationRow.cursor_decided_at_ms ?? -1;
    const afterSequence = generationRow.cursor_decision_sequence ?? -1;
    const afterTx = generationRow.cursor_tx_id ?? "";
    const rows = this.ctx.storage.sql
      .exec<{
        readonly [key: string]: SqlStorageValue;
        tx_id: string;
        commit_decided_at_ms: number;
        decision_sequence: number;
        record_hash: string | null;
        resolution_attestation_hash: string | null;
      }>(
        `SELECT tx_id, commit_decided_at_ms, decision_sequence, record_hash, resolution_attestation_hash
           FROM manifest_reservations
          WHERE state = 'FINALIZED' AND commit_decided_at_ms <= ?
            AND (commit_decided_at_ms > ?
              OR (commit_decided_at_ms = ? AND decision_sequence > ?)
              OR (commit_decided_at_ms = ? AND decision_sequence = ? AND tx_id > ?))
          ORDER BY commit_decided_at_ms, decision_sequence, tx_id
          LIMIT ?`,
        generationRow.cutoff_ms,
        afterDecided,
        afterDecided,
        afterSequence,
        afterDecided,
        afterSequence,
        afterTx,
        pageSize,
      )
      .toArray();
    if (rows.some((row) => row.record_hash === null)) {
      await this.scheduleAlarmPurpose(`seal:${generation}`, Date.now() + 1_000, generation);
      return { ok: true, status: "pending", generation, cutoff_ms: generationRow.cutoff_ms };
    }
    const entryHashes: string[] = [];
    let rollingRoot = generationRow.digest_root;
    for (const row of rows) {
      const entryHash = await hashCanonicalJson([
        row.record_hash,
        row.resolution_attestation_hash ?? ZERO_HASH,
      ]);
      entryHashes.push(entryHash);
      rollingRoot = await hashCanonicalJson([rollingRoot, entryHash]);
    }
    const progressed = this.ctx.storage.transactionSync(() => {
      const current = this.ctx.storage.sql
        .exec<SealGenerationRow>("SELECT * FROM manifest_seal_generations WHERE generation = ?", generation)
        .one();
      if (
        current.status !== "DRAINING"
        || current.cursor_decided_at_ms !== generationRow.cursor_decided_at_ms
        || current.cursor_decision_sequence !== generationRow.cursor_decision_sequence
        || current.cursor_tx_id !== generationRow.cursor_tx_id
      ) return false;
      rows.forEach((row, index) => {
        this.ctx.storage.sql.exec(
          `INSERT OR IGNORE INTO manifest_seal_digest_entries
            (generation, commit_decided_at_ms, decision_sequence, tx_id, entry_hash)
           VALUES (?, ?, ?, ?, ?)`,
          generation,
          row.commit_decided_at_ms,
          row.decision_sequence,
          row.tx_id,
          entryHashes[index],
        );
      });
      const last = rows.at(-1);
      this.ctx.storage.sql.exec(
        `UPDATE manifest_seal_generations
            SET cursor_decided_at_ms = ?, cursor_decision_sequence = ?, cursor_tx_id = ?,
                digest_count = ?, digest_root = ?, updated_at_ms = ?
          WHERE generation = ?`,
        last?.commit_decided_at_ms ?? current.cursor_decided_at_ms,
        last?.decision_sequence ?? current.cursor_decision_sequence,
        last?.tx_id ?? current.cursor_tx_id,
        current.digest_count + rows.length,
        rollingRoot,
        Date.now(),
        generation,
      );
      return true;
    });
    if (!progressed || rows.length === pageSize) {
      await this.scheduleAlarmPurpose(`seal:${generation}`, Date.now() + 1_000, generation);
      return { ok: true, status: "pending", generation, cutoff_ms: generationRow.cutoff_ms };
    }

    const finalState = this.ctx.storage.transactionSync(() => {
      const current = this.ctx.storage.sql
        .exec<SealGenerationRow>("SELECT * FROM manifest_seal_generations WHERE generation = ?", generation)
        .one();
      const counts = this.ctx.storage.sql
        .exec<{ count: number; incomplete: number; max_sequence: number | null }>(
          `SELECT COUNT(*) AS count,
                  COALESCE(SUM(record_hash IS NULL), 0) AS incomplete,
                  MAX(decision_sequence) AS max_sequence
             FROM manifest_reservations
            WHERE state = 'FINALIZED' AND commit_decided_at_ms <= ?`,
          current.cutoff_ms,
        )
        .one();
      if (counts.incomplete > 0 || counts.count !== current.digest_count) return null;
      const bucket = this.ctx.storage.sql
        .exec<BucketStateRow>("SELECT * FROM manifest_bucket_state WHERE id = 1")
        .one();
      const prior = this.ctx.storage.sql
        .exec<{ receipt_hash: string }>(
          `SELECT receipt_hash FROM manifest_seal_generations
            WHERE status = 'COMPLETE' AND generation < ?
            ORDER BY generation DESC LIMIT 1`,
          generation,
        )
        .toArray()[0];
      const conflictRoots = this.ctx.storage.sql
        .exec<{ conflict_root: string; resolution_attestation_hash: string | null }>(
          `SELECT conflict_root, resolution_attestation_hash FROM manifest_reservations
            WHERE conflict_root <> '' ORDER BY tx_id`,
        )
        .toArray();
      return { current, counts, bucket, prior_hash: prior?.receipt_hash ?? null, conflictRoots };
    });
    if (finalState === null || finalState.bucket.legacy_certificate_hash === null) {
      await this.scheduleAlarmPurpose(`seal:${generation}`, Date.now() + 1_000, generation);
      return { ok: true, status: "pending", generation, cutoff_ms: generationRow.cutoff_ms };
    }
    const conflictResolutionRoot = finalState.conflictRoots.length === 0
      ? ZERO_HASH
      : await hashCanonicalJson(finalState.conflictRoots);
    const receiptWithoutHash = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_SEAL_FORMAT_VERSION,
      fleet_id: request.fleet_id,
      reservation_utc_day: request.reservation_utc_day,
      partition: request.partition,
      partition_count: request.partition_count,
      routing_key: request.routing_key,
      partition_config_hash: request.partition_config_hash,
      cutoff: new Date(finalState.current.cutoff_ms).toISOString(),
      generation,
      decision_floor_ms: finalState.bucket.decision_floor_ms,
      sealed_through_ms: finalState.current.cutoff_ms,
      record_count: finalState.counts.count,
      maximum_decision_sequence: finalState.counts.max_sequence,
      records_deleted_through_ms: finalState.bucket.records_deleted_through_ms === 0
        ? null
        : finalState.bucket.records_deleted_through_ms,
      retention_epoch: finalState.bucket.retention_epoch,
      records_root: finalState.current.digest_root,
      conflict_resolution_root: conflictResolutionRoot,
      local_legacy_certificate_hash: finalState.bucket.legacy_certificate_hash,
      prior_receipt_hash: finalState.prior_hash,
    } as const;
    const receiptHash = await hashCanonicalJson(receiptWithoutHash);
    const receipt: ManifestSealReceiptV1 = { ...receiptWithoutHash, receipt_hash: receiptHash };
    const published = this.ctx.storage.transactionSync(() => {
      const current = this.ctx.storage.sql
        .exec<SealGenerationRow>("SELECT * FROM manifest_seal_generations WHERE generation = ?", generation)
        .one();
      const eligible = this.ctx.storage.sql
        .exec<{ count: number; unresolved: number; incomplete: number }>(
          `SELECT COUNT(*) AS count,
                  COALESCE(SUM(quarantine_state = 'UNRESOLVED'), 0) AS unresolved,
                  COALESCE(SUM(record_hash IS NULL), 0) AS incomplete
             FROM manifest_reservations
            WHERE state = 'FINALIZED' AND commit_decided_at_ms <= ?`,
          current.cutoff_ms,
        )
        .one();
      if (
        current.status !== "DRAINING"
        || current.digest_count !== finalState.current.digest_count
        || current.digest_root !== finalState.current.digest_root
        || eligible.count !== current.digest_count
        || eligible.unresolved > 0
        || eligible.incomplete > 0
      ) return false;
      this.ctx.storage.sql.exec(
        `UPDATE manifest_seal_generations
            SET status = 'COMPLETE', receipt_hash = ?, receipt_json = ?, updated_at_ms = ?
          WHERE generation = ?`,
        receiptHash,
        canonicalJson(receipt),
        Date.now(),
        generation,
      );
      if (current.mode === "ADVANCE") {
        this.ctx.storage.sql.exec(
          `UPDATE manifest_bucket_state
              SET sealed_through_ms = MAX(sealed_through_ms, ?), updated_at_ms = ?
            WHERE id = 1`,
          current.cutoff_ms,
          Date.now(),
        );
      }
      this.ctx.storage.sql.exec("DELETE FROM manifest_alarm_schedule WHERE purpose = ?", `seal:${generation}`);
      return true;
    });
    if (!published) {
      await this.scheduleAlarmPurpose(`seal:${generation}`, Date.now() + 1_000, generation);
      return { ok: true, status: "pending", generation, cutoff_ms: generationRow.cutoff_ms };
    }
    await this.rearmPhysicalAlarm();
    return { ok: true, status: "complete", generation, cutoff_ms: generationRow.cutoff_ms, receipt };
  }

  async localPage(input: unknown): Promise<ManifestLocalPageResult> {
    try {
      validateManifestLocalPageRequest(input);
      const request: ManifestLocalPageRequestV1 = input;
      const requestHash = await hashManifestRequest(request);
      if (request.cursor !== null) assertManifestCursorMatchesRequest(request.cursor, requestHash);
      const coverageStartMs = new Date(request.coverage_start).getTime();
      const cutoffMs = new Date(request.cutoff).getTime();
      const now = Date.now();
      const leaseExpiresAtMs = now + MANIFEST_CURSOR_LEASE_MS;
      // Pre-arm cursor cleanup before publishing a lease. A crash after the
      // alarm write leaves only a harmless wake-up; the inverse could strand
      // a lease that indefinitely blocks retention.
      await this.ensureAlarmAtOrBefore(leaseExpiresAtMs);
      const result = this.ctx.storage.transactionSync<ManifestLocalPageResult>(() => {
        if (request.cursor !== null) {
          const issued = this.ctx.storage.sql
            .exec<{ count: number }>(
              `SELECT COUNT(*) AS count FROM manifest_page_cursors
                WHERE cursor_json = ? AND request_hash = ? AND lease_expires_at_ms > ?`,
              canonicalJson(request.cursor),
              requestHash,
              now,
            )
            .one().count;
          if (issued !== 1) {
            return {
              ok: false,
              status: "cursor_mismatch",
              http_status: 400,
              error: manifestError("MANIFEST_CURSOR_MISMATCH", "Local cursor was not issued by this bucket."),
            };
          }
        }
        const metadata = this.ctx.storage.sql
          .exec<PartitionMetadataRow>(
            "SELECT fleet_id, utc_day, partition, partition_count, routing_key FROM partition_metadata WHERE id = 1",
          )
          .toArray()[0];
        const bucket = this.ctx.storage.sql
          .exec<BucketStateRow>("SELECT * FROM manifest_bucket_state WHERE id = 1")
          .one();
        if (
          metadata === undefined
          || request.fleet_id !== metadata.fleet_id
          || request.reservation_utc_day !== metadata.utc_day
          || request.partition !== metadata.partition
          || request.partition_count !== metadata.partition_count
          || request.routing_key !== metadata.routing_key
          || request.partition_config_hash !== bucket.partition_config_hash
        ) {
          return {
            ok: false,
            status: "coverage_gap",
            http_status: 409,
            error: manifestError("MANIFEST_COVERAGE_GAP", "Local page request does not identify this manifest bucket."),
          };
        }
        if (
          request.expected_retention_epoch !== bucket.retention_epoch
          || request.cursor?.retention_epoch !== undefined && request.cursor.retention_epoch !== bucket.retention_epoch
        ) {
          return {
            ok: false,
            status: "cursor_mismatch",
            http_status: 400,
            error: manifestError("MANIFEST_CURSOR_MISMATCH", "Bucket retention epoch changed; restart enumeration."),
          };
        }
        if (bucket.records_deleted_through_ms > 0 && coverageStartMs <= bucket.records_deleted_through_ms) {
          return {
            ok: false,
            status: "retention_expired",
            http_status: 410,
            error: manifestError("MANIFEST_RETENTION_EXPIRED", "Requested coverage intersects deleted manifest history."),
          };
        }
        const seal = this.ctx.storage.sql
          .exec<SealGenerationRow>(
            "SELECT * FROM manifest_seal_generations WHERE generation = ? AND cutoff_ms = ? AND status = 'COMPLETE'",
            request.seal_generation,
            cutoffMs,
          )
          .toArray()[0];
        if (seal === undefined || seal.receipt_hash !== request.seal_receipt_hash || seal.receipt_json === null) {
          return {
            ok: false,
            status: "coverage_gap",
            http_status: 409,
            error: manifestError("MANIFEST_COVERAGE_GAP", "Exact-cutoff seal evidence is unavailable or changed."),
          };
        }
        if (
          request.cursor !== null
          && (request.cursor.seal_generation !== seal.generation || request.cursor.seal_receipt_hash !== seal.receipt_hash)
        ) {
          return {
            ok: false,
            status: "cursor_mismatch",
            http_status: 400,
            error: manifestError("MANIFEST_CURSOR_MISMATCH", "Cursor seal evidence does not match the requested generation."),
          };
        }
        const unresolved = this.ctx.storage.sql
          .exec<{ count: number }>(
            `SELECT COUNT(*) AS count FROM manifest_reservations
              WHERE state = 'FINALIZED' AND commit_decided_at_ms BETWEEN ? AND ?
                AND quarantine_state = 'UNRESOLVED'`,
            coverageStartMs,
            cutoffMs,
          )
          .one().count;
        if (unresolved > 0) {
          return {
            ok: false,
            status: "quarantined",
            http_status: 409,
            error: manifestError("MANIFEST_QUARANTINED", "Requested local page intersects unresolved evidence."),
          };
        }
        const afterDecided = request.cursor?.last_commit_decided_at_ms ?? -1;
        const afterSequence = request.cursor?.last_decision_sequence ?? -1;
        const afterTx = request.cursor?.last_tx_id ?? "";
        const rows = this.ctx.storage.sql
          .exec<{
            readonly [key: string]: SqlStorageValue;
            record_json: string;
            commit_decided_at_ms: number;
            decision_sequence: number;
            tx_id: string;
          }>(
            `SELECT record_json, commit_decided_at_ms, decision_sequence, tx_id
               FROM manifest_reservations
              WHERE state = 'FINALIZED' AND record_json IS NOT NULL
                AND commit_decided_at_ms BETWEEN ? AND ?
                AND (commit_decided_at_ms > ?
                  OR (commit_decided_at_ms = ? AND decision_sequence > ?)
                  OR (commit_decided_at_ms = ? AND decision_sequence = ? AND tx_id > ?))
              ORDER BY commit_decided_at_ms, decision_sequence, tx_id LIMIT ?`,
            coverageStartMs,
            cutoffMs,
            afterDecided,
            afterDecided,
            afterSequence,
            afterDecided,
            afterSequence,
            afterTx,
            request.limit + 1,
          )
          .toArray();
        const hasMore = rows.length > request.limit;
        const pageRows = hasMore ? rows.slice(0, request.limit) : rows;
        const last = pageRows.at(-1);
        const nextCursor = hasMore && last !== undefined ? {
          protocol_version: CURRENT_PROTOCOL_VERSION,
          format_version: MANIFEST_CURSOR_FORMAT_VERSION,
          request_hash: requestHash,
          retention_epoch: bucket.retention_epoch,
          seal_generation: seal.generation,
          seal_receipt_hash: seal.receipt_hash!,
          last_commit_decided_at_ms: last.commit_decided_at_ms,
          last_decision_sequence: last.decision_sequence,
          last_tx_id: last.tx_id,
        } as const : null;
        this.ctx.storage.sql.exec(
          `INSERT INTO manifest_page_cursors (cursor_json, request_hash, created_at_ms, lease_expires_at_ms)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(cursor_json) DO UPDATE SET
             request_hash = excluded.request_hash,
             lease_expires_at_ms = excluded.lease_expires_at_ms`,
          `lease:${requestHash}`,
          requestHash,
          now,
          leaseExpiresAtMs,
        );
        if (nextCursor !== null) {
          this.ctx.storage.sql.exec(
            `INSERT INTO manifest_page_cursors (cursor_json, request_hash, created_at_ms, lease_expires_at_ms)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(cursor_json) DO UPDATE SET lease_expires_at_ms = excluded.lease_expires_at_ms`,
            canonicalJson(nextCursor),
            requestHash,
            now,
            leaseExpiresAtMs,
          );
        }
        return {
          ok: true,
          records: pageRows.map((row) => JSON.parse(row.record_json) as ManifestRecordV2),
          next_cursor: nextCursor,
          complete: !hasMore,
          retention_epoch: bucket.retention_epoch,
          records_deleted_through_ms: bucket.records_deleted_through_ms === 0 ? null : bucket.records_deleted_through_ms,
          lease_expires_at_ms: leaseExpiresAtMs,
          seal_receipt: JSON.parse(seal.receipt_json) as ManifestSealReceiptV1,
        };
      });
      return result;
    } catch (error) {
      const protocolError = error instanceof TransactionContractViolation
        ? toManifestRpcError(error.protocolError)
        : manifestError("MANIFEST_INVALID_REQUEST", "Local manifest page request is invalid."),
        status = protocolError.code === "MANIFEST_CURSOR_MISMATCH" ? "cursor_mismatch" : "rejected";
      return { ok: false, status, http_status: protocolError.http_status, error: protocolError };
    }
  }

  async sealReceipt(receiptHash: string): Promise<ManifestSealReceiptV1 | null> {
    if (!/^[a-f0-9]{64}$/.test(receiptHash)) return null;
    const row = this.ctx.storage.sql
      .exec<{ receipt_json: string }>(
        `SELECT receipt_json FROM manifest_seal_generations
          WHERE receipt_hash = ? AND status = 'COMPLETE'`,
        receiptHash,
      )
      .toArray()[0];
    return row === undefined ? null : JSON.parse(row.receipt_json) as ManifestSealReceiptV1;
  }

  async register(input: unknown): Promise<ManifestRegisterResult> {
    try {
      const registration = await validatedManifestRegistration(input);
      const deadlineMs = new Date(registration.record.retention_deadline).getTime();

      // Pre-arm before the insert. A crash after this call but before the SQL
      // transaction leaves a harmless empty alarm; the inverse ordering could
      // leave a durable record with no lifecycle wake-up.
      await this.ensureAlarmAtOrBefore(deadlineMs);

      const now = Date.now();
      return this.ctx.storage.transactionSync<ManifestRegisterResult>(() => {
        const metadata = this.ctx.storage.sql
          .exec<PartitionMetadataRow>(
            "SELECT fleet_id, utc_day, partition, partition_count, routing_key FROM partition_metadata WHERE id = 1",
          )
          .toArray()[0];
        if (metadata === undefined) {
          this.ctx.storage.sql.exec(
            "INSERT INTO partition_metadata (id, fleet_id, utc_day, partition, partition_count, routing_key) VALUES (1, ?, ?, ?, ?, ?)",
            registration.record.fleet_id,
            registration.record.utc_day,
            registration.record.partition,
            registration.record.partition_count,
            registration.record.routing_key,
          );
        } else if (!this.partitionMatches(registration.record, metadata)) {
          return {
            ok: false,
            status: "rejected",
            http_status: 409,
            error: manifestError("TX_MANIFEST_CONFLICT", "Registration was routed to a different manifest partition."),
          };
        }

        const existing = this.ctx.storage.sql
          .exec<{ record_hash: string; quarantined: number }>(
            "SELECT record_hash, quarantined FROM manifest_records WHERE tx_id = ?",
            registration.record.tx_id,
          )
          .toArray()[0];

        if (existing !== undefined) {
          if (existing.record_hash === registration.record_hash && existing.quarantined === 0) {
            return {
              ok: true,
              status: "already_registered",
              http_status: 200,
              record_hash: existing.record_hash,
              quarantined: false,
            };
          }

          if (existing.record_hash !== registration.record_hash) {
            this.ctx.storage.sql.exec(
              "UPDATE manifest_records SET quarantined = 1, updated_at_ms = ? WHERE tx_id = ?",
              now,
              registration.record.tx_id,
            );
            this.ctx.storage.sql.exec(
              `INSERT OR IGNORE INTO manifest_conflicts
                (tx_id, original_record_hash, conflicting_record_hash, conflicting_record_json, observed_at_ms)
               VALUES (?, ?, ?, ?, ?)`,
              registration.record.tx_id,
              existing.record_hash,
              registration.record_hash,
              canonicalJson(registration.record),
              now,
            );
          }

          const conflict = this.ctx.storage.sql
            .exec<{ conflicting_record_hash: string }>(
              "SELECT conflicting_record_hash FROM manifest_conflicts WHERE tx_id = ? ORDER BY observed_at_ms DESC LIMIT 1",
              registration.record.tx_id,
            )
            .toArray()[0];
          return {
            ok: false,
            status: "quarantined",
            http_status: 409,
            error: manifestError("TX_MANIFEST_CONFLICT", "Manifest identity has conflicting immutable content."),
            original_record_hash: existing.record_hash,
            ...(conflict === undefined ? {} : { conflicting_record_hash: conflict.conflicting_record_hash }),
          };
        }

        const legacyClosed = this.ctx.storage.sql
          .exec<{ legacy_closed: number }>(
            "SELECT legacy_closed FROM manifest_bucket_state WHERE id = 1",
          )
          .one().legacy_closed;
        if (legacyClosed === 1) {
          return {
            ok: false,
            status: "rejected",
            http_status: 409,
            error: manifestError("V1_CLOSED", "Legacy manifest insertion is permanently closed for this bucket."),
          };
        }

        const record = registration.record;
        this.ctx.storage.sql.exec(
          `INSERT INTO manifest_records (
            tx_id, protocol_version, format_version, fleet_id, utc_day,
            partition, partition_count, routing_key, coordinator_id,
            commit_decided_at, decision_epoch, envelope_hash,
            retention_deadline, retention_deadline_ms, record_hash, record_json,
            lifecycle_released, quarantined, created_at_ms, updated_at_ms
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?)`,
          record.tx_id,
          record.protocol_version,
          record.format_version,
          record.fleet_id,
          record.utc_day,
          record.partition,
          record.partition_count,
          record.routing_key,
          record.coordinator_id,
          record.commit_decided_at,
          record.decision_epoch,
          record.envelope_hash,
          record.retention_deadline,
          deadlineMs,
          registration.record_hash,
          canonicalJson(record),
          now,
          now,
        );
        return {
          ok: true,
          status: "registered",
          http_status: 200,
          record_hash: registration.record_hash,
          quarantined: false,
        };
      });
    } catch (error) {
      log("error", "journal_manifest.register_failed", {
        tx_id: manifestRegistrationTxId(input),
        error: error instanceof Error ? error.message : String(error),
      });
      return rejected(error);
    }
  }

  async lookup(txId: string): Promise<ManifestLookupResult> {
    const row = this.ctx.storage.sql
      .exec<ManifestRow>(
        `SELECT protocol_version, format_version, fleet_id, utc_day, partition,
                partition_count, routing_key, tx_id, coordinator_id,
                commit_decided_at, decision_epoch, envelope_hash,
                retention_deadline, retention_deadline_ms, record_hash,
                lifecycle_released, quarantined
           FROM manifest_records WHERE tx_id = ?`,
        txId,
      )
      .toArray()[0];
    if (row === undefined) return { ok: true, found: false };

    const conflictingHashes = this.ctx.storage.sql
      .exec<{ conflicting_record_hash: string }>(
        "SELECT conflicting_record_hash FROM manifest_conflicts WHERE tx_id = ? ORDER BY conflicting_record_hash",
        txId,
      )
      .toArray()
      .map((conflict) => conflict.conflicting_record_hash);
    return {
      ok: true,
      found: true,
      record: manifestRecordFromRow(row),
      record_hash: row.record_hash,
      quarantined: row.quarantined === 1,
      conflicting_record_hashes: conflictingHashes,
      lifecycle_released: row.lifecycle_released === 1,
    };
  }

  async release(txId: string, recordHash: string): Promise<ManifestReleaseResult> {
    if (!/^[a-f0-9]{64}$/.test(recordHash)) {
      return {
        ok: false,
        status: "rejected",
        http_status: 400,
        error: manifestError("TX_ENVELOPE_INVALID", "record_hash must be a lowercase SHA-256 digest."),
      };
    }

    try {
      const now = Date.now();
      const result = this.ctx.storage.transactionSync<ManifestReleaseResult & { readonly deadline_ms?: number }>(() => {
        const existing = this.ctx.storage.sql
          .exec<{ record_hash: string; quarantined: number; lifecycle_released: number; retention_deadline_ms: number }>(
            "SELECT record_hash, quarantined, lifecycle_released, retention_deadline_ms FROM manifest_records WHERE tx_id = ?",
            txId,
          )
          .toArray()[0];
        if (existing === undefined) return { ok: true, status: "not_found" };
        if (existing.quarantined === 1 || existing.record_hash !== recordHash) {
          return {
            ok: false,
            status: "quarantined",
            http_status: 409,
            error: manifestError("TX_MANIFEST_CONFLICT", "Manifest retention cannot be released for conflicting content."),
          };
        }
        if (existing.lifecycle_released === 1) {
          return { ok: true, status: "already_released", deadline_ms: existing.retention_deadline_ms };
        }
        this.ctx.storage.sql.exec(
          "UPDATE manifest_records SET lifecycle_released = 1, updated_at_ms = ? WHERE tx_id = ?",
          now,
          txId,
        );
        return { ok: true, status: "released", deadline_ms: existing.retention_deadline_ms };
      });

      if (result.ok && result.status !== "not_found" && result.deadline_ms !== undefined) {
        await this.ensureAlarmAtOrBefore(Math.max(Date.now(), result.deadline_ms));
      }
      if (result.ok) {
        const { deadline_ms: _deadline, ...publicResult } = result;
        return publicResult;
      }
      return result;
    } catch (error) {
      log("error", "journal_manifest.release_failed", {
        tx_id: txId,
        error: error instanceof Error ? error.message : String(error),
      });
      return {
        ok: false,
        status: "unavailable",
        http_status: 503,
        error: manifestError("TX_MANIFEST_UNAVAILABLE", "Manifest lifecycle release is temporarily unavailable."),
      };
    }
  }

  async releaseV2(txId: string, reservationHash: string, recordHash: string): Promise<ManifestReleaseResult> {
    if (!/^[a-f0-9]{64}$/.test(reservationHash) || !/^[a-f0-9]{64}$/.test(recordHash)) {
      return {
        ok: false,
        status: "rejected",
        http_status: 400,
        error: manifestError("MANIFEST_INVALID_REQUEST", "V2 release hashes must be lowercase SHA-256 digests."),
      };
    }
    try {
      const result = this.ctx.storage.transactionSync<ManifestReleaseResult & { readonly deadline_ms?: number }>(() => {
        const row = this.ctx.storage.sql
          .exec<{
            readonly [key: string]: SqlStorageValue;
            reservation_hash: string;
            record_hash: string | null;
            retention_deadline_ms: number | null;
            lifecycle_released: number;
            quarantine_state: string;
          }>(
            `SELECT reservation_hash, record_hash, retention_deadline_ms,
                    lifecycle_released, quarantine_state
               FROM manifest_reservations WHERE tx_id = ?`,
            txId,
          )
          .toArray()[0];
        if (row === undefined) return { ok: true, status: "not_found" };
        if (
          row.reservation_hash !== reservationHash
          || row.record_hash !== recordHash
          || row.quarantine_state === "UNRESOLVED"
          || row.retention_deadline_ms === null
        ) {
          return {
            ok: false,
            status: "quarantined",
            http_status: 409,
            error: manifestError("MANIFEST_QUARANTINED", "V2 retention release conflicts with canonical evidence."),
          };
        }
        if (row.lifecycle_released === 1) {
          return { ok: true, status: "already_released", deadline_ms: row.retention_deadline_ms };
        }
        this.ctx.storage.sql.exec(
          "UPDATE manifest_reservations SET lifecycle_released = 1, updated_at_ms = ? WHERE tx_id = ?",
          Date.now(),
          txId,
        );
        return { ok: true, status: "released", deadline_ms: row.retention_deadline_ms };
      });
      if (result.ok && result.status !== "not_found" && result.deadline_ms !== undefined) {
        await this.ensureAlarmAtOrBefore(Math.max(Date.now(), result.deadline_ms));
        const { deadline_ms: _deadline, ...publicResult } = result;
        return publicResult;
      }
      return result;
    } catch {
      return {
        ok: false,
        status: "unavailable",
        http_status: 503,
        error: manifestError("MANIFEST_TEMPORARILY_UNAVAILABLE", "V2 retention release is temporarily unavailable."),
      };
    }
  }

  private async sweepV2Retention(
    now: number,
    limit = 128,
  ): Promise<{ readonly deleted: number; readonly deferred_until_ms: number | null }> {
    this.ctx.storage.sql.exec(
      `DELETE FROM manifest_page_cursors WHERE cursor_json IN (
         SELECT cursor_json FROM manifest_page_cursors
          WHERE lease_expires_at_ms <= ? ORDER BY lease_expires_at_ms LIMIT ?
       )`,
      now,
      limit,
    );
    const cursorLease = this.ctx.storage.sql
      .exec<{ expires_at_ms: number | null }>(
        "SELECT MIN(lease_expires_at_ms) AS expires_at_ms FROM manifest_page_cursors",
      )
      .one().expires_at_ms;
    if (cursorLease !== null) return { deleted: 0, deferred_until_ms: Math.max(now, cursorLease) };
    const drainingSeal = this.ctx.storage.sql
      .exec<{ count: number }>(
        "SELECT COUNT(*) AS count FROM manifest_seal_generations WHERE status = 'DRAINING'",
      )
      .one().count;
    if (drainingSeal > 0) return { deleted: 0, deferred_until_ms: now + HELD_RETENTION_RECHECK_MS };
    const candidates = this.ctx.storage.sql
      .exec<{
        readonly [key: string]: SqlStorageValue;
        tx_id: string;
        record_hash: string;
        commit_decided_at_ms: number;
      }>(
        `SELECT tx_id, record_hash, commit_decided_at_ms
           FROM manifest_reservations
          WHERE state = 'FINALIZED' AND quarantine_state <> 'UNRESOLVED'
            AND lifecycle_released = 1 AND retention_deadline_ms <= ?
          ORDER BY commit_decided_at_ms, decision_sequence, tx_id LIMIT ?`,
        now,
        limit,
      )
      .toArray();
    if (candidates.length === 0) return { deleted: 0, deferred_until_ms: null };
    const priorRoot = this.ctx.storage.sql
      .exec<{ retention_evidence_root: string }>(
        "SELECT retention_evidence_root FROM manifest_bucket_state WHERE id = 1",
      )
      .one().retention_evidence_root;
    const evidenceRoot = await hashCanonicalJson([priorRoot || ZERO_HASH, candidates]);
    const deleted = this.ctx.storage.transactionSync(() => {
      const replay = this.ctx.storage.sql
        .exec<{
          readonly [key: string]: SqlStorageValue;
          tx_id: string;
          record_hash: string;
          commit_decided_at_ms: number;
        }>(
          `SELECT tx_id, record_hash, commit_decided_at_ms
             FROM manifest_reservations
            WHERE state = 'FINALIZED' AND quarantine_state <> 'UNRESOLVED'
              AND lifecycle_released = 1 AND retention_deadline_ms <= ?
            ORDER BY commit_decided_at_ms, decision_sequence, tx_id LIMIT ?`,
          now,
          limit,
        )
        .toArray();
      if (canonicalJson(replay) !== canonicalJson(candidates)) return 0;
      for (const candidate of candidates) {
        this.ctx.storage.sql.exec("DELETE FROM manifest_reservations WHERE tx_id = ?", candidate.tx_id);
      }
      const maximumDeleted = candidates.at(-1)?.commit_decided_at_ms ?? 0;
      this.ctx.storage.sql.exec(
        `UPDATE manifest_bucket_state
            SET records_deleted_through_ms = MAX(records_deleted_through_ms, ?),
                retention_epoch = retention_epoch + 1,
                retention_evidence_root = ?, updated_at_ms = ?
          WHERE id = 1`,
        maximumDeleted,
        evidenceRoot,
        now,
      );
      return candidates.length;
    });
    return { deleted, deferred_until_ms: null };
  }

  async alarm(): Promise<void> {
    const now = Date.now();
    const dueSeals = this.ctx.storage.sql
      .exec<{ purpose: string; generation: number }>(
        `SELECT purpose, generation FROM manifest_alarm_schedule
          WHERE purpose LIKE 'seal:%' AND fire_at_ms <= ?
          ORDER BY fire_at_ms, purpose LIMIT 4`,
        now,
      )
      .toArray();
    for (const due of dueSeals) {
      // Consume the logical wake-up before doing work. Pending seals schedule a
      // successor; terminal seals cannot leave a past-due purpose hot-looping.
      this.ctx.storage.sql.exec("DELETE FROM manifest_alarm_schedule WHERE purpose = ?", due.purpose);
      const seal = this.ctx.storage.sql
        .exec<SealGenerationRow>("SELECT * FROM manifest_seal_generations WHERE generation = ?", due.generation)
        .toArray()[0];
      const metadata = this.ctx.storage.sql
        .exec<PartitionMetadataRow>(
          "SELECT fleet_id, utc_day, partition, partition_count, routing_key FROM partition_metadata WHERE id = 1",
        )
        .toArray()[0];
      const bucket = this.ctx.storage.sql
        .exec<BucketStateRow>("SELECT * FROM manifest_bucket_state WHERE id = 1")
        .one();
      if (seal === undefined || metadata === undefined || bucket.partition_config_hash === null) {
        this.ctx.storage.sql.exec("DELETE FROM manifest_alarm_schedule WHERE purpose = ?", due.purpose);
        continue;
      }
      try {
        await this.resumeSeal(seal.generation, {
          protocol_version: CURRENT_PROTOCOL_VERSION,
          format_version: MANIFEST_SEAL_FORMAT_VERSION,
          fleet_id: metadata.fleet_id,
          reservation_utc_day: metadata.utc_day,
          partition: metadata.partition,
          partition_count: 16,
          routing_key: metadata.routing_key,
          partition_config_hash: bucket.partition_config_hash,
          cutoff: new Date(seal.cutoff_ms).toISOString(),
          idempotency_key: seal.idempotency_key,
        });
      } catch {
        await this.scheduleAlarmPurpose(due.purpose, Date.now() + LIFECYCLE_FAILURE_RETRY_MS, due.generation);
      }
    }
    const retentionSchedule = this.ctx.storage.sql
      .exec<{ fire_at_ms: number }>(
        "SELECT fire_at_ms FROM manifest_alarm_schedule WHERE purpose = 'retention'",
      )
      .toArray()[0];
    if (retentionSchedule === undefined || retentionSchedule.fire_at_ms > now) {
      await this.rearmPhysicalAlarm();
      return;
    }
    // Remove the consumed logical deadline before computing its successor.
    // Otherwise the MIN-on-upsert scheduler would preserve the already-due
    // timestamp and hot-loop forever.
    await this.clearAlarmPurpose("retention");
    await executeLifecycleAlarm(
      {
        setAlarm: async (timestampMs) => this.scheduleAlarmPurpose("retention", timestampMs),
        deleteAlarm: async () => this.clearAlarmPurpose("retention"),
      },
      async () => {
        const v2Sweep = await this.sweepV2Retention(now);
        const schedule = this.ctx.storage.transactionSync<{
          next_deadline_ms: number | null;
          held_expired: number;
          deleted: number;
        }>(() => {
          const before = this.ctx.storage.sql
            .exec<{ count: number }>("SELECT COUNT(*) AS count FROM manifest_records")
            .one().count;
          this.ctx.storage.sql.exec(
            "DELETE FROM manifest_records WHERE quarantined = 0 AND lifecycle_released = 1 AND retention_deadline_ms <= ?",
            now,
          );
          const after = this.ctx.storage.sql
            .exec<{ count: number }>("SELECT COUNT(*) AS count FROM manifest_records")
            .one().count;
          const nextV1 = this.ctx.storage.sql
            .exec<{ deadline: number | null }>(
              "SELECT MIN(retention_deadline_ms) AS deadline FROM manifest_records WHERE quarantined = 0 AND retention_deadline_ms > ?",
              now,
            )
            .one().deadline;
          const nextV2 = this.ctx.storage.sql
            .exec<{ deadline: number | null }>(
              `SELECT MIN(retention_deadline_ms) AS deadline FROM manifest_reservations
                WHERE quarantine_state <> 'UNRESOLVED' AND retention_deadline_ms > ?`,
              now,
            )
            .one().deadline;
          const next = nextV1 === null ? nextV2 : nextV2 === null ? nextV1 : Math.min(nextV1, nextV2);
          const heldExpiredV1 = this.ctx.storage.sql
            .exec<{ count: number }>(
              "SELECT COUNT(*) AS count FROM manifest_records WHERE quarantined = 0 AND lifecycle_released = 0 AND retention_deadline_ms <= ?",
              now,
            )
            .one().count;
          const heldExpiredV2 = this.ctx.storage.sql
            .exec<{ count: number }>(
              `SELECT COUNT(*) AS count FROM manifest_reservations
                WHERE quarantine_state <> 'UNRESOLVED' AND lifecycle_released = 0
                  AND retention_deadline_ms <= ?`,
              now,
            )
            .one().count;
          return {
            next_deadline_ms: next,
            held_expired: heldExpiredV1 + heldExpiredV2,
            deleted: before - after,
          };
        });

        let nextAlarm = schedule.next_deadline_ms;
        if (schedule.held_expired > 0) {
          const heldRecheck = now + HELD_RETENTION_RECHECK_MS;
          nextAlarm = nextAlarm === null ? heldRecheck : Math.min(nextAlarm, heldRecheck);
        }
        if (v2Sweep.deferred_until_ms !== null) {
          nextAlarm = nextAlarm === null
            ? v2Sweep.deferred_until_ms
            : Math.min(nextAlarm, v2Sweep.deferred_until_ms);
        }
        return {
          next_alarm_ms: nextAlarm,
          deleted: schedule.deleted + v2Sweep.deleted,
          held_expired: schedule.held_expired + (v2Sweep.deferred_until_ms === null ? 0 : 1),
        };
      },
      now,
    );
  }

  async stats(): Promise<{
    readonly records: number;
    readonly quarantined: number;
    readonly lifecycle_released: number;
    readonly conflicts: number;
    readonly v2_reservations: number;
    readonly v2_finalized: number;
    readonly v2_cancelled: number;
    readonly v2_quarantined: number;
    readonly decision_floor_ms: number;
    readonly sealed_through_ms: number;
    readonly retention_epoch: number;
    readonly next_alarm_ms: number | null;
  }> {
    const counts = this.ctx.storage.sql
      .exec<{ records: number; quarantined: number; lifecycle_released: number }>(
        `SELECT COUNT(*) AS records,
                COALESCE(SUM(quarantined), 0) AS quarantined,
                COALESCE(SUM(lifecycle_released), 0) AS lifecycle_released
           FROM manifest_records`,
      )
      .one();
    const conflicts = this.ctx.storage.sql.exec<{ count: number }>("SELECT COUNT(*) AS count FROM manifest_conflicts").one().count;
    const v2 = this.ctx.storage.sql
      .exec<{ reservations: number; finalized: number; cancelled: number; quarantined: number }>(
        `SELECT COUNT(*) AS reservations,
                COALESCE(SUM(state = 'FINALIZED'), 0) AS finalized,
                COALESCE(SUM(state = 'CANCELLED'), 0) AS cancelled,
                COALESCE(SUM(quarantine_state = 'UNRESOLVED'), 0) AS quarantined
           FROM manifest_reservations`,
      )
      .one();
    const bucket = this.ctx.storage.sql
      .exec<{ decision_floor_ms: number; sealed_through_ms: number; retention_epoch: number }>(
        "SELECT decision_floor_ms, sealed_through_ms, retention_epoch FROM manifest_bucket_state WHERE id = 1",
      )
      .one();
    return {
      ...counts,
      conflicts,
      v2_reservations: v2.reservations,
      v2_finalized: v2.finalized,
      v2_cancelled: v2.cancelled,
      v2_quarantined: v2.quarantined,
      ...bucket,
      next_alarm_ms: await this.ctx.storage.getAlarm(),
    };
  }
}
