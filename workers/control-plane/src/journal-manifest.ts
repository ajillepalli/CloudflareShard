import { DurableObject } from "cloudflare:workers";
import {
  TransactionContractViolation,
  canonicalJson,
  type ManifestRecordV1,
} from "../../../packages/contracts/src/index.js";
import {
  HELD_RETENTION_RECHECK_MS,
  manifestError,
  toManifestRpcError,
  type ManifestLookupResult,
  type ManifestRegisterResult,
  type ManifestReleaseResult,
} from "./manifest-types.js";
import { manifestRegistrationTxId, validatedManifestRegistration } from "./service.js";

export const LIFECYCLE_FAILURE_RETRY_MS = 5 * 60 * 1000;

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
  sweep: () => LifecycleSweepSchedule,
  nowMs: number,
): Promise<void> {
  try {
    const schedule = sweep();
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
    if (current >= 1) return;

    this.ctx.storage.transactionSync(() => {
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

  private async ensureAlarmAtOrBefore(timestampMs: number): Promise<void> {
    const existing = await this.ctx.storage.getAlarm();
    if (existing === null || existing > timestampMs) await this.ctx.storage.setAlarm(timestampMs);
  }

  async admission(): Promise<{ readonly ok: true; readonly status: "ready" }> {
    // A successful RPC proves the target partition is currently reachable.
    // The calling CoordinatorDO owns the durable 3/30s circuit because only it
    // can observe binding failures that prevent this method from running.
    return { ok: true, status: "ready" };
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

  async alarm(): Promise<void> {
    const now = Date.now();
    await executeLifecycleAlarm(
      this.ctx.storage,
      () => {
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
          const next = this.ctx.storage.sql
            .exec<{ deadline: number | null }>(
              "SELECT MIN(retention_deadline_ms) AS deadline FROM manifest_records WHERE quarantined = 0 AND retention_deadline_ms > ?",
              now,
            )
            .one().deadline;
          const heldExpired = this.ctx.storage.sql
            .exec<{ count: number }>(
              "SELECT COUNT(*) AS count FROM manifest_records WHERE quarantined = 0 AND lifecycle_released = 0 AND retention_deadline_ms <= ?",
              now,
            )
            .one().count;
          return { next_deadline_ms: next, held_expired: heldExpired, deleted: before - after };
        });

        let nextAlarm = schedule.next_deadline_ms;
        if (schedule.held_expired > 0) {
          const heldRecheck = now + HELD_RETENTION_RECHECK_MS;
          nextAlarm = nextAlarm === null ? heldRecheck : Math.min(nextAlarm, heldRecheck);
        }
        return {
          next_alarm_ms: nextAlarm,
          deleted: schedule.deleted,
          held_expired: schedule.held_expired,
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
    return {
      ...counts,
      conflicts,
      next_alarm_ms: await this.ctx.storage.getAlarm(),
    };
  }
}
