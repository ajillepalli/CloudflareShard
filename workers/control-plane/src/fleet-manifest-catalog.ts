import { DurableObject } from "cloudflare:workers";
import {
  canonicalJson,
  COORDINATOR_RETENTION_DAYS,
  createManifestReservation,
  hashCanonicalJson,
  hashManifestReservation,
  type CreateManifestReservationInput,
  type ManifestReservationV1,
} from "../../../packages/contracts/src/index.js";

export const FLEET_CATALOG_PROTOCOL_VERSION = 2;
export const INITIAL_PARTITION_COUNT = 16;
export const DEFAULT_CATALOG_PAGE_SIZE = 64;
export const MAX_CATALOG_PAGE_SIZE = 128;
export const CATALOG_ALARM_BATCH_SIZE = 16;
export const CATALOG_CURSOR_MAX_LEASE_MS = 15 * 60 * 1000;
export const CATALOG_HISTORY_RETENTION_MS = COORDINATOR_RETENTION_DAYS * 86_400_000;
export const MANIFEST_ROUTE_RECOVERY_MS = 60 * 60 * 1000;
const ZERO_HASH = "0".repeat(64);
const FIRST_EFFECTIVE_DAY = "0000-01-01";

type FleetManifestCatalogEnv = Record<string, never>;

interface MetadataRow {
  readonly [key: string]: SqlStorageValue;
  fleet_id: string;
  decision_floor_ms: number;
  next_event_sequence: number;
  current_snapshot_generation: number;
  last_snapshot_hash: string;
  partition_config_root_hash: string;
  legacy_scanned_through_day: string | null;
  legacy_grid_materialized_through_day: string | null;
  reservation_required_since_day: string | null;
  reservation_required_since_ms: number | null;
  legacy_admitted_through_ms: number;
}

interface ActiveBucketRow {
  readonly [key: string]: SqlStorageValue;
  reservation_day: string;
  partition: number;
  partition_count: number;
  partition_config_hash: string;
  activation_sequence: number;
  activation_key: string;
  entry_hash: string;
}

interface SnapshotRow {
  readonly [key: string]: SqlStorageValue;
  generation: number;
  idempotency_key: string;
  cutoff_ms: number;
  decision_floor_ms: number;
  fence_sequence: number;
  partition_config_root_hash: string;
  prior_snapshot_hash: string;
  status: string;
  cursor_sequence: number;
  entry_count: number;
  rolling_hash: string;
  snapshot_hash: string | null;
}

interface PartitionConfigRow {
  readonly [key: string]: SqlStorageValue;
  effective_from_day: string;
  protocol_version: number;
  partition_count: number;
  prior_hash: string;
  config_hash: string;
}

interface AlarmRow {
  readonly [key: string]: SqlStorageValue;
  purpose: string;
  fire_at_ms: number;
  generation: number;
  payload_hash: string;
}

export interface CatalogActivationRequest {
  readonly protocol_version: 2;
  readonly fleet_id: string;
  readonly reservation_day: string;
  readonly partition: number;
  readonly partition_count: number;
  readonly partition_config_hash: string;
  readonly activation_key: string;
}

export interface CatalogActivationResult {
  readonly ok: true;
  readonly status: "activated" | "already_active" | "reactivated";
  readonly activation_sequence: number;
  readonly entry_hash: string;
  readonly required_decision_floor_ms: number;
}

export interface CatalogSnapshotRequest {
  readonly protocol_version: 2;
  readonly fleet_id: string;
  readonly cutoff_ms: number;
  readonly idempotency_key: string;
  readonly page_size?: number;
}

export interface CatalogSnapshotResult {
  readonly ok: true;
  readonly status: "pending" | "complete";
  readonly generation: number;
  readonly cutoff_ms: number;
  readonly decision_floor_ms: number;
  readonly fence_sequence: number;
  readonly partition_config_root_hash: string;
  readonly prior_snapshot_hash: string;
  readonly entry_count: number;
  readonly snapshot_hash: string | null;
}

export interface CatalogSnapshotEntry {
  readonly reservation_day: string;
  readonly partition: number;
  readonly partition_count: number;
  readonly partition_config_hash: string;
  readonly activation_sequence: number;
  readonly entry_hash: string;
}

export interface CatalogCloseOperation {
  readonly close_key: string;
  readonly cutoff_ms: number;
  readonly snapshot_generation: number;
  readonly snapshot_hash: string;
  readonly status: "pending" | "complete" | "failed";
  readonly progress_cursor_sequence: number;
  readonly completed_entries: number;
  readonly total_entries: number;
  readonly fleet_root_hash: string | null;
}

export interface CatalogCloseProgressEntry {
  readonly activation_sequence: number;
  readonly reservation_day: string;
  readonly partition: number;
  readonly partition_count: number;
  readonly partition_config_hash: string;
  readonly bucket_close_key: string;
  readonly status: "pending" | "complete";
  readonly exact_receipt_hash: string | null;
  readonly covering_receipt_hash: string | null;
}

export interface CatalogEnumerationEntry extends CatalogSnapshotEntry {
  readonly exact_receipt_hash: string | null;
}

export interface CatalogAlarmPurpose {
  readonly purpose: string;
  readonly fire_at_ms: number;
  readonly generation: number;
  readonly payload_hash: string;
}

export interface CatalogRouteAssignment {
  readonly status: "assigned" | "already_assigned";
  readonly reservation: ManifestReservationV1;
  readonly reservation_hash: string;
}

export type CatalogLegacyAdmission =
  | { readonly ok: true; readonly admission_token: string; readonly activation_sequence: number }
  | { readonly ok: false; readonly status: "v1_closed" };

function assertText(value: string, name: string, max = 256): void {
  if (typeof value !== "string" || value.length === 0 || value.length > max) {
    throw new TypeError(`${name} must be a non-empty string no longer than ${max} characters.`);
  }
}

function assertHash(value: string, name: string): void {
  if (!/^[a-f0-9]{64}$/.test(value)) throw new TypeError(`${name} must be a lowercase SHA-256 digest.`);
}

function assertUtcDay(value: string, name: string): void {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value) || new Date(`${value}T00:00:00.000Z`).toISOString().slice(0, 10) !== value) {
    throw new TypeError(`${name} must be a valid UTC day.`);
  }
}

function pageSize(value: number | undefined): number {
  const selected = value ?? DEFAULT_CATALOG_PAGE_SIZE;
  if (!Number.isInteger(selected) || selected < 1 || selected > MAX_CATALOG_PAGE_SIZE) {
    throw new TypeError(`page_size must be an integer from 1 through ${MAX_CATALOG_PAGE_SIZE}.`);
  }
  return selected;
}

function todayUtc(nowMs: number): string {
  return new Date(nowMs).toISOString().slice(0, 10);
}

function addUtcDays(day: string, days: number): string {
  return new Date(Date.parse(`${day}T00:00:00.000Z`) + days * 86_400_000).toISOString().slice(0, 10);
}

/**
 * Storage implementation shared by the Durable Object and SQLite-backed tests.
 * Every catalog race boundary is a synchronous transaction; digest computation
 * is followed by an optimistic progress check before mutation.
 */
export class FleetManifestCatalogStore {
  constructor(private readonly storage: DurableObjectStorage) {}

  migrate(): void {
    this.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS _fleet_catalog_schema_migrations (
        id INTEGER PRIMARY KEY,
        applied_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS fleet_catalog_metadata (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        fleet_id TEXT NOT NULL,
        decision_floor_ms INTEGER NOT NULL DEFAULT 0,
        next_event_sequence INTEGER NOT NULL DEFAULT 0,
        current_snapshot_generation INTEGER NOT NULL DEFAULT 0,
        last_snapshot_hash TEXT NOT NULL,
        partition_config_root_hash TEXT NOT NULL,
        legacy_scanned_through_day TEXT
      );
      CREATE TABLE IF NOT EXISTS partition_config_history (
        effective_from_day TEXT PRIMARY KEY,
        protocol_version INTEGER NOT NULL,
        partition_count INTEGER NOT NULL,
        prior_hash TEXT NOT NULL,
        config_hash TEXT NOT NULL UNIQUE,
        created_at_ms INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS active_buckets (
        reservation_day TEXT NOT NULL,
        partition INTEGER NOT NULL,
        partition_count INTEGER NOT NULL,
        partition_config_hash TEXT NOT NULL,
        activation_sequence INTEGER NOT NULL UNIQUE,
        activation_key TEXT NOT NULL,
        entry_hash TEXT NOT NULL,
        activated_at_ms INTEGER NOT NULL,
        retired_sequence INTEGER,
        retirement_certificate_hash TEXT,
        retired_at_ms INTEGER,
        PRIMARY KEY (reservation_day, partition)
      );
      CREATE TABLE IF NOT EXISTS catalog_activation_keys (
        activation_key TEXT PRIMARY KEY,
        reservation_day TEXT NOT NULL,
        partition INTEGER NOT NULL,
        entry_hash TEXT NOT NULL,
        created_at_ms INTEGER NOT NULL DEFAULT 0
      );
      CREATE TABLE IF NOT EXISTS catalog_bucket_activation_history (
        activation_sequence INTEGER PRIMARY KEY,
        reservation_day TEXT NOT NULL,
        partition INTEGER NOT NULL,
        partition_count INTEGER NOT NULL,
        partition_config_hash TEXT NOT NULL,
        activation_key TEXT NOT NULL,
        entry_hash TEXT NOT NULL,
        deactivation_sequence INTEGER
      );
      CREATE TABLE IF NOT EXISTS catalog_snapshots (
        generation INTEGER PRIMARY KEY,
        idempotency_key TEXT NOT NULL UNIQUE,
        cutoff_ms INTEGER NOT NULL,
        decision_floor_ms INTEGER NOT NULL,
        fence_sequence INTEGER NOT NULL,
        partition_config_root_hash TEXT NOT NULL,
        prior_snapshot_hash TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('BUILDING', 'COMPLETE')),
        cursor_sequence INTEGER NOT NULL DEFAULT 0,
        entry_count INTEGER NOT NULL DEFAULT 0,
        rolling_hash TEXT NOT NULL,
        snapshot_hash TEXT,
        created_at_ms INTEGER NOT NULL,
        completed_at_ms INTEGER
      );
      CREATE TABLE IF NOT EXISTS catalog_snapshot_entries (
        generation INTEGER NOT NULL,
        reservation_day TEXT NOT NULL,
        partition INTEGER NOT NULL,
        partition_count INTEGER NOT NULL,
        partition_config_hash TEXT NOT NULL,
        activation_sequence INTEGER NOT NULL,
        entry_hash TEXT NOT NULL,
        PRIMARY KEY (generation, activation_sequence)
      );
      CREATE INDEX IF NOT EXISTS idx_catalog_snapshot_route
        ON catalog_snapshot_entries (generation, reservation_day, partition);
      CREATE TABLE IF NOT EXISTS catalog_close_operations (
        close_key TEXT PRIMARY KEY,
        cutoff_ms INTEGER NOT NULL,
        snapshot_generation INTEGER NOT NULL,
        snapshot_hash TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('pending', 'complete', 'failed')),
        progress_cursor_sequence INTEGER NOT NULL DEFAULT 0,
        completed_entries INTEGER NOT NULL DEFAULT 0,
        total_entries INTEGER NOT NULL,
        last_error TEXT,
        created_at_ms INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS catalog_close_progress (
        close_key TEXT NOT NULL,
        activation_sequence INTEGER NOT NULL,
        reservation_day TEXT NOT NULL,
        partition INTEGER NOT NULL,
        bucket_close_key TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        exact_receipt_hash TEXT,
        covering_receipt_hash TEXT,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY (close_key, activation_sequence)
      );
      CREATE TABLE IF NOT EXISTS alarm_schedule (
        purpose TEXT PRIMARY KEY,
        fire_at_ms INTEGER NOT NULL,
        generation INTEGER NOT NULL,
        payload_hash TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS manifest_route_assignments (
        assignment_key TEXT PRIMARY KEY,
        tx_id TEXT NOT NULL UNIQUE,
        request_hash TEXT NOT NULL,
        reservation_hash TEXT NOT NULL,
        reservation_json TEXT NOT NULL,
        created_at_ms INTEGER NOT NULL,
        delete_after_ms INTEGER
      );
      CREATE TABLE IF NOT EXISTS catalog_enumeration_cursors (
        cursor_json TEXT PRIMARY KEY,
        evidence_json TEXT NOT NULL,
        created_at_ms INTEGER NOT NULL,
        expires_at_ms INTEGER NOT NULL
      );
    `);
    const closeColumns = this.storage.sql.exec<{ readonly [key: string]: SqlStorageValue; name: string }>(
      "PRAGMA table_info(catalog_close_operations)",
    ).toArray();
    if (!closeColumns.some((column) => column.name === "fleet_root_hash")) {
      this.storage.sql.exec("ALTER TABLE catalog_close_operations ADD COLUMN fleet_root_hash TEXT");
    }
    const metadataColumns = this.storage.sql.exec<{ readonly [key: string]: SqlStorageValue; name: string }>(
      "PRAGMA table_info(fleet_catalog_metadata)",
    ).toArray();
    if (!metadataColumns.some((column) => column.name === "reservation_required_since_day")) {
      this.storage.sql.exec("ALTER TABLE fleet_catalog_metadata ADD COLUMN reservation_required_since_day TEXT");
    }
    if (!metadataColumns.some((column) => column.name === "reservation_required_since_ms")) {
      this.storage.sql.exec("ALTER TABLE fleet_catalog_metadata ADD COLUMN reservation_required_since_ms INTEGER");
    }
    if (!metadataColumns.some((column) => column.name === "legacy_grid_materialized_through_day")) {
      this.storage.sql.exec("ALTER TABLE fleet_catalog_metadata ADD COLUMN legacy_grid_materialized_through_day TEXT");
    }
    if (!metadataColumns.some((column) => column.name === "legacy_admitted_through_ms")) {
      this.storage.sql.exec("ALTER TABLE fleet_catalog_metadata ADD COLUMN legacy_admitted_through_ms INTEGER NOT NULL DEFAULT -1");
    }
    const cursorColumns = this.storage.sql.exec<{ readonly [key: string]: SqlStorageValue; name: string }>(
      "PRAGMA table_info(catalog_enumeration_cursors)",
    ).toArray();
    if (!cursorColumns.some((column) => column.name === "expires_at_ms")) {
      this.storage.sql.exec("ALTER TABLE catalog_enumeration_cursors ADD COLUMN expires_at_ms INTEGER NOT NULL DEFAULT 0");
    }
    const assignmentColumns = this.storage.sql.exec<{ readonly [key: string]: SqlStorageValue; name: string }>(
      "PRAGMA table_info(manifest_route_assignments)",
    ).toArray();
    if (!assignmentColumns.some((column) => column.name === "delete_after_ms")) {
      this.storage.sql.exec("ALTER TABLE manifest_route_assignments ADD COLUMN delete_after_ms INTEGER");
    }
    const activationKeyColumns = this.storage.sql.exec<{ readonly [key: string]: SqlStorageValue; name: string }>(
      "PRAGMA table_info(catalog_activation_keys)",
    ).toArray();
    if (!activationKeyColumns.some((column) => column.name === "created_at_ms")) {
      this.storage.sql.exec("ALTER TABLE catalog_activation_keys ADD COLUMN created_at_ms INTEGER NOT NULL DEFAULT 0");
    }
    this.storage.sql.exec(
      "INSERT OR IGNORE INTO _fleet_catalog_schema_migrations (id, applied_at) VALUES (1, ?)",
      new Date().toISOString(),
    );
  }

  private async ensureFleet(fleetId: string): Promise<MetadataRow> {
    assertText(fleetId, "fleet_id");
    const genesisConfigHash = await hashCanonicalJson({
      effective_from_day: FIRST_EFFECTIVE_DAY,
      partition_count: INITIAL_PARTITION_COUNT,
      prior_hash: ZERO_HASH,
      protocol_version: FLEET_CATALOG_PROTOCOL_VERSION,
    });
    return this.storage.transactionSync(() => {
      const current = this.storage.sql.exec<MetadataRow>("SELECT * FROM fleet_catalog_metadata WHERE id = 1").toArray()[0];
      if (current !== undefined) {
        if (current.fleet_id !== fleetId) throw new TypeError("Catalog Durable Object is already bound to another fleet_id.");
        return current;
      }
      const now = Date.now();
      this.storage.sql.exec(
        `INSERT INTO partition_config_history
          (effective_from_day, protocol_version, partition_count, prior_hash, config_hash, created_at_ms)
         VALUES (?, ?, ?, ?, ?, ?)`,
        FIRST_EFFECTIVE_DAY,
        FLEET_CATALOG_PROTOCOL_VERSION,
        INITIAL_PARTITION_COUNT,
        ZERO_HASH,
        genesisConfigHash,
        now,
      );
      this.storage.sql.exec(
        `INSERT INTO fleet_catalog_metadata
          (id, fleet_id, decision_floor_ms, next_event_sequence, current_snapshot_generation,
           last_snapshot_hash, partition_config_root_hash)
         VALUES (1, ?, 0, 0, 0, ?, ?)`,
        fleetId,
        ZERO_HASH,
        genesisConfigHash,
      );
      return this.storage.sql.exec<MetadataRow>("SELECT * FROM fleet_catalog_metadata WHERE id = 1").one();
    });
  }

  async partitionConfigForDay(fleetId: string, reservationDay: string): Promise<PartitionConfigRow> {
    assertUtcDay(reservationDay, "reservation_day");
    await this.ensureFleet(fleetId);
    return this.storage.sql
      .exec<PartitionConfigRow>(
        `SELECT effective_from_day, protocol_version, partition_count, prior_hash, config_hash
           FROM partition_config_history WHERE effective_from_day <= ?
          ORDER BY effective_from_day DESC LIMIT 1`,
        reservationDay,
      )
      .one();
  }

  async assignManifestRoute(
    draft: Omit<CreateManifestReservationInput, "partition_config_hash">,
    assignmentKey: string,
    nowMs = Date.now(),
  ): Promise<CatalogRouteAssignment> {
    assertText(assignmentKey, "idempotency_key");
    const requestHash = await hashCanonicalJson(draft);
    await this.ensureFleet(draft.fleet_id);
    const existing = this.storage.sql
      .exec<{ readonly [key: string]: SqlStorageValue; request_hash: string; reservation_hash: string; reservation_json: string }>(
        "SELECT request_hash, reservation_hash, reservation_json FROM manifest_route_assignments WHERE assignment_key = ?",
        assignmentKey,
      )
      .toArray()[0];
    if (existing !== undefined) {
      if (existing.request_hash !== requestHash) throw new TypeError("idempotency_key was already used with different route-assignment content.");
      return {
        status: "already_assigned",
        reservation: JSON.parse(existing.reservation_json) as ManifestReservationV1,
        reservation_hash: existing.reservation_hash,
      };
    }

    const reservedAt = new Date(nowMs).toISOString();
    const config = await this.partitionConfigForDay(draft.fleet_id, reservedAt.slice(0, 10));
    const reservation = await createManifestReservation(
      { ...draft, partition_config_hash: config.config_hash },
      reservedAt,
    );
    const reservationHash = await hashManifestReservation(reservation);
    return this.storage.transactionSync(() => {
      const raced = this.storage.sql
        .exec<{ readonly [key: string]: SqlStorageValue; request_hash: string; reservation_hash: string; reservation_json: string }>(
          `SELECT request_hash, reservation_hash, reservation_json
             FROM manifest_route_assignments WHERE assignment_key = ? OR tx_id = ?`,
          assignmentKey,
          draft.tx_id,
        )
        .toArray()[0];
      if (raced !== undefined) {
        if (raced.request_hash !== requestHash) throw new TypeError("Transaction route assignment conflicts with immutable content.");
        return {
          status: "already_assigned" as const,
          reservation: JSON.parse(raced.reservation_json) as ManifestReservationV1,
          reservation_hash: raced.reservation_hash,
        };
      }
      this.storage.sql.exec(
        `INSERT INTO manifest_route_assignments
          (assignment_key, tx_id, request_hash, reservation_hash, reservation_json, created_at_ms, delete_after_ms)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        assignmentKey,
        draft.tx_id,
        requestHash,
        reservationHash,
        JSON.stringify(reservation),
        nowMs,
        nowMs + MANIFEST_ROUTE_RECOVERY_MS,
      );
      const metadata = this.storage.sql.exec<MetadataRow>("SELECT * FROM fleet_catalog_metadata WHERE id = 1").one();
      const boundaryMs = Math.max(Date.now(), metadata.legacy_admitted_through_ms + 1);
      this.storage.sql.exec(
        `UPDATE fleet_catalog_metadata
            SET reservation_required_since_day = COALESCE(reservation_required_since_day, ?),
                reservation_required_since_ms = COALESCE(reservation_required_since_ms, ?)
          WHERE id = 1`,
        new Date(boundaryMs).toISOString().slice(0, 10),
        boundaryMs,
      );
      return { status: "assigned" as const, reservation, reservation_hash: reservationHash };
    });
  }

  releaseManifestRoute(fleetId: string, txId: string, reservationHash: string, deleteAfterMs: number): void {
    assertText(fleetId, "fleet_id");
    assertText(txId, "tx_id");
    assertHash(reservationHash, "reservation_hash");
    if (!Number.isSafeInteger(deleteAfterMs) || deleteAfterMs < 0) throw new TypeError("delete_after_ms must be a non-negative safe integer.");
    const metadata = this.storage.sql.exec<MetadataRow>("SELECT * FROM fleet_catalog_metadata WHERE id = 1").toArray()[0];
    if (metadata === undefined || metadata.fleet_id !== fleetId) throw new TypeError("Route release does not identify this fleet catalog.");
    const assignment = this.storage.sql
      .exec<{ reservation_hash: string }>(
        "SELECT reservation_hash FROM manifest_route_assignments WHERE tx_id = ?",
        txId,
      )
      .toArray()[0];
    if (assignment === undefined) return;
    if (assignment.reservation_hash !== reservationHash) throw new TypeError("Route release conflicts with the frozen reservation.");
    this.storage.sql.exec(
      `UPDATE manifest_route_assignments
          SET delete_after_ms = CASE WHEN delete_after_ms IS NULL THEN ? ELSE MIN(delete_after_ms, ?) END
        WHERE tx_id = ? AND reservation_hash = ?`,
      deleteAfterMs,
      deleteAfterMs,
      txId,
      reservationHash,
    );
  }

  purgeReleasedRoutes(nowMs = Date.now(), limit = 128): number | null {
    this.storage.sql.exec(
      `DELETE FROM manifest_route_assignments WHERE assignment_key IN (
         SELECT assignment_key FROM manifest_route_assignments
          WHERE delete_after_ms IS NOT NULL AND delete_after_ms <= ?
          ORDER BY delete_after_ms LIMIT ?
       )`,
      nowMs,
      limit,
    );
    return this.storage.sql
      .exec<{ delete_after_ms: number | null }>(
        "SELECT MIN(delete_after_ms) AS delete_after_ms FROM manifest_route_assignments WHERE delete_after_ms IS NOT NULL",
      )
      .one().delete_after_ms;
  }

  async coverageState(fleetId: string): Promise<{
    readonly reservation_required_since_day: string | null;
    readonly reservation_required_since_ms: number | null;
    readonly legacy_scanned_through_day: string | null;
    readonly legacy_grid_materialized_through_day: string | null;
    readonly current_snapshot_generation: number;
  }> {
    await this.ensureFleet(fleetId);
    return this.storage.sql
      .exec<MetadataRow>("SELECT * FROM fleet_catalog_metadata WHERE id = 1")
      .one();
  }

  async advanceLegacyGridMaterializedThrough(fleetId: string, day: string): Promise<string> {
    assertUtcDay(day, "legacy_grid_materialized_through_day");
    await this.ensureFleet(fleetId);
    return this.storage.transactionSync(() => {
      const metadata = this.storage.sql.exec<MetadataRow>("SELECT * FROM fleet_catalog_metadata WHERE id = 1").one();
      if (metadata.reservation_required_since_day === null) throw new TypeError("Reservation coverage boundary is not initialized.");
      if (
        metadata.legacy_grid_materialized_through_day !== null
        && day <= metadata.legacy_grid_materialized_through_day
      ) {
        return metadata.legacy_grid_materialized_through_day;
      }
      const expected = metadata.legacy_grid_materialized_through_day === null
        ? metadata.reservation_required_since_day
        : addUtcDays(metadata.legacy_grid_materialized_through_day, 1);
      if (day !== expected) throw new TypeError("Legacy grid cannot advance across an unmaterialized day.");
      const config = this.storage.sql
        .exec<PartitionConfigRow>(
          `SELECT effective_from_day, protocol_version, partition_count, prior_hash, config_hash
             FROM partition_config_history WHERE effective_from_day <= ?
            ORDER BY effective_from_day DESC LIMIT 1`,
          day,
        )
        .one();
      const active = this.storage.sql
        .exec<{ count: number }>(
          `SELECT COUNT(*) AS count FROM active_buckets
            WHERE reservation_day = ? AND retired_sequence IS NULL
              AND partition_count = ? AND partition_config_hash = ?`,
          day,
          config.partition_count,
          config.config_hash,
        )
        .one().count;
      if (active !== config.partition_count) throw new TypeError("Legacy grid day is missing partition buckets.");
      this.storage.sql.exec(
        "UPDATE fleet_catalog_metadata SET legacy_grid_materialized_through_day = ? WHERE id = 1",
        day,
      );
      return day;
    });
  }

  async advanceLegacyScannedThrough(fleetId: string, day: string, closeKey: string): Promise<string> {
    assertUtcDay(day, "legacy_scanned_through_day");
    assertHash(closeKey, "close_key");
    await this.ensureFleet(fleetId);
    return this.storage.transactionSync(() => {
      const metadata = this.storage.sql.exec<MetadataRow>("SELECT * FROM fleet_catalog_metadata WHERE id = 1").one();
      if (metadata.reservation_required_since_day === null) throw new TypeError("Reservation coverage boundary is not initialized.");
      if (metadata.legacy_scanned_through_day !== null && day <= metadata.legacy_scanned_through_day) {
        return metadata.legacy_scanned_through_day;
      }
      const expected = metadata.legacy_scanned_through_day === null
        ? metadata.reservation_required_since_day
        : addUtcDays(metadata.legacy_scanned_through_day, 1);
      if (day !== expected) throw new TypeError("Legacy scan horizon cannot advance across an uncertified day.");
      const config = this.storage.sql
        .exec<PartitionConfigRow>(
          `SELECT effective_from_day, protocol_version, partition_count, prior_hash, config_hash
             FROM partition_config_history WHERE effective_from_day <= ?
            ORDER BY effective_from_day DESC LIMIT 1`,
          day,
        )
        .one();
      const close = this.storage.sql
        .exec<CatalogCloseOperation & { readonly [key: string]: SqlStorageValue }>(
          "SELECT * FROM catalog_close_operations WHERE close_key = ?",
          closeKey,
        )
        .one();
      if (
        close.status !== "complete"
        || close.fleet_root_hash === null
        || new Date(close.cutoff_ms).toISOString().slice(0, 10) < day
      ) {
        throw new TypeError("Legacy scan horizon requires a completed close covering the day.");
      }
      const certified = this.storage.sql
        .exec<{ count: number }>(
          `SELECT COUNT(*) AS count
             FROM catalog_close_progress AS p
             JOIN catalog_close_operations AS o ON o.close_key = p.close_key
             JOIN catalog_snapshot_entries AS s
               ON s.generation = o.snapshot_generation
              AND s.activation_sequence = p.activation_sequence
            WHERE p.close_key = ? AND p.reservation_day = ?
              AND p.status = 'complete' AND p.exact_receipt_hash IS NOT NULL
              AND s.partition_count = ? AND s.partition_config_hash = ?`,
          closeKey,
          day,
          config.partition_count,
          config.config_hash,
        )
        .one().count;
      if (certified !== config.partition_count) {
        throw new TypeError("Legacy scan day is missing exact bucket-close evidence.");
      }
      this.storage.sql.exec(
        "UPDATE fleet_catalog_metadata SET legacy_scanned_through_day = ? WHERE id = 1",
        day,
      );
      return day;
    });
  }

  async appendPartitionConfig(input: {
    readonly fleet_id: string;
    readonly effective_from_day: string;
    readonly partition_count: number;
  }, nowMs = Date.now()): Promise<PartitionConfigRow> {
    assertUtcDay(input.effective_from_day, "effective_from_day");
    if (input.partition_count !== INITIAL_PARTITION_COUNT) {
      throw new TypeError(`partition_count must remain ${INITIAL_PARTITION_COUNT} until versioned routing supports resharding.`);
    }
    const metadata = await this.ensureFleet(input.fleet_id);
    if (input.effective_from_day <= addUtcDays(todayUtc(nowMs), 1)) {
      throw new TypeError("effective_from_day must be later than the next UTC day.");
    }
    const configHash = await hashCanonicalJson({
      effective_from_day: input.effective_from_day,
      partition_count: input.partition_count,
      prior_hash: metadata.partition_config_root_hash,
      protocol_version: FLEET_CATALOG_PROTOCOL_VERSION,
    });
    return this.storage.transactionSync(() => {
      const latest = this.storage.sql
        .exec<PartitionConfigRow>(
          `SELECT effective_from_day, protocol_version, partition_count, prior_hash, config_hash
             FROM partition_config_history ORDER BY effective_from_day DESC LIMIT 1`,
        )
        .one();
      const existing = this.storage.sql
        .exec<PartitionConfigRow>(
          `SELECT effective_from_day, protocol_version, partition_count, prior_hash, config_hash
             FROM partition_config_history WHERE effective_from_day = ?`,
          input.effective_from_day,
        )
        .toArray()[0];
      if (existing !== undefined) {
        if (existing.partition_count !== input.partition_count) throw new TypeError("Partition configuration conflicts at effective_from_day.");
        return existing;
      }
      if (latest.config_hash !== metadata.partition_config_root_hash || input.effective_from_day <= latest.effective_from_day) {
        throw new TypeError("Partition configuration history changed; retry against the latest root.");
      }
      this.storage.sql.exec(
        `INSERT INTO partition_config_history
          (effective_from_day, protocol_version, partition_count, prior_hash, config_hash, created_at_ms)
         VALUES (?, ?, ?, ?, ?, ?)`,
        input.effective_from_day,
        FLEET_CATALOG_PROTOCOL_VERSION,
        input.partition_count,
        latest.config_hash,
        configHash,
        nowMs,
      );
      this.storage.sql.exec("UPDATE fleet_catalog_metadata SET partition_config_root_hash = ? WHERE id = 1", configHash);
      return { ...latest, effective_from_day: input.effective_from_day, partition_count: input.partition_count, prior_hash: latest.config_hash, config_hash: configHash };
    });
  }

  async activateBucket(input: CatalogActivationRequest, nowMs = Date.now()): Promise<CatalogActivationResult> {
    if (input.protocol_version !== FLEET_CATALOG_PROTOCOL_VERSION) throw new TypeError("Unsupported catalog protocol_version.");
    assertUtcDay(input.reservation_day, "reservation_day");
    assertText(input.activation_key, "activation_key");
    assertHash(input.partition_config_hash, "partition_config_hash");
    if (!Number.isInteger(input.partition) || input.partition < 0) throw new TypeError("partition must be a non-negative integer.");
    const config = await this.partitionConfigForDay(input.fleet_id, input.reservation_day);
    if (
      input.partition_config_hash !== config.config_hash ||
      input.partition_count !== config.partition_count ||
      input.partition >= config.partition_count
    ) {
      throw new TypeError("Activation does not match the pinned partition configuration.");
    }
    const entryHash = await hashCanonicalJson({
      fleet_id: input.fleet_id,
      partition: input.partition,
      partition_config_hash: input.partition_config_hash,
      partition_count: input.partition_count,
      reservation_day: input.reservation_day,
    });

    return this.storage.transactionSync(() => {
      const metadata = this.storage.sql.exec<MetadataRow>("SELECT * FROM fleet_catalog_metadata WHERE id = 1").one();
      const replay = this.storage.sql
        .exec<{ readonly [key: string]: SqlStorageValue; reservation_day: string; partition: number; entry_hash: string }>(
          "SELECT reservation_day, partition, entry_hash FROM catalog_activation_keys WHERE activation_key = ?",
          input.activation_key,
        )
        .toArray()[0];
      if (replay !== undefined) {
        if (replay.reservation_day !== input.reservation_day || replay.partition !== input.partition || replay.entry_hash !== entryHash) {
          throw new TypeError("activation_key was already used for different immutable content.");
        }
        const row = this.storage.sql
          .exec<ActiveBucketRow & { retired_sequence: number | null }>("SELECT * FROM active_buckets WHERE reservation_day = ? AND partition = ?", input.reservation_day, input.partition)
          .one();
        if (row.retired_sequence !== null) {
          const sequence = metadata.next_event_sequence + 1;
          this.storage.sql.exec("UPDATE fleet_catalog_metadata SET next_event_sequence = ? WHERE id = 1", sequence);
          this.storage.sql.exec(
            `UPDATE active_buckets SET activation_sequence = ?, activation_key = ?, activated_at_ms = ?,
               retired_sequence = NULL, retirement_certificate_hash = NULL, retired_at_ms = NULL
             WHERE reservation_day = ? AND partition = ?`,
            sequence,
            input.activation_key,
            nowMs,
            input.reservation_day,
            input.partition,
          );
          this.storage.sql.exec(
            `INSERT INTO catalog_bucket_activation_history
              (activation_sequence, reservation_day, partition, partition_count,
               partition_config_hash, activation_key, entry_hash)
             VALUES (?, ?, ?, ?, ?, ?, ?)`,
            sequence,
            input.reservation_day,
            input.partition,
            input.partition_count,
            input.partition_config_hash,
            input.activation_key,
            entryHash,
          );
          return {
            ok: true,
            status: "reactivated",
            activation_sequence: sequence,
            entry_hash: entryHash,
            required_decision_floor_ms: metadata.decision_floor_ms,
          };
        }
        return {
          ok: true,
          status: "already_active",
          activation_sequence: row.activation_sequence,
          entry_hash: entryHash,
          required_decision_floor_ms: metadata.decision_floor_ms,
        };
      }

      const existing = this.storage.sql
        .exec<ActiveBucketRow & { retired_sequence: number | null }>(
          "SELECT * FROM active_buckets WHERE reservation_day = ? AND partition = ?",
          input.reservation_day,
          input.partition,
        )
        .toArray()[0];
      if (existing !== undefined && existing.entry_hash !== entryHash) throw new TypeError("Bucket route conflicts with immutable activation content.");
      if (existing !== undefined && existing.retired_sequence === null) {
        this.storage.sql.exec(
          "INSERT INTO catalog_activation_keys (activation_key, reservation_day, partition, entry_hash, created_at_ms) VALUES (?, ?, ?, ?, ?)",
          input.activation_key,
          input.reservation_day,
          input.partition,
          entryHash,
          nowMs,
        );
        return {
          ok: true,
          status: "already_active",
          activation_sequence: existing.activation_sequence,
          entry_hash: entryHash,
          required_decision_floor_ms: metadata.decision_floor_ms,
        };
      }

      const sequence = metadata.next_event_sequence + 1;
      this.storage.sql.exec("UPDATE fleet_catalog_metadata SET next_event_sequence = ? WHERE id = 1", sequence);
      if (existing === undefined) {
        this.storage.sql.exec(
          `INSERT INTO active_buckets
            (reservation_day, partition, partition_count, partition_config_hash, activation_sequence,
             activation_key, entry_hash, activated_at_ms)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
          input.reservation_day,
          input.partition,
          input.partition_count,
          input.partition_config_hash,
          sequence,
          input.activation_key,
          entryHash,
          nowMs,
        );
      } else {
        this.storage.sql.exec(
          `UPDATE active_buckets SET activation_sequence = ?, activation_key = ?, activated_at_ms = ?,
             retired_sequence = NULL, retirement_certificate_hash = NULL, retired_at_ms = NULL
           WHERE reservation_day = ? AND partition = ?`,
          sequence,
          input.activation_key,
          nowMs,
          input.reservation_day,
          input.partition,
        );
      }
      this.storage.sql.exec(
        `INSERT INTO catalog_bucket_activation_history
          (activation_sequence, reservation_day, partition, partition_count,
           partition_config_hash, activation_key, entry_hash)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        sequence,
        input.reservation_day,
        input.partition,
        input.partition_count,
        input.partition_config_hash,
        input.activation_key,
        entryHash,
      );
      this.storage.sql.exec(
        "INSERT INTO catalog_activation_keys (activation_key, reservation_day, partition, entry_hash, created_at_ms) VALUES (?, ?, ?, ?, ?)",
        input.activation_key,
        input.reservation_day,
        input.partition,
        entryHash,
        nowMs,
      );
      return {
        ok: true,
        status: existing === undefined ? "activated" : "reactivated",
        activation_sequence: sequence,
        entry_hash: entryHash,
        required_decision_floor_ms: metadata.decision_floor_ms,
      };
    });
  }

  async admitLegacyRegistration(input: {
    readonly fleet_id: string;
    readonly reservation_day: string;
    readonly partition: number;
    readonly partition_count: number;
    readonly partition_config_hash: string;
    readonly record_hash: string;
    readonly commit_decided_at_ms: number;
  }, nowMs = Date.now()): Promise<CatalogLegacyAdmission> {
    assertUtcDay(input.reservation_day, "reservation_day");
    assertHash(input.partition_config_hash, "partition_config_hash");
    assertHash(input.record_hash, "record_hash");
    if (!Number.isSafeInteger(input.commit_decided_at_ms) || input.commit_decided_at_ms < 0) {
      throw new TypeError("commit_decided_at_ms must be a non-negative safe integer.");
    }
    const config = await this.partitionConfigForDay(input.fleet_id, input.reservation_day);
    if (
      input.partition_config_hash !== config.config_hash
      || input.partition_count !== config.partition_count
      || input.partition < 0
      || input.partition >= config.partition_count
    ) throw new TypeError("Legacy admission does not match the effective partition configuration.");
    const activationKey = await hashCanonicalJson([
      "legacy-bucket-activation",
      input.fleet_id,
      input.reservation_day,
      input.partition,
      input.partition_config_hash,
    ]);
    const entryHash = await hashCanonicalJson({
      fleet_id: input.fleet_id,
      partition: input.partition,
      partition_config_hash: input.partition_config_hash,
      partition_count: input.partition_count,
      reservation_day: input.reservation_day,
    });
    return this.storage.transactionSync(() => {
      const metadata = this.storage.sql.exec<MetadataRow>("SELECT * FROM fleet_catalog_metadata WHERE id = 1").one();
      if (metadata.reservation_required_since_day !== null) return { ok: false, status: "v1_closed" };
      const replay = this.storage.sql
        .exec<{ readonly [key: string]: SqlStorageValue; reservation_day: string; partition: number; entry_hash: string }>(
          "SELECT reservation_day, partition, entry_hash FROM catalog_activation_keys WHERE activation_key = ?",
          activationKey,
        )
        .toArray()[0];
      if (replay !== undefined) {
        if (replay.reservation_day !== input.reservation_day || replay.partition !== input.partition || replay.entry_hash !== entryHash) {
          throw new TypeError("Legacy admission token conflicts with immutable content.");
        }
        const active = this.storage.sql
          .exec<ActiveBucketRow>(
            "SELECT * FROM active_buckets WHERE reservation_day = ? AND partition = ?",
            input.reservation_day,
            input.partition,
          )
          .one();
        this.storage.sql.exec(
          "UPDATE fleet_catalog_metadata SET legacy_admitted_through_ms = MAX(legacy_admitted_through_ms, ?) WHERE id = 1",
          input.commit_decided_at_ms,
        );
        return { ok: true, admission_token: activationKey, activation_sequence: active.activation_sequence };
      }
      const existing = this.storage.sql
        .exec<ActiveBucketRow & { retired_sequence: number | null }>(
          "SELECT * FROM active_buckets WHERE reservation_day = ? AND partition = ?",
          input.reservation_day,
          input.partition,
        )
        .toArray()[0];
      let sequence: number;
      if (existing !== undefined && existing.retired_sequence === null) {
        sequence = existing.activation_sequence;
      } else {
        sequence = metadata.next_event_sequence + 1;
        this.storage.sql.exec(
          `INSERT INTO active_buckets
            (reservation_day, partition, partition_count, partition_config_hash,
             activation_sequence, activation_key, entry_hash, activated_at_ms,
             retired_sequence, retirement_certificate_hash, retired_at_ms)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL)
           ON CONFLICT(reservation_day, partition) DO UPDATE SET
             partition_count = excluded.partition_count,
             partition_config_hash = excluded.partition_config_hash,
             activation_sequence = excluded.activation_sequence,
             activation_key = excluded.activation_key,
             entry_hash = excluded.entry_hash,
             activated_at_ms = excluded.activated_at_ms,
             retired_sequence = NULL,
             retirement_certificate_hash = NULL,
             retired_at_ms = NULL`,
          input.reservation_day,
          input.partition,
          input.partition_count,
          input.partition_config_hash,
          sequence,
          activationKey,
          entryHash,
          nowMs,
        );
        this.storage.sql.exec(
          `INSERT INTO catalog_bucket_activation_history
            (activation_sequence, reservation_day, partition, partition_count,
             partition_config_hash, activation_key, entry_hash)
           VALUES (?, ?, ?, ?, ?, ?, ?)`,
          sequence,
          input.reservation_day,
          input.partition,
          input.partition_count,
          input.partition_config_hash,
          activationKey,
          entryHash,
        );
        this.storage.sql.exec(
          "UPDATE fleet_catalog_metadata SET next_event_sequence = ? WHERE id = 1",
          sequence,
        );
      }
      this.storage.sql.exec(
        `INSERT INTO catalog_activation_keys (activation_key, reservation_day, partition, entry_hash, created_at_ms)
         VALUES (?, ?, ?, ?, ?)`,
        activationKey,
        input.reservation_day,
        input.partition,
        entryHash,
        nowMs,
      );
      this.storage.sql.exec(
        "UPDATE fleet_catalog_metadata SET legacy_admitted_through_ms = MAX(legacy_admitted_through_ms, ?) WHERE id = 1",
        input.commit_decided_at_ms,
      );
      return { ok: true, admission_token: activationKey, activation_sequence: sequence };
    });
  }

  async retireBucket(input: {
    readonly fleet_id: string;
    readonly reservation_day: string;
    readonly partition: number;
    readonly retirement_certificate_hash: string;
  }, nowMs = Date.now()): Promise<{ readonly ok: true; readonly status: "retired" | "already_retired"; readonly retirement_sequence: number }> {
    assertUtcDay(input.reservation_day, "reservation_day");
    assertHash(input.retirement_certificate_hash, "retirement_certificate_hash");
    await this.ensureFleet(input.fleet_id);
    return this.storage.transactionSync(() => {
      const metadata = this.storage.sql.exec<MetadataRow>("SELECT * FROM fleet_catalog_metadata WHERE id = 1").one();
      const row = this.storage.sql
        .exec<ActiveBucketRow & { retired_sequence: number | null; retirement_certificate_hash: string | null }>(
          "SELECT * FROM active_buckets WHERE reservation_day = ? AND partition = ?",
          input.reservation_day,
          input.partition,
        )
        .toArray()[0];
      if (row === undefined) throw new TypeError("Cannot retire a bucket that is not active in the catalog.");
      if (row.retired_sequence !== null) {
        if (row.retirement_certificate_hash !== input.retirement_certificate_hash) throw new TypeError("Retirement certificate conflicts with the recorded certificate.");
        return { ok: true, status: "already_retired", retirement_sequence: row.retired_sequence };
      }
      const sequence = metadata.next_event_sequence + 1;
      this.storage.sql.exec("UPDATE fleet_catalog_metadata SET next_event_sequence = ? WHERE id = 1", sequence);
      this.storage.sql.exec(
        `UPDATE active_buckets SET retired_sequence = ?, retirement_certificate_hash = ?, retired_at_ms = ?
          WHERE reservation_day = ? AND partition = ?`,
        sequence,
        input.retirement_certificate_hash,
        nowMs,
        input.reservation_day,
        input.partition,
      );
      this.storage.sql.exec(
        "UPDATE catalog_bucket_activation_history SET deactivation_sequence = ? WHERE activation_sequence = ?",
        sequence,
        row.activation_sequence,
      );
      return { ok: true, status: "retired", retirement_sequence: sequence };
    });
  }

  private snapshotResult(row: SnapshotRow): CatalogSnapshotResult {
    return {
      ok: true,
      status: row.status === "COMPLETE" ? "complete" : "pending",
      generation: row.generation,
      cutoff_ms: row.cutoff_ms,
      decision_floor_ms: row.decision_floor_ms,
      fence_sequence: row.fence_sequence,
      partition_config_root_hash: row.partition_config_root_hash,
      prior_snapshot_hash: row.prior_snapshot_hash,
      entry_count: row.entry_count,
      snapshot_hash: row.snapshot_hash,
    };
  }

  async snapshotThrough(input: CatalogSnapshotRequest, nowMs = Date.now()): Promise<CatalogSnapshotResult> {
    if (input.protocol_version !== FLEET_CATALOG_PROTOCOL_VERSION) throw new TypeError("Unsupported catalog protocol_version.");
    assertText(input.idempotency_key, "idempotency_key");
    if (!Number.isSafeInteger(input.cutoff_ms) || input.cutoff_ms < 0 || input.cutoff_ms > nowMs) {
      throw new TypeError("cutoff_ms must be a non-future integer timestamp.");
    }
    const limit = pageSize(input.page_size);
    await this.ensureFleet(input.fleet_id);

    const generation = this.storage.transactionSync(() => {
      const replay = this.storage.sql
        .exec<SnapshotRow>("SELECT * FROM catalog_snapshots WHERE idempotency_key = ?", input.idempotency_key)
        .toArray()[0];
      if (replay !== undefined) {
        if (replay.cutoff_ms !== input.cutoff_ms) throw new TypeError("idempotency_key conflicts with another cutoff.");
        return replay.generation;
      }
      const building = this.storage.sql
        .exec<{ readonly [key: string]: SqlStorageValue; generation: number }>("SELECT generation FROM catalog_snapshots WHERE status = 'BUILDING' LIMIT 1")
        .toArray()[0];
      if (building !== undefined) throw new TypeError(`Snapshot generation ${building.generation} is still in progress.`);
      const metadata = this.storage.sql.exec<MetadataRow>("SELECT * FROM fleet_catalog_metadata WHERE id = 1").one();
      const nextGeneration = metadata.current_snapshot_generation + 1;
      const floor = Math.max(metadata.decision_floor_ms, input.cutoff_ms);
      this.storage.sql.exec(
        "UPDATE fleet_catalog_metadata SET decision_floor_ms = ?, current_snapshot_generation = ? WHERE id = 1",
        floor,
        nextGeneration,
      );
      this.storage.sql.exec(
        `INSERT INTO catalog_snapshots
          (generation, idempotency_key, cutoff_ms, decision_floor_ms, fence_sequence,
           partition_config_root_hash, prior_snapshot_hash, status, rolling_hash, created_at_ms)
         VALUES (?, ?, ?, ?, ?, ?, ?, 'BUILDING', ?, ?)`,
        nextGeneration,
        input.idempotency_key,
        input.cutoff_ms,
        floor,
        metadata.next_event_sequence,
        metadata.partition_config_root_hash,
        metadata.last_snapshot_hash,
        ZERO_HASH,
        nowMs,
      );
      return nextGeneration;
    });
    return await this.resumeSnapshot(generation, limit, nowMs);
  }

  async resumeSnapshot(generation: number, requestedPageSize = DEFAULT_CATALOG_PAGE_SIZE, nowMs = Date.now()): Promise<CatalogSnapshotResult> {
    const limit = pageSize(requestedPageSize);
    const state = this.storage.transactionSync(() => {
      const snapshot = this.storage.sql.exec<SnapshotRow>("SELECT * FROM catalog_snapshots WHERE generation = ?", generation).one();
      if (snapshot.status === "COMPLETE") return { snapshot, rows: [] as ActiveBucketRow[], hasMore: false };
      const candidates = this.storage.sql
        .exec<ActiveBucketRow>(
          `SELECT reservation_day, partition, partition_count, partition_config_hash,
                  activation_sequence, activation_key, entry_hash
             FROM catalog_bucket_activation_history
            WHERE activation_sequence > ? AND activation_sequence <= ?
              AND (deactivation_sequence IS NULL OR deactivation_sequence > ?)
            ORDER BY activation_sequence LIMIT ?`,
          snapshot.cursor_sequence,
          snapshot.fence_sequence,
          snapshot.fence_sequence,
          limit + 1,
        )
        .toArray();
      return { snapshot, rows: candidates.slice(0, limit), hasMore: candidates.length > limit };
    });
    if (state.snapshot.status === "COMPLETE") return this.snapshotResult(state.snapshot);

    let rollingHash = state.snapshot.rolling_hash;
    for (const row of state.rows) {
      rollingHash = await hashCanonicalJson({
        entry_hash: row.entry_hash,
        kind: "fleet-catalog-snapshot-entry-v1",
        previous_hash: rollingHash,
      });
    }
    const newCount = state.snapshot.entry_count + state.rows.length;
    const finalSnapshotHash = state.hasMore
      ? null
      : await hashCanonicalJson({
          cutoff_ms: state.snapshot.cutoff_ms,
          decision_floor_ms: state.snapshot.decision_floor_ms,
          entry_count: newCount,
          entry_root_hash: rollingHash,
          fence_sequence: state.snapshot.fence_sequence,
          generation: state.snapshot.generation,
          kind: "fleet-catalog-snapshot-v1",
          partition_config_root_hash: state.snapshot.partition_config_root_hash,
          prior_snapshot_hash: state.snapshot.prior_snapshot_hash,
        });

    return this.storage.transactionSync(() => {
      const current = this.storage.sql.exec<SnapshotRow>("SELECT * FROM catalog_snapshots WHERE generation = ?", generation).one();
      if (current.status === "COMPLETE") return this.snapshotResult(current);
      if (current.cursor_sequence !== state.snapshot.cursor_sequence || current.rolling_hash !== state.snapshot.rolling_hash) {
        return this.snapshotResult(current);
      }
      for (const row of state.rows) {
        this.storage.sql.exec(
          `INSERT OR IGNORE INTO catalog_snapshot_entries
            (generation, reservation_day, partition, partition_count, partition_config_hash, activation_sequence, entry_hash)
           VALUES (?, ?, ?, ?, ?, ?, ?)`,
          generation,
          row.reservation_day,
          row.partition,
          row.partition_count,
          row.partition_config_hash,
          row.activation_sequence,
          row.entry_hash,
        );
      }
      const cursor = state.rows.at(-1)?.activation_sequence ?? state.snapshot.cursor_sequence;
      if (state.hasMore) {
        this.storage.sql.exec(
          "UPDATE catalog_snapshots SET cursor_sequence = ?, entry_count = ?, rolling_hash = ? WHERE generation = ?",
          cursor,
          newCount,
          rollingHash,
          generation,
        );
      } else {
        this.storage.sql.exec(
          `UPDATE catalog_snapshots SET status = 'COMPLETE', cursor_sequence = ?, entry_count = ?,
             rolling_hash = ?, snapshot_hash = ?, completed_at_ms = ? WHERE generation = ?`,
          cursor,
          newCount,
          rollingHash,
          finalSnapshotHash,
          nowMs,
          generation,
        );
        this.storage.sql.exec("UPDATE fleet_catalog_metadata SET last_snapshot_hash = ? WHERE id = 1", finalSnapshotHash);
      }
      return this.snapshotResult(this.storage.sql.exec<SnapshotRow>("SELECT * FROM catalog_snapshots WHERE generation = ?", generation).one());
    });
  }

  snapshotEntries(generation: number, afterSequence = 0, requestedPageSize = DEFAULT_CATALOG_PAGE_SIZE): readonly CatalogSnapshotEntry[] {
    const limit = pageSize(requestedPageSize);
    if (!Number.isSafeInteger(generation) || generation < 1 || !Number.isSafeInteger(afterSequence) || afterSequence < 0) {
      throw new TypeError("generation and afterSequence must be non-negative safe integers.");
    }
    return this.storage.sql
      .exec<CatalogSnapshotEntry & { readonly [key: string]: SqlStorageValue }>(
        `SELECT reservation_day, partition, partition_count, partition_config_hash, activation_sequence, entry_hash
           FROM catalog_snapshot_entries WHERE generation = ? AND activation_sequence > ?
          ORDER BY activation_sequence LIMIT ?`,
        generation,
        afterSequence,
        limit,
      )
      .toArray();
  }

  snapshotByGeneration(generation: number): CatalogSnapshotResult {
    if (!Number.isSafeInteger(generation) || generation < 1) throw new TypeError("generation must be a positive safe integer.");
    return this.snapshotResult(
      this.storage.sql.exec<SnapshotRow>("SELECT * FROM catalog_snapshots WHERE generation = ?", generation).one(),
    );
  }

  closeForSnapshot(generation: number): CatalogCloseOperation | null {
    return this.storage.sql
      .exec<CatalogCloseOperation & { readonly [key: string]: SqlStorageValue }>(
        "SELECT * FROM catalog_close_operations WHERE snapshot_generation = ? ORDER BY created_at_ms DESC LIMIT 1",
        generation,
      )
      .toArray()[0] ?? null;
  }

  enumerationEntries(
    generation: number,
    afterDay = "",
    afterPartition = -1,
    requestedPageSize = DEFAULT_CATALOG_PAGE_SIZE,
  ): readonly CatalogEnumerationEntry[] {
    const limit = pageSize(requestedPageSize);
    return this.storage.sql
      .exec<CatalogEnumerationEntry & { readonly [key: string]: SqlStorageValue }>(
        `SELECT s.reservation_day, s.partition, s.partition_count,
                s.partition_config_hash, s.activation_sequence, s.entry_hash,
                p.exact_receipt_hash
           FROM catalog_snapshot_entries AS s
           LEFT JOIN catalog_close_operations AS o
             ON o.snapshot_generation = s.generation AND o.status = 'complete'
           LEFT JOIN catalog_close_progress AS p
             ON p.close_key = o.close_key AND p.activation_sequence = s.activation_sequence
          WHERE s.generation = ?
            AND (s.reservation_day > ? OR (s.reservation_day = ? AND s.partition > ?))
          ORDER BY s.reservation_day, s.partition LIMIT ?`,
        generation,
        afterDay,
        afterDay,
        afterPartition,
        limit,
      )
      .toArray();
  }

  issueEnumerationCursor(cursor: unknown, evidence: unknown, nowMs = Date.now()): number {
    const evidenceLeases = Array.isArray(evidence)
      ? evidence.flatMap((item) => {
          if (typeof item !== "object" || item === null || !("lease_expires_at_ms" in item)) return [];
          const expiry = (item as { lease_expires_at_ms?: unknown }).lease_expires_at_ms;
          return Number.isSafeInteger(expiry) ? [expiry as number] : [];
        })
      : [];
    const expiresAtMs = Math.min(nowMs + CATALOG_CURSOR_MAX_LEASE_MS, ...evidenceLeases);
    this.storage.sql.exec(
      `INSERT INTO catalog_enumeration_cursors (cursor_json, evidence_json, created_at_ms, expires_at_ms)
       VALUES (?, ?, ?, ?)
       ON CONFLICT(cursor_json) DO UPDATE SET
         evidence_json = excluded.evidence_json, expires_at_ms = excluded.expires_at_ms`,
      canonicalJson(cursor),
      canonicalJson(evidence),
      nowMs,
      expiresAtMs,
    );
    return expiresAtMs;
  }

  enumerationCursorEvidence(cursor: unknown, nowMs = Date.now()): unknown[] | null {
    this.purgeEnumerationCursors(nowMs);
    const row = this.storage.sql
      .exec<{ evidence_json: string }>(
        "SELECT evidence_json FROM catalog_enumeration_cursors WHERE cursor_json = ? AND expires_at_ms > ?",
        canonicalJson(cursor),
        nowMs,
      )
      .toArray()[0];
    return row === undefined ? null : JSON.parse(row.evidence_json) as unknown[];
  }

  purgeEnumerationCursors(nowMs = Date.now(), limit = 128): number | null {
    this.storage.sql.exec(
      `DELETE FROM catalog_enumeration_cursors WHERE cursor_json IN (
         SELECT cursor_json FROM catalog_enumeration_cursors
          WHERE expires_at_ms <= ? ORDER BY expires_at_ms LIMIT ?
       )`,
      nowMs,
      limit,
    );
    return this.storage.sql
      .exec<{ expires_at_ms: number | null }>(
        "SELECT MIN(expires_at_ms) AS expires_at_ms FROM catalog_enumeration_cursors",
      )
      .one().expires_at_ms;
  }

  async beginClose(input: {
    readonly fleet_id: string;
    readonly cutoff_ms: number;
    readonly snapshot_generation: number;
  }, nowMs = Date.now()): Promise<CatalogCloseOperation> {
    const metadata = await this.ensureFleet(input.fleet_id);
    const snapshot = this.storage.sql.exec<SnapshotRow>("SELECT * FROM catalog_snapshots WHERE generation = ?", input.snapshot_generation).one();
    if (snapshot.status !== "COMPLETE" || snapshot.cutoff_ms !== input.cutoff_ms || snapshot.snapshot_hash === null) {
      throw new TypeError("Close requires the complete exact-cutoff catalog snapshot.");
    }
    const closeKey = await hashCanonicalJson([
      "catalog-close",
      input.fleet_id,
      input.cutoff_ms,
      FLEET_CATALOG_PROTOCOL_VERSION,
      snapshot.partition_config_root_hash,
    ]);
    return this.storage.transactionSync(() => {
      const existing = this.storage.sql
        .exec<CatalogCloseOperation & { readonly [key: string]: SqlStorageValue }>("SELECT * FROM catalog_close_operations WHERE close_key = ?", closeKey)
        .toArray()[0];
      if (existing !== undefined) return existing;
      if (metadata.fleet_id !== input.fleet_id) throw new TypeError("Close fleet does not match catalog fleet.");
      this.storage.sql.exec(
        `INSERT INTO catalog_close_operations
          (close_key, cutoff_ms, snapshot_generation, snapshot_hash, status,
           progress_cursor_sequence, completed_entries, total_entries, created_at_ms, updated_at_ms)
         VALUES (?, ?, ?, ?, 'pending', 0, 0, ?, ?, ?)`,
        closeKey,
        input.cutoff_ms,
        input.snapshot_generation,
        snapshot.snapshot_hash,
        snapshot.entry_count,
        nowMs,
        nowMs,
      );
      return this.storage.sql
        .exec<CatalogCloseOperation & { readonly [key: string]: SqlStorageValue }>("SELECT * FROM catalog_close_operations WHERE close_key = ?", closeKey)
        .one();
    });
  }

  async materializeCloseProgress(closeKey: string, requestedPageSize = DEFAULT_CATALOG_PAGE_SIZE, nowMs = Date.now()): Promise<CatalogCloseOperation> {
    assertHash(closeKey, "close_key");
    const limit = pageSize(requestedPageSize);
    const operation = this.storage.sql
      .exec<CatalogCloseOperation & { readonly [key: string]: SqlStorageValue }>("SELECT * FROM catalog_close_operations WHERE close_key = ?", closeKey)
      .one();
    if (operation.status !== "pending") return operation;
    const rows = this.snapshotEntries(operation.snapshot_generation, operation.progress_cursor_sequence, limit);
    const closeKeys = await Promise.all(
      rows.map(async (row) => await hashCanonicalJson([
        "bucket-close",
        operation.cutoff_ms,
        operation.snapshot_generation,
        row.reservation_day,
        row.partition,
      ])),
    );
    return this.storage.transactionSync(() => {
      const current = this.storage.sql
        .exec<CatalogCloseOperation & { readonly [key: string]: SqlStorageValue }>("SELECT * FROM catalog_close_operations WHERE close_key = ?", closeKey)
        .one();
      if (current.progress_cursor_sequence !== operation.progress_cursor_sequence) return current;
      rows.forEach((row, index) => {
        this.storage.sql.exec(
          `INSERT OR IGNORE INTO catalog_close_progress
            (close_key, activation_sequence, reservation_day, partition, bucket_close_key, updated_at_ms)
           VALUES (?, ?, ?, ?, ?, ?)`,
          closeKey,
          row.activation_sequence,
          row.reservation_day,
          row.partition,
          closeKeys[index],
          nowMs,
        );
      });
      const cursor = rows.at(-1)?.activation_sequence ?? current.progress_cursor_sequence;
      this.storage.sql.exec(
        "UPDATE catalog_close_operations SET progress_cursor_sequence = ?, updated_at_ms = ? WHERE close_key = ?",
        cursor,
        nowMs,
        closeKey,
      );
      return this.storage.sql
        .exec<CatalogCloseOperation & { readonly [key: string]: SqlStorageValue }>("SELECT * FROM catalog_close_operations WHERE close_key = ?", closeKey)
        .one();
    });
  }

  closeProgress(
    closeKey: string,
    afterSequence = 0,
    requestedPageSize = DEFAULT_CATALOG_PAGE_SIZE,
  ): readonly CatalogCloseProgressEntry[] {
    assertHash(closeKey, "close_key");
    const limit = pageSize(requestedPageSize);
    return this.storage.sql
      .exec<CatalogCloseProgressEntry & { readonly [key: string]: SqlStorageValue }>(
        `SELECT p.activation_sequence, p.reservation_day, p.partition,
                s.partition_count, s.partition_config_hash, p.bucket_close_key,
                p.status, p.exact_receipt_hash, p.covering_receipt_hash
           FROM catalog_close_progress AS p
           JOIN catalog_close_operations AS o ON o.close_key = p.close_key
           JOIN catalog_snapshot_entries AS s
             ON s.generation = o.snapshot_generation
            AND s.activation_sequence = p.activation_sequence
          WHERE p.close_key = ? AND p.activation_sequence > ? AND p.status = 'pending'
          ORDER BY p.activation_sequence LIMIT ?`,
        closeKey,
        afterSequence,
        limit,
      )
      .toArray();
  }

  recordCloseReceipt(input: {
    readonly close_key: string;
    readonly activation_sequence: number;
    readonly exact_receipt_hash: string;
    readonly covering_receipt_hash?: string | null;
  }, nowMs = Date.now()): CatalogCloseOperation {
    assertHash(input.close_key, "close_key");
    assertHash(input.exact_receipt_hash, "exact_receipt_hash");
    if (input.covering_receipt_hash !== undefined && input.covering_receipt_hash !== null) {
      assertHash(input.covering_receipt_hash, "covering_receipt_hash");
    }
    return this.storage.transactionSync(() => {
      const progress = this.storage.sql
        .exec<CatalogCloseProgressEntry & { readonly [key: string]: SqlStorageValue }>(
          "SELECT * FROM catalog_close_progress WHERE close_key = ? AND activation_sequence = ?",
          input.close_key,
          input.activation_sequence,
        )
        .one();
      if (progress.status === "complete" && progress.exact_receipt_hash !== input.exact_receipt_hash) {
        throw new TypeError("Close progress already contains different exact-cutoff evidence.");
      }
      if (progress.status !== "complete") {
        this.storage.sql.exec(
          `UPDATE catalog_close_progress
              SET status = 'complete', exact_receipt_hash = ?, covering_receipt_hash = ?, updated_at_ms = ?
            WHERE close_key = ? AND activation_sequence = ?`,
          input.exact_receipt_hash,
          input.covering_receipt_hash ?? null,
          nowMs,
          input.close_key,
          input.activation_sequence,
        );
        this.storage.sql.exec(
          `UPDATE catalog_close_operations
              SET completed_entries = completed_entries + 1, updated_at_ms = ?
            WHERE close_key = ?`,
          nowMs,
          input.close_key,
        );
      }
      return this.storage.sql
        .exec<CatalogCloseOperation & { readonly [key: string]: SqlStorageValue }>(
          "SELECT * FROM catalog_close_operations WHERE close_key = ?",
          input.close_key,
        )
        .one();
    });
  }

  async finalizeClose(closeKey: string, nowMs = Date.now()): Promise<CatalogCloseOperation> {
    assertHash(closeKey, "close_key");
    const operation = this.storage.sql
      .exec<CatalogCloseOperation & { readonly [key: string]: SqlStorageValue }>(
        "SELECT * FROM catalog_close_operations WHERE close_key = ?",
        closeKey,
      )
      .one();
    if (operation.status === "complete") return operation;
    const receipts = this.storage.sql
      .exec<{
        readonly [key: string]: SqlStorageValue;
        activation_sequence: number;
        exact_receipt_hash: string | null;
      }>(
        `SELECT activation_sequence, exact_receipt_hash FROM catalog_close_progress
          WHERE close_key = ? ORDER BY activation_sequence`,
        closeKey,
      )
      .toArray();
    if (receipts.length !== operation.total_entries || receipts.some((entry) => entry.exact_receipt_hash === null)) {
      return operation;
    }
    const fleetRootHash = await hashCanonicalJson({
      close_key: closeKey,
      cutoff_ms: operation.cutoff_ms,
      snapshot_generation: operation.snapshot_generation,
      snapshot_hash: operation.snapshot_hash,
      receipts,
    });
    return this.storage.transactionSync(() => {
      const current = this.storage.sql
        .exec<CatalogCloseOperation & { readonly [key: string]: SqlStorageValue }>(
          "SELECT * FROM catalog_close_operations WHERE close_key = ?",
          closeKey,
        )
        .one();
      const completed = this.storage.sql
        .exec<{ count: number }>(
          "SELECT COUNT(*) AS count FROM catalog_close_progress WHERE close_key = ? AND status = 'complete'",
          closeKey,
        )
        .one().count;
      if (current.status !== "pending" || completed !== current.total_entries) return current;
      this.storage.sql.exec(
        `UPDATE catalog_close_operations
            SET status = 'complete', fleet_root_hash = ?, updated_at_ms = ? WHERE close_key = ?`,
        fleetRootHash,
        nowMs,
        closeKey,
      );
      return this.storage.sql
        .exec<CatalogCloseOperation & { readonly [key: string]: SqlStorageValue }>(
          "SELECT * FROM catalog_close_operations WHERE close_key = ?",
          closeKey,
        )
        .one();
    });
  }

  purgeHistory(nowMs = Date.now(), limit = 8): { readonly deleted: number; readonly next_deadline_ms: number | null } {
    if (!Number.isInteger(limit) || limit < 1 || limit > 128) throw new TypeError("Invalid catalog history purge limit.");
    const cutoffMs = nowMs - CATALOG_HISTORY_RETENTION_MS;
    const deleted = this.storage.transactionSync(() => {
      let count = 0;
      const closeKeys = this.storage.sql
        .exec<{ close_key: string }>(
          `SELECT close_key FROM catalog_close_operations
            WHERE updated_at_ms <= ? ORDER BY updated_at_ms, close_key LIMIT ?`,
          cutoffMs,
          limit,
        )
        .toArray();
      for (const { close_key: closeKey } of closeKeys) {
        this.storage.sql.exec("DELETE FROM catalog_close_progress WHERE close_key = ?", closeKey);
        this.storage.sql.exec("DELETE FROM catalog_close_operations WHERE close_key = ?", closeKey);
        count += 1;
      }

      const metadata = this.storage.sql.exec<MetadataRow>("SELECT * FROM fleet_catalog_metadata WHERE id = 1").one();
      const generations = this.storage.sql
        .exec<{ generation: number }>(
          `SELECT generation FROM catalog_snapshots AS s
            WHERE s.generation < ?
              AND COALESCE(s.completed_at_ms, s.created_at_ms) <= ?
              AND NOT EXISTS (
                SELECT 1 FROM catalog_close_operations AS o WHERE o.snapshot_generation = s.generation
              )
            ORDER BY generation LIMIT ?`,
          metadata.current_snapshot_generation,
          cutoffMs,
          limit,
        )
        .toArray();
      for (const { generation } of generations) {
        this.storage.sql.exec("DELETE FROM catalog_snapshot_entries WHERE generation = ?", generation);
        this.storage.sql.exec("DELETE FROM catalog_snapshots WHERE generation = ?", generation);
        count += 1;
      }

      const staleKeys = this.storage.sql
        .exec<{ activation_key: string }>(
          "SELECT activation_key FROM catalog_activation_keys WHERE created_at_ms <= ? ORDER BY created_at_ms LIMIT ?",
          cutoffMs,
          limit * MAX_CATALOG_PAGE_SIZE,
        )
        .toArray();
      for (const { activation_key: activationKey } of staleKeys) {
        this.storage.sql.exec("DELETE FROM catalog_activation_keys WHERE activation_key = ?", activationKey);
      }
      count += staleKeys.length;

      const staleHistory = this.storage.sql
        .exec<{ activation_sequence: number }>(
          `SELECT h.activation_sequence FROM catalog_bucket_activation_history AS h
            WHERE h.deactivation_sequence IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM catalog_snapshot_entries AS s
                 WHERE s.activation_sequence = h.activation_sequence
              )
            ORDER BY h.activation_sequence LIMIT ?`,
          limit * MAX_CATALOG_PAGE_SIZE,
        )
        .toArray();
      for (const { activation_sequence: activationSequence } of staleHistory) {
        this.storage.sql.exec("DELETE FROM catalog_bucket_activation_history WHERE activation_sequence = ?", activationSequence);
      }
      count += staleHistory.length;
      return count;
    });

    const deadlines = [
      this.storage.sql.exec<{ deadline: number | null }>(
        "SELECT MIN(updated_at_ms + ?) AS deadline FROM catalog_close_operations",
        CATALOG_HISTORY_RETENTION_MS,
      ).one().deadline,
      this.storage.sql.exec<{ deadline: number | null }>(
        `SELECT MIN(COALESCE(completed_at_ms, created_at_ms) + ?) AS deadline
           FROM catalog_snapshots
          WHERE generation < (SELECT current_snapshot_generation FROM fleet_catalog_metadata WHERE id = 1)`,
        CATALOG_HISTORY_RETENTION_MS,
      ).one().deadline,
      this.storage.sql.exec<{ deadline: number | null }>(
        "SELECT MIN(created_at_ms + ?) AS deadline FROM catalog_activation_keys",
        CATALOG_HISTORY_RETENTION_MS,
      ).one().deadline,
    ].filter((deadline): deadline is number => deadline !== null);
    const nextDeadline = deadlines.length === 0 ? null : Math.min(...deadlines);
    return { deleted, next_deadline_ms: nextDeadline === null ? null : Math.max(nowMs, nextDeadline) };
  }

  schedulePurpose(purpose: CatalogAlarmPurpose): void {
    assertText(purpose.purpose, "purpose");
    assertHash(purpose.payload_hash, "payload_hash");
    if (!Number.isSafeInteger(purpose.fire_at_ms) || purpose.fire_at_ms < 0 || !Number.isSafeInteger(purpose.generation) || purpose.generation < 0) {
      throw new TypeError("Alarm time and generation must be non-negative safe integers.");
    }
    this.storage.transactionSync(() => {
      const existing = this.storage.sql.exec<AlarmRow>("SELECT * FROM alarm_schedule WHERE purpose = ?", purpose.purpose).toArray()[0];
      if (existing !== undefined && existing.generation > purpose.generation) return;
      this.storage.sql.exec(
        `INSERT INTO alarm_schedule (purpose, fire_at_ms, generation, payload_hash) VALUES (?, ?, ?, ?)
         ON CONFLICT(purpose) DO UPDATE SET fire_at_ms = excluded.fire_at_ms,
           generation = excluded.generation, payload_hash = excluded.payload_hash`,
        purpose.purpose,
        purpose.fire_at_ms,
        purpose.generation,
        purpose.payload_hash,
      );
    });
  }

  nextAlarmAt(): number | null {
    return this.storage.sql.exec<{ readonly [key: string]: SqlStorageValue; fire_at_ms: number | null }>("SELECT MIN(fire_at_ms) AS fire_at_ms FROM alarm_schedule").one().fire_at_ms;
  }

  duePurposes(nowMs: number, limit = CATALOG_ALARM_BATCH_SIZE): readonly CatalogAlarmPurpose[] {
    if (!Number.isInteger(limit) || limit < 1 || limit > CATALOG_ALARM_BATCH_SIZE) throw new TypeError("Invalid alarm batch size.");
    return this.storage.sql
      .exec<AlarmRow>("SELECT purpose, fire_at_ms, generation, payload_hash FROM alarm_schedule WHERE fire_at_ms <= ? ORDER BY fire_at_ms, purpose LIMIT ?", nowMs, limit)
      .toArray();
  }

  completePurpose(purpose: string, generation: number): void {
    this.storage.sql.exec("DELETE FROM alarm_schedule WHERE purpose = ? AND generation = ?", purpose, generation);
  }
}

export class FleetManifestCatalogDO extends DurableObject<FleetManifestCatalogEnv> {
  private readonly catalog: FleetManifestCatalogStore;

  constructor(ctx: DurableObjectState, env: FleetManifestCatalogEnv) {
    super(ctx, env);
    this.catalog = new FleetManifestCatalogStore(this.ctx.storage);
    void this.ctx.blockConcurrencyWhile(async () => this.catalog.migrate());
  }

  private async armNextAlarm(): Promise<void> {
    const next = this.catalog.nextAlarmAt();
    const current = await this.ctx.storage.getAlarm();
    if (next === null) {
      if (current !== null) await this.ctx.storage.deleteAlarm();
    } else if (current === null || current !== next) {
      await this.ctx.storage.setAlarm(next);
    }
  }

  private async refreshHistoryGc(): Promise<void> {
    const history = this.catalog.purgeHistory(Date.now());
    this.catalog.completePurpose("history_gc", 0);
    if (history.next_deadline_ms !== null) {
      this.catalog.schedulePurpose({
        purpose: "history_gc",
        fire_at_ms: history.next_deadline_ms,
        generation: 0,
        payload_hash: await hashCanonicalJson(["history_gc", history.next_deadline_ms]),
      });
    }
    await this.armNextAlarm();
  }

  async partitionConfigForDay(fleetId: string, reservationDay: string): Promise<PartitionConfigRow> {
    return await this.catalog.partitionConfigForDay(fleetId, reservationDay);
  }

  async assignManifestRoute(
    draft: Omit<CreateManifestReservationInput, "partition_config_hash">,
    assignmentKey: string,
  ): Promise<CatalogRouteAssignment> {
    const result = await this.catalog.assignManifestRoute(draft, assignmentKey);
    const nextRelease = this.catalog.purgeReleasedRoutes(Date.now());
    if (nextRelease !== null) {
      this.catalog.schedulePurpose({
        purpose: "route_gc",
        fire_at_ms: Math.max(Date.now(), nextRelease),
        generation: 0,
        payload_hash: await hashCanonicalJson(["route_gc", nextRelease]),
      });
      await this.armNextAlarm();
    }
    return result;
  }

  async releaseManifestRoute(fleetId: string, txId: string, reservationHash: string, deleteAfterMs: number): Promise<void> {
    const existing = this.catalog.purgeReleasedRoutes();
    const next = Math.min(existing ?? Number.POSITIVE_INFINITY, deleteAfterMs);
    this.catalog.schedulePurpose({
      purpose: "route_gc",
      fire_at_ms: next,
      generation: 0,
      payload_hash: await hashCanonicalJson(["route_gc", next]),
    });
    await this.armNextAlarm();
    this.catalog.releaseManifestRoute(fleetId, txId, reservationHash, deleteAfterMs);
  }

  async coverageState(fleetId: string): Promise<Awaited<ReturnType<FleetManifestCatalogStore["coverageState"]>>> {
    return await this.catalog.coverageState(fleetId);
  }

  async advanceLegacyGridMaterializedThrough(fleetId: string, day: string): Promise<string> {
    return await this.catalog.advanceLegacyGridMaterializedThrough(fleetId, day);
  }

  async advanceLegacyScannedThrough(fleetId: string, day: string, closeKey: string): Promise<string> {
    return await this.catalog.advanceLegacyScannedThrough(fleetId, day, closeKey);
  }

  async appendPartitionConfig(input: Parameters<FleetManifestCatalogStore["appendPartitionConfig"]>[0]): Promise<PartitionConfigRow> {
    return await this.catalog.appendPartitionConfig(input);
  }

  async activateBucket(input: CatalogActivationRequest): Promise<CatalogActivationResult> {
    return await this.catalog.activateBucket(input);
  }

  async admitLegacyRegistration(
    input: Parameters<FleetManifestCatalogStore["admitLegacyRegistration"]>[0],
  ): Promise<CatalogLegacyAdmission> {
    return await this.catalog.admitLegacyRegistration(input);
  }

  async retireBucket(input: Parameters<FleetManifestCatalogStore["retireBucket"]>[0]): Promise<Awaited<ReturnType<FleetManifestCatalogStore["retireBucket"]>>> {
    return await this.catalog.retireBucket(input);
  }

  async snapshotThrough(input: CatalogSnapshotRequest): Promise<CatalogSnapshotResult> {
    const result = await this.catalog.snapshotThrough(input);
    if (result.status === "pending") {
      this.catalog.schedulePurpose({
        purpose: "snapshot_resume",
        fire_at_ms: Date.now(),
        generation: result.generation,
        payload_hash: await hashCanonicalJson(["snapshot_resume", result.generation]),
      });
      await this.armNextAlarm();
    }
    await this.refreshHistoryGc();
    return result;
  }

  async snapshotEntries(generation: number, afterSequence?: number, requestedPageSize?: number): Promise<readonly CatalogSnapshotEntry[]> {
    return this.catalog.snapshotEntries(generation, afterSequence, requestedPageSize);
  }

  async snapshotByGeneration(generation: number): Promise<CatalogSnapshotResult> {
    return this.catalog.snapshotByGeneration(generation);
  }

  async closeForSnapshot(generation: number): Promise<CatalogCloseOperation | null> {
    return this.catalog.closeForSnapshot(generation);
  }

  async enumerationEntries(
    generation: number,
    afterDay?: string,
    afterPartition?: number,
    requestedPageSize?: number,
  ): Promise<readonly CatalogEnumerationEntry[]> {
    return this.catalog.enumerationEntries(generation, afterDay, afterPartition, requestedPageSize);
  }

  async issueEnumerationCursor(cursor: unknown, evidence: unknown): Promise<void> {
    const now = Date.now();
    const existingExpiry = this.catalog.purgeEnumerationCursors(now);
    const prearmedExpiry = Math.min(existingExpiry ?? Number.POSITIVE_INFINITY, now + CATALOG_CURSOR_MAX_LEASE_MS);
    this.catalog.schedulePurpose({
      purpose: "cursor_gc",
      fire_at_ms: prearmedExpiry,
      generation: 0,
      payload_hash: await hashCanonicalJson(["cursor_gc", prearmedExpiry]),
    });
    await this.armNextAlarm();
    this.catalog.issueEnumerationCursor(cursor, evidence, now);
    const nextExpiry = this.catalog.purgeEnumerationCursors(now);
    if (nextExpiry !== null) {
      this.catalog.schedulePurpose({
        purpose: "cursor_gc",
        fire_at_ms: nextExpiry,
        generation: 0,
        payload_hash: await hashCanonicalJson(["cursor_gc", nextExpiry]),
      });
      await this.armNextAlarm();
    }
  }

  async enumerationCursorEvidence(cursor: unknown): Promise<unknown[] | null> {
    return this.catalog.enumerationCursorEvidence(cursor);
  }

  async beginClose(input: Parameters<FleetManifestCatalogStore["beginClose"]>[0]): Promise<CatalogCloseOperation> {
    return await this.catalog.beginClose(input);
  }

  async materializeCloseProgress(closeKey: string, requestedPageSize?: number): Promise<CatalogCloseOperation> {
    return await this.catalog.materializeCloseProgress(closeKey, requestedPageSize);
  }

  async closeProgress(closeKey: string, afterSequence?: number, requestedPageSize?: number): Promise<readonly CatalogCloseProgressEntry[]> {
    return this.catalog.closeProgress(closeKey, afterSequence, requestedPageSize);
  }

  async recordCloseReceipt(input: Parameters<FleetManifestCatalogStore["recordCloseReceipt"]>[0]): Promise<CatalogCloseOperation> {
    return this.catalog.recordCloseReceipt(input);
  }

  async finalizeClose(closeKey: string): Promise<CatalogCloseOperation> {
    const result = await this.catalog.finalizeClose(closeKey);
    await this.refreshHistoryGc();
    return result;
  }

  async alarm(): Promise<void> {
    const due = this.catalog.duePurposes(Date.now());
    for (const purpose of due) {
      if (purpose.purpose === "snapshot_resume") {
        const result = await this.catalog.resumeSnapshot(purpose.generation);
        this.catalog.completePurpose(purpose.purpose, purpose.generation);
        if (result.status === "pending") {
          this.catalog.schedulePurpose({ ...purpose, fire_at_ms: Date.now() });
        }
      } else if (purpose.purpose === "cursor_gc") {
        this.catalog.completePurpose(purpose.purpose, purpose.generation);
        const nextExpiry = this.catalog.purgeEnumerationCursors(Date.now());
        if (nextExpiry !== null) {
          this.catalog.schedulePurpose({
            purpose: "cursor_gc",
            fire_at_ms: Math.max(Date.now(), nextExpiry),
            generation: 0,
            payload_hash: await hashCanonicalJson(["cursor_gc", nextExpiry]),
          });
        }
      } else if (purpose.purpose === "route_gc") {
        this.catalog.completePurpose(purpose.purpose, purpose.generation);
        const nextRelease = this.catalog.purgeReleasedRoutes(Date.now());
        if (nextRelease !== null) {
          this.catalog.schedulePurpose({
            purpose: "route_gc",
            fire_at_ms: Math.max(Date.now(), nextRelease),
            generation: 0,
            payload_hash: await hashCanonicalJson(["route_gc", nextRelease]),
          });
        }
      } else if (purpose.purpose === "history_gc") {
        await this.refreshHistoryGc();
      } else {
        // Unknown/future handlers never erase durable operation state. Removing
        // only the wake-up prevents a hot alarm loop until its owner retries.
        this.catalog.completePurpose(purpose.purpose, purpose.generation);
      }
    }
    await this.armNextAlarm();
  }
}
