/**
 * Request/response shapes for CloudflareShard's HTTP API. Hand-mirrored from
 * the Worker's actual handlers (src/index.ts, src/catalog.ts, src/shard.ts)
 * and docs/SPEC.md -- not generated, so keep this in sync when those routes'
 * shapes change.
 */

export type MutateOp = "insert" | "update" | "delete" | "upsert";

export interface MutateRequest {
  op: MutateOp;
  table: string;
  tenantId: string;
  partitionKey: string;
  values?: Record<string, unknown>;
  where?: Record<string, unknown>;
  requestId?: string;
  /** upsert only: the columns of the ON CONFLICT target. Defaults to
   * [partitionKeyColumn] on the server when omitted (src/structured-op.ts). */
  conflictColumns?: string[];
}

export interface MutateResponse {
  ok: true;
  rowsAffected: number;
}

export interface TxRequest {
  mutations: MutateRequest[];
  requestId: string;
}

export interface TxResponse {
  ok: true;
  txId: string;
  /** "committed_pending_ack": durably committed, but one or more
   * participants' commit acknowledgement is still outstanding and queued
   * for alarm-driven retry (src/coordinator.ts) -- the transaction is
   * committed either way, only the ack is pending. */
  status: "commit_pending_manifest" | "committed" | "committed_pending_ack";
}

export type TransactionState =
  | "new"
  | "manifest_reserving"
  | "preparing"
  | "prepared"
  | "abort_decided"
  | "aborting"
  | "aborted"
  | "aborted_pending_manifest_cancel"
  | "commit_deciding"
  | "commit_decided"
  | "commit_pending_manifest"
  | "manifest_registered"
  | "committing"
  | "committed_pending_ack"
  | "committed"
  | "quarantined";

export type TransactionDecision = "undecided" | "commit" | "abort" | "quarantined";

export interface IndexQueryRequest {
  table: string;
  indexName: string;
  tenantId: string;
  values: Record<string, unknown>;
  limit?: number;
}

export interface IndexQueryResponse {
  rows: Array<Record<string, unknown>>;
}

export interface TableScanRequest {
  tenantId: string;
  table: string;
  limit?: number;
  cursor?: string;
}

export interface TableScanResponse {
  rows: Array<Record<string, unknown>>;
  nextCursor?: string;
  provenance: { complete: boolean; fix?: string };
  scan: { catalogShardId: string; shardCount: number; successCount: number; scanMs: number };
}

export interface InitRequest {
  numShards?: number;
  totalVBuckets?: number;
  force?: boolean;
}

export interface InitResponse {
  ok: true;
  catalogShardCount: number;
  catalogs: Array<{ catalogShardId: string } & Record<string, unknown>>;
}

export interface RegisterTableRequest {
  table: string;
  partitionKeyColumn: string;
  schemaSql?: string | null;
}

export interface RegisterTableResponse {
  ok: true;
  catalogShardCount: number;
}

export interface CreateTableRequest {
  table: string;
  schema: string;
  partitionKeyColumn: string;
}

export interface CreateTableResponse {
  ok: true;
  table: string;
  shardsApplied: number;
}

export interface SetPartitionKeyColumnRequest {
  table: string;
  partitionKeyColumn: string;
}

export interface RegisterTenantRequest {
  tenantId: string;
  rotate?: boolean;
}

export interface RegisterTenantResponse {
  ok: true;
  tenantId: string;
  token: string;
}

export interface CreateIndexRequest {
  indexName: string;
  table: string;
  columns: string[];
}

export interface CreateIndexResponse {
  ok: true;
  indexName: string;
  table: string;
  columns: string[];
  status: "building" | "ready";
}

export interface CreateIndexStatusResponse {
  indexName: string;
  table: string;
  status: "building" | "ready" | "failed";
  rowsCopied: number;
  totalShards: number;
  currentShardIndex: number;
  currentShardId: string | null;
}

export interface ListedIndex {
  indexName: string;
  table: string;
  columns: string[];
  status: string;
  createdAt: string;
  placementRing: string[];
}

export interface ListIndexesResponse {
  indexes: ListedIndex[];
}

export interface ListedTable {
  table_name: string;
  partitioning: string;
  partition_key_column: string;
  created_at: string;
}

export interface ListTablesResponse {
  tables: ListedTable[];
}

export interface ShardCounts {
  total: number;
  active: number;
  draining: number;
}

export interface CatalogStatus {
  catalogShardId: string;
  initialized: boolean;
  shards?: ShardCounts;
  totalVBuckets?: number;
  metadataVersion?: number;
  initializedAt?: string;
}

export interface StatusResponse {
  initialized: boolean;
  catalogShardCount: number;
  shards: ShardCounts;
  catalogs: CatalogStatus[];
}

export interface ShardStatsResponse {
  ok: true;
  tables: Array<{ table: string; rowCount: number }>;
  idempotencyTableSize: number;
  pendingIntentCount: number;
  indexPendingJobCount: number;
  indexEntryCount: number;
  rowOwnerCount: number;
}

/** Minimal admin-only /v1/sql response used by the onboarding verifier to
 * prove that two partition keys resolve to different physical shards. The
 * verifier issues only `SELECT 1`; this is not a general raw-SQL SDK surface. */
export interface RouteProbeResponse {
  route: {
    shardId: string;
    catalogShardId: string;
  };
}

export interface TopologyLockStatusResponse {
  held: boolean;
  operationId?: string;
  operationType?: string;
  acquiredAt?: string;
  heartbeatAt?: string;
  expiresAt?: string;
  expired?: boolean;
}

export interface SplitVbucketRequest {
  catalogShardId: string;
  vbucket: number;
  newShardId?: string;
}

export interface SplitVbucketResponse {
  ok: true;
  vbucket: number;
  fromShard: string;
  toShard: string;
  metadataVersion: number;
  migrationStarted: true;
}

export interface MigrateVbucketRequest {
  catalogShardId: string;
  vbucket: number;
  targetShardId?: string;
}

export interface MigrateVbucketResponse {
  ok: true;
  vbucket: number;
  fromShard: string;
  toShard: string;
  status: "backfilling";
}

export interface MigrateVbucketStatusRequest {
  catalogShardId: string;
  vbucket: number;
}

export interface MigrateVbucketStatusResponse {
  vbucket: number;
  /** Usually one of "none" | "backfilling" | "cutover" |
   * "cutover-blocked-on-prepared-intents" | "complete", but left as a plain
   * string here rather than narrowed to a union -- src/catalog.ts treats
   * migration_status as free-form and this SDK doesn't want to fall out of
   * sync every time a new status value is added there. */
  status: string;
  fromShard: string;
  /** null before a migration has ever targeted this vbucket, or once one
   * completes/aborts and target_shard_id is cleared (src/catalog.ts's
   * handleMigrateVbucketStatus). */
  toShard: string | null;
  rowsCopied: number;
  mirrorQueueDepth: number;
  /** null for the same "no active/completed migration" cases as toShard. */
  startedAt: string | null;
  /** Only present when status is 'cutover-blocked-on-prepared-intents' --
   * the txId(s) an operator needs to /admin/tx-force-abort to unstick it. */
  blockedTxIds?: string[];
}

export interface DrainShardRequest {
  catalogShardId: string;
  shardId: string;
}

export interface DrainShardResponse {
  ok: true;
  shardId: string;
  metadataVersion: number;
  evacuationStarted: true;
}

export interface DrainShardStatusRequest {
  catalogShardId: string;
  shardId: string;
}

export interface DrainShardStatusResponse {
  shardId: string;
  vbucketsRemaining: number;
  ringsRemaining: number;
  status: string;
  stallReason: string | null;
}

export interface BackfillProvenanceRequest {
  /** Omit to run against every catalog shard -- only a full-cluster run
   * (catalogShardId omitted) can ever flip a table's
   * table_rules.provenance_complete to true (docs/SPEC.md). A scoped,
   * single-catalog-shard run never certifies a table, since it only ever
   * sees that one catalog shard's own shard pool. */
  catalogShardId?: string;
}

export interface BackfillProvenanceResponse {
  attributed: number;
  ambiguous: unknown[];
  orphaned: unknown[];
}

export interface SetRowOwnerRequest {
  catalogShardId: string;
  shardId: string;
  table: string;
  partitionKey: string;
  tenantId: string;
}

export interface SetRowOwnerResponse {
  ok: true;
}

export interface TxStatusRequest {
  txId: string;
}

/** src/coordinator.ts's handleTxStatus: {found: false} for an unknown
 * txId, {found: true, status} for a known one -- never a bare txId/status
 * pair, and no txId echoed back either way. Always check `found` before
 * reading `status`. */
export type TxStatusResponse =
  | { found: false }
  | {
      found: true;
      status: TransactionState;
      decision: TransactionDecision;
      epoch: number;
      operationHash: string;
      commitAuthorized: boolean;
      quarantineCandidates?: Array<{
        kind: "record" | "finalize_intent" | "cancel_intent";
        hash: string;
      }>;
    };

export interface TxForceAbortRequest {
  txId: string;
}

export interface TxForceAbortResponse {
  ok: true;
  txId: string;
  status: "aborted";
}

// Fleet restore requests are ergonomic SDK inputs. The admin client projects
// them to the versioned snake_case wire contracts expected by the Worker.
export interface RestorePreviewRequest {
  fleetId: string;
  cutoff: string;
  idempotencyKey: string;
}

export interface RestoreExecuteRequest {
  restoreId: string;
  planHash: string;
}

export interface RestoreStatusRequest {
  restoreId: string;
}

export type RestoreReconcileRequest = RestoreExecuteRequest;
export type RestoreRollbackRequest = RestoreExecuteRequest;

export interface RestoreTopologyPin {
  topology_epoch: number;
  topology_hash: string;
}

export interface RestoreManifestPin {
  coverage_start: string;
  catalog_close_key: string;
  catalog_generation: number;
  catalog_snapshot_hash: string;
  fleet_root_hash: string;
  partition_config_hash: string;
  record_count: number;
}

export interface RestoreParticipantPlan {
  participant_id: string;
  target_bookmark: string;
  preview_bookmark: string;
}

export interface RestorePlan {
  protocol_version: 1;
  format_version: 1;
  restore_id: string;
  fleet_id: string;
  cutoff: string;
  previewed_at: string;
  execute_before: string;
  parameter_hash: string;
  topology: RestoreTopologyPin;
  manifest: RestoreManifestPin;
  participants: RestoreParticipantPlan[];
  impact: {
    participant_count: number;
    transaction_count: number;
    intentional_loss_from: string;
    intentional_loss_through: string;
  };
  rollback: {
    undo_supported: boolean;
    undo_expires_at: string | null;
    limitations: string[];
  };
  plan_hash: string;
}

export type RestorePreviewResponse =
  | { ok: true; status: "previewing"; restore_id: string; retry_after_ms: number }
  | { ok: true; status: "previewed"; plan: RestorePlan };

export interface RestoreAcceptedResponse {
  ok: true;
  status: "accepted" | "already_started";
  restore_id: string;
  plan_hash: string;
}

export type RestorePhase =
  | "previewing"
  | "previewed"
  | "fencing"
  | "restoring"
  | "reconciliation_pending"
  | "reconciling"
  | "verifying"
  | "rolling_back"
  | "parked_lease_lost"
  | "complete"
  | "rolled_back"
  | "manual_repair_required"
  | "failed";

export interface RestoreStatusResponse {
  protocol_version: 1;
  format_version: 1;
  restore_id: string;
  plan_hash: string | null;
  fleet_id: string;
  cutoff: string;
  phase: RestorePhase;
  started_at: string | null;
  updated_at: string;
  completed_at: string | null;
  progress: {
    participants_total: number;
    participants_restored: number;
    transactions_total: number;
    transactions_reconciled: number;
  };
  blockers: Array<{
    code: string;
    message: string;
    participant_id: string | null;
    tx_id: string | null;
  }>;
  report: null | {
    discarded_write_count: number;
    discarded_write_report_hash: string;
    discarded_write_report_complete: boolean;
    measured_rpo_ms: number;
    measured_rto_ms: number;
    verified_at: string;
  };
}
