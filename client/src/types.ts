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
  status: "committed" | "committed_pending_ack";
}

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
export type TxStatusResponse = { found: false } | { found: true; status: string };

export interface TxForceAbortRequest {
  txId: string;
}

export interface TxForceAbortResponse {
  ok: true;
  txId: string;
  status: "aborted";
}
