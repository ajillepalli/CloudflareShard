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
  status: "committed";
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
  status: string;
  fromShard: string;
  toShard: string;
  rowsCopied: number;
  mirrorQueueDepth: number;
  startedAt: string;
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
  catalogShardId: string;
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

export interface TxStatusResponse {
  txId: string;
  status: string;
  [key: string]: unknown;
}

export interface TxForceAbortRequest {
  txId: string;
}

export interface TxForceAbortResponse {
  ok: true;
  txId: string;
}
