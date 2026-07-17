import { CloudflareShardClient } from "./client.js";
import type { ClientOptions } from "./http.js";
import type {
  BackfillProvenanceRequest,
  BackfillProvenanceResponse,
  CreateIndexRequest,
  CreateIndexResponse,
  CreateIndexStatusResponse,
  CreateTableRequest,
  CreateTableResponse,
  DrainShardRequest,
  DrainShardResponse,
  DrainShardStatusRequest,
  DrainShardStatusResponse,
  InitRequest,
  InitResponse,
  ListIndexesResponse,
  ListTablesResponse,
  MigrateVbucketRequest,
  MigrateVbucketResponse,
  MigrateVbucketStatusRequest,
  MigrateVbucketStatusResponse,
  RegisterTableRequest,
  RegisterTableResponse,
  RegisterTenantRequest,
  RegisterTenantResponse,
  SetPartitionKeyColumnRequest,
  SetRowOwnerRequest,
  SetRowOwnerResponse,
  ShardStatsResponse,
  SplitVbucketRequest,
  SplitVbucketResponse,
  StatusResponse,
  TopologyLockStatusResponse,
  TxForceAbortRequest,
  TxForceAbortResponse,
  TxStatusRequest,
  TxStatusResponse,
} from "./types.js";

/** Admin/operator client: every /admin/* route, plus everything
 * CloudflareShardClient already offers (an admin token CAN call the tenant
 * data-plane routes too, though there's rarely a reason to). Construct with
 * ADMIN_TOKEN, not a tenant token -- /admin/* routes reject a tenant token
 * with 401. */
export class CloudflareShardAdminClient extends CloudflareShardClient {
  constructor(options: ClientOptions) {
    super(options);
  }

  /** Provisions cluster metadata and the shard map. numShards clamps to
   * 1-256 (default 8), totalVBuckets to 64-65536 (default 1024). Pass
   * force: true to reinitialize an already-initialized cluster (destructive
   * -- resets shard/vbucket assignment). */
  async init(request: InitRequest = {}): Promise<InitResponse> {
    return this.post<InitResponse>("/admin/init", request);
  }

  /** Registers table metadata without pushing any schema to a shard --
   * use this for a table whose physical schema already exists some other
   * way (e.g. createTable() below already ran once). partitionKeyColumn is
   * mandatory: it's what lets mutate/tx structurally enforce that a
   * mutation only ever touches the one row/partition it claims to. */
  async registerTable(request: RegisterTableRequest): Promise<RegisterTableResponse> {
    return this.post<RegisterTableResponse>("/admin/register-table", request);
  }

  /** Creates the table's schema on every physical shard AND registers it
   * -- the usual way to provision a brand-new table. `schema`'s CREATE
   * TABLE name must match `table` exactly, and must NOT use
   * "IF NOT EXISTS" (rejected 400) -- see README's API quickstart §3 for
   * why. */
  async createTable(request: CreateTableRequest): Promise<CreateTableResponse> {
    return this.post<CreateTableResponse>("/admin/create-table", request);
  }

  /** One-time upgrade for a table registered before partitionKeyColumn
   * validation existed (carrying the '__unset__' sentinel). Rejected 409
   * PARTITION_KEY_ALREADY_SET if the table's partition key is already
   * configured -- there's no supported way to repoint an already-configured
   * table to a different column. */
  async setPartitionKeyColumn(request: SetPartitionKeyColumnRequest): Promise<{ ok: true; table: string; partitionKeyColumn: string }> {
    return this.post("/admin/set-partition-key-column", request);
  }

  /** Registers a tenant and returns its bearer token -- returned in
   * plaintext exactly once; store it (e.g. to construct a
   * CloudflareShardClient for that tenant). Pass rotate: true to reissue a
   * token for an already-registered tenantId. */
  async registerTenant(request: RegisterTenantRequest): Promise<RegisterTenantResponse> {
    return this.post<RegisterTenantResponse>("/admin/register-tenant", request);
  }

  async revokeTenant(tenantId: string): Promise<{ ok: true }> {
    return this.post("/admin/revoke-tenant", { tenantId });
  }

  /** Registers a secondary index and starts its alarm-driven background
   * backfill (see #20) -- this call returns as soon as backfill STARTS, not
   * once it finishes; poll createIndexStatus() until status flips from
   * 'building' to 'ready' (or 'failed' -- see docs/SPEC.md and TODOS.md for
   * the permanent-vs-transient-failure distinction). Idempotent: retrying
   * with the same indexName+table+columns against an already-'ready' index
   * is a no-op success. */
  async createIndex(request: CreateIndexRequest): Promise<CreateIndexResponse> {
    return this.post<CreateIndexResponse>("/admin/create-index", request);
  }

  async createIndexStatus(indexName: string): Promise<CreateIndexStatusResponse> {
    return this.post<CreateIndexStatusResponse>("/admin/create-index-status", { indexName });
  }

  /** Polls createIndexStatus() until the index reaches 'ready' or 'failed',
   * sleeping intervalMs between polls (default 500ms) up to maxWaitMs
   * (default 5 minutes). Throws if it times out or reaches 'failed' (the
   * thrown error's .body carries the last status response so a caller can
   * inspect why). Convenience wrapper for the common "create then wait"
   * pattern -- for anything needing finer control (progress reporting,
   * custom backoff), poll createIndexStatus() directly instead. */
  async waitForIndexReady(indexName: string, options: { intervalMs?: number; maxWaitMs?: number } = {}): Promise<CreateIndexStatusResponse> {
    const intervalMs = options.intervalMs ?? 500;
    const maxWaitMs = options.maxWaitMs ?? 5 * 60 * 1000;
    const deadline = Date.now() + maxWaitMs;
    for (;;) {
      const status = await this.createIndexStatus(indexName);
      if (status.status === "ready") return status;
      if (status.status === "failed") {
        throw new Error(
          `Index ${indexName} backfill failed permanently. Check the operator remediation this index's PermanentIndexBackfillError named (see server logs / docs/SPEC.md), fix the underlying issue, then retry createIndex().`,
        );
      }
      if (Date.now() >= deadline) {
        throw new Error(`Timed out after ${maxWaitMs}ms waiting for index ${indexName} to become ready (still '${status.status}').`);
      }
      await new Promise((resolve) => setTimeout(resolve, intervalMs));
    }
  }

  /** `warning` is present when physical __cf_indexes cleanup failed on one
   * or more shards -- the index is still unregistered and unqueryable
   * either way, but stale rows may remain on those shards. Always check
   * for it rather than assuming a 200 means fully clean. */
  async dropIndex(indexName: string): Promise<{ ok: true; indexName: string; warning?: string }> {
    return this.post("/admin/drop-index", { indexName });
  }

  async listIndexes(): Promise<ListIndexesResponse> {
    return this.post<ListIndexesResponse>("/admin/list-indexes", {});
  }

  async listTables(): Promise<ListTablesResponse> {
    return this.post<ListTablesResponse>("/admin/list-tables", {});
  }

  /** Cluster-wide status, aggregated across every catalog shard. */
  async status(): Promise<StatusResponse> {
    return this.post<StatusResponse>("/admin/status", {});
  }

  /** Row counts and internal bookkeeping-table sizes for ONE physical
   * shard (not a cluster-wide aggregate) -- discover shard IDs via
   * status()'s catalogs, or an index's placementRing from listIndexes(). */
  async shardStats(shardId: string): Promise<ShardStatsResponse> {
    return this.post<ShardStatsResponse>("/admin/shard-stats", { shardId });
  }

  async topologyLockStatus(): Promise<TopologyLockStatusResponse> {
    return this.post<TopologyLockStatusResponse>("/admin/topology-lock-status", {});
  }

  /** Only for a genuinely stuck lock (a crashed operation that will never
   * heartbeat again) -- see TODOS.md/README for when this is actually
   * safe to use. Force-releasing a lock a live operation still holds can
   * let two topology operations run concurrently against the same state.
   * released: false means operationId was stale/didn't match the current
   * holder -- nothing was actually released; always check it rather than
   * assuming a 200 means the lock is now free. */
  async forceReleaseTopologyLock(operationId: string): Promise<{ ok: true; released: boolean }> {
    return this.post("/admin/force-release-topology-lock", { operationId });
  }

  /** Starts a real online migration of one vbucket to a NEW shard
   * (dual-write backfill, then a fenced checksum-verified cutover) -- not a
   * routing-only repoint. Use migrateVbucket() instead to target an
   * existing shard explicitly. */
  async splitVbucket(request: SplitVbucketRequest): Promise<SplitVbucketResponse> {
    return this.post<SplitVbucketResponse>("/admin/split-vbucket", request);
  }

  /** Same migration primitive as splitVbucket(), with the target shard
   * explicit/optional rather than always a fresh one. */
  async migrateVbucket(request: MigrateVbucketRequest): Promise<MigrateVbucketResponse> {
    return this.post<MigrateVbucketResponse>("/admin/migrate-vbucket", request);
  }

  async migrateVbucketStatus(request: MigrateVbucketStatusRequest): Promise<MigrateVbucketStatusResponse> {
    return this.post<MigrateVbucketStatusResponse>("/admin/migrate-vbucket-status", request);
  }

  async migrateVbucketAbort(request: MigrateVbucketStatusRequest): Promise<{ ok: true }> {
    return this.post("/admin/migrate-vbucket-abort", request);
  }

  /** Starts (or resumes) evacuating a shard: migrates every vbucket it
   * still owns off it, then evacuates any index placement ring containing
   * it. Rejected 409 RING_EVACUATION_NO_CANDIDATE up front if an index's
   * ring can't be evacuated (add a shard via splitVbucket() first). */
  async drainShard(request: DrainShardRequest): Promise<DrainShardResponse> {
    return this.post<DrainShardResponse>("/admin/drain-shard", request);
  }

  async drainShardStatus(request: DrainShardStatusRequest): Promise<DrainShardStatusResponse> {
    return this.post<DrainShardStatusResponse>("/admin/drain-shard-status", request);
  }

  /** Attributes rows written before row-provenance tracking existed to
   * their owning tenant. Omit catalogShardId (the default) to run against
   * every catalog shard -- only a full-cluster run can ever certify a
   * table's provenance as complete (see BackfillProvenanceRequest's doc
   * comment); pass catalogShardId to scope it to one catalog shard's own
   * pool instead. `ambiguous` rows need a manual setRowOwner() call. */
  async backfillProvenance(request: BackfillProvenanceRequest = {}): Promise<BackfillProvenanceResponse> {
    return this.post<BackfillProvenanceResponse>("/admin/backfill-provenance", request);
  }

  /** Manually attributes one row to a tenant -- verifies the tenant's
   * hash for (table, partitionKey) actually maps to the claimed shard
   * before writing (409 ROW_OWNER_SHARD_MISMATCH otherwise), so it can't be
   * used to misattribute a row to the wrong tenant by mistake. */
  async setRowOwner(request: SetRowOwnerRequest): Promise<SetRowOwnerResponse> {
    return this.post<SetRowOwnerResponse>("/admin/set-row-owner", request);
  }

  async txStatus(request: TxStatusRequest): Promise<TxStatusResponse> {
    return this.post<TxStatusResponse>("/admin/tx-status", request);
  }

  /** Escape hatch for a transaction stuck mid-2PC (e.g. an unreachable
   * participant shard) -- forces it to abort rather than wait indefinitely
   * for the recovery loop. */
  async txForceAbort(request: TxForceAbortRequest): Promise<TxForceAbortResponse> {
    return this.post<TxForceAbortResponse>("/admin/tx-force-abort", request);
  }
}
