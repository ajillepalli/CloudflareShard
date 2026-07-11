import { DurableObject } from "cloudflare:workers";
import { json } from "./http";
import { log } from "./log";
import { INTERNAL_TABLE_NAMES, isDeleteStatement, isMutation } from "./sql-safety";
import { rowKey } from "./structured-op";

type ExecutePayload = {
  sql: string;
  params?: unknown[];
  requestId: string;
  /** Caller's classification, unused for the routing decision — ShardDO derives
   * this itself from the SQL so a caller (or a caller-side classification bug)
   * can't disguise a mutation as a read by sending isMutation: false. Kept only
   * for logging/back-compat. */
  isMutation?: boolean;
  /** Optional routing context, forwarded by the Worker so raw /v1/sql mutations
   * can be checked against row_locks too — see the "honestly-labeled caller"
   * limitation documented on the lock check below. */
  tenantId?: string;
  table?: string;
  partitionKey?: string;
  /** Milestone 3, Chunk 0: the vbucket this (tenantId, table, partitionKey)
   * hashes to, forwarded by the Worker from CatalogDO's /route response.
   * Present alongside tenantId/table/partitionKey for any /v1/sql or
   * /v1/mutate write — written into `__cf_row_owners` so a later migration
   * can recover a row's logical tenant/vbucket identity from the row itself
   * (see docs/SPEC.md §14's trust-model limitation: the row's own columns
   * carry no tenant identity). */
  vbucket?: number;
  /** Milestone 3, Chunk 4 (review Tier 1 #2/#3): when set, this write's
   * vbucket is mid-migration and must be mirrored to this target shard. The
   * source shard enqueues a __cf_mirror_pending row for it ATOMICALLY inside
   * the same transaction that applies the write — so an in-flight mirror is
   * always counted by /mirror-pending-count and cutover's drain-to-zero gate
   * can't flip while a mirror is merely slow. */
  mirrorTargetShardId?: string;
  /** Milestone 3 (review Tier 1 #3): set on a mirrored write delivered to the
   * TARGET shard — skips the fence/lock checks (the source already vetted
   * both) while still writing __cf_row_owners provenance on the target so a
   * row whose first target appearance is a mirror is visible to the cutover
   * checksum's provenance-scoped selection. */
  isMirror?: boolean;
  /** Milestone 3 (review Tier 1 #5): the ORIGINAL client requestId a mirror
   * corresponds to. The mirror APPLIES under an unforgeable derived id (so a
   * forged pre-existing entry can't livelock it), but ALSO records an
   * INSERT-OR-IGNORE applied_requests entry under this client id — so a
   * client that replays its requestId AFTER the vbucket flipped to this
   * target still dedupes (cross-migration idempotency, criterion 2). */
  clientRequestId?: string;
};

type PrepareIntent = {
  sql: string;
  params?: unknown[];
  tenantId: string;
  table: string;
  partitionKey: string;
  /** Milestone 3, Chunk 0: present for a genuine base-row mutation intent
   * (never for a synthetic __cf_indexes-maintenance intent piggybacked onto
   * the same 2PC transaction — those describe an index shard write, not a
   * row identity, and must never populate __cf_row_owners for it), used the
   * same way as ExecutePayload.vbucket/op to update `__cf_row_owners` at
   * commit time. */
  vbucket?: number;
  op?: "insert" | "update" | "delete" | "upsert";
  /** Milestone 3 (review Tier 1 #2): when set, this intent's vbucket is
   * mid-migration; handleCommit enqueues a mirror job for it atomically with
   * the commit. */
  mirrorTargetShardId?: string;
};

/** A committed 2PC intent as read back from pending_intents for apply. */
type CommittableIntent = {
  intent_seq: number;
  sql: string;
  params_json: string;
  status: string;
  table_name: string | null;
  partition_key: string | null;
  tenant_id: string | null;
  vbucket: number | null;
  op: string | null;
  mirror_target_shard_id: string | null;
};

/** Milestone 3 (review Tier 1 #5): requestIds ShardDO generates for its own
 * internal replication (mirror deliveries) live under this reserved prefix.
 * The gateway rejects any CLIENT-supplied requestId that starts with it, so a
 * tenant can never pre-poison the future target shard's applied_requests with
 * a colliding id and permanently block a mirror's dedupe (which would stall
 * cutover forever). */
const RESERVED_REQUEST_ID_PREFIX = "__cf:";

const APPLIED_REQUESTS_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const PRUNE_INTERVAL_MS = 60 * 60 * 1000;
const PENDING_INTENT_TTL_MS = 5 * 60 * 1000;
const INDEX_JOB_BASE_DELAY_MS = 5000;
const INDEX_JOB_MAX_DELAY_MS = 60000;
const INDEX_JOB_BATCH_SIZE = 20;
// Milestone 3, Chunk 3: dual-write mirror retry cadence — deliberately
// faster/uncapped-attempt compared to the index-job queue (INDEX_JOB_*
// above), per spec: "exponential backoff starting 1s, cap 60s, no attempt
// limit — cutover gates on empty queue, so jobs must eventually land."
const MIRROR_JOB_BASE_DELAY_MS = 1000;
const MIRROR_JOB_MAX_DELAY_MS = 60000;
const MIRROR_JOB_BATCH_SIZE = 20;

// Single source of truth in sql-safety.ts (INTERNAL_TABLE_NAMES), so the
// tenant-facing internal-table write guard and this /stats filter can't drift.
const INTERNAL_TABLES = new Set<string>(INTERNAL_TABLE_NAMES);

// Milestone 3, Chunk 4: page size shared by /migrate-export and
// /migrate-checksum — the spec's checksum definition is "streamed in the
// same 500-row pages", so both sides paging identically is part of the
// contract, not a tunable. Re-exported so catalog.ts's backfill/evacuation
// loops use the identical constant (a clamp mismatch would silently drop rows).
export const MIGRATE_PAGE_SIZE = 500;

/** Deliberate sentinel thrown inside handlePrepare's validation transactionSync
 * to force a rollback — distinguishes "validation succeeded, roll back on
 * purpose" from a genuine SQL execution error. */
class PrepareValidationRollback extends Error {}

export class ShardDO extends DurableObject {
  private readonly sql: SqlStorage;
  private readonly shardEnv: Cloudflare.Env;
  private readonly routes: Record<string, (request: Request) => Promise<Response>>;
  /** ensureSchema() is idempotent but not free (a dozen-plus DDL/PRAGMA
   * statements) — running it once per in-memory instance instead of once
   * per request is safe because the schema lives in durable storage: a
   * fresh instance (new isolate) re-runs it on its first request, and
   * nothing ever drops these tables mid-lifetime. */
  private schemaEnsured = false;

  constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.shardEnv = env;
    this.routes = {
      "/stats": this.handleStats.bind(this),
      "/execute": this.handleExecute.bind(this),
      "/prepare": this.handlePrepare.bind(this),
      "/commit": this.handleCommit.bind(this),
      "/abort": this.handleAbort.bind(this),
      "/tx-status": this.handleTxStatus.bind(this),
      "/pending-intent-count": this.handlePendingIntentCount.bind(this),
      "/prepared-intent-count-for-vbucket": this.handlePreparedIntentCountForVbucket.bind(this),
      "/invalidate-request": this.handleInvalidateRequest.bind(this),
      "/enqueue-index-job": this.handleEnqueueIndexJob.bind(this),
      "/enqueue-mirror-job": this.handleEnqueueMirrorJob.bind(this),
      "/mirror-pending-count": this.handleMirrorPendingCount.bind(this),
      "/drain-mirror-jobs": this.handleDrainMirrorJobs.bind(this),
      "/migrate-export": this.handleMigrateExport.bind(this),
      "/migrate-import": this.handleMigrateImport.bind(this),
      "/migrate-checksum": this.handleMigrateChecksum.bind(this),
      "/migrate-checksums": this.handleMigrateChecksums.bind(this),
      "/vbucket-tables": this.handleVbucketTables.bind(this),
      "/fence-vbucket": this.handleFenceVbucket.bind(this),
      "/unfence-vbucket": this.handleUnfenceVbucket.bind(this),
      "/delete-vbucket-rows": this.handleDeleteVbucketRows.bind(this),
      "/unattributed-count": this.handleUnattributedCount.bind(this),
      "/purge-mirror-jobs": this.handlePurgeMirrorJobs.bind(this),
      "/index-entries-export": this.handleIndexEntriesExport.bind(this),
      "/index-entries-import": this.handleIndexEntriesImport.bind(this),
    };
    ctx.blockConcurrencyWhile(async () => {
      if ((await ctx.storage.getAlarm()) === null) {
        await ctx.storage.setAlarm(Date.now() + PRUNE_INTERVAL_MS);
      }
    });
  }

  async alarm(): Promise<void> {
    this.ensureSchema();
    const cutoff = new Date(Date.now() - APPLIED_REQUESTS_TTL_MS).toISOString();
    this.sql.exec("DELETE FROM applied_requests WHERE applied_at < ?", cutoff);
    await this.sweepStalePendingIntents();
    const nextIndexJobRetry = await this.processIndexPendingJobs();
    const nextMirrorJobRetry = await this.processMirrorPendingJobs();
    const candidates = [Date.now() + PRUNE_INTERVAL_MS, nextIndexJobRetry, nextMirrorJobRetry].filter(
      (t): t is number => t !== null,
    );
    await this.ctx.storage.setAlarm(Math.min(...candidates));
  }

  /** Milestone 2, Chunk 2: retries index writes that failed on their first
   * (best-effort, ctx.waitUntil()-driven) attempt from the Worker. Mirrors
   * CoordinatorDO's recovery_queue backoff pattern. Returns the timestamp
   * (ms) the next retry is due, or null if the queue is empty — the caller
   * uses this to decide whether to re-arm the alarm sooner than the regular
   * hourly prune cycle. */
  private async processIndexPendingJobs(): Promise<number | null> {
    const due = this.many<{
      job_id: number;
      target_shard_id: string;
      sql: string;
      params_json: string;
      request_id: string;
      attempt_count: number;
    }>(
      "SELECT job_id, target_shard_id, sql, params_json, request_id, attempt_count FROM index_pending_jobs WHERE next_attempt_at <= ? ORDER BY job_id ASC LIMIT ?",
      new Date().toISOString(),
      INDEX_JOB_BATCH_SIZE,
    );

    for (const job of due) {
      try {
        const id = this.shardEnv.SHARD.idFromName(job.target_shard_id);
        const stub = this.shardEnv.SHARD.get(id);
        const res = await stub.fetch("https://shard.internal/execute", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            sql: job.sql,
            params: JSON.parse(job.params_json),
            requestId: job.request_id,
            isMutation: true,
          }),
        });
        if (res.ok) {
          this.sql.exec("DELETE FROM index_pending_jobs WHERE job_id = ?", job.job_id);
          continue;
        }
        throw new Error(`shard responded ${res.status}`);
      } catch (error) {
        const attemptCount = job.attempt_count + 1;
        const delay = Math.min(INDEX_JOB_MAX_DELAY_MS, INDEX_JOB_BASE_DELAY_MS * 2 ** job.attempt_count);
        this.sql.exec(
          "UPDATE index_pending_jobs SET attempt_count = ?, next_attempt_at = ? WHERE job_id = ?",
          attemptCount,
          new Date(Date.now() + delay).toISOString(),
          job.job_id,
        );
        log("shard.index_job_retry_failed", {
          jobId: job.job_id,
          targetShardId: job.target_shard_id,
          attemptCount,
          message: error instanceof Error ? error.message : String(error),
        });
      }
    }

    const next = this.one<{ next_attempt_at: string }>(
      "SELECT next_attempt_at FROM index_pending_jobs ORDER BY next_attempt_at ASC LIMIT 1",
    );
    return next ? new Date(next.next_attempt_at).getTime() : null;
  }

  /** Called by the Worker when a best-effort index write (issued via
   * ctx.waitUntil() from handleV1Mutate) fails — records it for alarm-driven
   * retry instead of losing it. Lives on the BASE shard (where the write
   * originated), not the index shard (which may be the one that's
   * unreachable), and schedules an alarm soon rather than waiting for the
   * next hourly prune cycle. */
  private async handleEnqueueIndexJob(request: Request): Promise<Response> {
    const body = (await request.json()) as { targetShardId?: string; sql?: string; params?: unknown[]; requestId?: string };
    if (!body.targetShardId || !body.sql || !body.requestId) {
      return json({ error: "Missing targetShardId, sql, or requestId" }, 400);
    }
    const now = new Date().toISOString();
    this.sql.exec(
      "INSERT INTO index_pending_jobs (target_shard_id, sql, params_json, request_id, next_attempt_at, attempt_count, created_at) VALUES (?, ?, ?, ?, ?, 0, ?)",
      body.targetShardId,
      body.sql,
      JSON.stringify(body.params ?? []),
      body.requestId,
      now,
      now,
    );
    const retrySoon = Date.now() + INDEX_JOB_BASE_DELAY_MS;
    const existingAlarm = await this.ctx.storage.getAlarm();
    if (existingAlarm === null || existingAlarm > retrySoon) {
      await this.ctx.storage.setAlarm(retrySoon);
    }
    return json({ ok: true });
  }

  /** Milestone 3, Chunk 3: retries a dual-write mirror that failed on its
   * first attempt (from the Worker's ctx.waitUntil() call after a source
   * write succeeds, or from CoordinatorDO's post-commit mirroring for
   * /v1/tx). Mirrors processIndexPendingJobs' shape, on its own table with
   * its own (faster, uncapped-attempt) backoff cadence — Chunk 4's cutover
   * gates on this queue reaching zero for the migrating vbucket, so a job
   * here must eventually land, never give up. */
  /** Delivers one mirror job to its target and, on success, deletes it. On
   * failure, applies exponential backoff to next_attempt_at. Returns true on
   * delivery success. Shared by the alarm loop (respects next_attempt_at)
   * and CatalogDO's cutover-driven drain (forces every job for a vbucket). */
  private async deliverMirrorJob(job: {
    job_id: number;
    target_shard_id: string;
    sql: string;
    params_json: string;
    request_id: string;
    vbucket: number;
    tenant_id: string | null;
    table_name: string | null;
    partition_key: string | null;
    client_request_id: string | null;
    attempt_count: number;
  }): Promise<boolean> {
    try {
      const id = this.shardEnv.SHARD.idFromName(job.target_shard_id);
      const stub = this.shardEnv.SHARD.get(id);
      const res = await stub.fetch("https://shard.internal/execute", {
        method: "POST",
        headers: { "content-type": "application/json" },
        // Review Tier 1 #3: forward routing context so the target writes its
        // own __cf_row_owners provenance (without it a mirror-first row is
        // invisible to the cutover checksum's provenance INNER JOIN,
        // spuriously mismatching -> wipe/re-copy livelock). isMirror skips
        // the target's fence/lock checks — the source already vetted both.
        body: JSON.stringify({
          sql: job.sql,
          params: JSON.parse(job.params_json),
          requestId: job.request_id,
          isMutation: true,
          isMirror: true,
          tenantId: job.tenant_id ?? undefined,
          table: job.table_name ?? undefined,
          partitionKey: job.partition_key ?? undefined,
          vbucket: job.vbucket,
          clientRequestId: job.client_request_id ?? undefined,
        }),
      });
      if (res.ok) {
        this.sql.exec("DELETE FROM __cf_mirror_pending WHERE job_id = ?", job.job_id);
        return true;
      }
      throw new Error(`shard responded ${res.status}`);
    } catch (error) {
      const attemptCount = job.attempt_count + 1;
      const delay = Math.min(MIRROR_JOB_MAX_DELAY_MS, MIRROR_JOB_BASE_DELAY_MS * 2 ** job.attempt_count);
      this.sql.exec(
        "UPDATE __cf_mirror_pending SET attempt_count = ?, next_attempt_at = ? WHERE job_id = ?",
        attemptCount,
        new Date(Date.now() + delay).toISOString(),
        job.job_id,
      );
      log("shard.mirror_job_retry_failed", {
        jobId: job.job_id,
        targetShardId: job.target_shard_id,
        attemptCount,
        message: error instanceof Error ? error.message : String(error),
      });
      return false;
    }
  }

  private mirrorJobColumns =
    "job_id, target_shard_id, sql, params_json, request_id, vbucket, tenant_id, table_name, partition_key, client_request_id, attempt_count";

  private async processMirrorPendingJobs(): Promise<number | null> {
    const due = this.many<Parameters<ShardDO["deliverMirrorJob"]>[0]>(
      `SELECT ${this.mirrorJobColumns} FROM __cf_mirror_pending WHERE next_attempt_at <= ? ORDER BY job_id ASC LIMIT ?`,
      new Date().toISOString(),
      MIRROR_JOB_BATCH_SIZE,
    );
    for (const job of due) {
      await this.deliverMirrorJob(job);
    }
    const next = this.one<{ next_attempt_at: string }>(
      "SELECT next_attempt_at FROM __cf_mirror_pending ORDER BY next_attempt_at ASC LIMIT 1",
    );
    return next ? new Date(next.next_attempt_at).getTime() : null;
  }

  /** Milestone 3 (review Tier 1 #2): CatalogDO's cutover step 2 calls this to
   * ACTIVELY drain a vbucket's mirror queue rather than passively waiting for
   * the source shard's alarm cadence — it attempts every queued job for the
   * vbucket now (ignoring next_attempt_at) and returns how many remain. Only
   * jobs whose target is genuinely unreachable stay behind, to be retried on
   * the next cutover tick. */
  private async handleDrainMirrorJobs(request: Request): Promise<Response> {
    const body = (await request.json()) as { vbucket?: number };
    if (body.vbucket === undefined) {
      return json({ error: "Missing vbucket" }, 400);
    }
    const jobs = this.many<Parameters<ShardDO["deliverMirrorJob"]>[0]>(
      `SELECT ${this.mirrorJobColumns} FROM __cf_mirror_pending WHERE vbucket = ? ORDER BY job_id ASC`,
      body.vbucket,
    );
    for (const job of jobs) {
      await this.deliverMirrorJob(job);
    }
    const remaining = this.one<{ n: number }>("SELECT COUNT(*) AS n FROM __cf_mirror_pending WHERE vbucket = ?", body.vbucket);
    return json({ remaining: remaining?.n ?? 0 });
  }

  /** Milestone 3 (review Tier 1 #5): the unforgeable requestId a mirror is
   * delivered to the target under. Reserved-prefixed (a tenant can't forge
   * it — the gateway rejects client requestIds starting with the prefix) and
   * keyed on the row identity plus the original client requestId, so retries
   * of the same write dedupe on the target while two genuinely different
   * writes (even cross-tenant with a colliding client requestId) never do. */
  private mirrorRequestId(tenantId: string, table: string, partitionKey: string, originalRequestId: string): string {
    return `${RESERVED_REQUEST_ID_PREFIX}mirror:${JSON.stringify([tenantId, table, partitionKey, originalRequestId])}`;
  }

  /** Inserts a durable mirror job. MUST be called inside the same
   * transactionSync that applies the source write (handleExecute /
   * handleCommit), so the mirror is recorded atomically with the write and
   * can never be lost-yet-uncounted. Delivery is alarm-driven
   * (processMirrorPendingJobs), delete-on-success. */
  private enqueueMirrorJob(job: {
    targetShardId: string;
    sql: string;
    params: unknown[];
    requestId: string;
    vbucket: number;
    tenantId: string;
    table: string;
    partitionKey: string;
    clientRequestId?: string;
  }): void {
    const now = new Date().toISOString();
    this.sql.exec(
      "INSERT INTO __cf_mirror_pending (target_shard_id, sql, params_json, request_id, vbucket, tenant_id, table_name, partition_key, client_request_id, next_attempt_at, attempt_count, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)",
      job.targetShardId,
      job.sql,
      JSON.stringify(job.params),
      job.requestId,
      job.vbucket,
      job.tenantId,
      job.table,
      job.partitionKey,
      job.clientRequestId ?? null,
      now,
      now,
    );
  }

  private async scheduleMirrorAlarmSoon(): Promise<void> {
    const retrySoon = Date.now() + MIRROR_JOB_BASE_DELAY_MS;
    const existingAlarm = await this.ctx.storage.getAlarm();
    if (existingAlarm === null || existingAlarm > retrySoon) {
      await this.ctx.storage.setAlarm(retrySoon);
    }
  }

  /** Internal/test route: enqueue a mirror job directly. Production writes
   * enqueue atomically via enqueueMirrorJob inside their own transaction;
   * this route exists for direct testing of the retry loop. */
  private async handleEnqueueMirrorJob(request: Request): Promise<Response> {
    const body = (await request.json()) as {
      targetShardId?: string;
      sql?: string;
      params?: unknown[];
      requestId?: string;
      vbucket?: number;
      tenantId?: string;
      table?: string;
      partitionKey?: string;
    };
    if (!body.targetShardId || !body.sql || !body.requestId || body.vbucket === undefined) {
      return json({ error: "Missing targetShardId, sql, requestId, or vbucket" }, 400);
    }
    this.enqueueMirrorJob({
      targetShardId: body.targetShardId,
      sql: body.sql,
      params: body.params ?? [],
      requestId: body.requestId,
      vbucket: body.vbucket,
      tenantId: body.tenantId ?? "",
      table: body.table ?? "",
      partitionKey: body.partitionKey ?? "",
    });
    await this.scheduleMirrorAlarmSoon();
    return json({ ok: true });
  }

  /** Polled by CatalogDO's cutover orchestration ("source drains
   * __cf_mirror_pending for that vbucket to zero" before the map flip) —
   * scoped to one vbucket so cutover isn't blocked by unrelated mirror debt
   * for a different, non-migrating vbucket on the same shard. Now that
   * mirrors are enqueued atomically with the write (review Tier 1 #2), this
   * count includes every outstanding mirror, so the drain-to-zero gate is
   * correct rather than blind to in-flight ones. */
  private async handleMirrorPendingCount(request: Request): Promise<Response> {
    const body = (await request.json()) as { vbucket?: number };
    const row =
      body.vbucket === undefined
        ? this.one<{ n: number }>("SELECT COUNT(*) AS n FROM __cf_mirror_pending")
        : this.one<{ n: number }>("SELECT COUNT(*) AS n FROM __cf_mirror_pending WHERE vbucket = ?", body.vbucket);
    return json({ count: row?.n ?? 0 });
  }

  /** Milestone 3, Chunk 4: canonical row JSON for export/checksum —
   * JSON.stringify of the row object with keys sorted lexicographically.
   * Both source and target run this identical code, which is what makes the
   * per-table content checksums comparable across shards. */
  private canonicalRowJson(row: Record<string, unknown>): string {
    const sorted: Record<string, unknown> = {};
    for (const key of Object.keys(row).sort()) {
      sorted[key] = row[key];
    }
    return JSON.stringify(sorted);
  }

  private async sha256HexOf(input: string): Promise<string> {
    const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(input));
    return Array.from(new Uint8Array(digest))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  }

  /** Milestone 3, Chunk 4 (internal, driven by CatalogDO's migration
   * orchestration). One page of a migrating vbucket's rows for one table,
   * selected via __cf_row_owners (the only place a row's vbucket identity
   * exists), keyed after `afterPartitionKey` for stable cursor paging. */
  private async handleMigrateExport(request: Request): Promise<Response> {
    const body = (await request.json()) as {
      vbucket?: number;
      table?: string;
      partitionKeyColumn?: string;
      afterPartitionKey?: string;
      limit?: number;
    };
    if (body.vbucket === undefined || !body.table || !body.partitionKeyColumn) {
      return json({ error: "Missing vbucket, table, or partitionKeyColumn" }, 400);
    }
    const limit = Math.min(MIGRATE_PAGE_SIZE, Math.max(1, body.limit ?? MIGRATE_PAGE_SIZE));
    const safeTable = body.table.replace(/"/g, '""');
    const safePk = body.partitionKeyColumn.replace(/"/g, '""');

    let raw: Array<Record<string, unknown>> = [];
    try {
      raw = this.many<Record<string, unknown>>(
        `
        SELECT b.*, ro.partition_key AS __cf_export_pk, ro.tenant_id AS __cf_export_tenant
        FROM __cf_row_owners ro
        JOIN "${safeTable}" b ON b."${safePk}" = ro.partition_key
        WHERE ro.table_name = ? AND ro.vbucket = ? AND ro.partition_key > ?
        ORDER BY ro.partition_key ASC
        LIMIT ?
        `,
        body.table,
        body.vbucket,
        body.afterPartitionKey ?? "",
        limit,
      );
    } catch (error) {
      // The table isn't physically present on this shard (registered in
      // table_rules but never created here) — nothing to export for it.
      log("shard.migrate_export_table_missing", { table: body.table, message: error instanceof Error ? error.message : String(error) });
      return json({ rows: [] });
    }

    const rows = raw.map((r) => {
      const { __cf_export_pk, __cf_export_tenant, ...rest } = r;
      return { partitionKey: String(__cf_export_pk), tenantId: String(__cf_export_tenant), row: rest };
    });
    return json({ rows });
  }

  /** Milestone 3, Chunk 4 (internal): applies one exported batch — base rows
   * via INSERT OR REPLACE (idempotent, so a re-pushed page is harmless) plus
   * each row's provenance, in one transaction. */
  private async handleMigrateImport(request: Request): Promise<Response> {
    const body = (await request.json()) as {
      vbucket?: number;
      table?: string;
      rows?: Array<{ partitionKey: string; tenantId: string; row: Record<string, unknown> }>;
    };
    if (body.vbucket === undefined || !body.table || !body.rows) {
      return json({ error: "Missing vbucket, table, or rows" }, 400);
    }
    const table = body.table;
    const vbucket = body.vbucket;
    const rows = body.rows;
    const safeTable = table.replace(/"/g, '""');
    const now = new Date().toISOString();

    try {
      this.ctx.storage.transactionSync(() => {
        for (const entry of rows) {
          const columns = Object.keys(entry.row);
          if (columns.length === 0) continue;
          const columnSql = columns.map((c) => `"${c.replace(/"/g, '""')}"`).join(", ");
          const placeholders = columns.map(() => "?").join(", ");
          this.sql.exec(
            `INSERT OR REPLACE INTO "${safeTable}" (${columnSql}) VALUES (${placeholders})`,
            ...columns.map((c) => entry.row[c]),
          );
          this.sql.exec(
            `
            INSERT INTO __cf_row_owners (table_name, partition_key, tenant_id, vbucket, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (table_name, partition_key) DO UPDATE SET tenant_id = excluded.tenant_id, vbucket = excluded.vbucket, updated_at = excluded.updated_at
            `,
            table,
            entry.partitionKey,
            entry.tenantId,
            vbucket,
            now,
          );
        }
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      log("shard.migrate_import_failed", { table, vbucket, message });
      return json({ error: { code: "MIGRATE_IMPORT_FAILED", message: "Failed to apply the imported batch.", fix: "Ensure the table schema exists on the target shard, then retry." } }, 500);
    }
    return json({ ok: true, imported: rows.length });
  }

  /** Milestone 3, Chunk 4 (internal): per-table content checksum for one
   * vbucket's rows — sha256 over the concatenation of (partition_key,
   * canonical row JSON) ordered by partition key, read in the same 500-row
   * pages /migrate-export uses. Cutover's step-3 verify compares source and
   * target digests per table before any flip. */
  private async computeVbucketTableChecksum(
    vbucket: number,
    table: string,
    partitionKeyColumn: string,
  ): Promise<{ checksum: string; rowCount: number }> {
    const safeTable = table.replace(/"/g, '""');
    const safePk = partitionKeyColumn.replace(/"/g, '""');

    // Review Tier 2 #9: hash INCREMENTALLY — sha256 of each 500-row page's
    // (partition_key, canonical row JSON) concatenation, then sha256 of the
    // concatenated page digests. Memory stays O(page) instead of O(vbucket),
    // which previously accumulated every row's JSON in one array on both
    // shards, per cutover attempt, in a 128MB DO. Both shards page identically
    // (same MIGRATE_PAGE_SIZE, same ORDER BY), so the page boundaries — and
    // therefore the digest sequence — are deterministic and comparable.
    const pageDigests: string[] = [];
    let rowCount = 0;
    let afterPk = "";
    for (;;) {
      let page: Array<Record<string, unknown>> = [];
      try {
        page = this.many<Record<string, unknown>>(
          `
          SELECT b.*, ro.partition_key AS __cf_export_pk
          FROM __cf_row_owners ro
          JOIN "${safeTable}" b ON b."${safePk}" = ro.partition_key
          WHERE ro.table_name = ? AND ro.vbucket = ? AND ro.partition_key > ?
          ORDER BY ro.partition_key ASC
          LIMIT ?
          `,
          table,
          vbucket,
          afterPk,
          MIGRATE_PAGE_SIZE,
        );
      } catch {
        // Table not physically present on this shard — checksum of an empty
        // row set, identical on any other shard where it's also absent.
        break;
      }
      if (page.length === 0) break;
      let pageParts = "";
      for (const r of page) {
        const { __cf_export_pk, ...rest } = r;
        pageParts += `${String(__cf_export_pk)}${this.canonicalRowJson(rest)}`;
        rowCount += 1;
      }
      pageDigests.push(await this.sha256HexOf(pageParts));
      afterPk = String(page[page.length - 1].__cf_export_pk);
      if (page.length < MIGRATE_PAGE_SIZE) break;
    }

    const checksum = await this.sha256HexOf(pageDigests.join(""));
    return { checksum, rowCount };
  }

  private async handleMigrateChecksum(request: Request): Promise<Response> {
    const body = (await request.json()) as { vbucket?: number; table?: string; partitionKeyColumn?: string };
    if (body.vbucket === undefined || !body.table || !body.partitionKeyColumn) {
      return json({ error: "Missing vbucket, table, or partitionKeyColumn" }, 400);
    }
    return json(await this.computeVbucketTableChecksum(body.vbucket, body.table, body.partitionKeyColumn));
  }

  /** Batched variant of /migrate-checksum: one round trip computes every
   * registered table's per-table checksum for the vbucket — the cutover
   * verify still compares each registered table individually (the spec's
   * step-3 rule), it just doesn't pay a DO subrequest per table to do it. */
  private async handleMigrateChecksums(request: Request): Promise<Response> {
    const body = (await request.json()) as { vbucket?: number; tables?: Array<{ table: string; partitionKeyColumn: string }> };
    if (body.vbucket === undefined || !body.tables) {
      return json({ error: "Missing vbucket or tables" }, 400);
    }
    const checksums: Record<string, { checksum: string; rowCount: number }> = {};
    for (const t of body.tables) {
      checksums[t.table] = await this.computeVbucketTableChecksum(body.vbucket, t.table, t.partitionKeyColumn);
    }
    return json({ checksums });
  }

  /** Which tables actually own rows of this vbucket on this shard —
   * backfill only needs to export those; every other registered table has
   * nothing to page through here. */
  private async handleVbucketTables(request: Request): Promise<Response> {
    const body = (await request.json()) as { vbucket?: number };
    if (body.vbucket === undefined) {
      return json({ error: "Missing vbucket" }, 400);
    }
    const rows = this.many<{ table_name: string }>(
      "SELECT DISTINCT table_name FROM __cf_row_owners WHERE vbucket = ? ORDER BY table_name ASC",
      body.vbucket,
    );
    return json({ tables: rows.map((r) => r.table_name) });
  }

  /** Milestone 3, Chunk 4 (internal): cutover step 1 — from this instant,
   * any NEW data write whose payload vbucket matches is rejected 409
   * VBUCKET_FENCED. Idempotent (INSERT OR REPLACE) so the catalog can
   * re-assert the fence on every cutover tick. */
  private async handleFenceVbucket(request: Request): Promise<Response> {
    const body = (await request.json()) as { vbucket?: number };
    if (body.vbucket === undefined) {
      return json({ error: "Missing vbucket" }, 400);
    }
    this.sql.exec(
      "INSERT OR REPLACE INTO __cf_fenced_vbuckets (vbucket, fenced_at) VALUES (?, ?)",
      body.vbucket,
      new Date().toISOString(),
    );
    return json({ ok: true, vbucket: body.vbucket });
  }

  /** Milestone 3, Chunk 4 (internal): lifts the cutover fence — step 5 after
   * a successful flip, or any abort path. Idempotent. */
  private async handleUnfenceVbucket(request: Request): Promise<Response> {
    const body = (await request.json()) as { vbucket?: number };
    if (body.vbucket === undefined) {
      return json({ error: "Missing vbucket" }, 400);
    }
    this.sql.exec("DELETE FROM __cf_fenced_vbuckets WHERE vbucket = ?", body.vbucket);
    return json({ ok: true, vbucket: body.vbucket });
  }

  /** Milestone 3, Chunk 4 (internal): removes one vbucket's base rows and
   * provenance from this shard — cutover step 5 on the source after a
   * successful flip, or the target wipe on abort/checksum mismatch. Also
   * clears any queued mirror jobs for the vbucket (an aborted migration's
   * unsent mirrors must not fire later against a target that was wiped). */
  private async handleDeleteVbucketRows(request: Request): Promise<Response> {
    const body = (await request.json()) as { vbucket?: number; tables?: Array<{ table: string; partitionKeyColumn: string }> };
    if (body.vbucket === undefined || !body.tables) {
      return json({ error: "Missing vbucket or tables" }, 400);
    }
    const vbucket = body.vbucket;
    const tables = body.tables;
    this.ctx.storage.transactionSync(() => {
      for (const t of tables) {
        const safeTable = t.table.replace(/"/g, '""');
        const safePk = t.partitionKeyColumn.replace(/"/g, '""');
        try {
          this.sql.exec(
            `DELETE FROM "${safeTable}" WHERE "${safePk}" IN (SELECT partition_key FROM __cf_row_owners WHERE table_name = ? AND vbucket = ?)`,
            t.table,
            vbucket,
          );
        } catch {
          // Table not physically present here — nothing to delete.
        }
        this.sql.exec("DELETE FROM __cf_row_owners WHERE table_name = ? AND vbucket = ?", t.table, vbucket);
      }
      this.sql.exec("DELETE FROM __cf_mirror_pending WHERE vbucket = ?", vbucket);
    });
    return json({ ok: true, vbucket });
  }

  /** Milestone 3, Chunk 5 (internal, driven by CatalogDO's drain
   * orchestration): one page of this shard's __cf_indexes rows for one
   * index, cursored by rowid — ring evacuation copies a draining shard's
   * entries to its deterministic substitute. */
  private async handleIndexEntriesExport(request: Request): Promise<Response> {
    const body = (await request.json()) as { indexName?: string; afterRowid?: number; limit?: number };
    if (!body.indexName) {
      return json({ error: "Missing indexName" }, 400);
    }
    const limit = Math.min(MIGRATE_PAGE_SIZE, Math.max(1, body.limit ?? MIGRATE_PAGE_SIZE));
    const rows = this.many<{
      rowid: number;
      table_name: string;
      index_name: string;
      index_key_json: string;
      partition_key: string;
      source_shard_id: string;
      tenant_id: string;
      updated_at: string;
    }>(
      "SELECT rowid, table_name, index_name, index_key_json, partition_key, source_shard_id, tenant_id, updated_at FROM __cf_indexes WHERE index_name = ? AND rowid > ? ORDER BY rowid ASC LIMIT ?",
      body.indexName,
      body.afterRowid ?? 0,
      limit,
    );
    return json({ rows });
  }

  /** Milestone 3, Chunk 5 (internal): applies one exported page of index
   * entries — INSERT OR REPLACE, idempotent, so re-pushed pages are
   * harmless. */
  private async handleIndexEntriesImport(request: Request): Promise<Response> {
    const body = (await request.json()) as {
      rows?: Array<{
        table_name: string;
        index_name: string;
        index_key_json: string;
        partition_key: string;
        source_shard_id: string;
        tenant_id: string;
        updated_at: string;
      }>;
    };
    if (!body.rows) {
      return json({ error: "Missing rows" }, 400);
    }
    const rows = body.rows;
    this.ctx.storage.transactionSync(() => {
      for (const r of rows) {
        this.sql.exec(
          "INSERT OR REPLACE INTO __cf_indexes (table_name, index_name, index_key_json, partition_key, source_shard_id, tenant_id, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
          r.table_name,
          r.index_name,
          r.index_key_json,
          r.partition_key,
          r.source_shard_id,
          r.tenant_id,
          r.updated_at,
        );
      }
    });
    return json({ ok: true, imported: rows.length });
  }

  /** Milestone 3, Chunk 4 (internal): drops every queued-but-unsent mirror
   * job for one vbucket — called on the SOURCE whenever the migration's
   * target is wiped (abort, or a cutover checksum mismatch). Without this,
   * a stale queued mirror would later fire against the wiped ex-target and
   * recreate rows there with no provenance (junk that would then trip that
   * shard's own provenance gate). Any content a purged job carried is not
   * lost: the source stayed authoritative, so the next backfill pass (or
   * nothing, on a full abort) re-derives the target's state from it. */
  private async handlePurgeMirrorJobs(request: Request): Promise<Response> {
    const body = (await request.json()) as { vbucket?: number };
    if (body.vbucket === undefined) {
      return json({ error: "Missing vbucket" }, 400);
    }
    this.sql.exec("DELETE FROM __cf_mirror_pending WHERE vbucket = ?", body.vbucket);
    return json({ ok: true, vbucket: body.vbucket });
  }

  /** Milestone 3, Chunk 4 (internal): how many rows across the given
   * registered tables have no __cf_row_owners entry on this shard — the
   * migration provenance gate (409 VBUCKET_PROVENANCE_INCOMPLETE names this
   * count). Shard-wide by necessity: an unattributed row's vbucket is
   * exactly the thing that's unknown. */
  private async handleUnattributedCount(request: Request): Promise<Response> {
    const body = (await request.json()) as { tables?: Array<{ table: string; partitionKeyColumn: string }> };
    if (!body.tables) {
      return json({ error: "Missing tables" }, 400);
    }
    let total = 0;
    for (const t of body.tables) {
      const safeTable = t.table.replace(/"/g, '""');
      const safePk = t.partitionKeyColumn.replace(/"/g, '""');
      try {
        const row = this.one<{ n: number }>(
          `
          SELECT COUNT(*) AS n FROM "${safeTable}" b
          LEFT JOIN __cf_row_owners ro ON ro.table_name = ? AND ro.partition_key = b."${safePk}"
          WHERE ro.partition_key IS NULL
          `,
          t.table,
        );
        total += row?.n ?? 0;
      } catch {
        // Table not physically present on this shard — zero rows, attributed
        // or otherwise.
      }
    }
    return json({ count: total });
  }

  /** A participant that voted "prepared" cannot independently decide to
   * abort — the coordinator may already have collected every participant's
   * vote and told some of them to commit, so an unprompted local abort here
   * could produce a torn outcome (some shards committed, this one aborted).
   * Past the TTL, ask the coordinator for its authoritative decision instead
   * of deciding locally; if the coordinator can't be reached or hasn't
   * decided yet, this is standard 2PC's inherent "blocking" property during
   * genuine uncertainty, not a bug to engineer around. */
  private async sweepStalePendingIntents(): Promise<void> {
    const staleCutoff = new Date(Date.now() - PENDING_INTENT_TTL_MS).toISOString();
    const stale = this.many<{ coordinator_tx_id: string }>(
      "SELECT DISTINCT coordinator_tx_id FROM pending_intents WHERE status = 'prepared' AND prepared_at < ?",
      staleCutoff,
    );

    for (const { coordinator_tx_id: coordinatorTxId } of stale) {
      try {
        const decision = await this.queryCoordinatorDecision(coordinatorTxId);
        if (decision === "committed") {
          const intents = this.many<CommittableIntent>(
            "SELECT intent_seq, sql, params_json, status, table_name, partition_key, tenant_id, vbucket, op, mirror_target_shard_id FROM pending_intents WHERE coordinator_tx_id = ? ORDER BY intent_seq ASC",
            coordinatorTxId,
          );
          // Same apply-and-mirror path as handleCommit (review Tier 1 #2) so
          // a recovery-driven commit mirrors identically.
          const enqueuedMirror = this.applyCommittedIntents(coordinatorTxId, intents);
          if (enqueuedMirror) await this.scheduleMirrorAlarmSoon();
        } else if (decision === "aborted" || decision === "not_found") {
          this.ctx.storage.transactionSync(() => {
            this.sql.exec("DELETE FROM pending_intents WHERE coordinator_tx_id = ?", coordinatorTxId);
            this.sql.exec("DELETE FROM row_locks WHERE coordinator_tx_id = ?", coordinatorTxId);
          });
        } else {
          log("shard.stuck_transaction_pending", { coordinatorTxId, decision });
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        log("shard.sweep_action_failed", { coordinatorTxId, message });
        // Leave the intent for the next tick rather than losing track of it.
      }
    }
  }

  private async queryCoordinatorDecision(
    coordinatorTxId: string,
  ): Promise<"committed" | "aborted" | "not_found" | "pending" | "unreachable"> {
    try {
      // One CoordinatorDO instance per transaction (env.COORDINATOR.idFromName
      // is keyed directly on coordinatorTxId) — see Chunk 3's cost-model
      // decision. No sharding/hashing needed to find the right instance.
      const id = this.shardEnv.COORDINATOR.idFromName(coordinatorTxId);
      const stub = this.shardEnv.COORDINATOR.get(id);
      const res = await stub.fetch("https://coordinator.internal/tx-status", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ txId: coordinatorTxId }),
      });
      if (!res.ok) return "unreachable";
      const body = (await res.json()) as { found: boolean; status?: string };
      if (!body.found) return "not_found";
      if (body.status === "committed") return "committed";
      if (body.status === "aborted") return "aborted";
      return "pending";
    } catch {
      return "unreachable";
    }
  }

  private ensureSchema(): void {
    if (this.schemaEnsured) return;
    this.schemaEnsured = true;
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS applied_requests (
        request_id TEXT PRIMARY KEY,
        request_hash TEXT NOT NULL DEFAULT '',
        result_json TEXT NOT NULL,
        applied_at TEXT NOT NULL
      )
    `);
    this.ensureColumn("applied_requests", "request_hash", "TEXT NOT NULL DEFAULT ''");

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS pending_intents (
        coordinator_tx_id TEXT NOT NULL,
        intent_seq INTEGER NOT NULL,
        sql TEXT NOT NULL,
        params_json TEXT NOT NULL,
        status TEXT NOT NULL,
        lock_keys_json TEXT NOT NULL,
        prepared_at TEXT NOT NULL,
        resolved_at TEXT,
        PRIMARY KEY (coordinator_tx_id, intent_seq)
      )
    `);
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS row_locks (
        lock_key TEXT PRIMARY KEY,
        coordinator_tx_id TEXT NOT NULL,
        acquired_at TEXT NOT NULL
      )
    `);

    // Milestone 3, Chunk 0: per-intent routing context, needed at commit time
    // to write/delete __cf_row_owners provenance for a 2PC-committed base-row
    // mutation. Nullable/additive — a pending_intents row created before this
    // migration simply won't have provenance tracked for it (that
    // transaction was already in flight across a deploy boundary, an
    // acceptable edge the same way applied_requests.request_hash's
    // pre-migration '' default is).
    this.ensureColumn("pending_intents", "tenant_id", "TEXT");
    this.ensureColumn("pending_intents", "table_name", "TEXT");
    this.ensureColumn("pending_intents", "partition_key", "TEXT");
    this.ensureColumn("pending_intents", "vbucket", "INTEGER");
    this.ensureColumn("pending_intents", "op", "TEXT");
    // Review Tier 1 #2: which target (if any) a committed base-row intent
    // must be mirrored to, enqueued atomically with the commit.
    this.ensureColumn("pending_intents", "mirror_target_shard_id", "TEXT");

    // Milestone 3, Chunk 0. One row per (table_name, partition_key) currently
    // owned by a base row on THIS shard, recording the logical (tenantId,
    // vbucket) identity that hashed it here — recoverable nowhere else, since
    // base table rows themselves carry no tenant/vbucket column (see
    // docs/SPEC.md §14's trust model). This is what lets a later migration
    // (Chunk 4) find every row belonging to a given vbucket without scanning
    // every table's every row through the hash function against every
    // candidate tenant (that's Chunk 1's one-time re-attribution backfill,
    // for rows written before this table existed). Known limitation,
    // inherited not widened: the PK mirrors the base tables' own physical
    // layout, which already collides if two tenants share a partition key on
    // the same shard (§14).
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS __cf_row_owners (
        table_name    TEXT NOT NULL,
        partition_key TEXT NOT NULL,
        tenant_id     TEXT NOT NULL,
        vbucket       INTEGER NOT NULL,
        updated_at    TEXT NOT NULL,
        PRIMARY KEY (table_name, partition_key)
      )
    `);
    // Review Tier 2 #11: migrate-export / migrate-checksum / delete-vbucket-rows
    // / vbucket-tables all filter by (vbucket, table_name) and page by
    // partition_key. Without this index each of those is a full table scan of
    // __cf_row_owners; the composite index makes them range scans. The PK is
    // (table_name, partition_key), so a vbucket-scoped query has no covering
    // index otherwise.
    this.sql.exec(
      "CREATE INDEX IF NOT EXISTS idx_cf_row_owners_vb ON __cf_row_owners (vbucket, table_name, partition_key)",
    );

    // Milestone 2 (Index Service). Lives on a shard chosen by hashing
    // (table, indexName, indexKeyJson) — independent of the base row's own
    // shard, so /v1/index-query resolves a lookup on one shard rather than
    // scattering (see the Milestone 2 design doc's index-placement decision).
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS __cf_indexes (
        table_name TEXT NOT NULL,
        index_name TEXT NOT NULL,
        index_key_json TEXT NOT NULL,
        partition_key TEXT NOT NULL,
        source_shard_id TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (table_name, index_name, index_key_json, partition_key)
      )
    `);
    // Milestone 3, Chunk 2 (index-topology v2): the logical tenant identity
    // that owns the entry's base row — this is what lets hydration re-route
    // to the base row's CURRENT shard (vbucket = hashKey(tenant_id:table:
    // partition_key) % total_vbuckets -> vbucket_map -> shard) instead of
    // following the physical source_shard_id snapshot above, which goes
    // stale the moment the base row migrates to a different shard.
    // source_shard_id itself is left in place, unread by any new code path,
    // per the additive-migration convention (never drop columns). Default
    // '' only matters for a pre-Chunk-2 __cf_indexes row; such an index must
    // be recreated via the documented /admin/drop-index + /admin/create-index
    // upgrade flow to get real tenant_id values.
    this.ensureColumn("__cf_indexes", "tenant_id", "TEXT NOT NULL DEFAULT ''");

    // Milestone 2, Chunk 2: retry queue for a best-effort index write that
    // failed on its first attempt from the Worker's ctx.waitUntil() call.
    // Lives on the base shard (the write's origin), not the index shard.
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS index_pending_jobs (
        job_id INTEGER PRIMARY KEY AUTOINCREMENT,
        target_shard_id TEXT NOT NULL,
        sql TEXT NOT NULL,
        params_json TEXT NOT NULL,
        request_id TEXT NOT NULL,
        next_attempt_at TEXT NOT NULL,
        attempt_count INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
      )
    `);

    // Milestone 3, Chunk 3 (redesigned per review Tier 1 #2/#3): durable
    // record of a dual-write mirror (source -> target during an active
    // vbucket migration). Lives on the SOURCE shard and is now inserted
    // ATOMICALLY inside the write's own transaction (handleExecute /
    // handleCommit), delivered by the alarm, and deleted only on delivery
    // success — so /mirror-pending-count reflects EVERY outstanding mirror,
    // including one merely in flight, and cutover's drain-to-zero gate is
    // correct (an insert-on-failure-only queue silently missed slow mirrors,
    // which then double-applied after the map flip). tenant_id/table_name/
    // partition_key are carried so the retry delivery can write the target's
    // __cf_row_owners provenance (#3).
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS __cf_mirror_pending (
        job_id          INTEGER PRIMARY KEY AUTOINCREMENT,
        target_shard_id TEXT NOT NULL,
        sql             TEXT NOT NULL,
        params_json     TEXT NOT NULL,
        request_id      TEXT NOT NULL,
        vbucket         INTEGER NOT NULL,
        tenant_id       TEXT,
        table_name      TEXT,
        partition_key   TEXT,
        client_request_id TEXT,
        next_attempt_at TEXT NOT NULL,
        attempt_count   INTEGER NOT NULL DEFAULT 0,
        created_at      TEXT NOT NULL
      )
    `);
    this.ensureColumn("__cf_mirror_pending", "tenant_id", "TEXT");
    this.ensureColumn("__cf_mirror_pending", "table_name", "TEXT");
    this.ensureColumn("__cf_mirror_pending", "partition_key", "TEXT");
    this.ensureColumn("__cf_mirror_pending", "client_request_id", "TEXT");

    // Milestone 3, Chunk 4: cutover write fence. A data write whose payload
    // vbucket appears here is rejected 409 VBUCKET_FENCED (retryable) —
    // enforced at the data (this shard-side check), not at routing, so a
    // write that resolved its route BEFORE the fence and physically arrives
    // AFTER it is still caught.
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS __cf_fenced_vbuckets (
        vbucket   INTEGER PRIMARY KEY,
        fenced_at TEXT NOT NULL
      )
    `);
  }

  /** Add a column if a table predating it doesn't have it yet — mirrors
   * CatalogDO's migration guard for the same reason (CREATE TABLE IF NOT
   * EXISTS doesn't retroactively alter already-provisioned tables). */
  private ensureColumn(table: string, column: string, definition: string): void {
    const existing = Array.from(this.sql.exec(`PRAGMA table_info(${table})`)) as Array<{ name: string }>;
    if (!existing.some((col) => col.name === column)) {
      this.sql.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
    }
  }

  /** SHA-256 of the request's (sql, params) — detects a requestId reused with
   * different content instead of silently replaying a stale cached result.
   * Must be collision-resistant: a 32-bit hash (e.g. hashKey()) collides
   * easily enough that a reused requestId with different content could pass
   * this check undetected and silently serve a stale result instead of
   * either rejecting or applying the new request. */
  private async requestHash(sql: string, params: unknown[]): Promise<string> {
    const data = new TextEncoder().encode(JSON.stringify({ sql, params }));
    const digest = await crypto.subtle.digest("SHA-256", data);
    return Array.from(new Uint8Array(digest))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  }

  private one<T extends object>(sql: string, ...params: unknown[]): T | null {
    const cursor = this.sql.exec(sql, ...params);
    for (const row of cursor) {
      return row as T;
    }
    return null;
  }

  private many<T extends object>(sql: string, ...params: unknown[]): T[] {
    return Array.from(this.sql.exec(sql, ...params)) as T[];
  }

  private rows(sql: string, ...params: unknown[]): unknown[] {
    return Array.from(this.sql.exec(sql, ...params));
  }

  async fetch(request: Request): Promise<Response> {
    try {
      return await this.handle(request);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      log("shard.unhandled_error", { path: new URL(request.url).pathname, message });
      return json({ error: "Internal error." }, 500);
    }
  }

  private async handle(request: Request): Promise<Response> {
    this.ensureSchema();

    const url = new URL(request.url);
    if (request.method.toUpperCase() !== "POST") {
      return json({ error: "Only POST allowed for shard endpoints." }, 405);
    }

    const handler = this.routes[url.pathname];
    if (handler) {
      return handler(request);
    }
    return json({ error: `Unknown shard route: ${url.pathname}` }, 404);
  }

  private async handleStats(): Promise<Response> {
    const tables = this.rows(
      "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\' ORDER BY name ASC",
    ) as Array<{ name: string }>;

    const counts: Array<{ table: string; rowCount: number }> = [];
    for (const t of tables) {
      if (INTERNAL_TABLES.has(t.name)) continue;
      const safeName = t.name.replace(/"/g, '""');
      const result = this.one<{ n: number }>(`SELECT COUNT(*) AS n FROM "${safeName}"`);
      counts.push({ table: t.name, rowCount: result?.n ?? 0 });
    }

    const idempotencyCount = this.one<{ n: number }>("SELECT COUNT(*) AS n FROM applied_requests");
    const pendingIntentCount = this.one<{ n: number }>(
      "SELECT COUNT(DISTINCT coordinator_tx_id) AS n FROM pending_intents WHERE status = 'prepared'",
    );
    const indexPendingJobCount = this.one<{ n: number }>("SELECT COUNT(*) AS n FROM index_pending_jobs");
    const indexEntryCount = this.one<{ n: number }>("SELECT COUNT(*) AS n FROM __cf_indexes");
    const rowOwnerCount = this.one<{ n: number }>("SELECT COUNT(*) AS n FROM __cf_row_owners");

    return json({
      ok: true,
      tables: counts,
      idempotencyTableSize: idempotencyCount?.n ?? 0,
      pendingIntentCount: pendingIntentCount?.n ?? 0,
      indexPendingJobCount: indexPendingJobCount?.n ?? 0,
      indexEntryCount: indexEntryCount?.n ?? 0,
      rowOwnerCount: rowOwnerCount?.n ?? 0,
    });
  }

  /** Deletes a specific applied_requests entry, letting a subsequent /execute
   * with the same requestId genuinely re-run instead of replaying a cached
   * result. Needed when a caller undoes the mutation's effect out from under
   * the idempotency layer (e.g. /admin/create-table's DROP TABLE rollback on
   * a partitionKeyColumn mismatch) — without this, the cached "success"
   * becomes a lie the moment the caller rolls back what it recorded. Trusts
   * its caller the same way every other DO-binding-only route does. */
  private async handleInvalidateRequest(request: Request): Promise<Response> {
    const body = (await request.json()) as { requestId?: string };
    if (!body.requestId) {
      return json({ error: "Missing requestId" }, 400);
    }
    this.sql.exec("DELETE FROM applied_requests WHERE request_id = ?", body.requestId);
    return json({ ok: true });
  }

  private async handleExecute(request: Request): Promise<Response> {
    const payload = (await request.json()) as ExecutePayload;
    if (!payload.sql || !payload.requestId) {
      return json({ error: "Missing sql or requestId" }, 400);
    }

    const mutating = isMutation(payload.sql);
    let enqueuedMirror = false;

    try {
      const execStart = Date.now();

      if (mutating) {
        const incomingHash = await this.requestHash(payload.sql, payload.params ?? []);
        const prior = this.one<{ result_json: string; request_hash: string }>(
          "SELECT result_json, request_hash FROM applied_requests WHERE request_id = ?",
          payload.requestId,
        );
        if (prior) {
          if (prior.request_hash !== incomingHash) {
            return json(
              { error: "requestId was already used with different sql/params — refusing to replay a mismatched result." },
              409,
            );
          }
          return json({ duplicated: true, ...(JSON.parse(prior.result_json) as object) });
        }

        // Milestone 3, Chunk 4: cutover write fence. Checked AFTER the
        // dedupe lookup above — a replay of an already-applied write is
        // harmless (returns the cached result) and must not be turned into
        // a spurious 409 by the fence; the fence only blocks NEW writes.
        // Only fires for a payload that carries its vbucket (i.e. routed
        // gateway traffic). A mirrored write (isMirror) is exempt: it's a
        // committed source write being replicated to the migration TARGET,
        // which never fences its own destination vbucket, and the source
        // already enforced the fence at write time.
        if (payload.vbucket !== undefined && !payload.isMirror) {
          const fenced = this.one<{ vbucket: number }>(
            "SELECT vbucket FROM __cf_fenced_vbuckets WHERE vbucket = ?",
            payload.vbucket,
          );
          if (fenced) {
            return this.fencedResponse(payload.vbucket);
          }
        }

        // Raw /v1/sql must respect row locks too — but this only closes the
        // hole for an honestly-labeled caller (see NOT in Scope: nothing
        // verifies the SQL text itself only touches this one row). A mirror
        // is exempt: it's a committed source write, not a fresh client
        // write, and the target holds no 2PC lock for this row.
        if (payload.tenantId && payload.table && payload.partitionKey && !payload.isMirror) {
          const lockKeyValue = rowKey(payload.tenantId, payload.table, payload.partitionKey);
          const lockRow = this.one<{ coordinator_tx_id: string }>(
            "SELECT coordinator_tx_id FROM row_locks WHERE lock_key = ?",
            lockKeyValue,
          );
          if (lockRow) {
            return json(
              {
                error: {
                  code: "TX_PARTICIPANT_LOCKED",
                  message: "This row is locked by an in-flight coordinated transaction.",
                  fix: "Retry after the transaction resolves, or check /admin/tx-status.",
                },
              },
              409,
            );
          }
        }

        const result = this.ctx.storage.transactionSync(() => {
          this.sql.exec(payload.sql, ...(payload.params ?? []));
          const changedRow = this.one<{ count: number }>("SELECT changes() AS count");
          const rowsAffected = changedRow?.count ?? 0;

          const txResult = {
            ok: true,
            type: "mutation",
            rowsAffected,
            executeMs: Date.now() - execStart,
          };

          this.sql.exec(
            `
            INSERT INTO applied_requests (request_id, request_hash, result_json, applied_at)
            VALUES (?, ?, ?, ?)
            `,
            payload.requestId,
            incomingHash,
            JSON.stringify(txResult),
            new Date().toISOString(),
          );

          // Review Tier 1 #5: a mirror applies under an unforgeable derived
          // requestId (payload.requestId above) but also records the ORIGINAL
          // client requestId, INSERT OR IGNORE, so a client replaying its
          // requestId AFTER this vbucket flips to this target still dedupes
          // (cross-migration idempotency). IGNORE (not REPLACE) so a genuine
          // pre-existing entry for a different write that happens to share the
          // requestId — a client-requestId collision, the documented §14-class
          // limitation — is never clobbered; the mirrored row itself already
          // landed under the derived id regardless.
          if (payload.isMirror && payload.clientRequestId) {
            this.sql.exec(
              `INSERT OR IGNORE INTO applied_requests (request_id, request_hash, result_json, applied_at) VALUES (?, ?, ?, ?)`,
              payload.clientRequestId,
              incomingHash,
              JSON.stringify(txResult),
              new Date().toISOString(),
            );
          }

          // Milestone 3, Chunk 0: track row provenance for /v1/sql and
          // /v1/mutate writes (both land here — /v1/mutate compiles its
          // StructuredMutation to SQL and calls this same /execute route).
          // Only when the Worker forwarded full routing context AND the
          // mutation actually touched a row — a `where` that matched nothing
          // (rowsAffected === 0) must not fabricate or clear provenance for a
          // row this write never touched.
          if (payload.tenantId && payload.table && payload.partitionKey && payload.vbucket !== undefined && rowsAffected > 0) {
            this.writeOrDeleteProvenance(
              payload.sql,
              payload.table,
              payload.partitionKey,
              payload.tenantId,
              payload.vbucket,
            );

            // Review Tier 1 #2: if this vbucket is mid-migration, record the
            // mirror ATOMICALLY with the write (not best-effort after it), so
            // it's always counted by /mirror-pending-count. A mirrored write
            // itself (isMirror) is never re-mirrored.
            if (payload.mirrorTargetShardId && !payload.isMirror) {
              enqueuedMirror = true;
              this.enqueueMirrorJob({
                targetShardId: payload.mirrorTargetShardId,
                sql: payload.sql,
                params: payload.params ?? [],
                requestId: this.mirrorRequestId(payload.tenantId, payload.table, payload.partitionKey, payload.requestId),
                vbucket: payload.vbucket,
                tenantId: payload.tenantId,
                table: payload.table,
                partitionKey: payload.partitionKey,
                clientRequestId: payload.requestId,
              });
            }
          }

          return txResult;
        });
        if (enqueuedMirror) await this.scheduleMirrorAlarmSoon();
        return json(result);
      }

      const rows = this.rows(payload.sql, ...(payload.params ?? []));
      return json({
        ok: true,
        type: "query",
        rowCount: rows.length,
        executeMs: Date.now() - execStart,
        rows,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      log("shard.execution_failed", { requestId: payload.requestId, isMutation: mutating, message });
      return json({ error: "SQL execution failed." }, 400);
    }
  }

  /** Shared 409 VBUCKET_FENCED response (review Tier 3 DRY) — the identical
   * retryable rejection used by both the /execute write path and 2PC prepare
   * when a write targets a vbucket fenced for cutover. */
  private fencedResponse(vbucket: number): Response {
    return json(
      {
        error: {
          code: "VBUCKET_FENCED",
          message: `vbucket ${vbucket} is fenced for migration cutover on this shard.`,
          fix: "Retry — the fence lifts when the migration's map flip completes, and the retry will route to the new shard.",
        },
      },
      409,
    );
  }

  /** Milestone 3, Chunk 0: writes (insert/update/upsert) or deletes (delete)
   * a row's `__cf_row_owners` provenance entry, called from inside the same
   * transactionSync as the base mutation itself so provenance never
   * diverges from what was actually written. `op` is derived from the SQL
   * text (isDeleteStatement), the same "ShardDO classifies its own writes"
   * approach isMutation() already uses — trusting a caller-supplied
   * classification here would let a mismatched hint desync provenance from
   * reality the same way a spoofed isMutation flag could disguise a write. */
  private writeOrDeleteProvenance(sql: string, table: string, partitionKey: string, tenantId: string, vbucket: number): void {
    if (isDeleteStatement(sql)) {
      this.sql.exec("DELETE FROM __cf_row_owners WHERE table_name = ? AND partition_key = ?", table, partitionKey);
      return;
    }
    this.sql.exec(
      `
      INSERT INTO __cf_row_owners (table_name, partition_key, tenant_id, vbucket, updated_at)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT (table_name, partition_key) DO UPDATE SET tenant_id = excluded.tenant_id, vbucket = excluded.vbucket, updated_at = excluded.updated_at
      `,
      table,
      partitionKey,
      tenantId,
      vbucket,
      new Date().toISOString(),
    );
  }

  private async handlePrepare(request: Request): Promise<Response> {
    const body = (await request.json()) as { coordinatorTxId?: string; intents?: PrepareIntent[] };
    if (!body.coordinatorTxId || !body.intents || body.intents.length === 0) {
      return json({ error: { code: "MISSING_FIELDS", message: "Missing coordinatorTxId or intents.", fix: "Provide both." } }, 400);
    }
    const coordinatorTxId = body.coordinatorTxId;
    const intents = body.intents;

    // Idempotent on retry.
    const existing = this.many<{ intent_seq: number }>(
      "SELECT intent_seq FROM pending_intents WHERE coordinator_tx_id = ?",
      coordinatorTxId,
    );
    if (existing.length > 0) {
      return json({ ok: true, prepared: existing.length });
    }

    // Milestone 3, Chunk 4: cutover write fence — a 2PC prepare is a NEW
    // write (nothing is durably recorded for this txId yet, per the
    // idempotency check above), so an intent targeting a fenced vbucket is
    // rejected outright. The coordinator aborts the whole transaction and
    // the client's retry re-routes post-flip.
    for (const intent of intents) {
      if (intent.vbucket === undefined) continue;
      const fenced = this.one<{ vbucket: number }>("SELECT vbucket FROM __cf_fenced_vbuckets WHERE vbucket = ?", intent.vbucket);
      if (fenced) {
        return this.fencedResponse(intent.vbucket);
      }
    }

    const lockKeys = intents.map((intent) => rowKey(intent.tenantId, intent.table, intent.partitionKey));

    // Fail-fast, no queueing: reject before touching anything if any row is
    // locked by a DIFFERENT in-flight transaction.
    for (const lockKeyValue of lockKeys) {
      const lockRow = this.one<{ coordinator_tx_id: string; acquired_at: string }>(
        "SELECT coordinator_tx_id, acquired_at FROM row_locks WHERE lock_key = ?",
        lockKeyValue,
      );
      if (lockRow && lockRow.coordinator_tx_id !== coordinatorTxId) {
        const heldForMs = Date.now() - new Date(lockRow.acquired_at).getTime();
        const retryAfterMs = Math.max(0, PENDING_INTENT_TTL_MS - heldForMs);
        return json(
          {
            error: {
              code: "TX_PARTICIPANT_LOCKED",
              message: "One or more rows in this transaction are locked by another in-flight transaction.",
              retryAfterMs,
              fix: "Retry with backoff, or check /admin/tx-status.",
            },
          },
          409,
        );
      }
    }

    // Validate the whole batch together: execute every intent's SQL inside
    // one transactionSync, then force a rollback via a sentinel throw —
    // nothing here is ever visible to a concurrent /execute SELECT. This
    // proves the batch applies cleanly as a unit before any lock is
    // durably recorded.
    try {
      this.ctx.storage.transactionSync(() => {
        for (const intent of intents) {
          this.sql.exec(intent.sql, ...(intent.params ?? []));
        }
        throw new PrepareValidationRollback();
      });
    } catch (error) {
      if (!(error instanceof PrepareValidationRollback)) {
        const message = error instanceof Error ? error.message : String(error);
        log("shard.prepare_validation_failed", { coordinatorTxId, message });
        return json({ error: { code: "PREPARE_VALIDATION_FAILED", message, fix: "Check the compiled SQL/params." } }, 400);
      }
    }

    // Record — lock acquisition and pending_intents rows commit atomically
    // together, in this separate transactionSync (the validation pass above
    // already rolled back everything it did; this is prepare's actual
    // durable side effect).
    const now = new Date().toISOString();
    this.ctx.storage.transactionSync(() => {
      intents.forEach((intent, i) => {
        // OR IGNORE: a batch may legitimately contain multiple mutations
        // against the same row (e.g. insert then update in one /v1/tx call —
        // nothing caps mutation count, only distinct participant keys). The
        // pre-check loop above already guarantees any existing lock on this
        // key belongs to this same coordinatorTxId, so re-acquiring it here
        // is a safe no-op rather than a PRIMARY KEY violation.
        this.sql.exec(
          "INSERT OR IGNORE INTO row_locks (lock_key, coordinator_tx_id, acquired_at) VALUES (?, ?, ?)",
          lockKeys[i],
          coordinatorTxId,
          now,
        );
        this.sql.exec(
          `
          INSERT INTO pending_intents (coordinator_tx_id, intent_seq, sql, params_json, status, lock_keys_json, prepared_at, tenant_id, table_name, partition_key, vbucket, op, mirror_target_shard_id)
          VALUES (?, ?, ?, ?, 'prepared', ?, ?, ?, ?, ?, ?, ?, ?)
          `,
          coordinatorTxId,
          i,
          intent.sql,
          JSON.stringify(intent.params ?? []),
          JSON.stringify([lockKeys[i]]),
          now,
          intent.tenantId,
          intent.table,
          intent.partitionKey,
          intent.vbucket ?? null,
          intent.op ?? null,
          intent.mirrorTargetShardId ?? null,
        );
      });
    });

    return json({ ok: true, prepared: intents.length });
  }

  private async handleCommit(request: Request): Promise<Response> {
    const body = (await request.json()) as { coordinatorTxId?: string };
    if (!body.coordinatorTxId) {
      return json({ error: { code: "MISSING_FIELDS", message: "Missing coordinatorTxId." } }, 400);
    }
    const coordinatorTxId = body.coordinatorTxId;

    const intents = this.many<CommittableIntent>(
      "SELECT intent_seq, sql, params_json, status, table_name, partition_key, tenant_id, vbucket, op, mirror_target_shard_id FROM pending_intents WHERE coordinator_tx_id = ? ORDER BY intent_seq ASC",
      coordinatorTxId,
    );
    if (intents.length === 0) {
      // Idempotent: either never prepared here, or already committed and
      // this is a retry after the row was later cleaned up. Treat as success
      // rather than erroring — the coordinator only calls commit after every
      // participant confirmed prepared.
      return json({ ok: true, alreadyResolved: true });
    }
    if (intents[0].status === "committed") {
      return json({ ok: true, alreadyResolved: true });
    }
    if (intents[0].status === "aborted") {
      return json({ error: { code: "ALREADY_ABORTED", message: "This transaction was already aborted on this shard." } }, 409);
    }

    const enqueuedMirror = this.applyCommittedIntents(coordinatorTxId, intents);
    if (enqueuedMirror) await this.scheduleMirrorAlarmSoon();

    return json({ ok: true });
  }

  /** Applies a committed transaction's intents to the real tables, updates
   * provenance, enqueues any mirror jobs (atomically, review Tier 1 #2),
   * marks the intents committed, and releases the locks — all in one
   * transactionSync. Shared by handleCommit and the alarm's stale-intent
   * recovery sweep so both paths mirror identically. Returns whether any
   * mirror job was enqueued (the caller re-arms the alarm). */
  private applyCommittedIntents(coordinatorTxId: string, intents: CommittableIntent[]): boolean {
    let enqueuedMirror = false;
    this.ctx.storage.transactionSync(() => {
      for (const intent of intents) {
        this.sql.exec(intent.sql, ...(JSON.parse(intent.params_json) as unknown[]));
        // Milestone 3, Chunk 0: only genuine base-row intents carry op +
        // vbucket (see PrepareIntent's doc comment) — a synthetic
        // __cf_indexes-maintenance intent piggybacked onto the same
        // transaction never does, so it's skipped here automatically.
        if (intent.op && intent.vbucket !== null && intent.table_name && intent.partition_key && intent.tenant_id) {
          this.writeOrDeleteProvenance(intent.sql, intent.table_name, intent.partition_key, intent.tenant_id, intent.vbucket);
          // Review Tier 1 #2: a migrating vbucket's committed intent enqueues
          // its mirror atomically with the commit.
          if (intent.mirror_target_shard_id) {
            enqueuedMirror = true;
            this.enqueueMirrorJob({
              targetShardId: intent.mirror_target_shard_id,
              sql: intent.sql,
              params: JSON.parse(intent.params_json) as unknown[],
              requestId: `${RESERVED_REQUEST_ID_PREFIX}mirror:tx:${JSON.stringify([coordinatorTxId, intent.intent_seq])}`,
              vbucket: intent.vbucket,
              tenantId: intent.tenant_id,
              table: intent.table_name,
              partitionKey: intent.partition_key,
            });
          }
        }
      }
      this.sql.exec(
        "UPDATE pending_intents SET status = 'committed', resolved_at = ? WHERE coordinator_tx_id = ?",
        new Date().toISOString(),
        coordinatorTxId,
      );
      this.sql.exec("DELETE FROM row_locks WHERE coordinator_tx_id = ?", coordinatorTxId);
    });
    return enqueuedMirror;
  }

  private async handleAbort(request: Request): Promise<Response> {
    const body = (await request.json()) as { coordinatorTxId?: string };
    if (!body.coordinatorTxId) {
      return json({ error: { code: "MISSING_FIELDS", message: "Missing coordinatorTxId." } }, 400);
    }
    const coordinatorTxId = body.coordinatorTxId;

    const intents = this.many<{ status: string }>(
      "SELECT status FROM pending_intents WHERE coordinator_tx_id = ?",
      coordinatorTxId,
    );
    if (intents.length === 0) {
      return json({ ok: true, alreadyResolved: true });
    }
    if (intents.some((i) => i.status === "committed")) {
      return json(
        { error: { code: "ALREADY_COMMITTED", message: "This transaction was already committed on this shard — aborting now would violate atomicity." } },
        409,
      );
    }

    // Nothing was ever applied to the real table (shadow-write isolation —
    // prepare only validated-then-rolled-back), so abort has nothing to
    // undo. Just delete the bookkeeping — leaves no trace, matching commit's
    // "guaranteed to succeed" symmetry.
    this.ctx.storage.transactionSync(() => {
      this.sql.exec("DELETE FROM pending_intents WHERE coordinator_tx_id = ?", coordinatorTxId);
      this.sql.exec("DELETE FROM row_locks WHERE coordinator_tx_id = ?", coordinatorTxId);
    });

    return json({ ok: true });
  }

  /** Reports THIS shard's own intent state for a given coordinatorTxId — the
   * coordinator calls this during recovery (Chunk 3). Distinct from
   * CoordinatorDO's own /tx-status (coordinator.ts), which answers the
   * opposite direction: a shard asking the coordinator for its decision. */
  private async handleTxStatus(request: Request): Promise<Response> {
    const body = (await request.json()) as { coordinatorTxId?: string };
    if (!body.coordinatorTxId) {
      return json({ error: "Missing coordinatorTxId" }, 400);
    }
    const intents = this.many<{ status: string }>(
      "SELECT status FROM pending_intents WHERE coordinator_tx_id = ?",
      body.coordinatorTxId,
    );
    if (intents.length === 0) {
      return json({ found: false });
    }
    return json({ found: true, status: intents[0].status });
  }

  private async handlePendingIntentCount(): Promise<Response> {
    const row = this.one<{ n: number }>(
      "SELECT COUNT(DISTINCT coordinator_tx_id) AS n FROM pending_intents WHERE status = 'prepared'",
    );
    // Milestone 2, Chunk 5: reported alongside the 2PC pending-intent count
    // (same route, not a new one) so the Worker's drain-shard check can
    // block on either kind of unfinished work with one round-trip.
    const indexJobRow = this.one<{ n: number }>("SELECT COUNT(*) AS n FROM index_pending_jobs");
    return json({ count: row?.n ?? 0, indexPendingJobCount: indexJobRow?.n ?? 0 });
  }

  /** Milestone 3 (review Tier 1 #7): how many prepared 2PC intents on this
   * shard touch the given vbucket. Cutover polls this before flipping the
   * map — a tx that prepared BEFORE the migration started carries no mirror
   * target, so if it committed after the flip its write would strand on the
   * old source. The fence blocks NEW prepares, so this count only decreases;
   * once it's zero (all such txs committed-or-aborted) the flip is safe (a
   * committed one applied to the source with provenance, caught by the
   * cutover checksum and re-copied if needed). */
  private async handlePreparedIntentCountForVbucket(request: Request): Promise<Response> {
    const body = (await request.json()) as { vbucket?: number };
    if (body.vbucket === undefined) {
      return json({ error: "Missing vbucket" }, 400);
    }
    const row = this.one<{ n: number }>(
      "SELECT COUNT(*) AS n FROM pending_intents WHERE status = 'prepared' AND vbucket = ?",
      body.vbucket,
    );
    return json({ count: row?.n ?? 0 });
  }
}
