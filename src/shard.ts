import { DurableObject } from "cloudflare:workers";
import { json } from "./http";
import { log } from "./log";
import { isMutation } from "./sql-safety";
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
};

type PrepareIntent = {
  sql: string;
  params?: unknown[];
  tenantId: string;
  table: string;
  partitionKey: string;
};

const APPLIED_REQUESTS_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const PRUNE_INTERVAL_MS = 60 * 60 * 1000;
const PENDING_INTENT_TTL_MS = 5 * 60 * 1000;

const INTERNAL_TABLES = new Set(["applied_requests", "sqlite_sequence", "pending_intents", "row_locks", "__cf_indexes"]);

/** Deliberate sentinel thrown inside handlePrepare's validation transactionSync
 * to force a rollback — distinguishes "validation succeeded, roll back on
 * purpose" from a genuine SQL execution error. */
class PrepareValidationRollback extends Error {}

export class ShardDO extends DurableObject {
  private readonly sql: SqlStorage;
  private readonly shardEnv: Cloudflare.Env;
  private readonly routes: Record<string, (request: Request) => Promise<Response>>;

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
      "/invalidate-request": this.handleInvalidateRequest.bind(this),
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
    await this.ctx.storage.setAlarm(Date.now() + PRUNE_INTERVAL_MS);
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
          const intents = this.many<{ sql: string; params_json: string }>(
            "SELECT sql, params_json FROM pending_intents WHERE coordinator_tx_id = ? ORDER BY intent_seq ASC",
            coordinatorTxId,
          );
          this.ctx.storage.transactionSync(() => {
            for (const intent of intents) {
              this.sql.exec(intent.sql, ...(JSON.parse(intent.params_json) as unknown[]));
            }
            this.sql.exec(
              "UPDATE pending_intents SET status = 'committed', resolved_at = ? WHERE coordinator_tx_id = ?",
              new Date().toISOString(),
              coordinatorTxId,
            );
            this.sql.exec("DELETE FROM row_locks WHERE coordinator_tx_id = ?", coordinatorTxId);
          });
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

    // Milestone 2 (Index Service). Lives on a shard chosen by hashing
    // (table, indexName, indexKeyJson) — independent of the base row's own
    // shard, so /v1/index-query resolves a lookup on one shard rather than
    // scattering (see the Milestone 2 design doc's index-placement decision).
    // No tenant_id column — matches base table rows, which also carry no
    // tenant_id physically (docs/SPEC.md §14's documented trust model).
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

    return json({
      ok: true,
      tables: counts,
      idempotencyTableSize: idempotencyCount?.n ?? 0,
      pendingIntentCount: pendingIntentCount?.n ?? 0,
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

        // Raw /v1/sql must respect row locks too — but this only closes the
        // hole for an honestly-labeled caller (see NOT in Scope: nothing
        // verifies the SQL text itself only touches this one row).
        if (payload.tenantId && payload.table && payload.partitionKey) {
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

          const txResult = {
            ok: true,
            type: "mutation",
            rowsAffected: changedRow?.count ?? 0,
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
          return txResult;
        });
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
          INSERT INTO pending_intents (coordinator_tx_id, intent_seq, sql, params_json, status, lock_keys_json, prepared_at)
          VALUES (?, ?, ?, ?, 'prepared', ?, ?)
          `,
          coordinatorTxId,
          i,
          intent.sql,
          JSON.stringify(intent.params ?? []),
          JSON.stringify([lockKeys[i]]),
          now,
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

    const intents = this.many<{ intent_seq: number; sql: string; params_json: string; status: string }>(
      "SELECT intent_seq, sql, params_json, status FROM pending_intents WHERE coordinator_tx_id = ? ORDER BY intent_seq ASC",
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

    this.ctx.storage.transactionSync(() => {
      for (const intent of intents) {
        this.sql.exec(intent.sql, ...(JSON.parse(intent.params_json) as unknown[]));
      }
      this.sql.exec(
        "UPDATE pending_intents SET status = 'committed', resolved_at = ? WHERE coordinator_tx_id = ?",
        new Date().toISOString(),
        coordinatorTxId,
      );
      this.sql.exec("DELETE FROM row_locks WHERE coordinator_tx_id = ?", coordinatorTxId);
    });

    return json({ ok: true });
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
    return json({ count: row?.n ?? 0 });
  }
}
