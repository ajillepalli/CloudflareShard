import { DurableObject } from "cloudflare:workers";
import { json } from "./http";
import { log } from "./log";
import { sha256Hex } from "./auth";

type BeginParticipant = {
  shardId: string;
  intents: Array<{
    sql: string;
    params: unknown[];
    tenantId: string;
    table: string;
    partitionKey: string;
    /** Milestone 3, Chunk 3: present when this intent's vbucket is
     * mid-migration — the coordinator mirrors the committed intent to this
     * target shard post-commit, enqueuing on the source shard's
     * __cf_mirror_pending on failure. Never present on a synthetic
     * __cf_indexes-maintenance intent. */
    mirrorTargetShardId?: string;
    vbucket?: number;
  }>;
};

type BeginPayload = {
  txId: string;
  participants: BeginParticipant[];
};

const RECOVERY_BASE_DELAY_MS = 5000;
const RECOVERY_MAX_DELAY_MS = 60000;

/** One CoordinatorDO instance per transaction (env.COORDINATOR.idFromName(txId)
 * directly, no sharding) — see the Milestone 1 plan's cost-model decision.
 * Drives the coordinator side of 2PC: persist the transaction durably, fan
 * out /prepare to every participant shard, then /commit (or /abort everyone
 * on any prepare failure). A failed commit/abort acknowledgement is queued
 * for alarm-driven retry rather than blocking the caller — since each
 * instance only ever tracks its own single transaction, this queue never
 * holds more than one row. */
export class CoordinatorDO extends DurableObject {
  private readonly sql: SqlStorage;
  private readonly coordinatorEnv: Cloudflare.Env;
  private readonly routes: Record<string, (request: Request) => Promise<Response>>;
  /** ensureSchema() is idempotent but not free (several DDL/PRAGMA
   * statements) — running it once per in-memory instance instead of once
   * per request is safe because the schema lives in durable storage: a
   * fresh instance (new isolate) re-runs it on its first request, and
   * nothing ever drops these tables mid-lifetime. */
  private schemaEnsured = false;

  constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.coordinatorEnv = env;
    this.routes = {
      "/tx-status": this.handleTxStatus.bind(this),
      "/begin": this.handleBegin.bind(this),
      "/force-abort": this.handleForceAbort.bind(this),
      "/stats": this.handleStats.bind(this),
    };
  }

  private ensureColumn(table: string, column: string, definition: string): void {
    const existing = Array.from(this.sql.exec(`PRAGMA table_info(${table})`)) as Array<{ name: string }>;
    if (!existing.some((col) => col.name === column)) {
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
    // Migration guard: a transactions table created before operation_hash
    // existed won't get it from CREATE TABLE IF NOT EXISTS alone (a no-op
    // once the table exists) — without this, /begin's SELECT below would
    // throw "no such column" and 500 instead of degrading to the same
    // fail-closed mismatch rejection applied_requests.request_hash already
    // uses for pre-migration rows (shard.ts) — old rows compare against '',
    // never match a real hash, and are rejected rather than silently
    // trusted, but at least don't crash.
    this.ensureColumn("transactions", "operation_hash", "TEXT NOT NULL DEFAULT ''");
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS transaction_participants (
        tx_id TEXT NOT NULL,
        shard_id TEXT NOT NULL,
        phase_status TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (tx_id, shard_id)
      )
    `);
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS recovery_queue (
        tx_id TEXT PRIMARY KEY,
        action TEXT NOT NULL,
        next_attempt_at TEXT NOT NULL,
        attempt_count INTEGER NOT NULL DEFAULT 0
      )
    `);
  }

  private one<T extends object>(sql: string, ...params: unknown[]): T | null {
    const cursor = this.sql.exec(sql, ...params);
    for (const row of cursor) {
      return row as T;
    }
    return null;
  }

  private async callShard(shardId: string, path: string, payload: unknown): Promise<Response> {
    const id = this.coordinatorEnv.SHARD.idFromName(shardId);
    const stub = this.coordinatorEnv.SHARD.get(id);
    return stub.fetch(`https://shard.internal${path}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    });
  }

  private async ensureAlarmScheduled(atLeastByMs: number): Promise<void> {
    const existing = await this.ctx.storage.getAlarm();
    if (existing === null || existing > atLeastByMs) {
      await this.ctx.storage.setAlarm(atLeastByMs);
    }
  }

  async alarm(): Promise<void> {
    this.ensureSchema();
    const row = this.one<{ tx_id: string; action: string; next_attempt_at: string; attempt_count: number }>(
      "SELECT tx_id, action, next_attempt_at, attempt_count FROM recovery_queue LIMIT 1",
    );
    if (!row) return; // nothing to recover — don't re-arm

    if (new Date(row.next_attempt_at).getTime() > Date.now()) {
      await this.ctx.storage.setAlarm(new Date(row.next_attempt_at).getTime());
      return;
    }

    const tx = this.one<{ operation_json: string }>(
      "SELECT operation_json FROM transactions WHERE tx_id = ?",
      row.tx_id,
    );
    if (!tx) {
      this.sql.exec("DELETE FROM recovery_queue WHERE tx_id = ?", row.tx_id);
      return;
    }

    const participants = JSON.parse(tx.operation_json) as BeginParticipant[];
    const results = await Promise.allSettled(
      participants.map((p) => this.callShard(p.shardId, row.action, { coordinatorTxId: row.tx_id })),
    );
    const allOk = results.every((r) => r.status === "fulfilled" && r.value.ok);

    if (allOk) {
      this.sql.exec("DELETE FROM recovery_queue WHERE tx_id = ?", row.tx_id);
      return;
    }

    const nextAttemptCount = row.attempt_count + 1;
    const delay = Math.min(RECOVERY_MAX_DELAY_MS, RECOVERY_BASE_DELAY_MS * 2 ** row.attempt_count);
    const nextAttemptAt = Date.now() + delay;
    this.sql.exec(
      "UPDATE recovery_queue SET attempt_count = ?, next_attempt_at = ? WHERE tx_id = ?",
      nextAttemptCount,
      new Date(nextAttemptAt).toISOString(),
      row.tx_id,
    );
    log("coordinator.recovery_retry_failed", { txId: row.tx_id, action: row.action, attemptCount: nextAttemptCount });
    await this.ctx.storage.setAlarm(nextAttemptAt);
  }

  async fetch(request: Request): Promise<Response> {
    try {
      return await this.handle(request);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      log("coordinator.unhandled_error", { path: new URL(request.url).pathname, message });
      return json({ error: "Internal error." }, 500);
    }
  }

  private async handle(request: Request): Promise<Response> {
    this.ensureSchema();

    const url = new URL(request.url);
    if (request.method.toUpperCase() !== "POST") {
      return json({ error: "Only POST allowed for coordinator endpoints." }, 405);
    }

    const handler = this.routes[url.pathname];
    if (handler) {
      return handler(request);
    }
    return json({ error: `Unknown coordinator route: ${url.pathname}` }, 404);
  }

  /** Internal, DO-binding-only route — only ever called by ShardDO's own
   * alarm-driven sweep (via env.COORDINATOR), never exposed through the
   * public Worker. No admin-token gate: there's no human caller to gate. */
  private async handleTxStatus(request: Request): Promise<Response> {
    const body = (await request.json()) as { txId?: string };
    if (!body.txId) {
      return json({ error: "Missing txId" }, 400);
    }
    const row = this.one<{ status: string }>("SELECT status FROM transactions WHERE tx_id = ?", body.txId);
    if (!row) {
      return json({ found: false });
    }
    return json({ found: true, status: row.status });
  }

  private async handleBegin(request: Request): Promise<Response> {
    const body = (await request.json()) as BeginPayload;
    if (!body.txId || !body.participants || body.participants.length === 0) {
      return json({ error: { code: "MISSING_FIELDS", message: "Missing txId or participants." } }, 400);
    }
    const txId = body.txId;
    const participants = body.participants;
    const operationJson = JSON.stringify(participants);
    const operationHash = await sha256Hex(operationJson);

    const existing = this.one<{ status: string; operation_hash: string }>(
      "SELECT status, operation_hash FROM transactions WHERE tx_id = ?",
      txId,
    );
    if (existing) {
      // txId is derived from (tenantId, requestId) — a client reusing a
      // requestId with a different mutation set must never silently resume
      // 2PC with the new data, or return a stale "committed" for content it
      // never actually applied. Mirrors ShardDO.handleExecute's request_hash
      // mismatch rejection for the same class of bug.
      if (existing.operation_hash !== operationHash) {
        return json(
          {
            error: {
              code: "TX_ID_REQUEST_MISMATCH",
              message: "This txId was already used with a different mutation set — refusing to resume or replay with mismatched data.",
              fix: "Use a new requestId for a different transaction.",
            },
          },
          409,
        );
      }
      if (existing.status === "committed") {
        return json({ ok: true, txId, status: "committed" });
      }
      if (existing.status === "aborted") {
        return json({ error: { code: "TX_ABORTED", message: "Transaction was aborted." } }, 409);
      }
      // Otherwise (preparing/prepared/committing/aborting): a prior /begin
      // attempt didn't finish — fall through and resume orchestration rather
      // than restarting, since prepare/commit/abort are all idempotent.
    } else {
      const now = new Date().toISOString();
      this.ctx.storage.transactionSync(() => {
        this.sql.exec(
          `
          INSERT INTO transactions (tx_id, status, participant_shards_json, operation_json, operation_hash, created_at, updated_at)
          VALUES (?, 'preparing', ?, ?, ?, ?, ?)
          `,
          txId,
          JSON.stringify(participants.map((p) => p.shardId)),
          operationJson,
          operationHash,
          now,
          now,
        );
        for (const p of participants) {
          this.sql.exec(
            "INSERT INTO transaction_participants (tx_id, shard_id, phase_status, updated_at) VALUES (?, ?, 'pending', ?)",
            txId,
            p.shardId,
            now,
          );
        }
      });
    }

    const prepareResults = await Promise.all(
      participants.map(async (p) => {
        try {
          const res = await this.callShard(p.shardId, "/prepare", { coordinatorTxId: txId, intents: p.intents });
          const resBody = await res.json().catch(() => ({}));
          return { shardId: p.shardId, ok: res.ok, body: resBody };
        } catch (error) {
          return { shardId: p.shardId, ok: false, body: { error: error instanceof Error ? error.message : String(error) } };
        }
      }),
    );

    const failedPrepare = prepareResults.find((r) => !r.ok);
    if (failedPrepare) {
      await Promise.allSettled(participants.map((p) => this.callShard(p.shardId, "/abort", { coordinatorTxId: txId })));
      this.ctx.storage.transactionSync(() => {
        this.sql.exec("UPDATE transactions SET status = 'aborted', updated_at = ? WHERE tx_id = ?", new Date().toISOString(), txId);
      });
      return json(
        {
          error: {
            code: "TX_ABORTED",
            message: `Prepare failed on shard ${failedPrepare.shardId} — the entire transaction was aborted.`,
            details: failedPrepare.body,
          },
        },
        409,
      );
    }

    this.ctx.storage.transactionSync(() => {
      this.sql.exec("UPDATE transactions SET status = 'prepared', updated_at = ? WHERE tx_id = ?", new Date().toISOString(), txId);
    });

    const commitResults = await Promise.allSettled(
      participants.map((p) => this.callShard(p.shardId, "/commit", { coordinatorTxId: txId })),
    );
    const allCommitted = commitResults.every((r) => r.status === "fulfilled" && r.value.ok);

    if (!allCommitted) {
      const now = new Date().toISOString();
      this.ctx.storage.transactionSync(() => {
        this.sql.exec("UPDATE transactions SET status = 'committed', updated_at = ? WHERE tx_id = ?", now, txId);
        this.sql.exec(
          "INSERT OR REPLACE INTO recovery_queue (tx_id, action, next_attempt_at, attempt_count) VALUES (?, '/commit', ?, 0)",
          txId,
          now,
        );
      });
      await this.ensureAlarmScheduled(Date.now());
      return json({ ok: true, txId, status: "committed_pending_ack" });
    }

    this.ctx.storage.transactionSync(() => {
      this.sql.exec("UPDATE transactions SET status = 'committed', updated_at = ? WHERE tx_id = ?", new Date().toISOString(), txId);
    });

    return json({ ok: true, txId, status: "committed" });
  }

  /** Manual escape hatch for a transaction stuck past a reasonable window —
   * documented runbook response when /admin/tx-status shows something stuck
   * in `committing`/`preparing`. Worker-level /admin/tx-force-abort is
   * ADMIN_TOKEN-gated; this internal route trusts that gate the same way
   * ShardDO trusts the Worker/CatalogDO's gating for /execute. */
  private async handleForceAbort(request: Request): Promise<Response> {
    const body = (await request.json()) as { txId?: string };
    if (!body.txId) {
      return json({ error: "Missing txId" }, 400);
    }
    const tx = this.one<{ status: string; operation_json: string }>(
      "SELECT status, operation_json FROM transactions WHERE tx_id = ?",
      body.txId,
    );
    if (!tx) {
      return json({ error: "Transaction not found." }, 404);
    }
    if (tx.status === "committed") {
      return json({ error: { code: "ALREADY_COMMITTED", message: "Cannot force-abort a committed transaction." } }, 409);
    }

    const participants = JSON.parse(tx.operation_json) as BeginParticipant[];
    await Promise.allSettled(participants.map((p) => this.callShard(p.shardId, "/abort", { coordinatorTxId: body.txId! })));

    this.ctx.storage.transactionSync(() => {
      this.sql.exec("UPDATE transactions SET status = 'aborted', updated_at = ? WHERE tx_id = ?", new Date().toISOString(), body.txId);
      this.sql.exec("DELETE FROM recovery_queue WHERE tx_id = ?", body.txId);
    });

    return json({ ok: true, txId: body.txId, status: "aborted" });
  }

  private async handleStats(): Promise<Response> {
    const tx = this.one<{ status: string }>("SELECT status FROM transactions LIMIT 1");
    const recovery = this.one<{ attempt_count: number; action: string }>(
      "SELECT attempt_count, action FROM recovery_queue LIMIT 1",
    );
    return json({
      ok: true,
      status: tx?.status ?? null,
      recovery: recovery ? { action: recovery.action, attemptCount: recovery.attempt_count } : null,
    });
  }
}
