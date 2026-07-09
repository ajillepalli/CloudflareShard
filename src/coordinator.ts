import { DurableObject } from "cloudflare:workers";
import { json } from "./http";
import { log } from "./log";

/** Milestone 1 Chunk 3 fills this class in (sharded pool keying, /begin's
 * 2PC orchestration, recovery_queue, admin routes). This chunk (2) only needs
 * enough of a CoordinatorDO for ShardDO's alarm-driven TTL sweep to have a
 * real authoritative decision to query — a participant that voted "prepared"
 * cannot safely decide anything on its own (see shard.ts's alarm handler). */
export class CoordinatorDO extends DurableObject {
  private readonly sql: SqlStorage;
  private readonly routes: Record<string, (request: Request) => Promise<Response>>;

  constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.routes = {
      "/tx-status": this.handleTxStatus.bind(this),
    };
  }

  private ensureSchema(): void {
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS transactions (
        tx_id TEXT PRIMARY KEY,
        status TEXT NOT NULL,
        participant_shards_json TEXT NOT NULL,
        operation_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        attempt_count INTEGER NOT NULL DEFAULT 0,
        last_error TEXT
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
}
