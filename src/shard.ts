import { DurableObject } from "cloudflare:workers";

type ExecutePayload = {
  sql: string;
  params?: unknown[];
  requestId: string;
  isMutation: boolean;
};

export class ShardDO extends DurableObject {
  private readonly sql: SqlStorage;

  constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
  }

  private ensureSchema(): void {
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS applied_requests (
        request_id TEXT PRIMARY KEY,
        result_json TEXT NOT NULL,
        applied_at TEXT NOT NULL
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

  private rows(sql: string, ...params: unknown[]): unknown[] {
    return Array.from(this.sql.exec(sql, ...params));
  }

  async fetch(request: Request): Promise<Response> {
    this.ensureSchema();

    const url = new URL(request.url);
    const method = request.method.toUpperCase();

    const json = (data: unknown, status = 200): Response =>
      new Response(JSON.stringify(data, null, 2), {
        status,
        headers: { "content-type": "application/json; charset=utf-8" },
      });

    if (method !== "POST") {
      return json({ error: "Only POST allowed for shard endpoints." }, 405);
    }

    if (url.pathname === "/stats") {
      const tables = this.rows(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT IN ('applied_requests', 'sqlite_sequence') ORDER BY name ASC",
      ) as Array<{ name: string }>;

      const counts: Array<{ table: string; rowCount: number }> = [];
      for (const t of tables) {
        const safeName = t.name.replace(/"/g, '""');

        const result = this.one<{ n: number }>(`SELECT COUNT(*) AS n FROM "${safeName}"`);

        counts.push({ table: t.name, rowCount: result?.n ?? 0 });
      }

      const idempotencyCount = this.one<{ n: number }>(
        "SELECT COUNT(*) AS n FROM applied_requests",
      );

      return json({
        ok: true,
        tables: counts,
        idempotencyTableSize: idempotencyCount?.n ?? 0,
      });
    }

    if (url.pathname !== "/execute") {
      return json({ error: `Unknown shard route: ${url.pathname}` }, 404);
    }

    const payload = (await request.json()) as ExecutePayload;
    if (!payload.sql || !payload.requestId) {
      return json({ error: "Missing sql or requestId" }, 400);
    }

    try {
      const execStart = Date.now();

      if (payload.isMutation) {
        const prior = this.one<{ result_json: string }>(
          "SELECT result_json FROM applied_requests WHERE request_id = ?",
          payload.requestId,
        );
        if (prior) {
          return json({ duplicated: true, ...(JSON.parse(prior.result_json) as object) });
        }

        this.sql.exec("BEGIN");
        try {
          this.sql.exec(payload.sql, ...(payload.params ?? []));
          const changedRow = this.one<{ count: number }>(
            "SELECT changes() AS count",
          );

          const result = {
            ok: true,
            type: "mutation",
            rowsAffected: changedRow?.count ?? 0,
            executeMs: Date.now() - execStart,
          };

          this.sql.exec(
            `
            INSERT INTO applied_requests (request_id, result_json, applied_at)
            VALUES (?, ?, ?)
            `,
            payload.requestId,
            JSON.stringify(result),
            new Date().toISOString(),
          );
          this.sql.exec("COMMIT");
          return json(result);
        } catch (error) {
          this.sql.exec("ROLLBACK");
          throw error;
        }
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
      return json(
        {
          error: "Shard execution failed",
          details: error instanceof Error ? error.message : String(error),
        },
        400,
      );
    }
  }
}
