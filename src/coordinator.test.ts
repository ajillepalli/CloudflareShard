import { env } from "cloudflare:test";
import { describe, expect, it } from "vitest";

async function freshCoordinator() {
  const id = env.COORDINATOR.idFromName(`coordinator-${crypto.randomUUID()}`);
  return env.COORDINATOR.get(id);
}

function post(path: string, body: unknown) {
  return new Request(`https://coordinator.internal${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

describe("CoordinatorDO (Chunk 2 shell — /begin and the sharded pool land in Chunk 3)", () => {
  it("/tx-status reports found:false for an unknown txId", async () => {
    const stub = await freshCoordinator();
    const res = await stub.fetch(post("/tx-status", { txId: "never-existed" }));
    expect(res.status).toBe(200);
    const body = (await res.json()) as { found: boolean };
    expect(body.found).toBe(false);
  });

  it("/tx-status requires txId", async () => {
    const stub = await freshCoordinator();
    const res = await stub.fetch(post("/tx-status", {}));
    expect(res.status).toBe(400);
  });

  it("rejects non-POST methods with 405", async () => {
    const stub = await freshCoordinator();
    const res = await stub.fetch(new Request("https://coordinator.internal/tx-status", { method: "GET" }));
    expect(res.status).toBe(405);
  });

  it("returns 404 for an unknown coordinator route", async () => {
    const stub = await freshCoordinator();
    const res = await stub.fetch(post("/not-a-real-route", {}));
    expect(res.status).toBe(404);
  });

  it("returns a clean 500 instead of an unhandled crash on malformed JSON", async () => {
    const stub = await freshCoordinator();
    const res = await stub.fetch(
      new Request("https://coordinator.internal/tx-status", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "{not valid json",
      }),
    );
    expect(res.status).toBe(500);
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe("Internal error.");
  });
});
