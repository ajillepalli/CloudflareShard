import assert from "node:assert/strict";
import test from "node:test";
import { ApiRequestError, BenchmarkApiClient } from "../src/http-client.mjs";

test("per-request deadline aborts a stalled request with a sanitized timeout", async () => {
  const fetchImpl = (_url, init) =>
    new Promise((_resolve, reject) => {
      init.signal.addEventListener("abort", () => reject(new DOMException("secret body", "AbortError")), { once: true });
    });
  const client = new BenchmarkApiClient({ baseUrl: "https://example.com", fetchImpl });
  await assert.rejects(
    client.post("/v1/mutate", {}, "never-render-this-token", { requestTimeoutMs: 5, absoluteDeadlineMs: Date.now() + 1_000 }),
    (error) => error instanceof ApiRequestError && error.kind === "timeout" && error.code === "REQUEST_TIMEOUT" && !error.message.includes("secret"),
  );
});

test("HTTP failures retain only a bounded typed code, never a response body", async () => {
  const fetchImpl = async () => new Response(JSON.stringify({ error: { code: "RATE_LIMITED", message: "token=do-not-copy" } }), { status: 429 });
  const client = new BenchmarkApiClient({ baseUrl: "https://example.com", fetchImpl });
  await assert.rejects(
    client.post("/v1/mutate", {}, "secret", { requestTimeoutMs: 100, absoluteDeadlineMs: Date.now() + 1_000 }),
    (error) => error.code === "RATE_LIMITED" && error.status === 429 && !error.message.includes("do-not-copy"),
  );
});

test("an unrecognized server code is reduced to its HTTP class", async () => {
  const fetchImpl = async () => new Response(JSON.stringify({ error: { code: "SECRET_MATERIAL", message: "do not copy" } }), { status: 503 });
  const client = new BenchmarkApiClient({ baseUrl: "https://example.com", fetchImpl });
  await assert.rejects(
    client.post("/v1/mutate", {}, "secret", { requestTimeoutMs: 100, absoluteDeadlineMs: Date.now() + 1_000 }),
    (error) => error.code === "HTTP_503" && !error.message.includes("SECRET_MATERIAL"),
  );
});

test("the overall grace deadline wins when it is earlier than the request deadline", async () => {
  const fetchImpl = (_url, init) =>
    new Promise((_resolve, reject) => {
      init.signal.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")), { once: true });
    });
  const client = new BenchmarkApiClient({ baseUrl: "https://example.com", fetchImpl });
  await assert.rejects(
    client.post("/v1/mutate", {}, "secret", { requestTimeoutMs: 1_000, absoluteDeadlineMs: Date.now() + 5 }),
    (error) => error.code === "RUN_GRACE_EXPIRED",
  );
});
