import assert from "node:assert/strict";
import test from "node:test";
import { ApiRequestError } from "../src/http-client.mjs";
import { deterministicRunId, runClosedLoop } from "../src/workload.mjs";

const config = {
  baseUrl: "https://example.workers.dev",
  durationMs: 1_000,
  concurrency: 1,
  maxOperations: 4,
  requestTimeoutMs: 50,
  graceMs: 100,
  seed: 17,
};

test("run identity is deterministic over sanitized execution inputs", () => {
  assert.equal(deterministicRunId(config), deterministicRunId({ ...config }));
  assert.notEqual(deterministicRunId(config), deterministicRunId({ ...config, seed: 18 }));
});

test("closed-loop runner preserves success, failure, and timeout attempts", async () => {
  const execution = await runClosedLoop({
    runId: deterministicRunId(config),
    config,
    executeOperation: async ({ sequence }) => {
      if (sequence === 2) throw new ApiRequestError("failure", "HTTP_503", 503);
      if (sequence === 3) throw new ApiRequestError("timeout", "REQUEST_TIMEOUT");
    },
  });
  assert.equal(execution.status, "complete");
  assert.equal(execution.cutoffReason, "max_operations");
  assert.deepEqual(execution.attempts.map((attempt) => attempt.outcome), ["success", "failure", "timeout", "success"]);
});

test("overall grace bounds an executor that never settles and records the attempt", async () => {
  const bounded = { ...config, durationMs: 5, graceMs: 10, maxOperations: 1 };
  const wallStarted = Date.now();
  const execution = await runClosedLoop({
    runId: deterministicRunId(bounded),
    config: bounded,
    executeOperation: () => new Promise(() => {}),
  });
  assert.ok(Date.now() - wallStarted < 250, "overall grace should bound the run");
  assert.equal(execution.status, "incomplete");
  assert.equal(execution.cutoffReason, "overall_grace_deadline");
  assert.equal(execution.attempts.length, 1);
  assert.equal(execution.attempts[0].outcome, "timeout");
  assert.equal(execution.attempts[0].code, "RUN_GRACE_EXPIRED");
  assert.equal(execution.attempts[0].lateCompletion, true);
});

test("a success finishing after scheduling stops remains a visible late completion", async () => {
  const bounded = { ...config, durationMs: 5, graceMs: 100, maxOperations: 1 };
  const execution = await runClosedLoop({
    runId: deterministicRunId(bounded),
    config: bounded,
    executeOperation: () => new Promise((resolve) => setTimeout(resolve, 12)),
  });
  assert.equal(execution.status, "complete");
  assert.equal(execution.attempts.length, 1);
  assert.equal(execution.attempts[0].outcome, "success");
  assert.equal(execution.attempts[0].lateCompletion, true);
  assert.ok(execution.actualElapsedMs >= 5);
});
