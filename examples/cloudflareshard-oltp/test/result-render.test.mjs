import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { writeArtifacts } from "../src/artifacts.mjs";
import { buildBenchmarkResult, verifyResultChecksum } from "../src/result.mjs";
import { renderJson, renderMarkdown } from "../src/render.mjs";
import { OPERATION_MIX } from "../src/workload.mjs";

function fixtureResult() {
  return buildBenchmarkResult({
    runId: "oltp-0123456789abcdef",
    config: { durationMs: 1_000, concurrency: 2, maxOperations: 4, requestTimeoutMs: 100, graceMs: 200, seed: 7, operationMix: OPERATION_MIX },
    environment: {
      targetOrigin: "https://example.workers.dev",
      accountPlan: "unknown",
      topology: { catalogShardCount: 4, shardCount: 8 },
      nodeVersion: "v20.0.0",
      platform: "test",
      arch: "x64",
      sourceRevision: "0123456789abcdef0123456789abcdef01234567",
    },
    execution: {
      status: "complete",
      cutoffReason: "max_operations",
      startedMs: 1_700_000_000_000,
      finishedMs: 1_700_000_001_250,
      actualElapsedMs: 1_250,
      attempts: [
        { attemptId: "a1", sequence: 1, operation: "single-write", outcome: "success", code: null, latencyMs: 10, lateCompletion: false },
        { attemptId: "a2", sequence: 2, operation: "paired-write", outcome: "failure", code: "HTTP_503", latencyMs: 20, lateCompletion: false },
        { attemptId: "a3", sequence: 3, operation: "bounded-read", outcome: "timeout", code: "REQUEST_TIMEOUT", latencyMs: 100, lateCompletion: true },
        { attemptId: "a4", sequence: 4, operation: "single-write", outcome: "success", code: null, latencyMs: 15, lateCompletion: true },
      ],
    },
    reproduceCommand: "npm run benchmark -- --base-url https://example.workers.dev --seed 7",
  });
}

test("one result reconciles every outcome and carries a verifiable checksum", () => {
  const result = fixtureResult();
  assert.deepEqual(result.outcomes, {
    attempted: 4,
    successes: 2,
    failures: 1,
    timeouts: 1,
    lateCompletions: 2,
    reconciled: true,
    byOperation: { "bounded-read": 1, "paired-write": 1, "single-write": 2 },
    errorCodes: { HTTP_503: 1, REQUEST_TIMEOUT: 1 },
  });
  assert.equal(verifyResultChecksum(result), true);
});

test("JSON and Markdown render the same typed result with the preliminary caveat first", () => {
  const result = fixtureResult();
  const json = renderJson(result);
  const markdown = renderMarkdown(result);
  assert.deepEqual(JSON.parse(json), result);
  assert.ok(markdown.startsWith("# PRELIMINARY CLOUDFLARESHARD OLTP BASELINE\n\n> **PRELIMINARY:**"));
  for (const visible of ["HTTP_503", "REQUEST_TIMEOUT", "2 | 1 | 1 | 2", result.integrity.resultSha256, result.environment.sourceRevision]) {
    assert.ok(markdown.includes(visible), `expected Markdown to include ${visible}`);
  }
  assert.ok(markdown.includes("does not establish saturation, production capacity, or comparative performance"));
});

test("artifact writer persists JSON and Markdown from the identical result", async () => {
  const result = fixtureResult();
  const directory = await mkdtemp(path.join(tmpdir(), "cloudflareshard-oltp-"));
  try {
    const written = await writeArtifacts(result, directory);
    const [json, markdown] = await Promise.all([readFile(written.jsonPath, "utf8"), readFile(written.markdownPath, "utf8")]);
    assert.deepEqual(JSON.parse(json), result);
    assert.equal(markdown, renderMarkdown(result));
    assert.ok(path.basename(written.jsonPath).startsWith(result.run.runId));
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
