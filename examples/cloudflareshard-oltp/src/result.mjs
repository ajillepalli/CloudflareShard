import { createHash } from "node:crypto";

export const RESULT_SCHEMA_VERSION = "cloudflareshard.oltp-result.v1";

function percentile(sorted, percentileValue) {
  if (sorted.length === 0) return null;
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil((percentileValue / 100) * sorted.length) - 1));
  return sorted[index];
}

function latencySummary(attempts) {
  const values = attempts.map((attempt) => attempt.latencyMs).sort((left, right) => left - right);
  return {
    sampleCount: values.length,
    p50Ms: percentile(values, 50),
    p95Ms: percentile(values, 95),
    p99Ms: percentile(values, 99),
    maxMs: values.length ? values.at(-1) : null,
  };
}

function countBy(items, keyFn) {
  const counts = {};
  for (const item of items) {
    const key = keyFn(item);
    counts[key] = (counts[key] ?? 0) + 1;
  }
  return Object.fromEntries(Object.entries(counts).sort(([left], [right]) => left.localeCompare(right)));
}

function checksumPayload(result) {
  return JSON.stringify({ ...result, integrity: { ...result.integrity, resultSha256: "" } });
}

export function verifyResultChecksum(result) {
  return createHash("sha256").update(checksumPayload(result)).digest("hex") === result.integrity.resultSha256;
}

/** @returns {import("./types.d.ts").BenchmarkResult} */
export function buildBenchmarkResult({ runId, config, environment, execution, reproduceCommand }) {
  const successes = execution.attempts.filter((attempt) => attempt.outcome === "success");
  const failures = execution.attempts.filter((attempt) => attempt.outcome === "failure");
  const timeouts = execution.attempts.filter((attempt) => attempt.outcome === "timeout");
  const lateCompletions = execution.attempts.filter((attempt) => attempt.lateCompletion);
  const attempted = execution.attempts.length;
  const reconciled = successes.length + failures.length + timeouts.length === attempted;
  if (!reconciled) throw new Error("Benchmark outcome accounting did not reconcile.");

  const result = {
    schemaVersion: RESULT_SCHEMA_VERSION,
    status: execution.status,
    preliminary: true,
    caveat: "Preliminary closed-loop evidence only. This run does not establish saturation, production capacity, or comparative performance.",
    run: {
      runId,
      startedAt: new Date(execution.startedMs).toISOString(),
      finishedAt: new Date(execution.finishedMs).toISOString(),
      requestedDurationMs: config.durationMs,
      actualElapsedMs: execution.actualElapsedMs,
      cutoffReason: execution.cutoffReason,
    },
    environment,
    configuration: {
      concurrency: config.concurrency,
      maxOperations: config.maxOperations,
      requestTimeoutMs: config.requestTimeoutMs,
      graceMs: config.graceMs,
      seed: config.seed,
      operationMix: config.operationMix,
    },
    outcomes: {
      attempted,
      successes: successes.length,
      failures: failures.length,
      timeouts: timeouts.length,
      lateCompletions: lateCompletions.length,
      reconciled,
      byOperation: countBy(execution.attempts, (attempt) => attempt.operation),
      errorCodes: countBy(execution.attempts.filter((attempt) => attempt.code), (attempt) => attempt.code),
    },
    metrics: {
      successfulOperations: successes.length,
      successfulOperationsPerSecond: execution.actualElapsedMs > 0 ? successes.length / (execution.actualElapsedMs / 1_000) : null,
      allAttemptLatency: latencySummary(execution.attempts),
      successfulAttemptLatency: latencySummary(successes),
    },
    attempts: execution.attempts,
    limitations: [
      "The driver is closed-loop and does not discover saturation or correct for coordinated omission.",
      "This is one bounded run without independent repetitions or confidence intervals.",
      "Results are specific to the declared target, topology, client environment, configuration, and seed.",
      "The paired-write operation is atomic but this preliminary lane does not assert that each pair spans distinct physical shards.",
      "Cloudflare account quotas and plan-specific CPU limits can change observed outcomes and must be interpreted from the declared environment.",
    ],
    reproduce: {
      command: reproduceCommand,
      requiredEnvironment: ["CLOUDFLARESHARD_ADMIN_TOKEN"],
      freshDisposableTargetRequired: true,
    },
    integrity: {
      sourceRevision: environment.sourceRevision,
      resultSha256: "",
    },
  };
  result.integrity.resultSha256 = createHash("sha256").update(checksumPayload(result)).digest("hex");
  return result;
}
