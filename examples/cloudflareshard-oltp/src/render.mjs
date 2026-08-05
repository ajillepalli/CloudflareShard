function code(value) {
  return `\`${String(value).replaceAll("`", "\\`")}\``;
}

function metric(value, digits = 2) {
  return value === null ? "n/a" : Number(value).toFixed(digits);
}

function renderCounts(counts) {
  const entries = Object.entries(counts);
  return entries.length ? entries.map(([name, count]) => `| ${code(name)} | ${count} |`).join("\n") : "| None | 0 |";
}

/** @param {import("./types.d.ts").BenchmarkResult} result */
export function renderJson(result) {
  return `${JSON.stringify(result, null, 2)}\n`;
}

/** @param {import("./types.d.ts").BenchmarkResult} result */
export function renderMarkdown(result) {
  const latency = result.metrics.allAttemptLatency;
  return `# PRELIMINARY CLOUDFLARESHARD OLTP BASELINE

> **PRELIMINARY:** ${result.caveat}

Status: **${result.status === "complete" ? "PRELIMINARY RUN COMPLETE" : "INCOMPLETE RUN"}**

## Run identity

| Field | Value |
|---|---|
| Run ID | ${code(result.run.runId)} |
| Started | ${code(result.run.startedAt)} |
| Finished | ${code(result.run.finishedAt)} |
| Requested duration | ${metric(result.run.requestedDurationMs, 0)} ms |
| Actual elapsed | ${metric(result.run.actualElapsedMs, 0)} ms |
| Cutoff | ${code(result.run.cutoffReason)} |

## Environment

| Field | Value |
|---|---|
| Target origin | ${code(result.environment.targetOrigin)} |
| Account plan | ${code(result.environment.accountPlan)} |
| Catalog shards | ${result.environment.topology.catalogShardCount ?? "unknown"} |
| Data shards | ${result.environment.topology.shardCount ?? "unknown"} |
| Node | ${code(result.environment.nodeVersion)} |
| Platform | ${code(`${result.environment.platform}/${result.environment.arch}`)} |
| Source revision | ${code(result.environment.sourceRevision)} |

## Workload configuration

| Field | Value |
|---|---|
| Seed | ${result.configuration.seed} |
| Concurrency | ${result.configuration.concurrency} |
| Maximum operations | ${result.configuration.maxOperations} |
| Request deadline | ${result.configuration.requestTimeoutMs} ms |
| Overall grace | ${result.configuration.graceMs} ms |

| Neutral operation | Weight |
|---|---:|
${result.configuration.operationMix.map((item) => `| ${code(item.operation)} | ${item.weight}% |`).join("\n")}

## Outcome accounting

Late completions are a labeled subset of attempted outcomes. The primary
reconciliation is attempted = successes + failures + timeouts.

| Attempted | Successes | Failures | Timeouts | Late completions | Reconciled |
|---:|---:|---:|---:|---:|---|
| ${result.outcomes.attempted} | ${result.outcomes.successes} | ${result.outcomes.failures} | ${result.outcomes.timeouts} | ${result.outcomes.lateCompletions} | ${result.outcomes.reconciled ? "yes" : "no"} |

### Attempts by operation

| Operation | Attempts |
|---|---:|
${renderCounts(result.outcomes.byOperation)}

### Failure and timeout classes

| Sanitized code | Count |
|---|---:|
${renderCounts(result.outcomes.errorCodes)}

## Neutral metrics

| Metric | Value |
|---|---:|
| Successful operations | ${result.metrics.successfulOperations} |
| Successful operations / actual second | ${metric(result.metrics.successfulOperationsPerSecond)} |
| All-attempt latency p50 | ${metric(latency.p50Ms)} ms |
| All-attempt latency p95 | ${metric(latency.p95Ms)} ms |
| All-attempt latency p99 | ${metric(latency.p99Ms)} ms |
| All-attempt latency max | ${metric(latency.maxMs)} ms |

Latency rows do not remove failed or timed-out attempts. The JSON artifact also
contains the sanitized record for every attempt.

## Limitations

${result.limitations.map((limitation) => `- ${limitation}`).join("\n")}

## Reproduce

Set the required environment variable named below to the disposable target's
secret value, then run the command against a fresh target.

${result.reproduce.requiredEnvironment.map((name) => `- Required environment: ${code(name)}`).join("\n")}

${code(result.reproduce.command)}

## Revision and checksum

- Source revision: ${code(result.integrity.sourceRevision)}
- Canonical result SHA-256: ${code(result.integrity.resultSha256)}
`;
}
