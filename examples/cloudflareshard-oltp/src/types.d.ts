export type BenchmarkOutcome = "success" | "failure" | "timeout";
export type BenchmarkStatus = "complete" | "incomplete";

export interface BenchmarkAttempt {
  attemptId: string;
  sequence: number;
  operation: "single-write" | "paired-write" | "bounded-read";
  outcome: BenchmarkOutcome;
  code: string | null;
  latencyMs: number;
  lateCompletion: boolean;
}

export interface LatencySummary {
  sampleCount: number;
  p50Ms: number | null;
  p95Ms: number | null;
  p99Ms: number | null;
  maxMs: number | null;
}

export interface BenchmarkResult {
  schemaVersion: "cloudflareshard.oltp-result.v1";
  status: BenchmarkStatus;
  preliminary: true;
  caveat: string;
  run: {
    runId: string;
    startedAt: string;
    finishedAt: string;
    requestedDurationMs: number;
    actualElapsedMs: number;
    cutoffReason: "max_operations" | "duration_elapsed" | "overall_grace_deadline";
  };
  environment: {
    targetOrigin: string;
    accountPlan: string;
    topology: { catalogShardCount: number | null; shardCount: number | null };
    nodeVersion: string;
    platform: string;
    arch: string;
    sourceRevision: string;
  };
  configuration: {
    concurrency: number;
    maxOperations: number;
    requestTimeoutMs: number;
    graceMs: number;
    seed: number;
    operationMix: ReadonlyArray<{ operation: BenchmarkAttempt["operation"]; weight: number }>;
  };
  outcomes: {
    attempted: number;
    successes: number;
    failures: number;
    timeouts: number;
    lateCompletions: number;
    reconciled: boolean;
    byOperation: Record<string, number>;
    errorCodes: Record<string, number>;
  };
  metrics: {
    successfulOperations: number;
    successfulOperationsPerSecond: number | null;
    allAttemptLatency: LatencySummary;
    successfulAttemptLatency: LatencySummary;
  };
  attempts: BenchmarkAttempt[];
  limitations: string[];
  reproduce: {
    command: string;
    requiredEnvironment: string[];
    freshDisposableTargetRequired: true;
  };
  integrity: {
    sourceRevision: string;
    resultSha256: string;
  };
}
