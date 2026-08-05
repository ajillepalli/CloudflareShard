#!/usr/bin/env node
import { execFileSync } from "node:child_process";
import { pathToFileURL } from "node:url";
import process from "node:process";
import { parseArgs, usage, InvocationError } from "./args.mjs";
import { writeArtifacts } from "./artifacts.mjs";
import { BenchmarkApiClient, ApiRequestError } from "./http-client.mjs";
import { buildBenchmarkResult } from "./result.mjs";
import {
  OPERATION_MIX,
  createOperationExecutor,
  deterministicRunId,
  prepareDisposableTarget,
  runClosedLoop,
} from "./workload.mjs";

function sourceRevision() {
  try {
    const revision = execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
    return /^[0-9a-f]{7,40}$/i.test(revision) ? revision : "unknown";
  } catch {
    return "unknown";
  }
}

function reproduceCommand(args) {
  return [
    "npm run benchmark --",
    `--base-url ${args.baseUrl}`,
    `--duration-seconds ${args.durationMs / 1_000}`,
    `--concurrency ${args.concurrency}`,
    `--max-operations ${args.maxOperations}`,
    `--request-timeout-ms ${args.requestTimeoutMs}`,
    `--grace-ms ${args.graceMs}`,
    `--seed ${args.seed}`,
  ].join(" ");
}

export async function main(argv = process.argv.slice(2), environment = process.env) {
  let args;
  try {
    args = parseArgs(argv);
  } catch (error) {
    if (error instanceof InvocationError) {
      process.stderr.write(`Invalid invocation: ${error.message}\n\n${usage()}`);
      return 2;
    }
    throw error;
  }
  if (args.help) {
    process.stdout.write(usage());
    return 0;
  }

  const adminToken = environment.CLOUDFLARESHARD_ADMIN_TOKEN;
  if (!adminToken) {
    process.stderr.write("Blocked: set CLOUDFLARESHARD_ADMIN_TOKEN for a disposable initialized target.\n");
    return 3;
  }

  const runId = deterministicRunId(args);
  process.stderr.write(`Run ${runId}: preparing disposable target ${args.baseUrl}\n`);
  const client = new BenchmarkApiClient({ baseUrl: args.baseUrl });
  let prepared;
  try {
    prepared = await prepareDisposableTarget({ client, adminToken, runId, requestTimeoutMs: args.requestTimeoutMs });
  } catch (error) {
    const code = error instanceof ApiRequestError ? error.code : "UNCLASSIFIED_SETUP_ERROR";
    process.stderr.write(`Blocked during target preparation: ${code}. Use a fresh initialized disposable target.\n`);
    return 3;
  }

  process.stderr.write(`Run ${runId}: active; at most ${args.maxOperations} attempts before the declared bounds.\n`);
  const executeOperation = createOperationExecutor({
    client,
    tenantToken: prepared.tenantToken,
    tenantId: prepared.tenantId,
    table: prepared.table,
    runId,
    requestTimeoutMs: args.requestTimeoutMs,
  });
  const execution = await runClosedLoop({ runId, config: args, executeOperation });
  const result = buildBenchmarkResult({
    runId,
    config: { ...args, operationMix: OPERATION_MIX },
    execution,
    environment: {
      targetOrigin: args.baseUrl,
      accountPlan: "unknown",
      topology: prepared.topology,
      nodeVersion: process.version,
      platform: process.platform,
      arch: process.arch,
      sourceRevision: sourceRevision(),
    },
    reproduceCommand: reproduceCommand(args),
  });
  const artifactPaths = await writeArtifacts(result, args.outputDir);

  process.stdout.write(`${result.status === "complete" ? "PRELIMINARY RUN COMPLETE" : "INCOMPLETE RUN"}\n`);
  process.stdout.write(`Run: ${runId}\n`);
  process.stdout.write(
    `Outcomes: ${result.outcomes.attempted} attempted = ${result.outcomes.successes} success + ${result.outcomes.failures} failure + ${result.outcomes.timeouts} timeout; ${result.outcomes.lateCompletions} late\n`,
  );
  process.stdout.write(`JSON: ${artifactPaths.jsonPath}\nMarkdown: ${artifactPaths.markdownPath}\n`);
  process.stdout.write(`Checksum: ${result.integrity.resultSha256}\n`);
  return result.status === "complete" ? 0 : 5;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().then((exitCode) => {
    process.exitCode = exitCode;
  });
}
