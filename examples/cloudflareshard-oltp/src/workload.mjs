import { createHash } from "node:crypto";
import { ApiRequestError } from "./http-client.mjs";

export const OPERATION_MIX = Object.freeze([
  { operation: "single-write", weight: 55 },
  { operation: "paired-write", weight: 30 },
  { operation: "bounded-read", weight: 15 },
]);

function stableIdentityInput(config) {
  return JSON.stringify({
    schema: "cloudflareshard.oltp-run.v1",
    targetOrigin: config.baseUrl,
    durationMs: config.durationMs,
    concurrency: config.concurrency,
    maxOperations: config.maxOperations,
    requestTimeoutMs: config.requestTimeoutMs,
    graceMs: config.graceMs,
    seed: config.seed,
    mix: OPERATION_MIX,
  });
}

export function deterministicRunId(config) {
  return `oltp-${createHash("sha256").update(stableIdentityInput(config)).digest("hex").slice(0, 16)}`;
}

export function workloadNames(runId) {
  const suffix = runId.replace(/[^a-z0-9]/g, "_").slice(-21);
  return {
    table: `oltp_records_${suffix}`,
    tenantId: `oltp-tenant-${suffix.replaceAll("_", "-")}`,
  };
}

export async function prepareDisposableTarget({ client, adminToken, runId, requestTimeoutMs }) {
  const setupDeadline = Date.now() + Math.max(30_000, requestTimeoutMs * 4);
  const requestOptions = { requestTimeoutMs, absoluteDeadlineMs: setupDeadline };
  const status = await client.post("/admin/status", {}, adminToken, requestOptions);
  if (!status || status.initialized !== true) throw new ApiRequestError("failure", "TARGET_NOT_INITIALIZED");

  const names = workloadNames(runId);
  await client.post(
    "/admin/create-table",
    {
      table: names.table,
      schema: `CREATE TABLE ${names.table} (id TEXT PRIMARY KEY, kind TEXT NOT NULL, payload TEXT NOT NULL, sequence INTEGER NOT NULL)`,
      partitionKeyColumn: "id",
    },
    adminToken,
    requestOptions,
  );
  const tenant = await client.post("/admin/register-tenant", { tenantId: names.tenantId }, adminToken, requestOptions);
  if (!tenant || typeof tenant.token !== "string" || tenant.token.length === 0) {
    throw new ApiRequestError("failure", "TENANT_TOKEN_MISSING");
  }

  return {
    ...names,
    tenantToken: tenant.token,
    topology: {
      catalogShardCount: Number.isInteger(status.catalogShardCount) ? status.catalogShardCount : null,
      shardCount: Number.isInteger(status.shards?.total) ? status.shards.total : null,
    },
  };
}

function xorshift32(seed) {
  let state = seed >>> 0 || 1;
  return () => {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    return (state >>> 0) / 0x1_0000_0000;
  };
}

function chooseOperation(seed, sequence) {
  // Derive the choice from (seed, sequence), not whichever concurrent worker
  // happens to claim the sequence. This keeps the operation assigned to each
  // deterministic attempt ID stable even when response timing changes worker
  // scheduling between reproductions.
  const random = xorshift32((seed ^ Math.imul(sequence, 0x9e37_79b9)) >>> 0);
  const value = random() * 100;
  let cursor = 0;
  for (const item of OPERATION_MIX) {
    cursor += item.weight;
    if (value < cursor) return item.operation;
  }
  return OPERATION_MIX.at(-1).operation;
}

export function createOperationExecutor({ client, tenantToken, tenantId, table, runId, requestTimeoutMs }) {
  return async ({ sequence, operation, absoluteDeadlineMs }) => {
    const padded = String(sequence).padStart(8, "0");
    const common = { requestTimeoutMs, absoluteDeadlineMs };
    if (operation === "single-write") {
      const key = `single-${padded}`;
      return client.post(
        "/v1/mutate",
        {
          op: "insert",
          table,
          tenantId,
          partitionKey: key,
          values: { kind: "single", payload: `${runId}:${padded}`, sequence },
          requestId: `${runId}-single-${padded}`,
        },
        tenantToken,
        common,
      );
    }
    if (operation === "paired-write") {
      const left = `pair-a-${padded}`;
      const right = `pair-b-${padded}`;
      return client.post(
        "/v1/tx",
        {
          mutations: [
            { op: "insert", table, tenantId, partitionKey: left, values: { kind: "pair-a", payload: `${runId}:${padded}:a`, sequence } },
            { op: "insert", table, tenantId, partitionKey: right, values: { kind: "pair-b", payload: `${runId}:${padded}:b`, sequence } },
          ],
          requestId: `${runId}-pair-${padded}`,
        },
        tenantToken,
        common,
      );
    }
    return client.post("/v1/table-scan", { tenantId, table, limit: 20 }, tenantToken, common);
  };
}

function graceRace(promise, absoluteDeadlineMs, now) {
  const remainingMs = absoluteDeadlineMs - now();
  if (remainingMs <= 0) return Promise.reject(new ApiRequestError("timeout", "RUN_GRACE_EXPIRED"));
  let timer;
  const deadline = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new ApiRequestError("timeout", "RUN_GRACE_EXPIRED")), remainingMs);
  });
  return Promise.race([promise, deadline]).finally(() => clearTimeout(timer));
}

export async function runClosedLoop({ runId, config, executeOperation, now = Date.now }) {
  const startedMs = now();
  const schedulingDeadlineMs = startedMs + config.durationMs;
  const absoluteDeadlineMs = schedulingDeadlineMs + config.graceMs;
  const attempts = [];
  let issued = 0;
  let graceExpired = false;

  function claimSequence() {
    if (issued >= config.maxOperations || now() >= schedulingDeadlineMs) return null;
    issued += 1;
    return issued;
  }

  async function worker() {
    for (;;) {
      const sequence = claimSequence();
      if (sequence === null) return;
      const operation = chooseOperation(config.seed, sequence);
      const attemptStartedMs = now();
      let outcome = "success";
      let code = null;
      try {
        await graceRace(executeOperation({ sequence, operation, absoluteDeadlineMs }), absoluteDeadlineMs, now);
      } catch (error) {
        if (error instanceof ApiRequestError && error.kind === "timeout") {
          outcome = "timeout";
          code = error.code;
          if (error.code === "RUN_GRACE_EXPIRED") graceExpired = true;
        } else {
          outcome = "failure";
          code = error instanceof ApiRequestError ? error.code : "UNCLASSIFIED_ERROR";
        }
      }
      const finishedMs = now();
      attempts.push({
        attemptId: `${runId}-${String(sequence).padStart(8, "0")}`,
        sequence,
        operation,
        outcome,
        code,
        latencyMs: Math.max(0, finishedMs - attemptStartedMs),
        lateCompletion: finishedMs > schedulingDeadlineMs,
      });
      if (now() >= absoluteDeadlineMs) return;
    }
  }

  await Promise.all(Array.from({ length: config.concurrency }, () => worker()));
  const finishedMs = now();
  attempts.sort((left, right) => left.sequence - right.sequence);
  const cutoffReason = graceExpired ? "overall_grace_deadline" : issued >= config.maxOperations ? "max_operations" : "duration_elapsed";
  return {
    startedMs,
    finishedMs,
    actualElapsedMs: Math.max(0, finishedMs - startedMs),
    status: graceExpired ? "incomplete" : "complete",
    cutoffReason,
    attempts,
  };
}
