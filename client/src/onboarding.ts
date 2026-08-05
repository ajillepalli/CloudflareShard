import type { CloudflareShardAdminClient } from "./admin-client.js";
import type { CloudflareShardClient } from "./client.js";
import type { MutateRequest, StatusResponse, TableScanResponse, TxResponse } from "./types.js";

export const ONBOARDING_RESULT_VERSION = "cloudflareshard.onboarding-result.v1" as const;

export type CheckStatus = "PASS" | "WARN" | "FAIL";

export interface OnboardingCheck {
  id: string;
  label: string;
  status: CheckStatus;
  message: string;
}

export interface DoctorResult {
  schemaVersion: typeof ONBOARDING_RESULT_VERSION;
  command: "doctor";
  status: "READY" | "READY_WITH_WARNINGS" | "NOT_READY";
  startedAt: string;
  finishedAt: string;
  checks: OnboardingCheck[];
}

export interface VerifyResult {
  schemaVersion: typeof ONBOARDING_RESULT_VERSION;
  command: "verify";
  status: "VERIFIED" | "PENDING_RECONCILIATION" | "FAILED";
  runId: string;
  startedAt: string;
  finishedAt: string;
  checks: OnboardingCheck[];
  summary: {
    distinctPlacements: number;
    rowsVerified: number;
    replayMatched: boolean;
    resourcesRetained: true;
  };
}

export type OnboardingResult = DoctorResult | VerifyResult;

export interface DoctorApi {
  status(): Promise<StatusResponse>;
}

export interface VerifyAdminApi extends DoctorApi {
  init(request?: { numShards?: number; totalVBuckets?: number; force?: boolean }): Promise<unknown>;
  createTable(request: { table: string; schema: string; partitionKeyColumn: string }): Promise<unknown>;
  backfillProvenance(request?: { catalogShardId?: string }): Promise<{ ambiguous: unknown[]; orphaned: unknown[] }>;
  registerTenant(request: { tenantId: string; rotate?: boolean }): Promise<{ token: string }>;
  routeProbe(table: string, tenantId: string, partitionKey: string): Promise<{ route: { shardId: string } }>;
}

export interface VerifyTenantApi {
  tx(mutations: MutateRequest[], requestId?: string): Promise<TxResponse>;
  tableScan(request: { tenantId: string; table: string; limit?: number; cursor?: string }): Promise<TableScanResponse>;
}

export interface OnboardingClock {
  now(): Date;
}

const SYSTEM_CLOCK: OnboardingClock = { now: () => new Date() };

function iso(clock: OnboardingClock): string {
  return clock.now().toISOString();
}

const CREDENTIAL_LABEL = "(?:api[_-]?key|access[_-]?token|refresh[_-]?token|admin[_-]?token|tenant[_-]?token|client[_-]?secret|authorization|proxy[_-]?authorization|credential|credentials|cookie|token|secret|password)";
const JSON_CREDENTIAL = new RegExp(`(["']?)(${CREDENTIAL_LABEL})\\1\\s*:\\s*(["'])[^"'\\r\\n]*["']`, "giu");
const LABELED_CREDENTIAL = new RegExp(`\\b(${CREDENTIAL_LABEL})(["']?\\s*(?:=|:)\\s*)(?:["'][^"'\\r\\n]*["']|[^\\s,;}&]+)`, "giu");
const ENCODED_CREDENTIAL = new RegExp(`\\b(${CREDENTIAL_LABEL})(%3A|%3D)(?:%22|%27)?[^\\s&]+`, "giu");

/** Remove transport locations and credential-shaped values from text that can
 * cross an onboarding output boundary. URLs are intentionally removed in full:
 * receipts identify a target only by a hash of its credential-free origin. */
export function sanitizeOnboardingText(value: string): string {
  return value
    .replace(/\b[a-z][a-z0-9+.-]*%3A%2F%2F[^\s<>"'`]+/giu, "[REDACTED_URL]")
    .replace(/\b[a-z][a-z0-9+.-]*:\/\/[^\s<>"'`]+/giu, "[REDACTED_URL]")
    .replace(JSON_CREDENTIAL, (_match, keyQuote: string, label: string, valueQuote: string) => `${keyQuote}${label}${keyQuote}:${valueQuote}[REDACTED]${valueQuote}`)
    .replace(/\b(Bearer|Basic)\s+[^\s,;]+/giu, "$1 [REDACTED]")
    .replace(ENCODED_CREDENTIAL, (_match, label: string, separator: string) => `${label}${separator}[REDACTED]`)
    .replace(LABELED_CREDENTIAL, (_match, label: string, separator: string) => `${label}${separator}[REDACTED]`)
    .slice(0, 500);
}

export function sanitizeOnboardingResult<T extends OnboardingResult>(result: T): T {
  return {
    ...result,
    checks: result.checks.map((check) => ({
      ...check,
      id: sanitizeOnboardingText(check.id),
      label: sanitizeOnboardingText(check.label),
      message: sanitizeOnboardingText(check.message),
    })),
  };
}

function safeErrorMessage(error: unknown): string {
  return sanitizeOnboardingText(error instanceof Error ? error.message : String(error));
}

function finishDoctor(startedAt: string, checks: OnboardingCheck[], clock: OnboardingClock): DoctorResult {
  const status = checks.some((check) => check.status === "FAIL")
    ? "NOT_READY"
    : checks.some((check) => check.status === "WARN")
      ? "READY_WITH_WARNINGS"
      : "READY";
  return sanitizeOnboardingResult({ schemaVersion: ONBOARDING_RESULT_VERSION, command: "doctor", status, startedAt, finishedAt: iso(clock), checks });
}

/** Read-only deployment preflight. CloudflareShard's API can verify service
 * reachability, admin authentication, initialization, and topology. It cannot
 * read Cloudflare account-plan quota headroom, so that check is deliberately
 * WARN (never a fabricated PASS) with dashboard remediation. */
export async function runDoctor(api: DoctorApi, clock: OnboardingClock = SYSTEM_CLOCK): Promise<DoctorResult> {
  const startedAt = iso(clock);
  const checks: OnboardingCheck[] = [];
  let status: StatusResponse;
  try {
    status = await api.status();
    if (typeof status.initialized !== "boolean" || !status.shards || typeof status.shards.active !== "number") {
      throw new Error("Status response did not match the supported CloudflareShard schema.");
    }
    checks.push({ id: "service_auth", label: "Service and admin authentication", status: "PASS", message: "The Worker accepted the admin credential and returned cluster status." });
  } catch (error) {
    checks.push({ id: "service_auth", label: "Service and admin authentication", status: "FAIL", message: `Status check failed: ${safeErrorMessage(error)}` });
    return finishDoctor(startedAt, checks, clock);
  }

  if (status.initialized) {
    checks.push({ id: "cluster_initialized", label: "Cluster initialization", status: "PASS", message: "Cluster metadata is initialized." });
    checks.push(
      status.shards.active >= 2
        ? { id: "two_shard_topology", label: "Two-shard verification topology", status: "PASS", message: `${status.shards.active} active physical shards are available for distinct-placement proof.` }
        : { id: "two_shard_topology", label: "Two-shard verification topology", status: "FAIL", message: "The initialized cluster has fewer than two active physical shards; safe verification will not force-reset it." },
    );
  } else {
    checks.push({ id: "cluster_initialized", label: "Cluster initialization", status: "WARN", message: "Cluster is uninitialized. `verify --disposable-target` can initialize it with two shards without using force." });
    checks.push({ id: "two_shard_topology", label: "Two-shard verification topology", status: "PASS", message: "A fresh disposable target can be initialized with at least two physical shards." });
  }

  checks.push({
    id: "cloudflare_quota",
    label: "Cloudflare quota headroom",
    status: "WARN",
    message: "Live account-plan and remaining daily quota are not exposed by the CloudflareShard API. Check Workers & Pages usage in the Cloudflare dashboard before a larger sample.",
  });
  return finishDoctor(startedAt, checks, clock);
}

export interface VerifyOptions {
  runId?: string;
  maxRouteProbes?: number;
  clock?: OnboardingClock;
}

function safeRunId(value: string): string {
  const cleaned = value.toLowerCase().replace(/[^a-z0-9]/g, "").slice(0, 20);
  return cleaned || "run";
}

async function collectRows(tenant: VerifyTenantApi, tenantId: string, table: string): Promise<Record<string, unknown>[]> {
  const rows: Record<string, unknown>[] = [];
  let cursor: string | undefined;
  const seenCursors = new Set<string>();
  for (let pageNumber = 0; pageNumber < 100; pageNumber += 1) {
    const page = await tenant.tableScan({ tenantId, table, limit: 100, cursor });
    if (!page.provenance.complete) throw new Error("Verification table provenance is incomplete; readback cannot be certified.");
    if (page.scan.successCount !== page.scan.shardCount) throw new Error("Verification readback did not receive a successful response from every shard.");
    rows.push(...page.rows);
    if (rows.length > 1000) throw new Error("Verification scan exceeded its 1,000-row safety bound.");
    if (!page.nextCursor) return rows;
    if (seenCursors.has(page.nextCursor)) throw new Error("Verification scan cursor repeated without progress.");
    seenCursors.add(page.nextCursor);
    cursor = page.nextCursor;
  }
  throw new Error("Verification scan exceeded its 100-page safety bound.");
}

/** Mutating verification for an explicitly disposable target. It never calls
 * init(force:true). The caller owns the explicit acknowledgement; this domain
 * function owns bounded setup, authoritative route probing, a genuine
 * two-placement transaction, replay, and readback invariants. */
export async function runVerify(
  admin: VerifyAdminApi,
  tenantFactory: (token: string) => VerifyTenantApi,
  options: VerifyOptions = {},
): Promise<VerifyResult> {
  const clock = options.clock ?? SYSTEM_CLOCK;
  const startedAt = iso(clock);
  const runId = safeRunId(options.runId ?? crypto.randomUUID());
  const table = `cfs_verify_${runId}`;
  const tenantId = `cfs-verify-${runId}`;
  const requestId = `verify-tx-${runId}`;
  const marker = `marker-${runId}`;
  const checks: OnboardingCheck[] = [];
  let distinctPlacements = 0;
  let rowsVerified = 0;
  let replayMatched = false;

  try {
    let cluster = await admin.status();
    checks.push({ id: "target_status", label: "Disposable target status", status: "PASS", message: "The target accepted the admin credential." });
    if (!cluster.initialized) {
      await admin.init({ numShards: 2, totalVBuckets: 64, force: false });
      cluster = await admin.status();
      checks.push({ id: "safe_init", label: "Safe cluster initialization", status: "PASS", message: "Initialized the fresh target without force-resetting topology." });
    } else {
      checks.push({ id: "safe_init", label: "Safe cluster initialization", status: "PASS", message: "Used the existing initialized topology; no init or force-reset was attempted." });
    }
    if (cluster.shards.active < 2) {
      throw new Error("Distinct-placement proof requires at least two active physical shards; refusing to force-reset the initialized target.");
    }

    await admin.createTable({
      table,
      schema: `CREATE TABLE ${table} (id TEXT PRIMARY KEY, marker TEXT NOT NULL)`,
      partitionKeyColumn: "id",
    });
    checks.push({ id: "schema", label: "Disposable schema", status: "PASS", message: "Created an isolated verification table." });

    await admin.backfillProvenance();
    checks.push({ id: "provenance", label: "Readback provenance", status: "PASS", message: "Ran the existing full-cluster provenance certification before tenant-scoped readback." });

    const registration = await admin.registerTenant({ tenantId });
    const tenant = tenantFactory(registration.token);
    checks.push({ id: "tenant", label: "Disposable tenant", status: "PASS", message: "Registered an isolated tenant credential without writing it to output." });

    const maxRouteProbes = Math.max(2, Math.min(options.maxRouteProbes ?? 256, 1000));
    let first: { key: string; shardId: string } | undefined;
    let second: { key: string; shardId: string } | undefined;
    for (let index = 0; index < maxRouteProbes; index += 1) {
      const key = `row-${index}`;
      const route = await admin.routeProbe(table, tenantId, key);
      first ??= { key, shardId: route.route.shardId };
      if (route.route.shardId !== first.shardId) {
        second = { key, shardId: route.route.shardId };
        break;
      }
    }
    if (!first || !second) throw new Error(`Could not find two distinct shard placements within ${maxRouteProbes} bounded route probes.`);
    distinctPlacements = 2;
    checks.push({ id: "distinct_placement", label: "Distinct physical placement", status: "PASS", message: "Authoritative routing placed the two transaction keys on different physical shards." });

    const mutations: MutateRequest[] = [
      { op: "insert", table, tenantId, partitionKey: first.key, values: { marker } },
      { op: "insert", table, tenantId, partitionKey: second.key, values: { marker } },
    ];
    const firstTx = await tenant.tx(mutations, requestId);
    const replay = await tenant.tx(mutations, requestId);
    replayMatched = replay.txId === firstTx.txId;
    if (!replayMatched) throw new Error("Idempotent replay returned a different transaction ID.");
    const pendingReconciliation = replay.status !== "committed";
    checks.push({
      id: "cross_shard_tx",
      label: "Cross-shard transaction",
      status: pendingReconciliation ? "WARN" : "PASS",
      message: replay.status === "commit_pending_manifest"
        ? "The commit decision is durable, but manifest registration is pending and participant commit is not yet authorized."
        : replay.status === "committed_pending_ack"
          ? "The commit decision is durable, but at least one participant acknowledgement is still pending."
          : firstTx.status === "committed"
            ? "The two-placement transaction committed."
            : "The two-placement transaction converged to committed during idempotent replay.",
    });
    checks.push({ id: "idempotent_replay", label: "Idempotent replay", status: "PASS", message: "Replaying the same request ID returned the same transaction ID." });

    if (pendingReconciliation) {
      checks.push({
        id: "readback",
        label: "Tenant-scoped readback",
        status: "WARN",
        message: replay.status === "commit_pending_manifest"
          ? "Readback is deferred until manifest registration authorizes participant commit."
          : "Readback is deferred until every participant acknowledges the durable commit.",
      });
      return sanitizeOnboardingResult({
        schemaVersion: ONBOARDING_RESULT_VERSION,
        command: "verify",
        status: "PENDING_RECONCILIATION",
        runId,
        startedAt,
        finishedAt: iso(clock),
        checks,
        summary: { distinctPlacements, rowsVerified, replayMatched, resourcesRetained: true },
      });
    }

    const rows = await collectRows(tenant, tenantId, table);
    const expected = new Set([first.key, second.key]);
    const matching = rows.filter((row) => expected.has(String(row.id)) && row.marker === marker);
    const counts = new Map<string, number>();
    for (const row of matching) counts.set(String(row.id), (counts.get(String(row.id)) ?? 0) + 1);
    rowsVerified = matching.length;
    if (matching.length !== 2 || Array.from(expected).some((key) => counts.get(key) !== 1)) {
      throw new Error("Readback did not contain exactly one copy of each cross-shard row.");
    }
    checks.push({ id: "readback", label: "Tenant-scoped readback", status: "PASS", message: "Readback found exactly one copy of each committed row." });

    return sanitizeOnboardingResult({
      schemaVersion: ONBOARDING_RESULT_VERSION,
      command: "verify",
      status: pendingReconciliation ? "PENDING_RECONCILIATION" : "VERIFIED",
      runId,
      startedAt,
      finishedAt: iso(clock),
      checks,
      summary: { distinctPlacements, rowsVerified, replayMatched, resourcesRetained: true },
    });
  } catch (error) {
    checks.push({ id: "verification_failure", label: "Verification outcome", status: "FAIL", message: safeErrorMessage(error) });
    return sanitizeOnboardingResult({
      schemaVersion: ONBOARDING_RESULT_VERSION,
      command: "verify",
      status: "FAILED",
      runId,
      startedAt,
      finishedAt: iso(clock),
      checks,
      summary: { distinctPlacements, rowsVerified, replayMatched, resourcesRetained: true },
    });
  }
}

export type RealVerifyAdminApi = Pick<CloudflareShardAdminClient, "status" | "init" | "createTable" | "backfillProvenance" | "registerTenant" | "routeProbe">;
export type RealVerifyTenantApi = Pick<CloudflareShardClient, "tx" | "tableScan">;
