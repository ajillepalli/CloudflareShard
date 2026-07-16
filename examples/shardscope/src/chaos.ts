/** chaos.ts — Shardscope's "CHAOS — BREAK IT" panel server-side layer (T9).
 *
 * ============================================================================
 * THE THESIS (read this before adding or "fixing" an attack)
 * ============================================================================
 * Chaos mode's entire demo value is "I tried to break it and couldn't." That
 * ONLY works if every enabled attack is REAL: it must actually perform the
 * destructive thing against the live cluster over the real, already-shipped
 * surface (the gateway's /v1/mutate + ./reshard.ts's admin RPC wrappers) —
 * never a client-side simulation that just LOOKS destructive. A fake button
 * would poison this demo's credibility worse than not shipping it at all.
 *
 * Concretely:
 *   - double-submit and mismatched-replay (below) issue REAL /v1/mutate calls
 *     against the real gateway, with a real tenant bearer token (via
 *     ./load/tenant-token-store.ts), and classify the REAL HTTP response
 *     (status + body) — never a hand-simulated outcome.
 *   - drain-hot-node / split-hot-vbucket / migrate-hot-vbucket / abort-migration
 *     call ./reshard.ts's EXISTING adminXxx wrappers directly — the exact same
 *     code path the Reshard console's manual operator controls use. Nothing
 *     is reimplemented.
 *   - blip-shard-offline calls env.SHARD_API.adminFaultInject directly — the
 *     core's REAL, admin-gated fault-injection primitive (see env.d.ts's
 *     adminFaultInject/adminFaultClear + the main repo's src/shard.ts,
 *     FAULT_MAX_MS): a genuine 503 from the targeted shard's Durable Object
 *     for a real, bounded window (this file caps its own request at
 *     MAX_BLIP_DURATION_MS, itself well under the core's absolute 30s hard
 *     cap). It is OFF unless the core Worker sets
 *     FAULT_INJECTION_ENABLED="true" — a disabled cluster rejects with a
 *     403 this file classifies honestly (classifyBlipFaultInjectError below)
 *     as "needs the flag", never a generic failure and never a fabricated
 *     ✗ broke. Target selection (pickBlipShardTarget) deliberately avoids a
 *     shard that's currently mid-migration by default, so the clean "shard
 *     drops, cluster holds lost:0, shard recovers" story isn't muddied by
 *     the also-by-design "blipping a mid-reshard shard parks the topology
 *     op" interaction (see src/shard.ts's own header comment on that).
 *
 * ----------------------------------------------------------------------------
 * DESIGN — pure classification core + thin impure gatherers, mirroring
 * ./load/correctness.ts's own split (a pattern this whole demo already
 * leans on): every attack's PASS/FAIL judgment (classifyDoubleSubmit,
 * classifyMismatchedReplay) and every attack's TARGET RESOLUTION
 * (pickHotShardTarget, pickHotVbucketTarget, pickInFlightMigrationTarget) are
 * pure functions over plain data — no fetch, no DO binding, no SHARD_API
 * call — so chaos.test.ts can exercise the actual judgment/shaping logic
 * directly, without a live cluster. The impure runXxxAttack functions below
 * are thin: gather real data over the real surface, then hand it to the pure
 * core to judge.
 *
 * ----------------------------------------------------------------------------
 * WHY tpcc_stock FOR THE GATEWAY ATTACKS (double-submit / mismatched-replay):
 * same reasoning as ./load/correctness.ts's own header comment — tpcc_stock
 * is indexed (idx_stock_by_item), so a genuine before/after read-back is
 * cheap and reliable, and it's the exact table the hot-shard skew driver
 * targets, so it's realistic load-bearing data, not a synthetic scratch row.
 *
 * ----------------------------------------------------------------------------
 * THE MISMATCHED-REPLAY CONTRACT (verified by reading the core's
 * src/shard.ts, not assumed): ShardDO.handleExecute hashes (sql, params) via
 * SHA-256 (requestHash) and stores it alongside each requestId's cached
 * result in `applied_requests`. On a mutating /execute call:
 *   - no prior entry for this requestId -> executes fresh, records the hash.
 *   - prior entry, hash MATCHES -> returns the cached result with
 *     `duplicated: true` (never re-executes the SQL). Note: the gateway's own
 *     mutateCore (main repo's src/index.ts) does NOT forward `duplicated` in
 *     /v1/mutate's response body today — both a fresh ack and a deduped
 *     replay come back as plain `{ ok: true, rowsAffected }` at the HTTP
 *     layer. That's why double-submit below judges success by the ROW'S
 *     ACTUAL VALUE (a real read-back), not by a `duplicated` flag the wire
 *     format doesn't expose.
 *   - prior entry, hash MISMATCHES -> REJECTS with
 *     `409 { error: "requestId was already used with different sql/params —
 *     refusing to replay a mismatched result." }` (src/shard.ts's
 *     handleExecute, the `prior.request_hash !== incomingHash` branch).
 *     mutateCore forwards this shard response verbatim (status + body) to
 *     the /v1/mutate caller — see main repo's src/index.ts's mutateCore:
 *     `if (!shardRes.ok) return new Response(shardRes.body, { status:
 *     shardRes.status, ... })`. This exact string is
 *     MISMATCH_REJECTION_SUBSTRING below — mismatched-replay's whole job is
 *     confirming this contract holds against a live gateway.
 * ============================================================================
 */
import type { Env } from "./env";
import { drainShard, migrateVbucket, migrateVbucketAbort, splitVbucket } from "./reshard";
import { tenantIdForWarehouse } from "./load/transactions";
import { ownedVBuckets, type VBucketOwnership } from "./load/skew";
import { HttpTxExecutor } from "./load/gateway-client";
import { TenantTokenStoreTokenProvider } from "./load/tenant-token-store";
import type { TokenProvider } from "./load/token-provider";

/** Thrown when an attack CAN'T be attempted right now — missing setup data
 * (no stock row for the requested warehouse/item, no skew load running to
 * derive a "hot shard" from, no in-flight migration to abort), not a finding
 * about the cluster's correctness. Distinct from a ChaosOutcome with
 * `survived: false` (which means the attack DID fire and found a real
 * problem) — src/index.ts's route wrapper turns this into a calm 400,
 * exactly like ./reshard.ts's ReshardValidationError. */
export class ChaosPreconditionError extends Error {}

// ----------------------------------------------------------------------------
// Structured outcome — every attack (that actually fires) returns exactly
// this shape, whether it "survived" or genuinely found a break.
// ----------------------------------------------------------------------------

export interface ChaosOutcome {
  attack: string;
  /** What this attack actually did, in plain language (the real requestId /
   * table / target it fired against — never vague). */
  did: string;
  /** What a correct cluster is supposed to do in response. */
  expected: string;
  /** What was actually observed (real HTTP statuses / RPC response bodies /
   * before-after values) — the receipts. */
  observed: string;
  /** true iff the cluster's real behavior matched `expected` — the ONLY
   * thing that decides the button's ✓/✗ in the UI. */
  survived: boolean;
  /** Human-readable elaboration — which mechanism protected the write (or
   * didn't), and what it means. */
  note: string;
}

/** Every attack this module actually wires a route for. Exported so
 * src/index.ts's routing table and chaos.test.ts can both check membership
 * without hand-duplicating the list. */
export const CHAOS_ATTACKS = [
  "double-submit",
  "mismatched-replay",
  "drain-hot-node",
  "split-hot-vbucket",
  "migrate-hot-vbucket",
  "abort-migration",
  "blip-shard-offline",
] as const;
export type ChaosAttackKey = (typeof CHAOS_ATTACKS)[number];

// ----------------------------------------------------------------------------
// Small request-body parsing helpers (self-contained — deliberately not
// imported from ./reshard.ts's private helpers, which aren't exported).
// Every chaos attack input is OPTIONAL with a sane default: an operator
// firing a button with an empty POST body is the expected common case (the
// UI does not expose manual target pickers for chaos attacks — see
// public/app.js), unlike the Reshard console's forms, which require explicit
// targets.
// ----------------------------------------------------------------------------

function asRecord(body: unknown): Record<string, unknown> {
  if (typeof body !== "object" || body === null || Array.isArray(body)) return {};
  return body as Record<string, unknown>;
}

function positiveIntOrDefault(value: unknown, fallback: number): number {
  const n = typeof value === "string" && value.trim() !== "" ? Number(value) : value;
  return typeof n === "number" && Number.isInteger(n) && n > 0 ? n : fallback;
}

function optionalNonEmptyString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() !== "" ? value : undefined;
}

function optionalNonNegativeInt(value: unknown): number | undefined {
  const n = typeof value === "string" && value.trim() !== "" ? Number(value) : value;
  return typeof n === "number" && Number.isInteger(n) && n >= 0 ? n : undefined;
}

// Matches ./load/load-driver.ts's own DEFAULT_WAREHOUSE_IDS / a plausible
// always-present item id — good defaults for a one-click demo button, not a
// claim that warehouse 1 / item 1 is special.
const DEFAULT_WAREHOUSE_ID = 1;
const DEFAULT_ITEM_ID = 1;

// ============================================================================
// Attacks a/b — double-submit, mismatched-replay. Real /v1/mutate calls
// against the real gateway, judged by a real read-back. See this file's
// header comment for the exact mismatched-replay contract these confirm.
// ============================================================================

const MISMATCH_REJECTION_SUBSTRING = "requestId was already used with different sql/params";

export interface StockAttackInput {
  warehouseId: number;
  itemId: number;
}

export function parseStockAttackInput(body: unknown): StockAttackInput {
  const b = asRecord(body);
  return {
    warehouseId: positiveIntOrDefault(b.warehouseId, DEFAULT_WAREHOUSE_ID),
    itemId: positiveIntOrDefault(b.itemId, DEFAULT_ITEM_ID),
  };
}

// ---- pure classifiers -------------------------------------------------------

export interface DoubleSubmitRawResult {
  tenantId: string;
  partitionKey: string;
  requestId: string;
  originalQty: number;
  callAStatus: number;
  callARowsAffected: number | null;
  callBStatus: number;
  callBRowsAffected: number | null;
  /** null iff the post-attack read-back found no row at all (shouldn't
   * happen — the row existed a moment ago — but never assumed away). */
  finalQty: number | null;
}

/** Pure judge for double-submit. `survived` (and ONLY `survived`) decides the
 * demo's ✓/✗: exactly one decrement must have landed (delta === 1), not zero,
 * not two — see this file's header comment for why the response bodies alone
 * (both look like plain success at the /v1/mutate wire format) can't answer
 * this; only the row's actual before/after value can. */
export function classifyDoubleSubmit(r: DoubleSubmitRawResult): ChaosOutcome {
  const delta = r.finalQty === null ? null : r.originalQty - r.finalQty;
  const bothLookAppliedIndividually = r.callAStatus < 300 && r.callBStatus < 300 && r.callARowsAffected === 1 && r.callBRowsAffected === 1;
  const survived = delta === 1;

  let note: string;
  if (survived) {
    note = bothLookAppliedIndividually
      ? "Deduped: both concurrent submissions came back with the SAME success (rowsAffected:1) — that's requestId idempotency serving the second call its own cached result, not a lucky compare-and-swap race. Exactly one decrement landed."
      : "The row shows exactly one decrement (correct) — no double effect — though the two calls' own statuses (see 'observed') suggest the second one was blocked by the compare-and-swap guard rather than served the idempotency cache directly.";
  } else if (delta === 2) {
    note = "BOTH submissions applied — the requestId reuse was NOT deduped. Genuine double-write; the correctness meter should be showing this as a loss.";
  } else if (delta === 0) {
    note = "NEITHER submission's effect stuck (s_quantity unchanged) — investigate using the two calls' raw statuses in 'observed'.";
  } else {
    note = `Unexpected delta ${String(delta)} — investigate using the two calls' raw statuses in 'observed'.`;
  }

  return {
    attack: "double-submit",
    did: `fired the SAME requestId (${r.requestId}) twice, concurrently, as a tpcc_stock update on ${r.tenantId}/${r.partitionKey}`,
    expected: "requestId-based idempotency dedupes the second submission — the row is decremented exactly once, never twice, never zero times.",
    observed:
      `call A: HTTP ${r.callAStatus}${r.callARowsAffected != null ? ` rowsAffected=${r.callARowsAffected}` : ""} · ` +
      `call B: HTTP ${r.callBStatus}${r.callBRowsAffected != null ? ` rowsAffected=${r.callBRowsAffected}` : ""} · ` +
      `s_quantity ${r.originalQty} -> ${r.finalQty ?? "?"} (delta ${delta ?? "?"})`,
    survived,
    note,
  };
}

export interface MismatchedReplayRawResult {
  tenantId: string;
  partitionKey: string;
  requestId: string;
  originalQty: number;
  firstStatus: number;
  firstRowsAffected: number | null;
  secondStatus: number;
  secondErrorMessage: string | null;
  finalQty: number | null;
}

/** Pure judge for mismatched-replay. `survived` requires BOTH: (1) the
 * replay was rejected with exactly the 409 + contract string src/shard.ts
 * actually emits (MISMATCH_REJECTION_SUBSTRING — see this file's header
 * comment), not just any 409 (a lock/fence 409 would be a DIFFERENT failure
 * mode, not proof of correct mismatch handling), and (2) the row's final
 * value shows ONLY the first write's effect — a rejection that still let
 * some partial write through would be worse than no protection at all. */
export function classifyMismatchedReplay(r: MismatchedReplayRawResult): ChaosOutcome {
  const rejectedCorrectly = r.secondStatus === 409 && !!r.secondErrorMessage && r.secondErrorMessage.includes(MISMATCH_REJECTION_SUBSTRING);
  const expectedFinalQty = r.originalQty - 1; // only the FIRST write should ever have applied
  const rowUnchangedByReplay = r.finalQty === expectedFinalQty;
  const survived = rejectedCorrectly && rowUnchangedByReplay;

  let note: string;
  if (survived) {
    note = `Correctly rejected: the gateway returned 409 with the exact src/shard.ts contract string ("${MISMATCH_REJECTION_SUBSTRING}…") and the row shows only the FIRST write's effect — no stale replay, no corruption.`;
  } else if (!rejectedCorrectly && r.secondStatus < 300) {
    note = "The replay was ACCEPTED instead of rejected — a requestId collision silently smuggled a different write through. This is a genuine correctness bug the meter should be catching.";
  } else if (!rejectedCorrectly) {
    note = `The replay was rejected, but not with the expected mismatched-requestId contract (got HTTP ${r.secondStatus}: ${r.secondErrorMessage ?? "(no message)"}) — this might be a different 409 (e.g. a row lock or migration fence) rather than the idempotency-hash check; investigate.`;
  } else {
    note = `The rejection status/message were correct, but the row's final value (${String(r.finalQty)}) doesn't match the first write's expected effect (${expectedFinalQty}) — investigate a possible partial application.`;
  }

  return {
    attack: "mismatched-replay",
    did: `reused requestId ${r.requestId} for a SECOND, different tpcc_stock update on ${r.tenantId}/${r.partitionKey}`,
    expected: `the gateway rejects the replay with 409 and "${MISMATCH_REJECTION_SUBSTRING}…" — it must NEVER silently return the first write's stale cached result, and NEVER apply the second write under the reused id.`,
    observed:
      `first write: HTTP ${r.firstStatus}${r.firstRowsAffected != null ? ` rowsAffected=${r.firstRowsAffected}` : ""} · ` +
      `replay: HTTP ${r.secondStatus} ${r.secondErrorMessage ?? "(no error field)"} · ` +
      `s_quantity ${r.originalQty} -> ${r.finalQty ?? "?"}`,
    survived,
    note,
  };
}

// ---- impure gatherers: real fetch, real tenant token, real gateway --------

interface RawGatewayResponse {
  status: number;
  body: Record<string, unknown> | null;
}

/** Raw /v1/mutate call — deliberately NOT ./load/gateway-client.ts's
 * HttpTxExecutor.mutate() here: that helper throws a GatewayError on any
 * non-2xx response and, critically, its error message does NOT preserve the
 * response body's `error` text (see gateway-client.ts's GatewayError
 * constructor — it only extracts `.code`/`.message` off an OBJECT-shaped
 * `error` field, but src/shard.ts's mismatched-replay rejection returns
 * `error` as a plain STRING). Confirming the EXACT rejection contract
 * requires the real status + real body, so these two attacks talk to
 * /v1/mutate directly. Every OTHER call in this file (indexQuery reads, the
 * ./reshard.ts admin wrappers) reuses the existing, already-shipped
 * surface. */
async function rawMutate(baseUrl: string, token: string, body: Record<string, unknown>): Promise<RawGatewayResponse> {
  const res = await fetch(`${baseUrl}/v1/mutate`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${token}` },
    body: JSON.stringify(body),
  });
  const text = await res.text();
  let parsed: unknown = null;
  if (text) {
    try {
      parsed = JSON.parse(text);
    } catch {
      parsed = { raw: text };
    }
  }
  return { status: res.status, body: parsed && typeof parsed === "object" ? (parsed as Record<string, unknown>) : null };
}

function readRowsAffected(body: Record<string, unknown> | null): number | null {
  return body && typeof body.rowsAffected === "number" ? body.rowsAffected : null;
}

function readErrorMessage(body: Record<string, unknown> | null): string | null {
  return body && typeof body.error === "string" ? body.error : null;
}

/** Fixed-token TokenProvider — chaos attacks resolve one tenant token up
 * front (via TenantTokenStoreTokenProvider, T5's real durable store) and
 * reuse it for every read/write in the same attack, rather than having
 * HttpTxExecutor re-resolve it per call. */
class FixedTokenProvider implements TokenProvider {
  constructor(private readonly token: string) {}
  async getTenantToken(): Promise<string> {
    return this.token;
  }
}

interface StockRowLite {
  s_key: string;
  s_quantity: number;
}

async function readStockRow(exec: HttpTxExecutor, warehouseId: number, itemId: number): Promise<StockRowLite | null> {
  const res = await exec.indexQuery(warehouseId, "tpcc_stock", "idx_stock_by_item", { i_id: itemId });
  const row = (res.rows ?? [])[0] as Partial<StockRowLite> | undefined;
  if (!row || typeof row.s_key !== "string" || typeof row.s_quantity !== "number") return null;
  return { s_key: row.s_key, s_quantity: row.s_quantity };
}

async function resolveTenantToken(env: Env, warehouseId: number): Promise<string> {
  return new TenantTokenStoreTokenProvider(env).getTenantToken(warehouseId);
}

/** Shardscope doesn't have its own "gateway base URL" config — the only
 * place it's known is whatever an operator last passed to POST
 * /api/load/start (see ./load/load-driver.ts's LoadDriverConfig.baseUrl,
 * which persists across a stop/start same as the rest of its config).
 * Deliberately does NOT require the load run to still be RUNNING — only that
 * a baseUrl has been configured at least once — because double-submit /
 * mismatched-replay don't depend on skew mode or an active batch loop, only
 * on knowing which cluster to hit. */
async function requireLoadBaseUrl(env: Env): Promise<{ baseUrl: string }> {
  const status = await fetchLoadStatus(env);
  if (!status.config?.baseUrl) {
    throw new ChaosPreconditionError(
      "Shardscope doesn't know the gateway's baseUrl yet — start a load run at least once (POST /api/load/start with a baseUrl) so gateway-level chaos attacks know which cluster to hit.",
    );
  }
  return { baseUrl: status.config.baseUrl };
}

export interface DoubleSubmitInput extends StockAttackInput {}
export interface MismatchedReplayInput extends StockAttackInput {}

export const parseDoubleSubmitInput = parseStockAttackInput;
export const parseMismatchedReplayInput = parseStockAttackInput;

export async function runDoubleSubmitAttack(env: Env, input: DoubleSubmitInput): Promise<ChaosOutcome> {
  const { baseUrl } = await requireLoadBaseUrl(env);
  const tenantId = tenantIdForWarehouse(input.warehouseId);
  const token = await resolveTenantToken(env, input.warehouseId);
  const exec = new HttpTxExecutor(baseUrl, new FixedTokenProvider(token));

  const stockRow = await readStockRow(exec, input.warehouseId, input.itemId);
  if (!stockRow) {
    throw new ChaosPreconditionError(
      `No tpcc_stock row for warehouse ${input.warehouseId} item ${input.itemId} — start/seed load for this warehouse before firing this attack, or target a different {warehouseId, itemId}.`,
    );
  }

  const requestId = crypto.randomUUID();
  const body = {
    op: "update" as const,
    table: "tpcc_stock",
    tenantId,
    partitionKey: stockRow.s_key,
    values: { s_quantity: stockRow.s_quantity - 1 },
    where: { s_quantity: stockRow.s_quantity },
    requestId,
  };

  // THE ATTACK: the exact same requestId + body, fired concurrently.
  const [a, b] = await Promise.all([rawMutate(baseUrl, token, body), rawMutate(baseUrl, token, body)]);
  const after = await readStockRow(exec, input.warehouseId, input.itemId);

  return classifyDoubleSubmit({
    tenantId,
    partitionKey: stockRow.s_key,
    requestId,
    originalQty: stockRow.s_quantity,
    callAStatus: a.status,
    callARowsAffected: readRowsAffected(a.body),
    callBStatus: b.status,
    callBRowsAffected: readRowsAffected(b.body),
    finalQty: after ? after.s_quantity : null,
  });
}

export async function runMismatchedReplayAttack(env: Env, input: MismatchedReplayInput): Promise<ChaosOutcome> {
  const { baseUrl } = await requireLoadBaseUrl(env);
  const tenantId = tenantIdForWarehouse(input.warehouseId);
  const token = await resolveTenantToken(env, input.warehouseId);
  const exec = new HttpTxExecutor(baseUrl, new FixedTokenProvider(token));

  const stockRow = await readStockRow(exec, input.warehouseId, input.itemId);
  if (!stockRow) {
    throw new ChaosPreconditionError(
      `No tpcc_stock row for warehouse ${input.warehouseId} item ${input.itemId} — start/seed load for this warehouse before firing this attack, or target a different {warehouseId, itemId}.`,
    );
  }

  const requestId = crypto.randomUUID();
  const firstBody = {
    op: "update" as const,
    table: "tpcc_stock",
    tenantId,
    partitionKey: stockRow.s_key,
    values: { s_quantity: stockRow.s_quantity - 1 },
    where: { s_quantity: stockRow.s_quantity },
    requestId,
  };
  const first = await rawMutate(baseUrl, token, firstBody);
  if (first.status >= 300 || readRowsAffected(first.body) !== 1) {
    // The setup write itself didn't land cleanly — honest bail, not a
    // fabricated attack result. Concurrent live load could plausibly have
    // changed this exact row between the read above and this write.
    throw new ChaosPreconditionError(
      `Setup write for mismatched-replay didn't cleanly apply (HTTP ${first.status}) — the row may have changed concurrently under live load. Retry the attack.`,
    );
  }

  // THE ATTACK: the SAME requestId, a DIFFERENT mutation (decrement by 7
  // instead of 1, with a `where` matching the value the FIRST write left
  // behind) — src/shard.ts's requestHash(sql, params) will differ from what
  // it stored for this requestId, which is exactly what must trigger the 409
  // rejection rather than a silent replay or a corrupting re-application.
  const secondBody = {
    op: "update" as const,
    table: "tpcc_stock",
    tenantId,
    partitionKey: stockRow.s_key,
    values: { s_quantity: stockRow.s_quantity - 7 },
    where: { s_quantity: stockRow.s_quantity - 1 },
    requestId,
  };
  const second = await rawMutate(baseUrl, token, secondBody);
  const after = await readStockRow(exec, input.warehouseId, input.itemId);

  return classifyMismatchedReplay({
    tenantId,
    partitionKey: stockRow.s_key,
    requestId,
    originalQty: stockRow.s_quantity,
    firstStatus: first.status,
    firstRowsAffected: readRowsAffected(first.body),
    secondStatus: second.status,
    secondErrorMessage: readErrorMessage(second.body),
    finalQty: after ? after.s_quantity : null,
  });
}

// ============================================================================
// Attacks c/d/e — drain-hot-node, split-hot-vbucket, migrate-hot-vbucket,
// abort-migration. Every one of these REUSES ./reshard.ts's existing admin
// RPC wrappers verbatim (no reimplementation) — this section's own job is
// only TARGET RESOLUTION: finding the currently-hot shard/vbucket (or an
// in-flight migration) to aim the reused wrapper at.
// ============================================================================

/** Loosely-typed slice of ./load/load-driver.ts's toStatusJson() response —
 * only the fields target-resolution actually reads, mirroring this
 * repo's existing convention (aggregator.ts's own LoadDriverStatusResponse)
 * of not importing load-driver.ts's private types across files. */
export interface LoadStatusLike {
  running: boolean;
  config: {
    mode: string | null;
    targetShardId: string | null;
    baseUrl: string | null;
  } | null;
  /** Correctness meter at the instant of the read — same two fields
   * aggregator.ts's own LoadDriverStatusResponse.correctness carries (see
   * that file's Scoreboard type). Optional/nullable: older callers of this
   * type (every attack above blip-shard-offline) never populate it, and a
   * failed/cold-start status read degrades to no data, same non-fatal
   * contract as every other field on this loosely-typed slice. Deliberately
   * NOT zeroed-out here the way aggregator.ts's mergeScoreboard zeroes it for
   * display when `running` is false — that's a PRESENTATION choice for the
   * scoreboard; blip-shard-offline's own classifier (classifyBlipShardOfflineFire)
   * makes its own honest "no load running" call using `loadRunning` instead
   * of silently coercing a possibly-stale `lost` to 0. */
  correctness?: { lost: number; meterState: "green" | "red" } | null;
}

/** Loosely-typed slice of adminVbucketMap's response — only the fields
 * target-resolution reads (mirrors aggregator.ts's own local
 * AdminVbucketMapResponse). `targetShardId` is optional/nullable — only
 * blip-shard-offline's pickBlipShardTarget reads it (to also treat a
 * migration's TARGET shard as "currently mid-migration", not just its
 * source); every other target-resolution function in this file ignores it,
 * same as before this field was added. */
export interface VbucketMapLike {
  catalogs: Array<{
    catalogShardId: string;
    totalVBuckets: number;
    map: Array<{ vbucket: number; shardId: string; migrationStatus: string; targetShardId?: string | null }>;
  }>;
}

export interface HotShardTarget {
  catalogShardId: string;
  shardId: string;
}

export interface HotVbucketTarget extends HotShardTarget {
  vbucket: number;
}

export interface HotShardOverride {
  catalogShardId?: string;
  shardId?: string;
}

export interface HotVbucketOverride extends HotShardOverride {
  vbucket?: number;
}

export function parseHotShardOverride(body: unknown): HotShardOverride {
  const b = asRecord(body);
  return { catalogShardId: optionalNonEmptyString(b.catalogShardId), shardId: optionalNonEmptyString(b.shardId) };
}

export function parseHotVbucketOverride(body: unknown): HotVbucketOverride {
  const b = asRecord(body);
  return {
    catalogShardId: optionalNonEmptyString(b.catalogShardId),
    shardId: optionalNonEmptyString(b.shardId),
    vbucket: optionalNonNegativeInt(b.vbucket),
  };
}

/** Pure target resolution for drain-hot-node: an explicit
 * {catalogShardId, shardId} override wins outright; otherwise the "hot
 * shard" is, by construction, whatever shard ./load/load-driver.ts's skew
 * mode was last started against (config.targetShardId — the exact lever
 * ./load/skew.ts's generateSkewedKeys biases writes onto), resolved to a
 * catalog by finding a live vbucket-map row it actually owns right now. */
export function pickHotShardTarget(loadStatus: LoadStatusLike, vbucketMap: VbucketMapLike, override?: HotShardOverride): HotShardTarget {
  if (override?.catalogShardId && override?.shardId) {
    return { catalogShardId: override.catalogShardId, shardId: override.shardId };
  }
  if (!loadStatus.running || !loadStatus.config || loadStatus.config.mode !== "skew" || !loadStatus.config.targetShardId) {
    throw new ChaosPreconditionError(
      "No skew-mode load is currently running, so there's no hot shard to target automatically. Start skew load first (POST /api/load/start {mode:'skew', targetShardId}), or pass an explicit {catalogShardId, shardId}.",
    );
  }
  const shardId = loadStatus.config.targetShardId;
  for (const catalog of vbucketMap.catalogs) {
    if (catalog.map.some((row) => row.shardId === shardId)) {
      return { catalogShardId: catalog.catalogShardId, shardId };
    }
  }
  throw new ChaosPreconditionError(`Skew target shard ${shardId} doesn't currently own any vBucket in any catalog — nothing to drain right now.`);
}

/** Pure target resolution for split/migrate-hot-vbucket: an explicit
 * {catalogShardId, shardId, vbucket} override wins outright; otherwise
 * resolves the hot SHARD (pickHotShardTarget above) and picks the
 * lowest-numbered vBucket it currently owns (./load/skew.ts's own
 * ownedVBuckets — the exact routing-formula verification the skew driver
 * itself uses, reused rather than reimplemented) as a deterministic,
 * genuinely-hot representative vBucket to reshard. */
export function pickHotVbucketTarget(loadStatus: LoadStatusLike, vbucketMap: VbucketMapLike, override?: HotVbucketOverride): HotVbucketTarget {
  if (override?.catalogShardId && override?.shardId && override.vbucket !== undefined) {
    return { catalogShardId: override.catalogShardId, shardId: override.shardId, vbucket: override.vbucket };
  }
  const hotShard = pickHotShardTarget(loadStatus, vbucketMap, override);
  const catalog = vbucketMap.catalogs.find((c) => c.catalogShardId === hotShard.catalogShardId);
  if (!catalog) {
    throw new ChaosPreconditionError(`Catalog ${hotShard.catalogShardId} not found in the live vBucket map — topology may have just changed; retry.`);
  }
  const ownership: VBucketOwnership[] = catalog.map.map((row) => ({ vbucket: row.vbucket, shardId: row.shardId }));
  const owned = [...ownedVBuckets(ownership, hotShard.shardId)].sort((x, y) => x - y);
  const vbucket = owned[0];
  if (vbucket === undefined) {
    throw new ChaosPreconditionError(`Hot shard ${hotShard.shardId} owns no vBuckets in catalog ${hotShard.catalogShardId} right now.`);
  }
  return { ...hotShard, vbucket };
}

/** Pure target resolution for abort-migration: an explicit
 * {catalogShardId, vbucket} override wins outright (best-effort resolves the
 * owning shardId from the live map purely for a nicer `did` message —
 * migrateVbucketAbort itself doesn't need shardId); otherwise scans every
 * catalog's live vbucket map for the first row whose migrationStatus isn't
 * "none". Deliberately does NOT depend on load-driver's config at all — the
 * in-flight migration being aborted may have been started by
 * split-hot-vbucket/migrate-hot-vbucket, or manually from the Reshard
 * console; either is a legitimate "real mid-cutover disruption" to fire this
 * attack at. */
export function pickInFlightMigrationTarget(
  vbucketMap: VbucketMapLike,
  override?: { catalogShardId?: string; vbucket?: number },
): HotVbucketTarget {
  if (override?.catalogShardId && override.vbucket !== undefined) {
    const catalog = vbucketMap.catalogs.find((c) => c.catalogShardId === override.catalogShardId);
    const row = catalog?.map.find((r) => r.vbucket === override.vbucket);
    return { catalogShardId: override.catalogShardId, vbucket: override.vbucket, shardId: row?.shardId ?? "unknown" };
  }
  for (const catalog of vbucketMap.catalogs) {
    const row = catalog.map.find((r) => r.migrationStatus && r.migrationStatus !== "none");
    if (row) {
      return { catalogShardId: catalog.catalogShardId, vbucket: row.vbucket, shardId: row.shardId };
    }
  }
  throw new ChaosPreconditionError(
    "No migration is currently in-flight anywhere in the cluster — trigger split-hot-vbucket or migrate-hot-vbucket first, or start one manually from the Reshard room, before firing abort-migration.",
  );
}

export interface BlipShardTarget extends HotShardTarget {
  /** true iff this shard is currently the SOURCE or TARGET of an in-flight
   * migration (migrationStatus !== "none") anywhere in the live vbucket map
   * — computed and returned even for an explicit override, so the caller
   * can honestly warn instead of silently muddying the "clean blip" story. */
  isMigrating: boolean;
}

/** Pure target resolution for blip-shard-offline: an explicit
 * {catalogShardId, shardId} override wins outright; otherwise picks the
 * lowest shardId (deterministic, same "pick the lowest" convention as
 * pickHotVbucketTarget above) that is NOT currently the source or target of
 * any in-flight migration anywhere in the live map.
 *
 * WHY avoid migrating shards by default: blipping a shard mid-reshard PARKS
 * that topology op until its lock lease expires or an operator
 * force-releases it — a real, by-design recovery path (see src/shard.ts's
 * own header comment on this exact interaction), not a bug. That's a
 * legitimate thing to demonstrate on purpose, but it's a DIFFERENT story
 * than the clean "shard drops, cluster holds lost:0, shard recovers" demo
 * this attack is meant to tell — mixing them by accident would make the
 * clean story look flaky. If every known shard happens to be mid-migration
 * (or the map is empty of migration-status data), this still returns the
 * lowest shardId rather than refusing outright — `isMigrating` on the
 * result tells the caller (runBlipShardOfflineAttack) whether to warn, so
 * nothing is silently muddied either way. Throws only when there are no
 * shards at all in the live map (nothing to target, override or not, unless
 * the override itself supplies its own shardId). */
export function pickBlipShardTarget(vbucketMap: VbucketMapLike, override?: HotShardOverride): BlipShardTarget {
  const migratingShardIds = new Set<string>();
  const candidatesByShardId = new Map<string, HotShardTarget>();
  for (const catalog of vbucketMap.catalogs) {
    for (const row of catalog.map) {
      if (!candidatesByShardId.has(row.shardId)) {
        candidatesByShardId.set(row.shardId, { catalogShardId: catalog.catalogShardId, shardId: row.shardId });
      }
      if (row.migrationStatus && row.migrationStatus !== "none") {
        migratingShardIds.add(row.shardId);
        if (row.targetShardId) migratingShardIds.add(row.targetShardId);
      }
    }
  }

  if (override?.catalogShardId && override?.shardId) {
    return { catalogShardId: override.catalogShardId, shardId: override.shardId, isMigrating: migratingShardIds.has(override.shardId) };
  }

  const candidates = [...candidatesByShardId.values()].sort((a, b) => (a.shardId < b.shardId ? -1 : a.shardId > b.shardId ? 1 : 0));
  if (candidates.length === 0) {
    throw new ChaosPreconditionError(
      "No shards found in the live vBucket map — nothing to target for blip-shard-offline. Wait for the cluster to initialize, or pass an explicit {catalogShardId, shardId}.",
    );
  }
  const nonMigrating = candidates.find((c) => !migratingShardIds.has(c.shardId));
  const chosen = nonMigrating ?? candidates[0];
  return { ...chosen, isMigrating: migratingShardIds.has(chosen.shardId) };
}

// ---- impure gatherers: real DO/RPC calls -----------------------------------

interface RawLoadStatusResponse {
  running?: boolean;
  config?: { mode?: string; targetShardId?: string | null; baseUrl?: string | null } | null;
  correctness?: { lost?: number; meterState?: string } | null;
}

/** Same fetch()-over-DO-binding pattern as ./aggregator.ts's own
 * fetchLoadDriverStatus — never throws; a failure/cold-start degrades to
 * "not running", which every caller here already treats as "auto-detection
 * unavailable" (via ChaosPreconditionError), never a hard crash. Also
 * carries `correctness` (blip-shard-offline's own need — see
 * LoadStatusLike's doc comment) straight off the LoadDriver DO's raw
 * response, same as aggregator.ts's fetchLoadDriverStatus reads it, with no
 * "zero it out if not running" massaging here (that's a presentation
 * decision left to each caller/classifier). */
async function fetchLoadStatus(env: Env): Promise<LoadStatusLike> {
  try {
    const id = env.LOAD_DRIVER.idFromName("singleton");
    const stub = env.LOAD_DRIVER.get(id);
    const res = await stub.fetch("https://load-driver.internal/api/load/status");
    if (!res.ok) return { running: false, config: null, correctness: null };
    const body = (await res.json()) as RawLoadStatusResponse;
    return {
      running: !!body.running,
      config: body.config
        ? { mode: body.config.mode ?? null, targetShardId: body.config.targetShardId ?? null, baseUrl: body.config.baseUrl ?? null }
        : null,
      correctness:
        body.correctness && typeof body.correctness.lost === "number"
          ? { lost: body.correctness.lost, meterState: body.correctness.meterState === "red" ? "red" : "green" }
          : null,
    };
  } catch {
    return { running: false, config: null, correctness: null };
  }
}

async function fetchVbucketMap(env: Env): Promise<VbucketMapLike> {
  return (await env.SHARD_API.adminVbucketMap(env.ADMIN_TOKEN)) as VbucketMapLike;
}

export interface DrainHotNodeInput extends HotShardOverride {}
export const parseDrainHotNodeInput = parseHotShardOverride;

export async function runDrainHotNodeAttack(env: Env, input: DrainHotNodeInput): Promise<ChaosOutcome> {
  const [loadStatus, vbucketMap] = await Promise.all([fetchLoadStatus(env), fetchVbucketMap(env)]);
  const target = pickHotShardTarget(loadStatus, vbucketMap, input);
  const result = (await drainShard(env, { catalogShardId: target.catalogShardId, shardId: target.shardId })) as {
    ok?: boolean;
    evacuationStarted?: boolean;
  };
  return {
    attack: "drain-hot-node",
    did: `called reshard drain on the hot shard ${target.shardId} (catalog ${target.catalogShardId}) while load is running`,
    expected: "the cluster evacuates every vBucket (and any pinned index ring) off this shard onto others; the T4 scoreboard's lost stays 0 throughout",
    observed: JSON.stringify(result),
    survived: !!result?.evacuationStarted,
    note: "this attack only FIRES the drain (async, multi-tick) — watch the always-visible T4 scoreboard above, or poll GET /api/reshard/drain-status, to see the evacuation actually complete with lost:0.",
  };
}

export interface SplitHotVbucketInput extends HotVbucketOverride {
  newShardId?: string;
}
export function parseSplitHotVbucketInput(body: unknown): SplitHotVbucketInput {
  const b = asRecord(body);
  return { ...parseHotVbucketOverride(body), newShardId: optionalNonEmptyString(b.newShardId) };
}

export async function runSplitHotVbucketAttack(env: Env, input: SplitHotVbucketInput): Promise<ChaosOutcome> {
  const [loadStatus, vbucketMap] = await Promise.all([fetchLoadStatus(env), fetchVbucketMap(env)]);
  const target = pickHotVbucketTarget(loadStatus, vbucketMap, input);
  const result = (await splitVbucket(env, { catalogShardId: target.catalogShardId, vbucket: target.vbucket, newShardId: input.newShardId })) as {
    ok?: boolean;
    toShard?: string;
    migrationStarted?: boolean;
  };
  return {
    attack: "split-hot-vbucket",
    did: `called reshard split on the hot vBucket ${target.vbucket} (catalog ${target.catalogShardId}, currently on ${target.shardId}) while load is running`,
    expected: "a fresh target shard takes the hot vBucket via a real live-cutover migration; the T4 scoreboard's lost stays 0 throughout",
    observed: JSON.stringify(result),
    survived: !!result?.migrationStarted,
    note: "this attack only FIRES the split — watch the Topology room's migration path + the T4 scoreboard above, or poll GET /api/reshard/migrate-status, to see the cutover actually land with lost:0.",
  };
}

export interface MigrateHotVbucketInput extends HotVbucketOverride {
  targetShardId?: string;
}
export function parseMigrateHotVbucketInput(body: unknown): MigrateHotVbucketInput {
  const b = asRecord(body);
  return { ...parseHotVbucketOverride(body), targetShardId: optionalNonEmptyString(b.targetShardId) };
}

export async function runMigrateHotVbucketAttack(env: Env, input: MigrateHotVbucketInput): Promise<ChaosOutcome> {
  const [loadStatus, vbucketMap] = await Promise.all([fetchLoadStatus(env), fetchVbucketMap(env)]);
  const target = pickHotVbucketTarget(loadStatus, vbucketMap, input);
  const result = (await migrateVbucket(env, { catalogShardId: target.catalogShardId, vbucket: target.vbucket, targetShardId: input.targetShardId })) as {
    ok?: boolean;
    toShard?: string;
    status?: string;
  };
  return {
    attack: "migrate-hot-vbucket",
    did: `called reshard migrate on the hot vBucket ${target.vbucket} (catalog ${target.catalogShardId}, currently on ${target.shardId}) while load is running`,
    expected: "the hot vBucket migrates onto a new/target shard via a real live-cutover; the T4 scoreboard's lost stays 0 throughout",
    observed: JSON.stringify(result),
    survived: !!result?.ok && (result?.status === "backfilling" || result?.status === undefined),
    note: "this attack only FIRES the migrate — watch the Topology room's migration path + the T4 scoreboard above, or poll GET /api/reshard/migrate-status, to see the cutover actually land with lost:0.",
  };
}

export interface AbortMigrationInput {
  catalogShardId?: string;
  vbucket?: number;
}
export function parseAbortMigrationInput(body: unknown): AbortMigrationInput {
  const b = asRecord(body);
  return { catalogShardId: optionalNonEmptyString(b.catalogShardId), vbucket: optionalNonNegativeInt(b.vbucket) };
}

export async function runAbortMigrationAttack(env: Env, input: AbortMigrationInput): Promise<ChaosOutcome> {
  const vbucketMap = await fetchVbucketMap(env);
  const target = pickInFlightMigrationTarget(vbucketMap, input);
  const result = (await migrateVbucketAbort(env, { catalogShardId: target.catalogShardId, vbucket: target.vbucket })) as {
    ok?: boolean;
    status?: string;
  };
  return {
    attack: "abort-migration",
    did: `called reshard migrate-abort on the in-flight migration for vBucket ${target.vbucket} (catalog ${target.catalogShardId}, source ${target.shardId})`,
    expected: "the migration rolls back cleanly — the source shard stays authoritative, the partial target copy is discarded; the T4 scoreboard's lost stays 0",
    observed: JSON.stringify(result),
    survived: !!result?.ok && result?.status === "aborted",
    note: "this attack only FIRES the abort — watch the always-visible T4 scoreboard above to confirm lost stays 0 through the rollback.",
  };
}

// ============================================================================
// Attack f — blip-shard-offline. Calls env.SHARD_API.adminFaultInject
// directly (the core's real, admin-gated fault-injection primitive — see
// env.d.ts's doc comment on that method and this file's header comment) on a
// TARGET shard chosen by pickBlipShardTarget above. Unlike the topology
// attacks (which reuse ./reshard.ts's wrappers), this one calls SHARD_API
// itself because there is no Reshard-console equivalent to reuse — blipping
// a shard offline isn't a manual operator control this app exposes anywhere
// else, only a chaos attack.
// ============================================================================

// ~9s: long enough to be visibly watchable on the live dashboard (topology
// canvas + T4 scoreboard), comfortably under the core's absolute 30s hard
// cap (FAULT_MAX_MS in src/shard.ts).
const DEFAULT_BLIP_DURATION_MS = 9000;
// Shardscope's OWN conservative ceiling for this one-click button — well
// under the core's 30s cap, not a re-implementation of it (the core clamps
// independently and absolutely regardless of what this file sends; this is
// just a sane upper bound for a demo button an operator might otherwise type
// an enormous number into).
const MAX_BLIP_DURATION_MS = 15000;

/** Coerces a caller-supplied durationMs into a safe range for this button:
 * any non-positive-integer input (including omitted) falls back to
 * DEFAULT_BLIP_DURATION_MS; anything larger than MAX_BLIP_DURATION_MS is
 * clamped down to it. Never forwards an unbounded value to SHARD_API — even
 * though the core clamps independently too, this keeps the request itself
 * honest about what it's asking for. */
function clampBlipDurationMs(value: unknown): number {
  const n = typeof value === "string" && value.trim() !== "" ? Number(value) : value;
  if (typeof n !== "number" || !Number.isFinite(n) || !Number.isInteger(n) || n <= 0) return DEFAULT_BLIP_DURATION_MS;
  return Math.min(n, MAX_BLIP_DURATION_MS);
}

export interface BlipShardOfflineInput extends HotShardOverride {
  durationMs?: number;
}

export function parseBlipShardOfflineInput(body: unknown): BlipShardOfflineInput {
  const b = asRecord(body);
  return {
    catalogShardId: optionalNonEmptyString(b.catalogShardId),
    shardId: optionalNonEmptyString(b.shardId),
    durationMs: clampBlipDurationMs(b.durationMs),
  };
}

// Substrings this file matches against a thrown "CloudflareShard RPC error
// <status>: <body>" message (see the main repo's src/index.ts's
// unwrapForRpc) to tell apart the two SPECIFIC, expected rejection shapes
// adminFaultInjectCore can produce (see that function + requireFaultInjectionEnabled
// / rejectIfUnknownShard in the main repo's src/index.ts) from any other,
// unrelated failure. Matching on a stable substring of the core's own error
// text (not the full string, which also embeds a status-dependent JSON
// envelope) — same "match a documented contract substring, not the whole
// message" idiom this file already uses for MISMATCH_REJECTION_SUBSTRING
// above.
const FAULT_INJECTION_DISABLED_SUBSTRING = "Fault injection is disabled";
const UNKNOWN_SHARD_SUBSTRING = "UNKNOWN_SHARD";

/** Pure classification of a thrown SHARD_API.adminFaultInject rejection's
 * message into a specific, honest ChaosPreconditionError message — or `null`
 * if the message matches neither expected rejection shape, telling
 * runBlipShardOfflineAttack to let it propagate UNCHANGED (so an unrelated
 * failure, e.g. an unexpected 5xx, still surfaces via runChaosOp's own
 * generic "CloudflareShard RPC error <status>: <body>" unpacking, exactly
 * like every other chaos attack that calls a SHARD_API method directly).
 *
 * Exported and unit-tested directly (chaos.test.ts) so the "a 403 means the
 * flag is off, not a generic failure or a false ✗ broke" requirement is
 * provably correct without a live cluster or a mocked SHARD_API — this is
 * exactly the kind of "must not silently misclassify" logic this module's
 * pure-classification-core convention (see this file's header comment)
 * exists for. */
export function classifyBlipFaultInjectError(errorMessage: string, target: HotShardTarget): string | null {
  if (errorMessage.includes(FAULT_INJECTION_DISABLED_SUBSTRING)) {
    return (
      'blip-shard-offline requires the core\'s fault-injection primitive, which is OFF by default. ' +
      'Set FAULT_INJECTION_ENABLED="true" on the cloudflare-shard-mvp Worker (never in production) to enable it — ' +
      "this is intentional, not a bug: an admin-gated, off-by-default fault surface is the whole point."
    );
  }
  if (errorMessage.includes(UNKNOWN_SHARD_SUBSTRING)) {
    return `blip-shard-offline's target shard (${target.shardId}) is not a currently-known shard in the live vBucket map — topology may have just changed underneath this attack; retry.`;
  }
  return null;
}

export interface BlipShardOfflineRawResult {
  target: BlipShardTarget;
  durationMs: number;
  /** true iff the core's adminFaultInject call itself came back `{ ok: true,
   * ... }` — this DOES mean the shard is now genuinely returning 503, not a
   * simulation (a rejection never reaches this point at all — see
   * runBlipShardOfflineAttack, which throws before ever calling this
   * classifier). */
  injectOk: boolean;
  /** Raw success body (JSON.stringify'd) for the receipt — e.g.
   * `{"ok":true,"mode":"unreachable","faultExpiresAt":...}` (see
   * src/shard.ts's handleFaultInject). */
  injectResponseSummary: string;
  /** Correctness meter read at the MOMENT of firing — null when no load run
   * is currently active or the read failed (see LoadStatusLike's doc
   * comment). */
  lostAtFireTime: number | null;
  meterStateAtFireTime: "green" | "red" | null;
  loadRunning: boolean;
}

/** Pure judge for blip-shard-offline's FIRING step only. Unlike
 * classifyDoubleSubmit/classifyMismatchedReplay (which judge a FULLY
 * completed round trip), this attack's full claim — lost stays 0 through the
 * WHOLE injected window, AND the shard is reachable again after — can't be
 * verified synchronously inside one HTTP request without blocking the
 * button for the entire durationMs (and still wouldn't prove "reachable
 * again" without a THIRD round trip after that). So this classifier judges
 * only what's knowable the instant the fault was injected:
 *   (1) the core actually accepted the fault (`injectOk` — a genuine
 *       503-producing primitive fired, not a simulation), and
 *   (2) the correctness meter was NOT already showing a loss the moment it
 *       fired (a pre-existing red meter can't be attributed to this attack).
 * The returned outcome's `note` is explicit that the REAL proof — lost
 * staying 0 through the whole window, and the shard coming back — is a LIVE
 * thing to watch on the always-visible T4 scoreboard + Topology canvas
 * after this call returns, exactly like drain-hot-node/split-hot-vbucket/
 * migrate-hot-vbucket/abort-migration's own notes already say for their own
 * async, multi-tick completions. */
export function classifyBlipShardOfflineFire(r: BlipShardOfflineRawResult): ChaosOutcome {
  const meterAlreadyRed = r.lostAtFireTime !== null && r.lostAtFireTime > 0;
  const survived = r.injectOk && !meterAlreadyRed;

  const migrationNote = r.target.isMigrating
    ? `NOTE: ${r.target.shardId} is currently mid-migration (source or target of an in-flight reshard) — blipping it can PARK that topology op until its lock lease expires or an operator force-releases it (a real, by-design recovery path, not a bug — see src/shard.ts). That's a legitimately different story than the clean "shard drops, cluster holds, shard recovers" demo; for that cleaner story, retry once no migration is in flight, or pass an explicit non-migrating {catalogShardId, shardId}.`
    : `${r.target.shardId} was NOT part of any in-flight migration at fire time — a clean, isolated blip.`;

  let note: string;
  if (!r.injectOk) {
    note = "The core's adminFaultInject call did not come back ok — see 'observed'. This is NOT a confirmed fault; nothing was necessarily injected.";
  } else if (meterAlreadyRed) {
    note = `The fault WAS injected, but the correctness meter already showed lost:${r.lostAtFireTime} at the moment of firing — that predates this attack and should be investigated independently; this attack's own contribution can't be isolated from a meter that was already red. ${migrationNote}`;
  } else {
    note = `Fault genuinely injected on ${r.target.shardId} for ~${r.durationMs}ms (hard-capped at 30s by the core) — it will return 503 for that window, then recover on its own. ${migrationNote} This call only FIRES the blip and reads the meter/topology AT THAT INSTANT — the real proof is live: watch the always-visible T4 scoreboard above for lost staying 0 through the window, and the Topology canvas for ${r.target.shardId} marked unavailable then reachable again.`;
  }

  return {
    attack: "blip-shard-offline",
    did: `called adminFaultInject on shard ${r.target.shardId} (catalog ${r.target.catalogShardId}), mode "unreachable", for ~${r.durationMs}ms`,
    expected:
      'the core genuinely returns 503 from this shard\'s Durable Object for the injected window (real, admin-gated, off unless FAULT_INJECTION_ENABLED="true") while the rest of the cluster keeps serving; the T4 scoreboard\'s lost stays 0 throughout, and the shard recovers on its own once the window elapses.',
    observed: `adminFaultInject: ${r.injectResponseSummary} · correctness meter at fire time: ${
      r.loadRunning ? `lost ${r.lostAtFireTime ?? "?"} (${r.meterStateAtFireTime ?? "?"})` : "no load run currently active"
    }`,
    survived,
    note,
  };
}

export async function runBlipShardOfflineAttack(env: Env, input: BlipShardOfflineInput): Promise<ChaosOutcome> {
  const [loadStatus, vbucketMap] = await Promise.all([fetchLoadStatus(env), fetchVbucketMap(env)]);
  const target = pickBlipShardTarget(vbucketMap, input);
  const durationMs = input.durationMs ?? DEFAULT_BLIP_DURATION_MS;

  let injectResult: { ok?: boolean; mode?: string; faultExpiresAt?: number };
  try {
    injectResult = (await env.SHARD_API.adminFaultInject(env.ADMIN_TOKEN, {
      shardId: target.shardId,
      catalogShardId: target.catalogShardId,
      mode: "unreachable",
      durationMs,
    })) as typeof injectResult;
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const classified = classifyBlipFaultInjectError(message, target);
    if (classified) throw new ChaosPreconditionError(classified);
    // Anything else (an unexpected 5xx, an auth misconfiguration) surfaces
    // via runChaosOp's generic "CloudflareShard RPC error <status>: <body>"
    // unpacking in src/index.ts, same as every other chaos attack that calls
    // a SHARD_API method directly.
    throw err;
  }

  return classifyBlipShardOfflineFire({
    target,
    durationMs,
    injectOk: !!injectResult?.ok,
    injectResponseSummary: JSON.stringify(injectResult),
    lostAtFireTime: loadStatus.correctness?.lost ?? null,
    meterStateAtFireTime: loadStatus.correctness?.meterState ?? null,
    loadRunning: loadStatus.running,
  });
}
