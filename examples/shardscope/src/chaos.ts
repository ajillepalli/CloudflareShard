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
 *   - Exactly ONE attack this module could plausibly offer ("blip a shard
 *     offline mid-cutover" — make a shard's Durable Object genuinely
 *     unreachable) has NO real implementation, because cloudflare-shard-mvp
 *     has no fault-injection primitive to make that happen. There is
 *     DELIBERATELY no runXxxAttack function for it in this file, and
 *     src/index.ts wires NO working route for it either (see
 *     CHAOS_NOT_WIRED_ATTACK below and index.ts's explicit 501 stub) — the
 *     UI renders it as a disabled button labeled "needs core fault-injection
 *     — not wired", never a fake success.
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
] as const;
export type ChaosAttackKey = (typeof CHAOS_ATTACKS)[number];

/** The one attack this module deliberately does NOT implement — see this
 * file's header comment. Never appears in CHAOS_ATTACKS, has no runXxxAttack
 * function, and src/index.ts wires its route to an explicit, honest 501
 * rather than routing it here. Exported so the UI/tests can both point at
 * one canonical string instead of inventing their own. */
export const CHAOS_NOT_WIRED_ATTACK = "blip-shard-offline";

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
}

/** Loosely-typed slice of adminVbucketMap's response — only the fields
 * target-resolution reads (mirrors aggregator.ts's own local
 * AdminVbucketMapResponse). */
export interface VbucketMapLike {
  catalogs: Array<{
    catalogShardId: string;
    totalVBuckets: number;
    map: Array<{ vbucket: number; shardId: string; migrationStatus: string }>;
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

// ---- impure gatherers: real DO/RPC calls -----------------------------------

interface RawLoadStatusResponse {
  running?: boolean;
  config?: { mode?: string; targetShardId?: string | null; baseUrl?: string | null } | null;
}

/** Same fetch()-over-DO-binding pattern as ./aggregator.ts's own
 * fetchLoadDriverStatus — never throws; a failure/cold-start degrades to
 * "not running", which every caller here already treats as "auto-detection
 * unavailable" (via ChaosPreconditionError), never a hard crash. */
async function fetchLoadStatus(env: Env): Promise<LoadStatusLike> {
  try {
    const id = env.LOAD_DRIVER.idFromName("singleton");
    const stub = env.LOAD_DRIVER.get(id);
    const res = await stub.fetch("https://load-driver.internal/api/load/status");
    if (!res.ok) return { running: false, config: null };
    const body = (await res.json()) as RawLoadStatusResponse;
    return {
      running: !!body.running,
      config: body.config
        ? { mode: body.config.mode ?? null, targetShardId: body.config.targetShardId ?? null, baseUrl: body.config.baseUrl ?? null }
        : null,
    };
  } catch {
    return { running: false, config: null };
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
