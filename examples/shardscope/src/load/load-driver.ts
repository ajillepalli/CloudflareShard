/** load-driver.ts — LoadDriver, an alarm-driven Durable Object that runs
 * rolling batches of the TPC-C-style transaction mix (./transactions.ts)
 * against the CloudflareShard gateway, in either "uniform" (normal random
 * keys) or "skew" (deterministic hot-shard, ./skew.ts) mode.
 *
 * One LoadDriver instance exists per Shardscope deployment (addressed via
 * idFromName("singleton") from src/index.ts, mirroring TopologyAggregator's
 * own singleton pattern in src/aggregator.ts). fetch() exposes three routes:
 *   POST /api/load/start  {mode, targetShardId?, concurrency?, baseUrl?, warehouseIds?, ...}
 *   POST /api/load/stop
 *   GET  /api/load/status
 *
 * Each alarm tick issues a BOUNDED batch of transactions (see
 * MAX_SUBREQUESTS_PER_TICK below — Workers cap subrequests per invocation,
 * and this must stay well under that cap) and reschedules the next tick only
 * while still running. The actual per-transaction network I/O goes through
 * an injected TxExecutor (./gateway-client.ts's HttpTxExecutor, built from a
 * TokenProvider — see ./token-provider.ts). As of T5, that TokenProvider is
 * ./tenant-token-store.ts's TenantTokenStoreTokenProvider, backed by a
 * durable get-or-create tenant-token store, so real transactions actually
 * issue; the mix/skew/batch logic in this file is complete and correct
 * independent of that, and is exercised directly by the vitest suite
 * alongside ./transactions.ts and ./skew.ts.
 */
import type { Env } from "../env";
import { hashKey } from "../../../../src/hash";
import {
  UniformKeyPicker,
  runOneTransaction,
  stockKey,
  tenantIdForWarehouse,
  type KeyPicker,
  type Rng,
  type TpccWorldConfig,
  type TransactionOutcome,
  type TransactionType,
  type TxExecutor,
} from "./transactions";
import { generateSkewedKeys, type VBucketOwnership } from "./skew";
import type { TokenProvider } from "./token-provider";
import { TenantTokenStoreTokenProvider } from "./tenant-token-store";
import { HttpTxExecutor, HttpSqlPointReader } from "./gateway-client";
import {
  seedScenarioReferenceData,
  verifySeededDataIndexed,
  SCENARIO_DISTRICTS_PER_WAREHOUSE,
  SCENARIO_CUSTOMERS_PER_DISTRICT,
  SCENARIO_ITEM_COUNT,
} from "./scenario-seed";
import { ensureScenarioTables, ensureScenarioIndexesReady, HttpSchemaAdminClient } from "./schema-bootstrap";
import {
  CorrectnessTracker,
  TrackingTxExecutor,
  emptyCorrectnessCounters,
  gatewayReadBack,
  isExpectedAbort,
  meterStateFor,
  pickTrackedCandidates,
  type CatalogVBucketMap,
  type CorrectnessCounters,
  type TrackedCandidate,
  type VBucketMigrationRow,
} from "./correctness";

export type LoadMode = "uniform" | "skew";

const DEFAULT_CONCURRENCY = 8;
const MAX_CONCURRENCY = 32;
const DEFAULT_WAREHOUSE_IDS = [1];
const DEFAULT_DISTRICTS_PER_WAREHOUSE = 10;
const DEFAULT_CUSTOMERS_PER_DISTRICT = 100;
const DEFAULT_ITEM_COUNT = 200;

// Codex review P2 fix (round 12): self-seeding's whole bootstrap sequence
// (one /v1/mutate per seeded row, then verifySeededDataIndexed's own
// per-row index-visibility checks) runs synchronously inside ONE
// /api/load/start invocation, before the first alarm is ever scheduled —
// unlike a real benchmark harness (examples/tpc-c-benchmark), which seeds
// across many separate script invocations over minutes. A caller passing
// DEFAULT_*-scale custom counts (10 districts * 100 customers, 200 items)
// with self-seeding still on would multiply the row count (and therefore
// subrequest count — each row is a token resolution + an HTTP call) by
// roughly 10-30x over the SCENARIO_* defaults this whole feature was sized
// around, risking the Worker's own per-invocation subrequest budget before
// bootstrap even finishes. These caps bound self-seeding specifically —
// well above the SCENARIO_* defaults (room for real customization) but well
// below DEFAULT_*'s benchmark scale (which is exactly what
// `seedReferenceData: false` exists for, seeding externally instead).
const MAX_SELF_SEED_DISTRICTS_PER_WAREHOUSE = 12;
const MAX_SELF_SEED_CUSTOMERS_PER_DISTRICT = 20;
const MAX_SELF_SEED_ITEM_COUNT = 100;
// Codex review P2 fix (round 13): the caps above bound ONE warehouse's own
// row count; this bounds how many warehouses self-seeding loops over in the
// same synchronous request — see the rejection's own comment at its call
// site for the full reasoning.
const MAX_SELF_SEED_WAREHOUSES = 2;

// Codex review P2 fix (round 14): the independent per-field caps above don't
// compose safely — they can each individually pass while their COMBINATION
// still blows the budget. Worked example at the SCENARIO_* defaults alone (1
// warehouse, 6 districts, 10 customers/district, 60 items): 187 seeded rows
// + 120 index-visibility checks = 307 operations, each costing 2
// subrequests (a tenant-token resolution plus the actual gateway call — see
// gateway-client.ts's HttpTxExecutor.post) = 614 subrequests for ONE
// warehouse at the SHIPPED DEFAULT scenario alone. Two warehouses at those
// same defaults (previously allowed — MAX_SELF_SEED_WAREHOUSES is 2) would
// be ~1228, already over a 1000-subrequest budget; one warehouse at the
// individual MAX_SELF_SEED_* caps (12/20/100) is ~1586 on its own. Rather
// than keep discovering more under-budgeted combinations one Codex round at
// a time, this computes the ACTUAL projected subrequest count for the
// specific request about to run and rejects outright if it's not
// comfortably under a real Worker's subrequest ceiling — see
// projectedSelfSeedSubrequests's own doc comment for the exact formula.
const MAX_SELF_SEED_PROJECTED_SUBREQUESTS = 850;

/** Projects the total subrequest count self-seeding + its own post-seed
 * verification will make for `warehouseCount` warehouses at the given
 * per-warehouse district/customer/item counts — see
 * MAX_SELF_SEED_PROJECTED_SUBREQUESTS's own comment for why this exists and
 * the worked numbers behind the chosen budget. Per warehouse:
 *   seeded rows    = 1 (warehouse) + D (districts) + D*C (customers)
 *                     + I (items) + I (stock, one per item)
 *   verify checks  = I (item canaries) + D*C (customer canaries)
 *                     — see verifySeededDataIndexed
 *   subrequests    = 2 * (seeded rows + verify checks) — each operation is
 *                     a tenant-token resolution PLUS the actual gateway
 *                     call (see gateway-client.ts's HttpTxExecutor.post)
 * Multiplied by `warehouseCount` since seedScenarioReferenceData and
 * verifySeededDataIndexed both loop over every warehouse in sequence,
 * inside the SAME request. */
function projectedSelfSeedSubrequests(warehouseCount: number, districtsPerWarehouse: number, customersPerDistrict: number, itemCount: number): number {
  const seededRowsPerWarehouse = 1 + districtsPerWarehouse + districtsPerWarehouse * customersPerDistrict + itemCount + itemCount;
  const verifyChecksPerWarehouse = itemCount + districtsPerWarehouse * customersPerDistrict;
  return warehouseCount * 2 * (seededRowsPerWarehouse + verifyChecksPerWarehouse);
}

// How often the alarm fires while running.
const TICK_INTERVAL_MS = 1000;

// LoadDriver is a single shared singleton (idFromName("singleton") — see this
// file's header comment), not one instance per viewer, so nothing here can
// tell "the person who started this forgot about it" apart from "someone's
// still actively watching it run" — there is no per-viewer signal to key an
// idle-timeout off, unlike TopologyAggregator's per-subscriber SSE
// connections. A hard wall-clock cap is the only mechanism that can't be
// forgotten open: past this duration the run force-stops itself regardless
// of whether anyone's watching, capping worst-case cost from a demo scenario
// nobody remembered to stop rather than depending on that never happening.
export const MAX_LOAD_RUN_DURATION_MS = 15 * 60_000; // 15 minutes

// Workers hard-cap subrequests per Worker invocation (an alarm() call is one
// invocation) well below what an unbounded loop here could otherwise fan
// out. New-Order is this mix's worst case: 1 tableScan + 1 header tx + up to
// 15 lines * (~2 calls each: an indexQuery/mutate pair or a 2-mutation tx)
// + 1 marker mutate ≈ 1 + 1 + 15*2 + 1 = 33 subrequests for ONE transaction
// in the COMMON case. But a remote New-Order line that loses its race on the
// target stock row triggers compensation (transactions.ts's
// compensateFailedOrder), which reverses every already-committed line with
// ONE ADDITIONAL mutate call each — up to 15 more calls, roughly DOUBLING
// the worst case for a transaction that fails partway through. This constant
// is sized against that DOUBLED worst case (not just the common-case 33), so
// a batch sized against it stays safely under the platform's actual
// 1000-subrequest ceiling even on a tick where every in-flight transaction
// happens to hit the compensation path.
const WORST_CASE_SUBREQUESTS_PER_TRANSACTION = 85;
const MAX_SUBREQUESTS_PER_TICK = 800;
const MAX_TRANSACTIONS_PER_TICK = Math.max(1, Math.floor(MAX_SUBREQUESTS_PER_TICK / WORST_CASE_SUBREQUESTS_PER_TRANSACTION));

// How many skewed item-id candidates to precompute per warehouse, cached
// until the next vbucket-map refresh — see refreshSkewPoolsFromMap below.
const SKEW_POOL_SIZE = 25;
// Bounds skew.ts's own per-warehouse candidate scan (over item ids
// 1..itemCount) — see skew.ts's generateSkewedKeys maxAttempts doc comment
// for why a bound like this always terminates even when the target shard
// owns few or no vBuckets in a given warehouse's catalog.
const SKEW_SCAN_MAX_ATTEMPTS = 20000;
// Re-fetch the vbucket map at most this often — topology genuinely doesn't
// change every second, and this call itself counts against the tick's
// subrequest budget. Shared by BOTH skew-pool recomputation (skew mode only)
// and the correctness tracker's migrating-vbucket bias (every mode) — see
// refreshVbucketMapIfNeeded below, which is the ONE place this fetch happens
// regardless of how many features want the result this tick.
const SKEW_REFRESH_INTERVAL_MS = 5000;

// How often the correctness tracker's known-key verifier actually runs a
// read-back pass (Shardscope T4 — see ./correctness.ts's header comment).
// Deliberately a SEPARATE named constant from SKEW_REFRESH_INTERVAL_MS even
// though the two happen to share a value today: one governs "how fresh is
// our view of the vbucket map", the other "how often do we re-check tracked
// keys against it" — they are conceptually independent cadences that could
// diverge later (e.g. verifying more aggressively than the topology poll
// during a chaos run).
const VERIFY_INTERVAL_MS = 5000;

interface LoadDriverConfig {
  mode: LoadMode;
  targetShardId: string | null;
  concurrency: number;
  baseUrl: string | null;
  warehouseIds: number[];
  districtsPerWarehouse: number;
  customersPerDistrict: number;
  itemCount: number;
}

/** Pure resolution logic for handleStart's `baseUrl`, pulled out so it's
 * directly unit-testable without standing up a full DO test harness (this
 * file has none today — see load-driver.test.ts's header comment). An
 * explicit, non-empty body value always wins; otherwise falls back to the
 * Worker's own CORE_GATEWAY_BASE_URL env var (env.d.ts's doc comment); `null`
 * only if neither is a usable string, matching handleStart's pre-existing
 * "no base URL configured" contract. */
export function resolveLoadDriverBaseUrl(bodyBaseUrl: unknown, envBaseUrl: unknown): string | null {
  if (typeof bodyBaseUrl === "string" && bodyBaseUrl.length > 0) return bodyBaseUrl;
  if (typeof envBaseUrl === "string" && envBaseUrl.length > 0) return envBaseUrl;
  return null;
}

/** True iff a run started at `startedAt` should be force-stopped as of `now`
 * — see MAX_LOAD_RUN_DURATION_MS's doc comment for why this exists (no
 * per-viewer idle signal to key off, unlike TopologyAggregator's SSE
 * subscribers, so a hard wall-clock cap is the only mechanism that can't be
 * forgotten open). `startedAt: null` (never started, or already stopped) is
 * never over the cap — nothing to expire. */
export function hasLoadRunExceededMaxDuration(startedAt: number | null, now: number): boolean {
  if (startedAt === null) return false;
  return now - startedAt > MAX_LOAD_RUN_DURATION_MS;
}

interface TypeCounters {
  attempted: number;
  ok: number;
  err: number;
}

function emptyTypeCounters(): Record<TransactionType, TypeCounters> {
  return {
    "new-order": { attempted: 0, ok: 0, err: 0 },
    payment: { attempted: 0, ok: 0, err: 0 },
    "order-status": { attempted: 0, ok: 0, err: 0 },
    delivery: { attempted: 0, ok: 0, err: 0 },
    "stock-level": { attempted: 0, ok: 0, err: 0 },
  };
}

interface LoadDriverCounters {
  attempted: number;
  ok: number;
  err: number;
  byType: Record<TransactionType, TypeCounters>;
}

function emptyCounters(): LoadDriverCounters {
  return { attempted: 0, ok: 0, err: 0, byType: emptyTypeCounters() };
}

interface LoadDriverState {
  running: boolean;
  config: LoadDriverConfig | null;
  counters: LoadDriverCounters;
  // Shardscope T4 — persisted mirror of this.correctnessTracker's counters
  // (see the CorrectnessTracker instance field below). Persisted for the
  // same reason `counters` above is: a DO eviction/restart must not silently
  // reset the scoreboard to zero mid-run. The tracked-key SET itself
  // (which keys are being verified, and their last-acked values) is
  // deliberately NOT persisted here — like `skewPools` below, it's safe to
  // lose on eviction (the next verify cadence just rebuilds it from freshly
  // acked writes), and losing it can never cause a false green OR a false
  // red, only a brief gap in verification coverage.
  correctness: CorrectnessCounters;
  startedAt: number | null;
  lastTickAt: number | null;
  lastError: string | null;
}

function initialState(): LoadDriverState {
  return {
    running: false,
    config: null,
    counters: emptyCounters(),
    correctness: emptyCorrectnessCounters(),
    startedAt: null,
    lastTickAt: null,
    lastError: null,
  };
}

const STATE_STORAGE_KEY = "load-driver-state";

// ----------------------------------------------------------------------------
// Response shapes for the one admin call this DO makes over env.SHARD_API
// when refreshing skew pools — mirrors aggregator.ts's own local
// AdminVbucketMapResponse (which mirrors adminVbucketMapCore's actual JSON
// body in src/index.ts). Kept local here for the same reason aggregator.ts
// keeps its own copy: nothing else in this file needs it, and env.d.ts's
// ShardApiBinding intentionally returns `unknown` for callers to narrow.
// ----------------------------------------------------------------------------

interface VbucketMapRow {
  vbucket: number;
  shardId: string;
  migrationStatus: string;
  targetShardId: string | null;
}

interface AdminVbucketMapResponse {
  catalogShardCount: number;
  totalVBuckets: number;
  catalogs: Array<{ catalogShardId: string; totalVBuckets: number; map: VbucketMapRow[] }>;
}

/** Which catalog shard governs a given tenant. Deliberately duplicated (not
 * imported) from src/index.ts's private, non-exported `catalogShardIdForTenant`
 * — that file is a separate deployable Worker's internal module, not a
 * shared library, so this small pure formula is mirrored here instead.
 * MUST stay in sync with src/index.ts's version: `catalog-${hashKey(tenantId)
 * % catalogShardCount}`. `catalogShardCount` here comes from the live
 * AdminVbucketMapResponse (not a locally-guessed env var), so this can never
 * drift from whatever the cluster was actually initialized with. */
function catalogShardIdForTenant(tenantId: string, catalogShardCount: number): string {
  return `catalog-${hashKey(tenantId) % catalogShardCount}`;
}

/** KeyPicker whose pickItemId is backed by a precomputed per-warehouse pool
 * of item ids verified (via ./skew.ts) to route tpcc_stock writes to the
 * target shard — the deliberate hot-shard lever (see transactions.ts's
 * header comment). Every other pick (warehouse/district/customer) stays
 * uniform, via a wrapped UniformKeyPicker, so "skew" mode still exercises a
 * realistic mix across every table, not just tpcc_stock. */
class SkewKeyPicker implements KeyPicker {
  constructor(
    private readonly base: KeyPicker,
    private readonly itemPools: Map<number, number[]>,
  ) {}

  pickWarehouseId(rng: Rng): number {
    return this.base.pickWarehouseId(rng);
  }

  pickDistrictId(rng: Rng): number {
    return this.base.pickDistrictId(rng);
  }

  pickCustomerId(rng: Rng): number {
    return this.base.pickCustomerId(rng);
  }

  pickItemId(rng: Rng, supplyWarehouseId: number): number {
    const pool = this.itemPools.get(supplyWarehouseId);
    if (!pool || pool.length === 0) {
      // No skewed candidates found for this warehouse as of the last
      // refresh (e.g. the target shard currently owns no vBuckets in this
      // warehouse's catalog) — fall back to a normal uniform pick rather
      // than failing the transaction outright. Best-effort skew per
      // warehouse, not a hard per-call guarantee.
      return this.base.pickItemId(rng, supplyWarehouseId);
    }
    return pool[Math.floor(rng() * pool.length)];
  }
}

export class LoadDriver {
  private readonly state: DurableObjectState;
  private readonly env: Env;
  private tokenProvider: TokenProvider;

  // In-memory only (like TopologyAggregator's lastSnapshot) — a transient
  // cache of skewed item-id pools, recomputed periodically while running in
  // skew mode. Safe to lose on eviction: the next tick just recomputes it.
  private skewPools: Map<number, number[]> = new Map();
  private lastSkewRefreshAt = 0;

  // Shared vbucket-map cache — see refreshVbucketMapIfNeeded below. Feeds
  // BOTH refreshSkewPoolsFromMap (skew mode only) and the correctness
  // tracker's migrating-vbucket bias (every mode), so this fetch happens at
  // most once per SKEW_REFRESH_INTERVAL_MS regardless of how many features
  // want it this tick. In-memory only, same "safe to lose on eviction"
  // reasoning as skewPools.
  private cachedVbucketMap: AdminVbucketMapResponse | null = null;
  private lastVbucketMapRefreshAt = 0;

  // Shardscope T4 — the correctness/loss-detection core (./correctness.ts).
  // In-memory only, like skewPools: a DO eviction loses the tracked-key SET
  // (which keys are currently being verified), but never the durable
  // COUNTERS (those live in LoadDriverState.correctness, reloaded into a
  // fresh tracker via `initialCounters` the first tick after a restart — see
  // hydrateCorrectnessTracker below). Losing the tracked set on eviction can
  // only ever narrow verification coverage for a while, never produce a
  // false green or a false red.
  private correctnessTracker: CorrectnessTracker = new CorrectnessTracker();
  private correctnessHydrated = false;
  private lastVerifyAt = 0;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
    // T5: real, durable tenant-token storage now exists (see
    // ./tenant-token-store.ts) — every warehouse's tenant token is
    // get-or-created (never rotated once issued) on first use and persisted
    // in the TenantTokenStore singleton DO. ./token-provider.ts's
    // EnvTokenProvider stub remains available as an explicit fallback (e.g.
    // for tests that want to inject fixed tokens) but is no longer the
    // default here.
    this.tokenProvider = new TenantTokenStoreTokenProvider(this.env);
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (request.method === "POST" && url.pathname === "/api/load/start") return this.handleStart(request);
    if (request.method === "POST" && url.pathname === "/api/load/stop") return this.handleStop();
    if (request.method === "GET" && url.pathname === "/api/load/status") return this.handleStatus();
    return json({ error: `Unknown load-driver route: ${url.pathname}` }, 404);
  }

  private async loadState(): Promise<LoadDriverState> {
    const stored = await this.state.storage.get<LoadDriverState>(STATE_STORAGE_KEY);
    if (!stored) return initialState();
    // Defensive backfill: a DO instance persisted BEFORE the T4 correctness
    // fields existed would round-trip through storage.get() with `correctness`
    // simply absent (an older stored object, not a type error) — never let a
    // missing field surface as `undefined` counters downstream.
    return { ...stored, correctness: stored.correctness ?? emptyCorrectnessCounters() };
  }

  private async saveState(s: LoadDriverState): Promise<void> {
    await this.state.storage.put(STATE_STORAGE_KEY, s);
  }

  private async handleStart(request: Request): Promise<Response> {
    let body: {
      mode?: string;
      targetShardId?: string;
      concurrency?: number;
      baseUrl?: string;
      warehouseIds?: number[];
      districtsPerWarehouse?: number;
      customersPerDistrict?: number;
      itemCount?: number;
      seedReferenceData?: boolean;
    };
    try {
      body = (await request.json()) ?? {};
    } catch {
      return json({ error: "Invalid JSON body." }, 400);
    }

    // Codex review P2 fix: a run already in progress must never be
    // reseeded — the block below rewrites tpcc_district/tpcc_customer/
    // tpcc_item/tpcc_stock rows via plain upserts with NO compare-and-swap
    // guard against whatever the CURRENT alarm-driven tick is concurrently
    // mutating (a district's d_ytd, a stock row's s_quantity, ...), so a
    // duplicate /api/load/start (a second browser tab, a double-click before
    // the UI's own disable-on-in-flight took effect, a stale client retrying
    // after a slow response) racing an in-flight run would silently clobber
    // its data mid-flight — exactly the kind of write the correctness
    // tracker has no way to distinguish from a genuine loss. A start against
    // an already-running instance is a no-op: return the current status
    // rather than starting a second, colliding run.
    const existing = await this.loadState();
    if (existing.running) {
      return json(toStatusJson(existing, this.correctnessTracker.snapshot()));
    }

    const mode = body.mode === "skew" ? "skew" : body.mode === "uniform" ? "uniform" : undefined;
    if (!mode) {
      return json({ error: "Missing or invalid 'mode'. Must be 'uniform' or 'skew'." }, 400);
    }
    const concurrency = Math.max(1, Math.min(MAX_CONCURRENCY, Number.isFinite(body.concurrency) ? Number(body.concurrency) : DEFAULT_CONCURRENCY));

    // Self-seeding (the default — see the seedReferenceData block below)
    // needs the transaction mix's own districtsPerWarehouse/customersPer
    // District/itemCount to match what actually gets seeded, not the much
    // larger benchmark-scale DEFAULT_* constants (10 districts * 100
    // customers, 200 items) — otherwise New-Order/Payment/Order-Status pick
    // random district/customer ids the bootstrap never created rows for and
    // fail almost every attempt. A caller with seedReferenceData: false
    // (bringing its own already-seeded, real-TPC-C-scale data) still wants
    // the normal DEFAULT_* fallback.
    const willSelfSeed = body.seedReferenceData !== false;
    const warehouseIds = Array.isArray(body.warehouseIds) && body.warehouseIds.length > 0 ? body.warehouseIds : DEFAULT_WAREHOUSE_IDS;

    // Codex review P2 fix (round 13): round 12's MAX_SELF_SEED_* caps bound
    // ONE warehouse's own row/subrequest count, but seedScenarioReferenceData
    // and verifySeededDataIndexed both loop over EVERY warehouse in
    // `warehouseIds`, synchronously, inside this same request — even at the
    // SCENARIO_* defaults, one warehouse is already ~188 seed calls + 120
    // index checks (each itself 2 subrequests: a tenant-token resolution
    // plus the actual HTTP call), so just TWO warehouses would already
    // exceed a Worker's 1000-subrequest budget before the first alarm ever
    // fires. Self-seeding is capped to a small, fixed number of warehouses;
    // a caller wanting a genuinely multi-warehouse scenario already has
    // `seedReferenceData: false` to bring its own externally-seeded data
    // (examples/tpc-c-benchmark's own Node harness seeds across many
    // separate script invocations for exactly this reason, not one request).
    if (willSelfSeed && warehouseIds.length > MAX_SELF_SEED_WAREHOUSES) {
      return json(
        {
          error: `Self-seeding supports at most ${MAX_SELF_SEED_WAREHOUSES} warehouse(s) per start (got ${warehouseIds.length}) — seeding more synchronously in one request risks the Worker's own subrequest budget. Pass 'seedReferenceData: false' for a larger, externally-seeded warehouse set.`,
        },
        400,
      );
    }

    // Codex review P2 fix (round 9: widened from "only when self-seeding"):
    // the admin-token clients (schema bootstrap, the correctness tracker's
    // /v1/sql read-back) are now ALWAYS pinned to env.CORE_GATEWAY_BASE_URL
    // (see the P1 fix below on why an admin-token client can never trust a
    // request-supplied baseUrl), and resolveDefaultSkewTarget always reads
    // env.SHARD_API's vbucket map (a fixed RPC service binding, not
    // influenced by config.baseUrl at all) — but the actual tenant TRAFFIC
    // (HttpTxExecutor) still uses config.baseUrl, which CAN come from the
    // request body. A caller supplying a baseUrl that genuinely differs from
    // the trusted gateway — with OR without self-seeding — would therefore
    // send real writes to one cluster while the correctness tracker reads
    // back from (and reports false losses against) a completely different
    // one, and/or resolve a skew target from the wrong cluster's topology
    // entirely. There is no longer any combination of options that makes a
    // genuinely different baseUrl behave correctly, so it's rejected
    // outright rather than silently splitting the pipeline across two
    // clusters: a caller bringing its own already-seeded cluster still has
    // `seedReferenceData: false` (see this block's own comment below) to
    // skip self-seeding specifically, but must still run against the SAME
    // gateway this deployment is configured for.
    //
    // Codex review P2 fix (round 12): compare with trailing slashes stripped
    // — gateway-client.ts's/schema-bootstrap.ts's joinUrl already treats
    // "https://gw.example" and "https://gw.example/" as the exact same
    // endpoint (a deliberate fix in an earlier round), so this rejection
    // must agree: an exact string comparison would otherwise reject a
    // caller who supplies the functionally-identical URL with a trailing
    // slash, even though it's not actually a different cluster at all.
    // Codex review P2 fix (round 13): CORE_GATEWAY_BASE_URL is deliberately
    // left unset in the committed wrangler.toml (see that file's own P2 fix
    // comment) — `.replace()` on `undefined` throws a TypeError, crashing
    // this whole request even for a caller who never supplied a baseUrl at
    // all. Guard with `?? ""` before normalizing either side.
    const normalizedRequestBaseUrl = typeof body.baseUrl === "string" ? body.baseUrl.replace(/\/+$/, "") : "";
    const normalizedTrustedBaseUrl = (this.env.CORE_GATEWAY_BASE_URL ?? "").replace(/\/+$/, "");
    if (normalizedRequestBaseUrl.length > 0 && normalizedRequestBaseUrl !== normalizedTrustedBaseUrl) {
      return json(
        {
          error:
            "A custom 'baseUrl' pointing at a different cluster isn't supported — schema bootstrap, the correctness tracker's read-back, and skew-target resolution all always target this deployment's own CORE_GATEWAY_BASE_URL, so a different baseUrl would split reads and writes across two clusters. Omit 'baseUrl' to use the default gateway.",
        },
        400,
      );
    }

    // Codex review P2 fix: `targetShardId` is no longer REQUIRED for skew
    // mode — an explicit one from the caller is honored as-is (synchronously,
    // no await needed) below; an omitted one is resolved via
    // resolveDefaultSkewTarget AFTER the running guard is persisted (see that
    // comment for why the ORDER matters — round 7 finding). The UI's own
    // picker (public/app.js) used to guess a shard id from catalog-0's
    // vbucket map regardless of which catalog the scenario's actual
    // warehouse tenant hashes to — on any deployment where that warehouse
    // ISN'T on catalog-0, the guessed shard id simply doesn't exist in the
    // catalog LoadDriver's own skew pools actually search (see
    // refreshSkewPoolsFromMap below, which resolves the warehouse's REAL
    // catalog via catalogShardIdForTenant), so the skew pool came back empty
    // and "skew mode" silently ran uniform traffic instead of creating a hot
    // shard. The server already has everything needed to pick a genuinely
    // correct target, so it now does.
    const explicitTargetShardId = mode === "skew" && typeof body.targetShardId === "string" && body.targetShardId.length > 0 ? body.targetShardId : null;

    // Codex review P2 fix (round 12): clamps a caller-supplied count to
    // MAX_SELF_SEED_* only while self-seeding — see those constants' own
    // comment for why. A non-self-seeding run (seedReferenceData: false)
    // never runs this Worker's own synchronous per-row seed loop at all, so
    // DEFAULT_*'s full benchmark scale is fine there.
    const clampIfSelfSeeding = (value: number, max: number): number => (willSelfSeed ? Math.min(value, max) : value);

    const config: LoadDriverConfig = {
      mode,
      targetShardId: explicitTargetShardId,
      concurrency,
      baseUrl: resolveLoadDriverBaseUrl(body.baseUrl, this.env.CORE_GATEWAY_BASE_URL),
      warehouseIds,
      districtsPerWarehouse: clampIfSelfSeeding(
        Number.isFinite(body.districtsPerWarehouse) ? Number(body.districtsPerWarehouse) : willSelfSeed ? SCENARIO_DISTRICTS_PER_WAREHOUSE : DEFAULT_DISTRICTS_PER_WAREHOUSE,
        MAX_SELF_SEED_DISTRICTS_PER_WAREHOUSE,
      ),
      customersPerDistrict: clampIfSelfSeeding(
        Number.isFinite(body.customersPerDistrict) ? Number(body.customersPerDistrict) : willSelfSeed ? SCENARIO_CUSTOMERS_PER_DISTRICT : DEFAULT_CUSTOMERS_PER_DISTRICT,
        MAX_SELF_SEED_CUSTOMERS_PER_DISTRICT,
      ),
      itemCount: clampIfSelfSeeding(
        Number.isFinite(body.itemCount) ? Number(body.itemCount) : willSelfSeed ? SCENARIO_ITEM_COUNT : DEFAULT_ITEM_COUNT,
        MAX_SELF_SEED_ITEM_COUNT,
      ),
    };

    // Codex review P2 fix (round 14): the individual per-field caps above
    // (districts/customers/items/warehouses) don't compose safely on their
    // own — see MAX_SELF_SEED_PROJECTED_SUBREQUESTS's own comment for the
    // worked numbers. This checks the ACTUAL combination about to run,
    // computed from the already-clamped config values, and rejects outright
    // if self-seeding this specific request would project over budget —
    // pure, synchronous, no I/O, so it's safe to check before any of the
    // state-mutating work below.
    if (willSelfSeed) {
      const projected = projectedSelfSeedSubrequests(config.warehouseIds.length, config.districtsPerWarehouse, config.customersPerDistrict, config.itemCount);
      if (projected > MAX_SELF_SEED_PROJECTED_SUBREQUESTS) {
        return json(
          {
            error: `This combination of warehouseIds/districtsPerWarehouse/customersPerDistrict/itemCount would need ~${projected} subrequests to self-seed and verify — over the ${MAX_SELF_SEED_PROJECTED_SUBREQUESTS} budget this feature stays under to avoid exceeding the Worker's own per-invocation limit. Reduce the counts, or pass 'seedReferenceData: false' for a larger, externally-seeded dataset.`,
          },
          400,
        );
      }
    }

    // Codex review P2 fix (round 4; ORDER fixed again in round 7 — see
    // below): mark this run as `running` and persist its config BEFORE any
    // of the long bootstrap/seed awaits below, not after. The
    // `existing.running` check above closes the window for a duplicate
    // start racing an ALREADY-established run, but a naive "persist
    // running=true only after bootstrap succeeds" still leaves the exact
    // same TOCTOU gap open for TWO start requests arriving close together
    // while NEITHER has finished bootstrapping yet: both would read
    // `running: false`, both would proceed, and the second could duplicate
    // schema-bootstrap calls or land its own seed values over the first's
    // mid-flight run. Persisting `running: true` here closes that window —
    // any request that reads state after this point sees a run already in
    // progress and no-ops via the check above. If bootstrap/seed below fails,
    // this is explicitly rolled back (see the catch blocks) so a failed start
    // never leaves the run stuck "running" with nothing actually happening.
    //
    // ROUND 7 FINDING: this block must come BEFORE resolveDefaultSkewTarget's
    // await, not after it — round 6 introduced that await (to resolve an
    // omitted targetShardId) ABOVE this persist step, which reopened the
    // exact race round 4 had just closed: two requests could both pass the
    // `existing.running` check, both start awaiting resolveDefaultSkewTarget
    // concurrently, and only THEN would either reach this persist step. Every
    // await between the `existing.running` check and this save is one more
    // chance for a second request to race through undetected — so this now
    // runs first, with target resolution folded in immediately after (still
    // before any bootstrap/seed) rather than before.
    const s = await this.loadState();
    s.running = true;
    s.config = config;
    s.counters = emptyCounters();
    s.correctness = emptyCorrectnessCounters();
    s.startedAt = Date.now();
    s.lastTickAt = null;
    s.lastError = null;
    await this.saveState(s);
    // Codex review P2 fix (round 8): anchors THIS specific start attempt —
    // see abortStart's and reloadIfStillMine's own doc comments for why
    // every subsequent save in this method re-checks against this value
    // before writing anything more.
    const myStartedAt = s.startedAt;

    // Codex review P2 fix (round 15): reset the correctness tracker HERE,
    // right alongside the durable `running: true`/zeroed-counters save
    // above, not after the (now potentially long) schema/seed/index-wait
    // sequence below. Between those two points, `running: true` is already
    // durable and publicly visible via /api/load/status, but this.correct-
    // nessTracker still held the PREVIOUS run's tracker — its snapshot()
    // was being combined with the NEW run's zeroed durable counters, so a
    // status poll mid-bootstrap could show the previous run's
    // trackedKeyCount/verified as if they belonged to the run that just
    // "started". A new run's tracked keys have nothing to do with a
    // previous run's (possibly a different targetShardId, warehouse set, or
    // table state entirely) regardless of when during startup they're
    // reset, so there's no reason not to do it immediately.
    this.correctnessTracker = new CorrectnessTracker();
    this.correctnessHydrated = true;
    this.lastVerifyAt = 0;

    if (mode === "skew" && !explicitTargetShardId) {
      let resolvedTargetShardId: string;
      try {
        resolvedTargetShardId = await this.resolveDefaultSkewTarget(warehouseIds[0]);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        return this.abortStart(myStartedAt, `Couldn't resolve a default skew target: ${message}. Pass 'targetShardId' explicitly instead.`);
      }
      config.targetShardId = resolvedTargetShardId;
      const current = await this.reloadIfStillMine(myStartedAt);
      if (!current) return this.supersededResponse();
      current.config = config;
      await this.saveState(current);
    }

    // Bootstrap reference data (warehouse/district/customer/item/stock) for
    // every warehouse this run targets — see scenario-seed.ts's header
    // comment for why this can't be a separate, external setup step: the
    // token that seeds a tenant's data and the token that later transacts
    // against it must be the SAME one (TenantTokenStoreTokenProvider
    // refuses to rotate a tenant already registered by a different caller),
    // so LoadDriver has to seed its own data through its own token. Opt out
    // with `seedReferenceData: false` for a warehouse something else (e.g.
    // the Node TPC-C harness) already owns and seeded. A bootstrap/seed
    // failure rolls `running` back to false (see abortStart) — the run
    // never actually starts rather than starting one guaranteed to fail
    // every transaction.
    if (willSelfSeed) {
      // Codex review P2 fix: a genuinely fresh cluster (only /admin/init run
      // — exactly what a real "Deploy to Cloudflare" visitor's core Worker
      // starts as) has none of the tpcc_* tables yet, so seeding below would
      // 502 on its very first upsert. Ensure the TABLES exist FIRST — see
      // schema-bootstrap.ts's own header comment for why this is safe to
      // call on every start rather than only once, and for why INDEXES are
      // deliberately handled separately, AFTER seeding, not here.
      //
      // Codex review P1 fix: this MUST use env.CORE_GATEWAY_BASE_URL, the
      // operator-configured trusted endpoint — NEVER config.baseUrl, which
      // (via resolveLoadDriverBaseUrl) can come straight from this request's
      // own JSON body. Sending ADMIN_TOKEN to an attacker-supplied baseUrl
      // would hand any caller of this gated-but-shared endpoint a live
      // exfiltration path for the cluster's admin credential. body.baseUrl
      // is only ever meant to override the TENANT-scoped HttpTxExecutor
      // below (a real but far less sensitive testing/flexibility knob —
      // still a tenant bearer token, never the admin one) — and even that is
      // rejected above when combined with self-seeding, to avoid bootstrapping
      // one cluster while seeding another.
      const schemaAdmin = new HttpSchemaAdminClient(this.env.CORE_GATEWAY_BASE_URL, this.env.ADMIN_TOKEN);
      try {
        await ensureScenarioTables(schemaAdmin);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        return this.abortStart(myStartedAt, `Couldn't bootstrap scenario schema: ${message}`);
      }

      const seedExecutor = new HttpTxExecutor(config.baseUrl ?? "", this.tokenProvider);
      try {
        for (const warehouseId of config.warehouseIds) {
          await seedScenarioReferenceData(seedExecutor, warehouseId, config.districtsPerWarehouse, config.customersPerDistrict, config.itemCount);
        }
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        return this.abortStart(myStartedAt, `Couldn't seed reference data: ${message}`);
      }

      // Codex review P2 fix: indexes are created/verified-ready AFTER
      // seeding, never before — see ensureScenarioIndexesReady's own doc
      // comment for exactly why (an index created before its rows exist only
      // picks them up via /v1/mutate's asynchronous index-maintenance path,
      // which the seeding calls above don't wait for; creating it after
      // seeding means its own backfill scan picks up every seeded row).
      try {
        await ensureScenarioIndexesReady(schemaAdmin);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        return this.abortStart(myStartedAt, `Couldn't finish bootstrapping scenario indexes: ${message}`);
      }

      // Codex review P2 fix (round 5): an index rule reporting 'ready' only
      // proves SOME prior data reached it — not THIS call's own freshly-
      // seeded rows (e.g. a new warehouseId, or expanded itemCount, seeded
      // onto an already-'ready' index from an earlier run). Canary-verify
      // each warehouse's own seeded data is actually visible through the
      // indexes the transaction mix depends on before declaring the run
      // ready to start — see verifySeededDataIndexed's own doc comment.
      try {
        for (const warehouseId of config.warehouseIds) {
          await verifySeededDataIndexed(seedExecutor, warehouseId, config.districtsPerWarehouse, config.customersPerDistrict, config.itemCount);
        }
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        return this.abortStart(myStartedAt, `Seeded data didn't become queryable in time: ${message}`);
      }
    }

    // Codex review P2 fix (round 8): the long bootstrap sequence above may
    // have taken long enough for a concurrent /api/load/stop (or an entirely
    // different /api/load/start) to have already changed durable state — see
    // reloadIfStillMine's own doc comment. Re-check ONE last time before the
    // final "declare this run ready" step: setting the alarm now if this
    // attempt has been superseded would incorrectly resurrect a run someone
    // already stopped.
    const finalState = await this.reloadIfStillMine(myStartedAt);
    if (!finalState) return this.supersededResponse();

    // Reset the transient skew cache — a new start may target a different
    // shard than any previously cached pool.
    this.skewPools = new Map();
    this.lastSkewRefreshAt = 0;
    this.cachedVbucketMap = null;
    this.lastVbucketMapRefreshAt = 0;

    // Kick off the first tick right away rather than waiting a full
    // TICK_INTERVAL_MS for the alarm to fire.
    await this.state.storage.setAlarm(Date.now());

    return json(toStatusJson(finalState, this.correctnessTracker.snapshot()));
  }

  /** Codex review P2 fix (round 8; corrected round 9 — see below): re-reads
   * durable state and returns it ONLY if it still belongs to the start
   * attempt anchored at `myStartedAt` — i.e. nothing (a concurrent stop, or
   * an entirely different start) has touched it since. `startedAt` is set
   * exactly once per start, to a millisecond timestamp, at the very first
   * save handleStart makes — for this DO's single-run-at-a-time model
   * that's a sufficient, simple generation marker for detecting a NEWER
   * start. Returns null when superseded; callers must not save anything in
   * that case (see abortStart / handleStart's own final check for how each
   * caller responds instead).
   *
   * ROUND 9 CORRECTION: this originally checked `startedAt` alone — but
   * handleStop() sets `running = false` WITHOUT touching `startedAt` (there
   * is no new run to anchor a fresh generation to; stopping isn't starting
   * anything). A stop arriving mid-bootstrap would therefore leave
   * `startedAt` matching, this check would report "still mine", and the
   * original start would carry on — finishing schema/seeding work for a run
   * that was just told to stop, and its final success path would set
   * `running = true` again, silently resurrecting the exact run the caller
   * just stopped. Requiring `running === true` too closes this: a stop is
   * now unambiguously a supersession of any start still in flight, exactly
   * like a genuinely different start would be. */
  private async reloadIfStillMine(myStartedAt: number): Promise<LoadDriverState | null> {
    const current = await this.loadState();
    return current.startedAt === myStartedAt && current.running ? current : null;
  }

  /** Shared response for "this start attempt was superseded by a newer
   * stop/start before it could finish" — deliberately not an error (nothing
   * about THIS request's own actions failed), but not a fabricated success
   * either, since whatever is currently running (or not) belongs to someone
   * else's request now. */
  private supersededResponse(): Response {
    return json({ error: "This start was superseded by a newer stop/start before it finished — check current status." }, 409);
  }

  /** Shared rollback for every bootstrap/seed failure path in handleStart:
   * rolls this run back to not-running IFF this start attempt is still the
   * current one (see reloadIfStillMine) — a failure from a start attempt
   * that's already been superseded must not stomp on whatever newer state
   * now exists. Always returns the caller's own 502, regardless of whether
   * the rollback actually wrote anything, since THIS request's own attempt
   * to start genuinely failed either way. */
  private async abortStart(myStartedAt: number, errorMessage: string): Promise<Response> {
    const current = await this.reloadIfStillMine(myStartedAt);
    if (current) {
      current.running = false;
      current.config = null;
      current.lastError = errorMessage;
      await this.saveState(current);
    }
    return json({ error: errorMessage }, 502);
  }

  private async handleStop(): Promise<Response> {
    const s = await this.loadState();
    s.running = false;
    await this.saveState(s);
    await this.state.storage.deleteAlarm();
    return json(toStatusJson(s, this.correctnessTracker.snapshot()));
  }

  private async handleStatus(): Promise<Response> {
    const s = await this.loadState();
    return json(toStatusJson(s, this.correctnessTracker.snapshot()));
  }

  /** alarm() — fired by the platform per the schedule set in handleStart /
   * runTick. Reschedules itself via setAlarm only while still running, per
   * this DO's contract (see file header comment). */
  async alarm(): Promise<void> {
    const s = await this.loadState();
    if (!s.running || !s.config) return; // stopped since the alarm was scheduled — go idle

    // Safety cap (see MAX_LOAD_RUN_DURATION_MS's doc comment): force-stop
    // regardless of who's watching, rather than let a forgotten scenario run
    // indefinitely. Checked BEFORE running this tick, so the run stops
    // cleanly at the cap instead of squeezing in one more tick past it.
    if (hasLoadRunExceededMaxDuration(s.startedAt, Date.now())) {
      s.running = false;
      // Not an actual error — reusing lastError as the "why isn't this
      // running" signal the frontend already has a field for, rather than
      // adding a second, parallel "stopReason" field for one case.
      s.lastError = `auto-stopped after ${Math.round(MAX_LOAD_RUN_DURATION_MS / 60_000)} min (safety cap) — start it again to keep watching`;
      await this.saveState(s);
      await this.state.storage.deleteAlarm();
      return;
    }

    try {
      await this.runTick(s);
    } catch (err) {
      s.lastError = err instanceof Error ? err.message : String(err);
    }
    s.lastTickAt = Date.now();
    await this.saveState(s);

    if (s.running) {
      await this.state.storage.setAlarm(Date.now() + TICK_INTERVAL_MS);
    }
  }

  /** Runs one bounded batch of transactions and folds the outcomes into
   * `s.counters` (and, Shardscope T4, `s.correctness`) in place. */
  private async runTick(s: LoadDriverState): Promise<void> {
    const config = s.config;
    if (!config) return;

    // Rehydrate the correctness tracker's DURABLE counters exactly once per
    // DO instance lifetime — see the `correctnessTracker`/`correctnessHydrated`
    // field doc comments above for why this can't happen in the constructor
    // (loadState() is async) and why it's safe to only do this once (a fresh
    // start already resets both s.correctness AND the tracker together, in
    // handleStart above).
    if (!this.correctnessHydrated) {
      this.correctnessTracker = new CorrectnessTracker({ initialCounters: s.correctness });
      this.correctnessHydrated = true;
    }

    const cfg: TpccWorldConfig = {
      warehouseIds: config.warehouseIds,
      districtsPerWarehouse: config.districtsPerWarehouse,
      customersPerDistrict: config.customersPerDistrict,
      itemCount: config.itemCount,
    };

    // ONE shared vbucket-map fetch per refresh cadence, feeding both skew
    // pools (skew mode only) and the correctness tracker's bias/verify path
    // (every mode) — see refreshVbucketMapIfNeeded's doc comment. `null` only
    // on the very first tick or a sustained admin-API outage; every
    // downstream consumer degrades gracefully rather than failing the tick.
    const vbucketMap = await this.refreshVbucketMapIfNeeded();

    let picker: KeyPicker = new UniformKeyPicker(cfg);
    if (config.mode === "skew" && config.targetShardId) {
      this.refreshSkewPoolsFromMap(config, cfg, vbucketMap);
      picker = new SkewKeyPicker(new UniformKeyPicker(cfg), this.skewPools);
    }

    // As of T5 the executor is fully wired end-to-end: real HTTP + real
    // token resolution via TenantTokenStoreTokenProvider (see this file's
    // header comment and ./tenant-token-store.ts). baseUrl is still optional
    // at /api/load/start time — if unset, every call's fetch() targets an
    // empty base URL and fails at the network layer, a clear, obvious
    // failure mode rather than a silent no-op.
    const httpExec = new HttpTxExecutor(config.baseUrl ?? "", this.tokenProvider);
    // Shardscope T4 / design round 3, point 2: the PRECISE read-back
    // adapter (a primary-key point SELECT via the admin-scoped /v1/sql —
    // see ./correctness.ts's SqlPointReader and ./gateway-client.ts's
    // HttpSqlPointReader doc comments for why this needs ADMIN_TOKEN rather
    // than a tenant bearer token). Built once per tick, reused for both the
    // periodic verify() pass below and TrackingTxExecutor's own optional
    // idempotent-replay verification.
    //
    // Codex review P1 fix: env.CORE_GATEWAY_BASE_URL, not config.baseUrl —
    // see handleStart's identical fix (HttpSchemaAdminClient construction)
    // for why an admin-token-bearing client must never be pointed at a
    // request-supplied baseUrl.
    const sqlReader = new HttpSqlPointReader(this.env.CORE_GATEWAY_BASE_URL, this.env.ADMIN_TOKEN);
    const readBack = gatewayReadBack(sqlReader);
    // Shardscope T4: every mutate()/tx() call this tick passes through the
    // correctness tracker on its way to the real gateway — see
    // ./correctness.ts's TrackingTxExecutor. This NEVER changes behavior or
    // error propagation; it only observes.
    //
    // The 3rd arg resolves a tenantId to its owning catalog shard id, using
    // THIS tick's already-fetched vbucketMap (see refreshVbucketMapIfNeeded
    // above — captured by this closure, not re-fetched) and the exact same
    // catalogShardIdForTenant formula refreshSkewPoolsFromMap/
    // refreshCorrectnessTrackedSet already use below. Returns null only when
    // vbucketMap itself is null (the very first tick, or a sustained
    // admin-API outage) — matching TrackingTxExecutor's own documented
    // "not yet resolvable" contract, so a candidate observed before the map
    // has ever been fetched simply isn't tracked this time around rather
    // than being resolved against a stale/guessed catalog count.
    const resolveCatalogShardIdForTenant = (tenantId: string): string | null =>
      vbucketMap ? catalogShardIdForTenant(tenantId, vbucketMap.catalogShardCount) : null;
    const exec: TxExecutor = new TrackingTxExecutor(httpExec, this.correctnessTracker, resolveCatalogShardIdForTenant, readBack);

    const batchSize = Math.min(config.concurrency, MAX_TRANSACTIONS_PER_TICK);
    const outcomes = await runBoundedBatch(batchSize, () => runOneTransaction(exec, cfg, picker, Math.random));
    for (const outcome of outcomes) {
      applyOutcome(s.counters, outcome);
      // Shardscope T4: a failed transaction whose error matches one of
      // TPC-C's own known/legitimate contention-abort patterns ALSO counts
      // toward txAbortedExpected — see ./correctness.ts's isExpectedAbort
      // for exactly which patterns, and why an unrecognized failure is
      // deliberately left unclassified rather than guessed at.
      if (!outcome.ok && isExpectedAbort(outcome.error)) {
        this.correctnessTracker.recordExpectedAbort();
      }
    }

    // Shardscope T4: fold this tick's newly-acked candidates into the
    // tracked set, biased toward migrating vbuckets per warehouse's own
    // catalog (mirrors refreshSkewPoolsFromMap's per-warehouse catalog
    // resolution below), then — on VERIFY_INTERVAL_MS's own cadence — read
    // every tracked key back and compare. This is deliberately unconditional
    // on load MODE (uniform or skew): a reshard can start at any time
    // regardless of which mode generated the traffic, and the verifier's own
    // bias (via pickTrackedCandidates) is what makes it useful either way.
    if (vbucketMap) {
      this.refreshCorrectnessTrackedSet(config, vbucketMap);
    }
    const now = Date.now();
    if (vbucketMap && now - this.lastVerifyAt >= VERIFY_INTERVAL_MS) {
      this.lastVerifyAt = now;
      await this.correctnessTracker.verify(readBack);
    }
    // Shardscope T4, design round 4, point 4 (Codex round-5 finding:
    // "verified stays green after a reshard that changes storage without a
    // tracked-set change") — invalidate `verified` for as long as THIS
    // tick's vbucket map shows any migration activity anywhere in the
    // cluster. Deliberately placed AFTER the verify() call above (not
    // before): this guarantees that even a verify() pass that happens to run
    // on a tick where migration is active gets its epoch bumped PAST what it
    // just covered, before this tick ends — so `verified` reads false for
    // every tick spent mid-reshard, not just intermittently. Once the
    // vbucket map reports no migration activity anywhere (post-cutover), no
    // more bumps happen, and the very next verify() pass genuinely re-earns
    // `verified: true` over the post-reshard state — see
    // CorrectnessTracker.notifyClusterChanged's own doc comment for why this
    // isn't a permanent wedge.
    if (vbucketMap && hasActiveMigration(vbucketMap)) {
      this.correctnessTracker.notifyClusterChanged();
    }
    // Persist the raw counters only — meterState/trackedKeyCount are derived
    // (see toStatusJson's use of meterStateFor) rather than stored, so
    // there's exactly one place that decides "is this red" from the
    // counters, not two copies that could drift.
    const snap = this.correctnessTracker.snapshot();
    s.correctness = { writesAcked: snap.writesAcked, writesRetriedIdempotent: snap.writesRetriedIdempotent, txAbortedExpected: snap.txAbortedExpected, lost: snap.lost };
  }

  /** Codex review P2 fix: resolves a genuinely correct default skew target
   * for `warehouseId` when the caller didn't supply one explicitly — a
   * real shard id from the SAME catalog `refreshSkewPoolsFromMap` will
   * later search for this warehouse (via the identical catalogShardIdForTenant
   * formula), rather than the client-side picker's old guess of "some shard
   * in catalog-0," which was simply wrong on any deployment where this
   * warehouse's tenant doesn't hash to catalog-0.
   *
   * ROUND 11 CORRECTION: picking "whichever shard happens to own vbucket 0"
   * (any owned vbucket, 0 just being a deterministic choice) was still
   * wrong — vbucket 0 has no relationship to which shard any of THIS
   * warehouse's actual seeded tpcc_stock keys (item ids 1..itemCount) route
   * to. On a deployment with many more vbuckets than seeded items,
   * refreshSkewPoolsFromMap's own search (generateSkewedKeys, bounded by
   * SKEW_SCAN_MAX_ATTEMPTS) could easily find ZERO of them landing on
   * vbucket 0's owner, leaving the skew pool empty and — via
   * SkewKeyPicker's own documented uniform fallback — silently running
   * uniform traffic instead of the promised hot shard. Item 1's stock key is
   * ALWAYS seeded (seedScenarioReferenceData seeds items 1..itemCount
   * unconditionally for any itemCount >= 1), so hashing THAT SPECIFIC key
   * with the exact same formula CloudflareShard's own routing uses
   * (src/hash.ts's hashKey — see skew.ts's own header comment) and looking
   * up its CURRENT owning shard guarantees the resolved target is a shard
   * that will genuinely receive at least one of this run's real writes —
   * not a blind guess that might own none of them. */
  private async resolveDefaultSkewTarget(warehouseId: number): Promise<string> {
    const raw = await this.env.SHARD_API.adminVbucketMap(this.env.ADMIN_TOKEN);
    const vbucketMap = raw as AdminVbucketMapResponse;
    const tenantId = tenantIdForWarehouse(warehouseId);
    const catalogShardId = catalogShardIdForTenant(tenantId, vbucketMap.catalogShardCount);
    const catalog = vbucketMap.catalogs.find((c) => c.catalogShardId === catalogShardId);
    if (!catalog) {
      throw new Error(`catalog ${catalogShardId} (owner of warehouse ${warehouseId}'s tenant) not found in the live vbucket map`);
    }
    if (catalog.totalVBuckets <= 0) {
      throw new Error(`catalog ${catalogShardId} has no vbuckets yet`);
    }
    // Item 1's stock key is always seeded — the same routing formula
    // production writes use (hashKey(`${tenantId}:${table}:${partitionKey}`)
    // % totalVBuckets — see skew.ts's own header comment) tells us exactly
    // which vbucket it lands on; the catalog map tells us who currently owns
    // that vbucket.
    const canaryVbucket = hashKey(`${tenantId}:tpcc_stock:${stockKey(warehouseId, 1)}`) % catalog.totalVBuckets;
    const owner = catalog.map.find((row) => row.vbucket === canaryVbucket);
    if (!owner) {
      throw new Error(`catalog ${catalogShardId}'s vbucket map has no owner recorded for vbucket ${canaryVbucket} (warehouse ${warehouseId}'s item-1 stock key)`);
    }
    return owner.shardId;
  }

  /** ONE shared vbucket-map fetch, cached for at most SKEW_REFRESH_INTERVAL_MS
   * — see this.cachedVbucketMap's field doc comment for why this exists as a
   * single method both refreshSkewPoolsFromMap (skew mode) and
   * refreshCorrectnessTrackedSet/the verify path (every mode) draw from,
   * instead of each independently deciding when to poll env.SHARD_API. A
   * failed fetch leaves the previous cached value in place (or null, on a
   * cold start) and is silently absorbed here — every caller already treats
   * a null/stale map as "nothing to bias against yet" rather than a hard
   * failure, so one transient admin-API hiccup shouldn't fail the whole
   * tick's batch of transactions over a feature that degrades gracefully. */
  private async refreshVbucketMapIfNeeded(): Promise<AdminVbucketMapResponse | null> {
    const now = Date.now();
    if (this.cachedVbucketMap && now - this.lastVbucketMapRefreshAt < SKEW_REFRESH_INTERVAL_MS) {
      return this.cachedVbucketMap;
    }
    try {
      const raw = await this.env.SHARD_API.adminVbucketMap(this.env.ADMIN_TOKEN);
      this.cachedVbucketMap = raw as AdminVbucketMapResponse;
      this.lastVbucketMapRefreshAt = now;
    } catch {
      // Keep whatever was cached before (possibly still null on a cold
      // start) — see this method's doc comment.
    }
    return this.cachedVbucketMap;
  }

  /** Recomputes `this.skewPools` (one entry per configured warehouse) from
   * `vbucketMap` (already fetched this tick via refreshVbucketMapIfNeeded —
   * this method no longer fetches on its own). For each warehouse, finds
   * that warehouse's tenant's own catalog (the SAME catalogShardIdForTenant
   * formula production routing uses), then asks ./skew.ts to scan item ids
   * 1..itemCount for ones whose stockKey(warehouseId, itemId) hashes into a
   * vBucket owned by config.targetShardId in THAT catalog's map — mirroring
   * exactly how processOrderLine (transactions.ts) will actually write that
   * stock row. A null `vbucketMap` (map not fetched yet, or a sustained
   * admin-API outage) leaves the existing skewPools untouched — best-effort
   * skew, never a hard failure (see SkewKeyPicker's own fallback-to-uniform
   * behavior for an empty/missing pool). */
  private refreshSkewPoolsFromMap(config: LoadDriverConfig, cfg: TpccWorldConfig, vbucketMap: AdminVbucketMapResponse | null): void {
    if (!config.targetShardId || !vbucketMap) return;
    const now = Date.now();
    if (now - this.lastSkewRefreshAt < SKEW_REFRESH_INTERVAL_MS && this.skewPools.size > 0) return;

    const pools = new Map<number, number[]>();
    for (const w of config.warehouseIds) {
      const tenantId = tenantIdForWarehouse(w);
      const catalogShardId = catalogShardIdForTenant(tenantId, vbucketMap.catalogShardCount);
      const catalog = vbucketMap.catalogs.find((c) => c.catalogShardId === catalogShardId);
      if (!catalog) {
        pools.set(w, []);
        continue;
      }
      const owned: VBucketOwnership[] = catalog.map.map((row) => ({ vbucket: row.vbucket, shardId: row.shardId }));
      const matches = generateSkewedKeys<number>({
        targetShardId: config.targetShardId,
        vbucketMap: owned,
        totalVBuckets: catalog.totalVBuckets,
        tenantId,
        table: "tpcc_stock",
        count: SKEW_POOL_SIZE,
        // Bounded by SKEW_SCAN_MAX_ATTEMPTS (the hard ceiling — see that
        // constant's doc comment), but scaled DOWN for a small itemCount:
        // candidateToKey cycles i_id through 1..itemCount, so once a full
        // cycle (itemCount attempts) has been scanned, every further attempt
        // just re-hashes an already-seen partition key — scanning past a
        // small multiple of itemCount is pure waste, not extra coverage.
        // (Previously this was `Math.min(MAX, Math.max(itemCount*4, MAX))`,
        // which always evaluated to MAX regardless of itemCount — a dead
        // expression that never actually scaled down for a small world.)
        maxAttempts: Math.min(SKEW_SCAN_MAX_ATTEMPTS, cfg.itemCount * 4),
        candidateToKey: (candidateIndex) => {
          const i_id = 1 + (candidateIndex % cfg.itemCount);
          return { value: i_id, partitionKey: stockKey(w, i_id) };
        },
      });
      pools.set(
        w,
        matches.map((m) => m.value),
      );
    }

    this.skewPools = pools;
    this.lastSkewRefreshAt = now;
  }

  /** Shardscope T4: drains the correctness tracker's pending-candidates
   * buffer (writes acked this tick that aren't yet tracked) and promotes
   * ALL of them into the tracked set, one catalog at a time — vbucket ids
   * are catalog-local, so this MUST resolve each candidate's catalog before
   * calling ./correctness.ts's pickTrackedCandidates (which itself has no
   * notion of "catalog"). Mirrors refreshSkewPoolsFromMap's own per-warehouse
   * catalog resolution above (same catalogShardIdForTenant formula) — the
   * two features independently need the same lookup, not because they share
   * any other logic.
   *
   * ROUND 7 (eviction removed from ./correctness.ts): pickTrackedCandidates
   * no longer caps its result — every drained candidate for every catalog is
   * promoted, not just a bounded per-catalog subset. It still BIASES the
   * order (migrating-vbucket candidates first — see its own doc comment),
   * which matters for what the live topology view highlights, but nothing
   * here is ever dropped: `tracked` is now the COMPLETE set of every
   * distinct tpcc_stock key this run has acked a write for (see
   * ./correctness.ts's header comment, "ROUND 7 — EVICTION REMOVED"). */
  private refreshCorrectnessTrackedSet(config: LoadDriverConfig, vbucketMap: AdminVbucketMapResponse): void {
    const pending = this.correctnessTracker.drainPendingCandidates();
    if (pending.length === 0) return;

    const byCatalog = new Map<string, TrackedCandidate[]>();
    for (const candidate of pending) {
      const catalogShardId = catalogShardIdForTenant(candidate.write.tenantId, vbucketMap.catalogShardCount);
      const bucket = byCatalog.get(catalogShardId);
      if (bucket) bucket.push(candidate);
      else byCatalog.set(catalogShardId, [candidate]);
    }

    const picked: TrackedCandidate[] = [];
    for (const [catalogShardId, candidates] of byCatalog) {
      const catalog = vbucketMap.catalogs.find((c) => c.catalogShardId === catalogShardId);
      if (!catalog) continue; // this warehouse's catalog isn't in the live map (shouldn't happen; skip rather than guess)
      const migrationRows: VBucketMigrationRow[] = catalog.map.map((row) => ({ vbucket: row.vbucket, migrationStatus: row.migrationStatus }));
      const catalogVBucketMap: CatalogVBucketMap = { catalogShardId, totalVBuckets: catalog.totalVBuckets, vbuckets: migrationRows };
      picked.push(...pickTrackedCandidates(candidates, catalogVBucketMap));
    }
    this.correctnessTracker.promoteToTracked(picked);
  }
}

/** True iff ANY vbucket in ANY catalog of this tick's live vbucket map is
 * mid-migration (any non-"none" migrationStatus — backfilling, cutover, or
 * aborting) — the same "is a reshard active anywhere right now" predicate
 * ./correctness.ts's migratingVBuckets uses per-catalog, generalized here to
 * "anywhere in the whole map" since notifyClusterChanged's invalidation
 * isn't scoped to one catalog (see runTick's own wiring comment for why this
 * check runs unconditionally on load MODE, mirroring
 * refreshCorrectnessTrackedSet immediately above it). */
function hasActiveMigration(vbucketMap: AdminVbucketMapResponse): boolean {
  return vbucketMap.catalogs.some((c) => c.map.some((row) => row.migrationStatus && row.migrationStatus !== "none"));
}

function applyOutcome(counters: LoadDriverCounters, outcome: TransactionOutcome): void {
  counters.attempted += 1;
  const typeCounters = counters.byType[outcome.type];
  typeCounters.attempted += 1;
  if (outcome.ok) {
    counters.ok += 1;
    typeCounters.ok += 1;
  } else {
    counters.err += 1;
    typeCounters.err += 1;
  }
}

/** Runs `count` independent transaction attempts concurrently — a fixed,
 * pre-sized batch (never an unbounded loop), matching this DO's
 * MAX_SUBREQUESTS_PER_TICK contract (see this file's header comment). */
async function runBoundedBatch(count: number, fn: () => Promise<TransactionOutcome>): Promise<TransactionOutcome[]> {
  return Promise.all(Array.from({ length: count }, () => fn()));
}

/** `trackerSnapshot` (trackedKeyCount / lastVerifyChecked / verified) is
 * passed in separately (not read off `s`) because it's IN-MEMORY-ONLY state
 * (see this.correctnessTracker's own field doc comments — the tracked-key
 * SET, and whether/how much its last verify() pass actually checked, are
 * deliberately never persisted to `s.correctness`, only the durable counters
 * are), so every caller reads it fresh off the live `this.correctnessTracker`
 * instance at request time rather than off whatever was last saved to
 * storage. These are HONEST scoreboard figures (see ./correctness.ts's own
 * header comment on why "lost 0" alone overclaims): a bare "lost 0" reads as
 * a cluster-wide zero-loss guarantee that was actually checked, but this
 * tracker only ever verifies a bounded, biased SAMPLE of keys, and only on
 * its own VERIFY_INTERVAL_MS cadence. `verified` is computed ONCE, inside
 * CorrectnessTracker.snapshot() (design round 3, point 3) — forwarded here
 * verbatim, never re-derived, so there is exactly one place that decides
 * "does this green claim genuinely still hold" the same way `meterState`
 * below is exactly one place that decides "is this red". See
 * aggregator.ts's Scoreboard.verified / public/app.js's renderScoreboard for
 * how this is surfaced. */
function toStatusJson(
  s: LoadDriverState,
  trackerSnapshot: { trackedKeyCount: number; lastVerifyChecked: number | null; verified: boolean },
): Record<string, unknown> {
  return {
    running: s.running,
    config: s.config,
    counters: s.counters,
    // Shardscope T4: the correctness scoreboard (see ./correctness.ts).
    // `meterState` is derived fresh from `s.correctness` here (never
    // persisted redundantly — see runTick's own comment on this) so there is
    // exactly one place that decides "is this red".
    correctness: {
      ...s.correctness,
      meterState: meterStateFor(s.correctness),
      trackedKeyCount: trackerSnapshot.trackedKeyCount,
      lastVerifyChecked: trackerSnapshot.lastVerifyChecked,
      verified: trackerSnapshot.verified,
    },
    startedAt: s.startedAt,
    lastTickAt: s.lastTickAt,
    lastError: s.lastError,
  };
}

const json = (data: unknown, status = 200): Response =>
  new Response(JSON.stringify(data, null, 2), { status, headers: { "content-type": "application/json" } });
