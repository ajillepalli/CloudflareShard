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
import { HttpTxExecutor } from "./gateway-client";
import {
  CorrectnessTracker,
  TrackingTxExecutor,
  emptyCorrectnessCounters,
  gatewayReadBack,
  isExpectedAbort,
  meterStateFor,
  pickTrackedCandidates,
  type CorrectnessCounters,
  type TrackedWrite,
  type VBucketMigrationRow,
} from "./correctness";

export type LoadMode = "uniform" | "skew";

const DEFAULT_CONCURRENCY = 8;
const MAX_CONCURRENCY = 32;
const DEFAULT_WAREHOUSE_IDS = [1];
const DEFAULT_DISTRICTS_PER_WAREHOUSE = 10;
const DEFAULT_CUSTOMERS_PER_DISTRICT = 100;
const DEFAULT_ITEM_COUNT = 200;

// How often the alarm fires while running.
const TICK_INTERVAL_MS = 1000;

// Workers hard-cap subrequests per Worker invocation (an alarm() call is one
// invocation) well below what an unbounded loop here could otherwise fan
// out. New-Order is this mix's worst case: 1 tableScan + 1 header tx + up to
// 15 lines * (~2 calls each: an indexQuery/mutate pair or a 2-mutation tx,
// plus occasional remote-line compensation) + 1 marker mutate ≈
// 1 + 1 + 15*2 + 1 = 33 subrequests for ONE transaction. Rounded up
// generously (compensation/retry paths can add a few more calls) so a batch
// sized against this constant can never come close to the platform's actual
// 1000-subrequest ceiling.
const WORST_CASE_SUBREQUESTS_PER_TRANSACTION = 40;
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

// How many tracked keys the correctness verifier keeps per load run — see
// ./correctness.ts's own DEFAULT_MAX_TRACKED_KEYS for the reasoning; kept as
// an explicit constant here too since load-driver.ts is what actually
// decides candidates per catalog before calling into the pure core.
const MAX_TRACKED_KEYS = 50;

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
    };
    try {
      body = (await request.json()) ?? {};
    } catch {
      return json({ error: "Invalid JSON body." }, 400);
    }

    const mode = body.mode === "skew" ? "skew" : body.mode === "uniform" ? "uniform" : undefined;
    if (!mode) {
      return json({ error: "Missing or invalid 'mode'. Must be 'uniform' or 'skew'." }, 400);
    }
    if (mode === "skew" && !body.targetShardId) {
      return json({ error: "'targetShardId' is required when mode is 'skew'." }, 400);
    }

    const concurrency = Math.max(1, Math.min(MAX_CONCURRENCY, Number.isFinite(body.concurrency) ? Number(body.concurrency) : DEFAULT_CONCURRENCY));

    const config: LoadDriverConfig = {
      mode,
      targetShardId: mode === "skew" ? (body.targetShardId as string) : null,
      concurrency,
      baseUrl: typeof body.baseUrl === "string" && body.baseUrl.length > 0 ? body.baseUrl : null,
      warehouseIds: Array.isArray(body.warehouseIds) && body.warehouseIds.length > 0 ? body.warehouseIds : DEFAULT_WAREHOUSE_IDS,
      districtsPerWarehouse: Number.isFinite(body.districtsPerWarehouse) ? Number(body.districtsPerWarehouse) : DEFAULT_DISTRICTS_PER_WAREHOUSE,
      customersPerDistrict: Number.isFinite(body.customersPerDistrict) ? Number(body.customersPerDistrict) : DEFAULT_CUSTOMERS_PER_DISTRICT,
      itemCount: Number.isFinite(body.itemCount) ? Number(body.itemCount) : DEFAULT_ITEM_COUNT,
    };

    const s = await this.loadState();
    s.running = true;
    s.config = config;
    s.counters = emptyCounters();
    s.correctness = emptyCorrectnessCounters();
    s.startedAt = Date.now();
    s.lastTickAt = null;
    s.lastError = null;
    await this.saveState(s);

    // Reset the transient skew cache — a new start may target a different
    // shard than any previously cached pool.
    this.skewPools = new Map();
    this.lastSkewRefreshAt = 0;
    this.cachedVbucketMap = null;
    this.lastVbucketMapRefreshAt = 0;

    // Fresh correctness tracker for a fresh run — a new run's tracked keys
    // have nothing to do with a previous run's (possibly a different
    // targetShardId, warehouse set, or table state entirely).
    this.correctnessTracker = new CorrectnessTracker();
    this.correctnessHydrated = true;
    this.lastVerifyAt = 0;

    // Kick off the first tick right away rather than waiting a full
    // TICK_INTERVAL_MS for the alarm to fire.
    await this.state.storage.setAlarm(Date.now());

    return json(toStatusJson(s));
  }

  private async handleStop(): Promise<Response> {
    const s = await this.loadState();
    s.running = false;
    await this.saveState(s);
    await this.state.storage.deleteAlarm();
    return json(toStatusJson(s));
  }

  private async handleStatus(): Promise<Response> {
    const s = await this.loadState();
    return json(toStatusJson(s));
  }

  /** alarm() — fired by the platform per the schedule set in handleStart /
   * runTick. Reschedules itself via setAlarm only while still running, per
   * this DO's contract (see file header comment). */
  async alarm(): Promise<void> {
    const s = await this.loadState();
    if (!s.running || !s.config) return; // stopped since the alarm was scheduled — go idle

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
    // Shardscope T4: every mutate()/tx() call this tick passes through the
    // correctness tracker on its way to the real gateway — see
    // ./correctness.ts's TrackingTxExecutor. This NEVER changes behavior or
    // error propagation; it only observes.
    const exec: TxExecutor = new TrackingTxExecutor(httpExec, this.correctnessTracker);

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
      await this.correctnessTracker.verify(gatewayReadBack(exec));
    }
    // Persist the raw counters only — meterState/trackedKeyCount are derived
    // (see toStatusJson's use of meterStateFor) rather than stored, so
    // there's exactly one place that decides "is this red" from the
    // counters, not two copies that could drift.
    const snap = this.correctnessTracker.snapshot();
    s.correctness = { writesAcked: snap.writesAcked, writesRetriedIdempotent: snap.writesRetriedIdempotent, txAbortedExpected: snap.txAbortedExpected, lost: snap.lost };
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
        maxAttempts: Math.min(SKEW_SCAN_MAX_ATTEMPTS, Math.max(cfg.itemCount * 4, SKEW_SCAN_MAX_ATTEMPTS)),
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
   * buffer (writes acked this tick that aren't yet tracked) and promotes a
   * biased subset into the tracked set, one catalog at a time — vbucket ids
   * are catalog-local, so this MUST resolve each candidate's catalog before
   * calling ./correctness.ts's pickTrackedCandidates (which itself has no
   * notion of "catalog"). Mirrors refreshSkewPoolsFromMap's own per-warehouse
   * catalog resolution above (same catalogShardIdForTenant formula) — the
   * two features independently need the same lookup, not because they share
   * any other logic. */
  private refreshCorrectnessTrackedSet(config: LoadDriverConfig, vbucketMap: AdminVbucketMapResponse): void {
    const pending = this.correctnessTracker.drainPendingCandidates();
    if (pending.length === 0) return;

    const byCatalog = new Map<string, TrackedWrite[]>();
    for (const write of pending) {
      const catalogShardId = catalogShardIdForTenant(write.tenantId, vbucketMap.catalogShardCount);
      const bucket = byCatalog.get(catalogShardId);
      if (bucket) bucket.push(write);
      else byCatalog.set(catalogShardId, [write]);
    }

    const picked: TrackedWrite[] = [];
    for (const [catalogShardId, writes] of byCatalog) {
      const catalog = vbucketMap.catalogs.find((c) => c.catalogShardId === catalogShardId);
      if (!catalog) continue; // this warehouse's catalog isn't in the live map (shouldn't happen; skip rather than guess)
      const migrationRows: VBucketMigrationRow[] = catalog.map.map((row) => ({ vbucket: row.vbucket, migrationStatus: row.migrationStatus }));
      picked.push(...pickTrackedCandidates(writes, migrationRows, catalog.totalVBuckets, Math.max(1, Math.floor(MAX_TRACKED_KEYS / byCatalog.size))));
    }
    this.correctnessTracker.promoteToTracked(picked);
  }
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

function toStatusJson(s: LoadDriverState): Record<string, unknown> {
  return {
    running: s.running,
    config: s.config,
    counters: s.counters,
    // Shardscope T4: the correctness scoreboard (see ./correctness.ts).
    // `meterState` is derived fresh from `s.correctness` here (never
    // persisted redundantly — see runTick's own comment on this) so there is
    // exactly one place that decides "is this red".
    correctness: { ...s.correctness, meterState: meterStateFor(s.correctness) },
    startedAt: s.startedAt,
    lastTickAt: s.lastTickAt,
    lastError: s.lastError,
  };
}

const json = (data: unknown, status = 200): Response =>
  new Response(JSON.stringify(data, null, 2), { status, headers: { "content-type": "application/json" } });
