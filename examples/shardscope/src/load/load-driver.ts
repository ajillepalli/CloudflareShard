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
// until the next vbucket-map refresh — see refreshSkewPoolsIfNeeded below.
const SKEW_POOL_SIZE = 25;
// Bounds skew.ts's own per-warehouse candidate scan (over item ids
// 1..itemCount) — see skew.ts's generateSkewedKeys maxAttempts doc comment
// for why a bound like this always terminates even when the target shard
// owns few or no vBuckets in a given warehouse's catalog.
const SKEW_SCAN_MAX_ATTEMPTS = 20000;
// Re-fetch the vbucket map (and recompute skew pools) at most this often —
// topology genuinely doesn't change every second, and this call itself
// counts against the tick's subrequest budget.
const SKEW_REFRESH_INTERVAL_MS = 5000;

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
  startedAt: number | null;
  lastTickAt: number | null;
  lastError: string | null;
}

function initialState(): LoadDriverState {
  return { running: false, config: null, counters: emptyCounters(), startedAt: null, lastTickAt: null, lastError: null };
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
    return stored ?? initialState();
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
    s.startedAt = Date.now();
    s.lastTickAt = null;
    s.lastError = null;
    await this.saveState(s);

    // Reset the transient skew cache — a new start may target a different
    // shard than any previously cached pool.
    this.skewPools = new Map();
    this.lastSkewRefreshAt = 0;

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
   * `s.counters` in place. */
  private async runTick(s: LoadDriverState): Promise<void> {
    const config = s.config;
    if (!config) return;

    const cfg: TpccWorldConfig = {
      warehouseIds: config.warehouseIds,
      districtsPerWarehouse: config.districtsPerWarehouse,
      customersPerDistrict: config.customersPerDistrict,
      itemCount: config.itemCount,
    };

    let picker: KeyPicker = new UniformKeyPicker(cfg);
    if (config.mode === "skew" && config.targetShardId) {
      await this.refreshSkewPoolsIfNeeded(config, cfg);
      picker = new SkewKeyPicker(new UniformKeyPicker(cfg), this.skewPools);
    }

    // As of T5 the executor is fully wired end-to-end: real HTTP + real
    // token resolution via TenantTokenStoreTokenProvider (see this file's
    // header comment and ./tenant-token-store.ts). baseUrl is still optional
    // at /api/load/start time — if unset, every call's fetch() targets an
    // empty base URL and fails at the network layer, a clear, obvious
    // failure mode rather than a silent no-op.
    const exec: TxExecutor = new HttpTxExecutor(config.baseUrl ?? "", this.tokenProvider);

    const batchSize = Math.min(config.concurrency, MAX_TRANSACTIONS_PER_TICK);
    const outcomes = await runBoundedBatch(batchSize, () => runOneTransaction(exec, cfg, picker, Math.random));
    for (const outcome of outcomes) {
      applyOutcome(s.counters, outcome);
    }
  }

  /** Recomputes `this.skewPools` (one entry per configured warehouse) from
   * the live vbucket map, at most every SKEW_REFRESH_INTERVAL_MS. For each
   * warehouse, finds that warehouse's tenant's own catalog (the SAME
   * catalogShardIdForTenant formula production routing uses), then asks
   * ./skew.ts to scan item ids 1..itemCount for ones whose
   * stockKey(warehouseId, itemId) hashes into a vBucket owned by
   * config.targetShardId in THAT catalog's map — mirroring exactly how
   * processOrderLine (transactions.ts) will actually write that stock row. */
  private async refreshSkewPoolsIfNeeded(config: LoadDriverConfig, cfg: TpccWorldConfig): Promise<void> {
    if (!config.targetShardId) return;
    const now = Date.now();
    if (now - this.lastSkewRefreshAt < SKEW_REFRESH_INTERVAL_MS && this.skewPools.size > 0) return;

    const vbucketMapRaw = await this.env.SHARD_API.adminVbucketMap(this.env.ADMIN_TOKEN);
    const vbucketMap = vbucketMapRaw as AdminVbucketMapResponse;

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
    startedAt: s.startedAt,
    lastTickAt: s.lastTickAt,
    lastError: s.lastError,
  };
}

const json = (data: unknown, status = 200): Response =>
  new Response(JSON.stringify(data, null, 2), { status, headers: { "content-type": "application/json" } });
