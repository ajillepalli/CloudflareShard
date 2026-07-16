/** correctness.ts — Shardscope's correctness/loss-detection core (T4).
 *
 * This is the module the whole demo's central claim rests on: "reshard under
 * load, lost: 0". A scoreboard that can only ever say `lost 0` is theater —
 * see this file's CRITICAL test ("THE GUARD") for the one thing that MUST be
 * true of this module: fed a scenario where a tracked write's value doesn't
 * read back correctly, it reports `lost > 0` and the meter goes RED. If that
 * test can't fail the meter, nothing else here matters.
 *
 * ----------------------------------------------------------------------------
 * DESIGN — pure, storage-agnostic core (mirrors ./tenant-token-store.ts's
 * split: a pure core exercised directly by vitest, plus a thin adapter that
 * wires it to the real Worker/DO runtime):
 *
 *   - `CorrectnessTracker` is the stateful counters + tracked-key-set engine.
 *     It knows nothing about HTTP, Durable Objects, or CloudflareShard's
 *     wire format — just counters, a bounded map of "keys we're watching",
 *     and a verify() pass driven by a caller-supplied `ReadBackFn`.
 *   - `pickTrackedCandidates` is the pure biasing function: given a pool of
 *     candidate writes and a vbucket map, decides which candidates should
 *     become tracked keys, preferring ones that hash into a MIGRATING
 *     vbucket (mirrors ./skew.ts's own routing-formula verification — see
 *     that file's header comment for the shared hashKey formula). It is
 *     deliberately NOT a method on CorrectnessTracker: vbucket ids are
 *     catalog-local (see aggregator.ts's repeated warnings on this), so the
 *     caller (./load-driver.ts) resolves each write's catalog first and
 *     calls this function once per catalog — CorrectnessTracker itself never
 *     needs to know what a "catalog" is.
 *   - `TrackingTxExecutor` is the one piece that touches the wire-level
 *     TxExecutor contract (./transactions.ts) — a transparent decorator
 *     around the real HttpTxExecutor (./gateway-client.ts) that observes
 *     every mutate()/tx() call in passing and classifies it (fresh ack vs
 *     idempotent replay) without changing its behavior or return value.
 *   - `gatewayReadBack` is the real adapter CorrectnessTracker.verify() uses
 *     against a live cluster — an indexQuery against the same
 *     idx_stock_by_item index processOrderLine already uses. It is the ONE
 *     piece of this file that only truly exercises against a live cluster
 *     (during the pending live-run milestone); everything else is complete
 *     and unit-tested here today with a trivial fake ReadBackFn.
 *
 * ----------------------------------------------------------------------------
 * WHAT IS TRACKED, AND WHY tpcc_stock SPECIFICALLY: this tracker only builds
 * a TrackedWrite descriptor for `tpcc_stock` UPDATE mutations (see
 * trackableWriteFromStockUpdate below). Every other write (orders,
 * order_line, customer, district, warehouse, history) still counts toward
 * `writesAcked` (see TrackingTxExecutor.observeMutateResult), but is not
 * individually verifiable via a stable index lookup the way a stock row is.
 * This is not a shortcut: tpcc_stock is *exactly* the table
 * ./skew.ts/./load-driver.ts's hot-shard skew driver targets, and therefore
 * exactly the table whose rows migrate under a reshard — it is the one table
 * where "did this write survive the reshard" is both meaningful to ask and
 * cheap to check (one indexQuery on idx_stock_by_item, the same index
 * processOrderLine itself reads).
 *
 * ----------------------------------------------------------------------------
 * COUNTER SEMANTICS (do not conflate these — see the CRITICAL tests):
 *   - writesAcked: incremented once per successful, FRESH (non-replayed)
 *     mutate()/tx() call this tracker observes, regardless of table. This is
 *     "writes", not "transactions" — one New-Order transaction fans out into
 *     many individual writes, and this counter reflects that raw volume, the
 *     same number the scoreboard's `writes N` figure (DESIGN.md) means.
 *   - writesRetriedIdempotent: incremented instead of writesAcked when the
 *     underlying call reports its result as a replay of an already-applied
 *     requestId (see TrackingTxExecutor's `duplicated` check) — a client
 *     retry that landed twice on the wire but only ever committed once. NOT
 *     a loss: nothing was lost, the same write just got acknowledged twice.
 *   - txAbortedExpected: incremented once per whole-transaction failure whose
 *     error message matches one of TPC-C's own known/legitimate abort
 *     patterns (see isExpectedAbort below — compare-and-swap contention
 *     exhausting its retry budget, a remote New-Order line losing a race on
 *     the target stock row). NOT a loss: nothing durable was ever supposed
 *     to commit for that specific attempt.
 *   - lost: incremented ONLY by verify() finding a tracked key's read-back
 *     value missing or mismatched against its last-acked value. This is the
 *     ONLY counter that can turn the meter RED (see meterStateFor below) —
 *     every other counter above is explicitly carved OUT of this one so a
 *     legitimate retry or a legitimate abort can never masquerade as (or
 *     silently suppress) a genuine write loss.
 */
import { hashKey } from "../../../../src/hash";
import { tenantIdForWarehouse, parseStockKey, type MutateCall, type MutateResult, type QueryResult, type TxExecutor } from "./transactions";

// ----------------------------------------------------------------------------
// Counters
// ----------------------------------------------------------------------------

export interface CorrectnessCounters {
  writesAcked: number;
  writesRetriedIdempotent: number;
  txAbortedExpected: number;
  lost: number;
}

export type MeterState = "green" | "red";

/** The ONE rule this whole file exists to enforce: the meter is RED iff
 * lost > 0. Every other counter is explicitly irrelevant to this decision —
 * see this file's header comment on why writesRetriedIdempotent and
 * txAbortedExpected must never leak into `lost`. */
export function meterStateFor(counters: CorrectnessCounters): MeterState {
  return counters.lost > 0 ? "red" : "green";
}

export function emptyCorrectnessCounters(): CorrectnessCounters {
  return { writesAcked: 0, writesRetriedIdempotent: 0, txAbortedExpected: 0, lost: 0 };
}

// ----------------------------------------------------------------------------
// Tracked writes + the known-key verifier
// ----------------------------------------------------------------------------

/** Everything the verifier needs to (a) decide whether a candidate write is
 * worth tracking (bias, via hashing tenantId:table:partitionKey — the exact
 * formula production routing uses, same as ./skew.ts) and (b) read the key
 * back and check it against the value this write actually committed. */
export interface TrackedWrite {
  tenantId: string;
  table: string;
  partitionKey: string;
  warehouseId: number;
  /** Index name + values to look this row up by (e.g. "idx_stock_by_item",
   * { i_id }) — CloudflareShard's tenant data plane has no "get by
   * partition key" primitive, only indexQuery/tableScan, so a read-back is
   * always an index lookup. */
  indexName: string;
  indexValues: Record<string, unknown>;
  /** The exact field/value pairs this write committed — what a correct
   * read-back's row must contain (subset match; the row may carry other
   * unrelated columns untouched by this write). */
  values: Record<string, unknown>;
}

export interface ReadBackResult {
  found: boolean;
  row?: Record<string, unknown>;
}

/** Reads back one tracked write's CURRENT row state. Implementations own the
 * actual query. See gatewayReadBack below for the real adapter (backed by a
 * TxExecutor's indexQuery — only genuinely exercised end-to-end during a
 * live cluster run) and correctness.test.ts for a trivial in-memory fake
 * that exercises the classification/comparison logic without any network. */
export type ReadBackFn = (write: TrackedWrite) => Promise<ReadBackResult>;

/** Returns true iff every field in `expected` is present in `row` with a
 * strictly-equal (===) value. A missing row (`row` undefined) never matches.
 * Extra fields on `row` are ignored — a read-back naturally returns the
 * WHOLE row, not just the fields one write touched. */
function matchesExpected(row: Record<string, unknown> | undefined, expected: Record<string, unknown>): boolean {
  if (!row) return false;
  for (const [k, v] of Object.entries(expected)) {
    if (row[k] !== v) return false;
  }
  return true;
}

function trackedKeyId(w: Pick<TrackedWrite, "tenantId" | "table" | "partitionKey">): string {
  return `${w.tenantId} ${w.table} ${w.partitionKey}`;
}

interface TrackedEntry {
  write: TrackedWrite;
  ackedAt: number;
}

// Sized generously relative to a realistic single-warehouse New-Order line
// count (5-15 lines) so a load run naturally offers more stock-write
// candidates per refresh than survive into the tracked set — see
// pickTrackedCandidates below for how that surplus gets biased toward
// migrating vbuckets instead of wasted.
const DEFAULT_MAX_TRACKED_KEYS = 50;
const DEFAULT_PENDING_BUFFER_SIZE = DEFAULT_MAX_TRACKED_KEYS * 4;

/** The subset of a vbucket-map row this module needs to know which vbuckets
 * are currently mid-migration — structurally compatible with
 * aggregator.ts's VbucketMapRow / ./skew.ts's VBucketOwnership without
 * importing either, so this stays a standalone routing utility. */
export interface VBucketMigrationRow {
  vbucket: number;
  migrationStatus: string;
}

/** vBuckets currently mid-migration (any non-"none" migrationStatus) in one
 * catalog's map — precisely the buckets a real reshard-induced loss would
 * show up on (see this file's header comment: "that's where loss would
 * happen"). A vbucket that isn't migrating has no reshard-related loss
 * surface at all, so biasing toward this set is what makes the verifier
 * actually likely to catch a real cutover bug instead of sampling
 * uniformly and (with a large keyspace) probably missing it. */
export function migratingVBuckets(vbucketMap: VBucketMigrationRow[]): Set<number> {
  const s = new Set<number>();
  for (const row of vbucketMap) {
    if (row.migrationStatus && row.migrationStatus !== "none") s.add(row.vbucket);
  }
  return s;
}

/** The pure biasing decision: given a pool of CANDIDATE writes (recently
 * acked, not yet tracked) and ONE catalog's live vbucket map, returns which
 * candidates should become tracked keys, capped at `maxTracked`.
 *
 * When a reshard is active in this catalog (>=1 vbucket mid-migration),
 * candidates whose (tenantId, table, partitionKey) hashes into a migrating
 * vbucket are STRICTLY preferred over everything else — the exact same
 * `hashKey(\`${tenantId}:${table}:${partitionKey}\`) % totalVBuckets`
 * formula ./skew.ts verifies against, just filtering an existing list
 * instead of brute-force searching for one. When no reshard is active there
 * is nothing to bias toward, so this just caps the pool, earliest
 * candidates first (deterministic — the tracked set doesn't churn
 * pointlessly tick to tick).
 *
 * Caller's responsibility (see ./load-driver.ts): resolve each candidate's
 * catalog first (vbucket ids are catalog-local) and call this once per
 * catalog — this function itself has no notion of "catalog", only one flat
 * vbucket map + totalVBuckets pair. */
export function pickTrackedCandidates(
  candidates: TrackedWrite[],
  vbucketMap: VBucketMigrationRow[],
  totalVBuckets: number,
  maxTracked: number = DEFAULT_MAX_TRACKED_KEYS,
): TrackedWrite[] {
  if (maxTracked <= 0 || candidates.length === 0) return [];
  const migrating = totalVBuckets > 0 ? migratingVBuckets(vbucketMap) : new Set<number>();
  if (migrating.size === 0) {
    return candidates.slice(0, maxTracked);
  }
  const biased: TrackedWrite[] = [];
  const rest: TrackedWrite[] = [];
  for (const c of candidates) {
    const vbucket = hashKey(`${c.tenantId}:${c.table}:${c.partitionKey}`) % totalVBuckets;
    if (migrating.has(vbucket)) biased.push(c);
    else rest.push(c);
  }
  return [...biased, ...rest].slice(0, maxTracked);
}

// ----------------------------------------------------------------------------
// CorrectnessTracker — the stateful core ./load-driver.ts wires in.
// ----------------------------------------------------------------------------

export class CorrectnessTracker {
  private counters: CorrectnessCounters;
  private tracked: Map<string, TrackedEntry> = new Map();
  private pendingCandidates: TrackedWrite[] = [];
  private readonly maxTracked: number;
  private readonly pendingBufferSize: number;

  constructor(opts?: { initialCounters?: CorrectnessCounters; maxTracked?: number; pendingBufferSize?: number }) {
    this.counters = opts?.initialCounters ? { ...opts.initialCounters } : emptyCorrectnessCounters();
    this.maxTracked = opts?.maxTracked ?? DEFAULT_MAX_TRACKED_KEYS;
    this.pendingBufferSize = opts?.pendingBufferSize ?? DEFAULT_PENDING_BUFFER_SIZE;
  }

  /** Records one successful, FRESH (non-replayed) write. Always increments
   * writesAcked. `candidate` is null for writes this tracker doesn't know
   * how to verify (anything but a tpcc_stock update — see this file's
   * header comment) — those still count toward writesAcked but never enter
   * the tracked set.
   *
   * CRITICAL: if `candidate` is a key ALREADY in the tracked set, its
   * expected value is refreshed IN PLACE, immediately — not just on the
   * next refreshTrackedSet/promoteToTracked cycle. Without this, a
   * legitimately-updated tracked key (its stock row written again by a
   * later New-Order line) would leave the verifier comparing against a
   * STALE earlier value, and a correct read-back of the NEW value would
   * wrongly look like a loss — a false RED, which is exactly the kind of
   * theater-in-the-other-direction this module must not produce either. */
  recordWriteAcked(candidate: TrackedWrite | null): void {
    this.counters.writesAcked += 1;
    if (!candidate) return;
    const id = trackedKeyId(candidate);
    if (this.tracked.has(id)) {
      this.tracked.set(id, { write: candidate, ackedAt: Date.now() });
      return;
    }
    this.pendingCandidates.push(candidate);
    if (this.pendingCandidates.length > this.pendingBufferSize) this.pendingCandidates.shift();
  }

  /** Records a write the executor detected as an idempotent replay (the
   * gateway reported the same requestId's cached result rather than
   * re-executing) — NOT a fresh ack, NOT a loss. */
  recordIdempotentReplay(): void {
    this.counters.writesRetriedIdempotent += 1;
  }

  /** Records a transaction failure recognized as one of TPC-C's own
   * known/legitimate abort patterns (see isExpectedAbort below) — NOT a
   * loss: nothing durable was ever supposed to commit for that attempt. */
  recordExpectedAbort(): void {
    this.counters.txAbortedExpected += 1;
  }

  /** Pops and clears the buffer of recently-acked, not-yet-tracked
   * candidates. Callers (./load-driver.ts) drain this on a cadence, resolve
   * each candidate's catalog, run pickTrackedCandidates per catalog, and
   * feed the result back via promoteToTracked — see this file's header
   * comment for why that resolution can't happen inside this class. */
  drainPendingCandidates(): TrackedWrite[] {
    const drained = this.pendingCandidates;
    this.pendingCandidates = [];
    return drained;
  }

  /** Adds `writes` to the tracked set (already bias-selected by the caller —
   * see pickTrackedCandidates). If the tracked set would exceed maxTracked,
   * the OLDEST entries (by insertion order — a plain Map iterates in
   * insertion order) are evicted first, so the set stays bounded without
   * ever exceeding its budget mid-run. */
  promoteToTracked(writes: TrackedWrite[]): void {
    for (const write of writes) {
      const id = trackedKeyId(write);
      if (!this.tracked.has(id)) {
        this.tracked.set(id, { write, ackedAt: Date.now() });
      }
    }
    if (this.tracked.size > this.maxTracked) {
      const excess = this.tracked.size - this.maxTracked;
      const it = this.tracked.keys();
      for (let i = 0; i < excess; i++) {
        const k = it.next().value;
        if (k !== undefined) this.tracked.delete(k);
      }
    }
  }

  /** THE GUARD: runs one verify pass over every currently-tracked key,
   * reading each back via `readBack` and comparing against its last-acked
   * expected value. A missing row OR any mismatched field increments
   * `lost` — the ONLY way this tracker's meter can go RED (see
   * meterStateFor). With a real gatewayReadBack against a live cluster,
   * this is a genuine check, not a simulation: if a reshard cutover
   * silently dropped a row, this is what notices. */
  async verify(readBack: ReadBackFn): Promise<{ checked: number; lostThisPass: number }> {
    let lostThisPass = 0;
    for (const entry of this.tracked.values()) {
      const result = await readBack(entry.write);
      if (!matchesExpected(result.row, entry.write.values)) {
        this.counters.lost += 1;
        lostThisPass += 1;
      }
    }
    return { checked: this.tracked.size, lostThisPass };
  }

  snapshot(): CorrectnessCounters & { meterState: MeterState; trackedKeyCount: number } {
    return { ...this.counters, meterState: meterStateFor(this.counters), trackedKeyCount: this.tracked.size };
  }

  /** Test/debug hook — the currently-tracked writes, without exposing the
   * internal Map. */
  trackedWrites(): TrackedWrite[] {
    return [...this.tracked.values()].map((e) => e.write);
  }
}

// ----------------------------------------------------------------------------
// Abort classification
// ----------------------------------------------------------------------------

// Substrings drawn directly from ./transactions.ts's own thrown error
// messages for its two documented, EXPECTED contention races: Payment's/
// Delivery's compare-and-swap retry loops exhausting MUTATION_RETRY_ATTEMPTS
// ("persistent contention"), and a remote New-Order line losing a race on
// the supply warehouse's stock row ("changed concurrently"). A New-Order
// that fails after compensation ("New-Order failed on N/M line(s)") is also
// expected: compensateFailedOrder has already reversed every committed
// sibling line, so nothing durable was left half-applied.
const KNOWN_EXPECTED_ABORT_PATTERNS: RegExp[] = [/persistent contention/i, /changed concurrently/i, /New-Order failed on \d+\/\d+ line/i];

/** Classifies a failed transaction's error message as a known/legitimate
 * TPC-C abort the mix is EXPECTED to produce under real contention, vs
 * anything else. Deliberately conservative: an unrecognized error is never
 * assumed to be a legitimate abort just because it also isn't a lost write —
 * it's simply left uncounted by txAbortedExpected (still visible via
 * ./load-driver.ts's existing generic attempted/ok/err counters). */
export function isExpectedAbort(errorMessage: string | undefined): boolean {
  if (!errorMessage) return false;
  return KNOWN_EXPECTED_ABORT_PATTERNS.some((re) => re.test(errorMessage));
}

// ----------------------------------------------------------------------------
// trackableWriteFromStockUpdate — builds a TrackedWrite from a raw
// MutateCall, for exactly the one write shape this file tracks (see header
// comment): a tpcc_stock UPDATE.
// ----------------------------------------------------------------------------

export function trackableWriteFromStockUpdate(call: MutateCall): TrackedWrite | null {
  if (call.table !== "tpcc_stock" || call.op !== "update" || !call.values) return null;
  const parsed = parseStockKey(call.partitionKey);
  if (!parsed) return null;
  return {
    tenantId: tenantIdForWarehouse(parsed.warehouseId),
    table: call.table,
    partitionKey: call.partitionKey,
    warehouseId: parsed.warehouseId,
    indexName: "idx_stock_by_item",
    indexValues: { i_id: parsed.itemId },
    values: { ...call.values },
  };
}

// ----------------------------------------------------------------------------
// TrackingTxExecutor — the TxExecutor decorator ./load-driver.ts wraps its
// real HttpTxExecutor in. Observes every mutate()/tx() call in passing;
// never changes behavior, return value, or error propagation — a failed
// call still throws exactly as the wrapped executor threw it, so
// transactions.ts's own compensation/retry control flow is untouched.
// ----------------------------------------------------------------------------

export class TrackingTxExecutor implements TxExecutor {
  constructor(
    private readonly inner: TxExecutor,
    private readonly tracker: CorrectnessTracker,
  ) {}

  async mutate(warehouseId: number, call: MutateCall): Promise<MutateResult> {
    const result = await this.inner.mutate(warehouseId, call);
    this.observe(call, result);
    return result;
  }

  async tx(warehouseId: number, mutations: MutateCall[], requestId?: string): Promise<{ committed?: boolean; [k: string]: unknown }> {
    const result = await this.inner.tx(warehouseId, mutations, requestId);
    // /v1/tx succeeding (not throwing) means every mutation in it committed —
    // see transactions.ts's header comment (adaptation #2) for why /v1/tx
    // doesn't report per-mutation rowsAffected. That same limitation means
    // this decorator cannot detect a per-mutation idempotent replay inside a
    // tx() call the way it can for a standalone mutate() call below — an
    // honest, documented gap, not a silent one: every mutation in a
    // successful tx() is always recorded as a fresh ack here, never as a
    // replay.
    for (const call of mutations) {
      this.observe(call, {});
    }
    return result;
  }

  async indexQuery(warehouseId: number, table: string, indexName: string, values: Record<string, unknown>, limit?: number): Promise<QueryResult> {
    return this.inner.indexQuery(warehouseId, table, indexName, values, limit);
  }

  async tableScan(warehouseId: number, table: string, limit: number, cursor?: unknown): Promise<QueryResult> {
    return this.inner.tableScan(warehouseId, table, limit, cursor);
  }

  private observe(call: MutateCall, result: MutateResult): void {
    // A delete (e.g. Delivery's marker claim, or a New-Order/Payment
    // compensation reversing an already-committed line) is still a real,
    // acknowledged write — it counts toward writesAcked the same as any
    // insert/update/upsert — it's just never trackable (no stable "expected
    // value" to verify a deleted row against; see trackableWriteFromStockUpdate,
    // which already returns null for anything but a tpcc_stock UPDATE).
    const candidate = trackableWriteFromStockUpdate(call);
    // `duplicated` is not part of MutateResult's declared shape (the real
    // gateway's /v1/mutate response today is just { ok, rowsAffected } — see
    // this file's header comment) but IS read defensively here via the
    // index signature, so this classification is correct the day the
    // gateway adds replay reporting without requiring a second change here.
    if ((result as { duplicated?: unknown }).duplicated === true) {
      this.tracker.recordIdempotentReplay();
    } else {
      this.tracker.recordWriteAcked(candidate);
    }
  }
}

// ----------------------------------------------------------------------------
// gatewayReadBack — the real ReadBackFn adapter, backed by a live TxExecutor.
// Only genuinely exercised end-to-end once a live cluster run exists; the
// classification/comparison logic it feeds (CorrectnessTracker.verify) is
// already fully exercised today via a trivial fake in correctness.test.ts.
// ----------------------------------------------------------------------------

export function gatewayReadBack(exec: TxExecutor): ReadBackFn {
  return async (write: TrackedWrite): Promise<ReadBackResult> => {
    const res = await exec.indexQuery(write.warehouseId, write.table, write.indexName, write.indexValues, 1);
    const row = (res.rows ?? [])[0] as Record<string, unknown> | undefined;
    return { found: !!row, row };
  };
}
