import { describe, expect, it } from "vitest";
import { hashKey } from "../../../../src/hash";
import {
  CorrectnessTracker,
  TrackingTxExecutor,
  emptyCorrectnessCounters,
  gatewayReadBack,
  isExpectedAbort,
  meterStateFor,
  migratingVBuckets,
  pickTrackedCandidates,
  trackableWriteFromStockUpdate,
  type CatalogVBucketMap,
  type ReadBackFn,
  type ReadBackResult,
  type SqlPointReader,
  type TrackedCandidate,
  type TrackedWrite,
  type VBucketMigrationRow,
} from "./correctness";
import { stockKey, tenantIdForWarehouse, type MutateCall, type MutateResult, type QueryResult, type TxExecutor } from "./transactions";

// ----------------------------------------------------------------------------
// Test fixtures
// ----------------------------------------------------------------------------

function stockWrite(overrides?: Partial<TrackedWrite>): TrackedWrite {
  const warehouseId = overrides?.warehouseId ?? 1;
  const itemId = 42;
  return {
    tenantId: tenantIdForWarehouse(warehouseId),
    table: "tpcc_stock",
    partitionKey: stockKey(warehouseId, itemId),
    warehouseId,
    catalogShardId: "catalog-0",
    indexName: "idx_stock_by_item",
    indexValues: { i_id: itemId },
    keyField: "s_key",
    values: { s_quantity: 88, s_ytd: 12, s_order_cnt: 3, s_remote_cnt: 0 },
    ...overrides,
  };
}

/** Builds a read-back row for `write` — its own `values` plus the row's
 * identity column (write.keyField) set to write.partitionKey, i.e. exactly
 * what a real gateway read-back returns for a row that genuinely IS the
 * tracked write. Overrides let a test deliberately corrupt one field
 * (a wrong value) or the identity column (a wrong/different row). */
function rowFor(write: TrackedWrite, overrides?: Record<string, unknown>): Record<string, unknown> {
  return { ...write.values, [write.keyField]: write.partitionKey, ...overrides };
}

/** A fake TxExecutor whose mutate() always succeeds with the given
 * rowsAffected/duplicated shape — enough to exercise TrackingTxExecutor
 * without any network. */
function fakeExecutor(opts?: { mutateResult?: MutateResult }): TxExecutor {
  return {
    mutate: async (_warehouseId: number, _call: MutateCall): Promise<MutateResult> => opts?.mutateResult ?? { rowsAffected: 1 },
    tx: async () => ({ committed: true }),
    indexQuery: async (): Promise<QueryResult> => ({ rows: [] }),
    tableScan: async (): Promise<QueryResult> => ({ rows: [] }),
  };
}

/** A fake SqlPointReader keyed by partitionKey (a PRECISE point lookup, not
 * an index scan — see ./correctness.ts's SqlPointReader doc comment) — the
 * fixture gatewayReadBack tests and TrackingTxExecutor's replay-verification
 * tests drive against, without any network. */
function fakeSqlPointReader(rowsByPartitionKey?: Map<string, Record<string, unknown>>): SqlPointReader {
  const rows = rowsByPartitionKey ?? new Map<string, Record<string, unknown>>();
  return {
    sqlSelect: async ({ partitionKey }) => {
      const row = rows.get(partitionKey);
      return { rows: row ? [row] : [] };
    },
  };
}

/** Always resolves to "catalog-0" — good enough for every test in this file
 * that doesn't care about catalog identity itself (i.e. everything except
 * the pickTrackedCandidates catalog-scoping tests, which build their own
 * CatalogVBucketMap fixtures directly). */
const resolveToCatalog0 = (_tenantId: string): string | null => "catalog-0";

/** Runs the ordinary drain -> promote pipeline for one or more acked
 * candidates — the common-path shorthand almost every test below uses. */
function drainAndPromote(tracker: CorrectnessTracker): void {
  tracker.promoteToTracked(tracker.drainPendingCandidates());
}

// ----------------------------------------------------------------------------
// CRITICAL TEST 1 — THE GUARD: a stale/missing read-back during a (simulated)
// cutover MUST turn the meter RED. If this test cannot fail, the whole
// module is theater.
// ----------------------------------------------------------------------------

describe("correctness.ts — CRITICAL: the meter can go RED", () => {
  it("THE GUARD: a tracked key that reads back stale/missing during cutover reports lost > 0 and meterState RED", async () => {
    const tracker = new CorrectnessTracker();
    const write = stockWrite();

    // The write was acked (committed) with s_quantity: 88 ...
    tracker.recordWriteAcked(write);
    drainAndPromote(tracker);
    expect(tracker.trackedWrites()).toHaveLength(1);
    expect(tracker.snapshot().meterState).toBe("green");
    expect(tracker.snapshot().lost).toBe(0);

    // ... but a read-back DURING a simulated cutover finds the row missing
    // entirely (the loss scenario a botched migration would produce).
    const missingReadBack: ReadBackFn = async (): Promise<ReadBackResult> => ({ found: false });
    const missingResult = await tracker.verify(missingReadBack);
    expect(missingResult.lostThisPass).toBe(1);
    expect(tracker.snapshot().lost).toBe(1);
    expect(tracker.snapshot().meterState).toBe("red");
    expect(meterStateFor(tracker.snapshot())).toBe("red");
  });

  it("THE GUARD (mismatch variant): a tracked key whose read-back value doesn't match what was acked also goes RED", async () => {
    const tracker = new CorrectnessTracker();
    const write = stockWrite({ values: { s_quantity: 88, s_ytd: 12, s_order_cnt: 3, s_remote_cnt: 0 } });
    tracker.recordWriteAcked(write);
    drainAndPromote(tracker);

    // A stale value landed instead of the acked one (e.g. a migration
    // cutover that silently reverted to a pre-migration snapshot).
    const staleReadBack: ReadBackFn = async (): Promise<ReadBackResult> => ({
      found: true,
      row: rowFor(write, { s_quantity: 91 }), // s_quantity wrong
    });
    const result = await tracker.verify(staleReadBack);
    expect(result.lostThisPass).toBe(1);
    expect(tracker.snapshot().lost).toBe(1);
    expect(tracker.snapshot().meterState).toBe("red");
  });

  it("multiple tracked keys: only the genuinely-lost ones count, the meter still goes RED", async () => {
    const tracker = new CorrectnessTracker();
    const good = stockWrite({ warehouseId: 1 });
    const lost = stockWrite({ warehouseId: 2 });
    tracker.recordWriteAcked(good);
    tracker.recordWriteAcked(lost);
    drainAndPromote(tracker);

    const readBack: ReadBackFn = async (write: TrackedWrite): Promise<ReadBackResult> => {
      if (write.warehouseId === 1) return { found: true, row: rowFor(write) };
      return { found: false };
    };
    const result = await tracker.verify(readBack);
    expect(result.checked).toBe(2);
    expect(result.lostThisPass).toBe(1);
    expect(tracker.snapshot().lost).toBe(1);
    expect(tracker.snapshot().meterState).toBe("red");
  });
});

// ----------------------------------------------------------------------------
// HOLE #1 — SAME-KEY LATEST-WINS AS A STRUCTURAL INVARIANT (design round 3).
// A tracked key's expected value is ALWAYS the value with the highest
// ack-seq ever observed for that key identity — ackSeq is minted in EXACTLY
// ONE place (recordWriteAcked), and every downstream function only ever
// forwards it, never invents one. See ./correctness.ts's header comment,
// point 1.
// ----------------------------------------------------------------------------

describe("correctness.ts — HOLE #1: same-key latest-wins is a structural invariant, not a comparison that can be bypassed", () => {
  it("two acked writes to the SAME row are deduped in pendingCandidates down to ONE entry holding the LATEST value", () => {
    const tracker = new CorrectnessTracker();
    const writeA = stockWrite({ values: { s_quantity: 88, s_ytd: 12, s_order_cnt: 3, s_remote_cnt: 0 } });
    const writeB = stockWrite({ values: { s_quantity: 50, s_ytd: 20, s_order_cnt: 4, s_remote_cnt: 0 } }); // SAME row, newer value
    tracker.recordWriteAcked(writeA);
    tracker.recordWriteAcked(writeB);

    const pending = tracker.drainPendingCandidates();
    expect(pending).toHaveLength(1); // deduped to ONE row, not two
    expect(pending[0].write.values).toEqual(writeB.values); // the LATEST value survives, not the first
    expect(pending[0].ackSeq).toBeGreaterThan(-1);
  });

  it("a lost LATEST write to an already-promoted row is caught, not masked by the earlier value that was actually promoted", async () => {
    const tracker = new CorrectnessTracker();
    const writeA = stockWrite({ values: { s_quantity: 88, s_ytd: 12, s_order_cnt: 3, s_remote_cnt: 0 } });
    const writeB = stockWrite({ values: { s_quantity: 50, s_ytd: 20, s_order_cnt: 4, s_remote_cnt: 0 } });
    tracker.recordWriteAcked(writeA);
    tracker.recordWriteAcked(writeB);
    drainAndPromote(tracker);
    expect(tracker.trackedWrites()).toHaveLength(1);
    expect(tracker.trackedWrites()[0].values).toEqual(writeB.values);

    // The read-back returns writeA's STALE value — writeB (the latest acked
    // write) never actually landed. If the tracker were still comparing
    // against writeA (the false-green this test guards against), this would
    // wrongly read GREEN.
    const readBack: ReadBackFn = async () => ({ found: true, row: rowFor(writeA) });
    const result = await tracker.verify(readBack);
    expect(result.lostThisPass).toBe(1);
    expect(tracker.snapshot().lost).toBe(1);
    expect(tracker.snapshot().meterState).toBe("red");
  });

  it("drain (hold v1) -> ack v2 (same key) -> promote the drained set -> tracks v2 (the LATEST ack), never the stale drained v1", () => {
    const tracker = new CorrectnessTracker();
    const v1 = stockWrite({ values: { s_quantity: 88, s_ytd: 12, s_order_cnt: 3, s_remote_cnt: 0 } });
    const v2 = stockWrite({ values: { s_quantity: 50, s_ytd: 20, s_order_cnt: 4, s_remote_cnt: 0 } }); // SAME row, newer value

    tracker.recordWriteAcked(v1);
    const drained = tracker.drainPendingCandidates(); // holds [v1], pendingCandidates now empty
    expect(drained).toHaveLength(1);
    expect(drained[0].write.values).toEqual(v1.values);

    // A newer ack for the SAME row lands AFTER the drain, before promotion.
    tracker.recordWriteAcked(v2);

    // The caller now promotes the STALE snapshot it drained earlier.
    tracker.promoteToTracked(drained);

    // THE FIX: the tracked expectation must be v2 (the latest acked value),
    // not the stale v1 the caller happened to be holding.
    expect(tracker.trackedWrites()).toHaveLength(1);
    expect(tracker.trackedWrites()[0].values).toEqual(v2.values);
  });

  it("REGRESSION: if the LATEST acked write (v2) never actually lands, verify() reports lost > 0 / RED — not a false green against stale v1", async () => {
    const tracker = new CorrectnessTracker();
    const v1 = stockWrite({ values: { s_quantity: 88, s_ytd: 12, s_order_cnt: 3, s_remote_cnt: 0 } });
    const v2 = stockWrite({ values: { s_quantity: 50, s_ytd: 20, s_order_cnt: 4, s_remote_cnt: 0 } });

    tracker.recordWriteAcked(v1);
    const drained = tracker.drainPendingCandidates();
    tracker.recordWriteAcked(v2); // v2 is the latest ack, but (in this scenario) never actually lands
    tracker.promoteToTracked(drained);

    // The read-back genuinely reflects reality here: the row still has v1's
    // OLD value (v2's write was lost / never applied).
    const readBack: ReadBackFn = async () => ({ found: true, row: rowFor(v1) });
    const result = await tracker.verify(readBack);
    expect(result.lostThisPass).toBe(1);
    expect(tracker.snapshot().lost).toBe(1);
    expect(tracker.snapshot().meterState).toBe("red");
  });

  it("no race: a normal drain -> promote with no interleaving ack still tracks the drained value (no regression on the common path)", () => {
    const tracker = new CorrectnessTracker();
    const write = stockWrite();
    tracker.recordWriteAcked(write);
    drainAndPromote(tracker);
    expect(tracker.trackedWrites()).toHaveLength(1);
    expect(tracker.trackedWrites()[0].values).toEqual(write.values);
  });

  it("REGRESSION: a direct promoteToTracked call carrying a fabricated, LOW ackSeq can never beat a genuinely fresher acked entry — ackSeq is only ever minted by recordWriteAcked", () => {
    const tracker = new CorrectnessTracker();
    const fresh = stockWrite({ values: { s_quantity: 2 } });
    tracker.recordWriteAcked(fresh);
    drainAndPromote(tracker);
    expect(tracker.trackedWrites()).toHaveLength(1);
    expect(tracker.trackedWrites()[0].values).toEqual(fresh.values);

    // A caller (a stale cached reference, a misbehaving retry path, or a
    // test deliberately simulating the old bug) tries to promote a
    // DIFFERENT, OLDER value for the SAME row directly, hand-carrying a low
    // ackSeq. Under the OLD design (a WeakMap lazily minting a seq the
    // first time it saw ANY object), a never-before-seen write reaching
    // promoteToTracked would get the CURRENT highest seq — always winning,
    // regardless of how stale its value actually was. Under this design,
    // ackSeq travels as plain data the caller must already have gotten
    // (legitimately) from recordWriteAcked — a fabricated low one simply
    // loses the "never regress" comparison in promoteToTracked.
    const stale: TrackedCandidate = { write: stockWrite({ values: { s_quantity: 1 } }), ackSeq: -1 };
    tracker.promoteToTracked([stale]);
    expect(tracker.trackedWrites()).toHaveLength(1);
    expect(tracker.trackedWrites()[0].values).toEqual(fresh.values); // unchanged — the stale candidate never wins
  });

  it("REGRESSION: latest-wins holds regardless of call ordering — acking v1, then v2, then promoting v1's OWN (already-stale) drained candidate object directly still ends up tracking v2", () => {
    const tracker = new CorrectnessTracker();
    const v1 = stockWrite({ values: { s_quantity: 1 } });
    const v2 = stockWrite({ values: { s_quantity: 2 } });

    tracker.recordWriteAcked(v1);
    const drainedV1 = tracker.drainPendingCandidates(); // [{write: v1, ackSeq: N}]
    tracker.recordWriteAcked(v2); // same key, strictly higher ackSeq, lands in pendingCandidates (not yet tracked)

    // Promoting the STALE v1 candidate object directly (out of call order)
    // must still resolve to v2 — pendingCandidates holds the fresher entry
    // for this identity, and promoteToTracked always consults it.
    tracker.promoteToTracked(drainedV1);
    expect(tracker.trackedWrites()).toHaveLength(1);
    expect(tracker.trackedWrites()[0].values).toEqual(v2.values);
  });
});

// ----------------------------------------------------------------------------
// ROUND 7 — EVICTION REMOVED. `tracked` is now the COMPLETE set of every
// distinct key this run has acked a write for; there is no capacity bound
// left to evict against. See ./correctness.ts's header comment, "ROUND 7 —
// EVICTION REMOVED", for the root-cause reasoning (rounds 1-6's HOLE #4
// tests — "eviction is by ack-sequence, never wall-clock" — are retired:
// there is no eviction code path left for them to exercise). The
// supersede-then-evict false-green rounds 1-6 could never fully close is
// now covered directly below, by proving the SCENARIO that used to defeat
// eviction-based tracking can no longer false-green with no eviction at all.
// ----------------------------------------------------------------------------

describe("correctness.ts — ROUND 7: eviction removed — supersede-then-evict can no longer false-green", () => {
  it("REGRESSION (round 7 primary fix): track key K, ack a newer value for K, then ack hundreds of OTHER keys (previously enough to evict K under the old bounded-sample design) — K's latest value is STILL tracked and verified; a bad read-back for it is a genuine loss, not silently dropped", async () => {
    const tracker = new CorrectnessTracker();
    const kOld = stockWrite({ warehouseId: 1, values: { s_quantity: 1, s_ytd: 1, s_order_cnt: 1, s_remote_cnt: 0 } });
    tracker.recordWriteAcked(kOld);
    drainAndPromote(tracker);

    // K's LATEST acked value — supersedes kOld while K is already tracked
    // (refreshed in place, per recordWriteAcked's same-key-latest-wins path).
    const kNew = stockWrite({ warehouseId: 1, values: { s_quantity: 999, s_ytd: 2, s_order_cnt: 2, s_remote_cnt: 0 } });
    tracker.recordWriteAcked(kNew);

    // Flood with far more than the OLD (pre-round-7) DEFAULT_MAX_TRACKED_KEYS
    // (50) worth of distinct OTHER keys. Under the pre-round-7 design, this
    // would have evicted K out of `tracked` (lowest ackSeq) long before any
    // verify() pass ever covered kNew — the round-6 root cause ("eviction
    // and complete verification are incompatible") made that loss
    // structurally undetectable, no matter how the eviction/high-water-mark
    // bookkeeping was tuned. With eviction removed, K must still be present.
    for (let i = 0; i < 500; i++) {
      tracker.recordWriteAcked(stockWrite({ warehouseId: 1000 + i }));
    }
    drainAndPromote(tracker);

    // K's latest value (kNew) is still in the tracked set — not evicted.
    expect(tracker.trackedWrites().find((w) => w.warehouseId === 1)?.values).toEqual(kNew.values);
    // K + the 500 flood keys are ALL tracked — complete over every key
    // acked this run, not a bounded/capped sample of them.
    expect(tracker.snapshot().trackedKeyCount).toBe(501);

    // Now verify() runs and K's LATEST value (kNew) reads back missing —
    // simulating a real reshard-induced drop of the superseding write.
    const readBack: ReadBackFn = async (write) => {
      if (write.warehouseId === 1) return { found: false }; // K's latest value genuinely lost
      return { found: true, row: rowFor(write) };
    };
    const result = await tracker.verify(readBack);

    // REVERT-TO-CONFIRM-RED: reinstating promoteToTracked's old
    // `if (this.tracked.size > this.maxTracked) { evictLeastAckSeq(...) }`
    // eviction block (with a default maxTracked of 50) makes this assertion
    // fail — K would have been evicted long before this verify() pass ever
    // ran, so this pass would never even attempt K's read-back, and
    // lostThisPass/lost would incorrectly stay 0 with meterState incorrectly
    // staying green.
    expect(result.lostThisPass).toBe(1);
    expect(tracker.snapshot().lost).toBe(1);
    expect(tracker.snapshot().meterState).toBe("red");
  });
});

// ----------------------------------------------------------------------------
// REGRESSION — read-back must match the exact tracked row's identity, not
// just its mutated fields.
// ----------------------------------------------------------------------------

describe("correctness.ts — REGRESSION: read-back must match the exact tracked row, not just its mutated fields", () => {
  it("a read-back that returns a DIFFERENT row whose mutated field coincidentally matches must NOT pass as green", async () => {
    const tracker = new CorrectnessTracker();
    const write = stockWrite({ warehouseId: 1 }); // tracked row's real key: s-0001-000042
    tracker.recordWriteAcked(write);
    drainAndPromote(tracker);

    const wrongRow = rowFor(write, { s_key: stockKey(1, 999) });
    const readBack: ReadBackFn = async () => ({ found: true, row: wrongRow });
    const result = await tracker.verify(readBack);
    expect(result.lostThisPass).toBe(1);
    expect(tracker.snapshot().lost).toBe(1);
    expect(tracker.snapshot().meterState).toBe("red");
  });

  it("a read-back with no identity column at all (row missing s_key) must NOT pass as green", async () => {
    const tracker = new CorrectnessTracker();
    const write = stockWrite();
    tracker.recordWriteAcked(write);
    drainAndPromote(tracker);

    const readBack: ReadBackFn = async () => ({ found: true, row: { ...write.values } }); // no s_key at all
    const result = await tracker.verify(readBack);
    expect(result.lostThisPass).toBe(1);
    expect(tracker.snapshot().meterState).toBe("red");
  });

  it("a read-back that IS the exact tracked row (matching identity + values) stays green", async () => {
    const tracker = new CorrectnessTracker();
    const write = stockWrite();
    tracker.recordWriteAcked(write);
    drainAndPromote(tracker);

    const readBack: ReadBackFn = async () => ({ found: true, row: rowFor(write) });
    const result = await tracker.verify(readBack);
    expect(result.lostThisPass).toBe(0);
    expect(tracker.snapshot().meterState).toBe("green");
  });
});

// ----------------------------------------------------------------------------
// CRITICAL TEST 2 — idempotent replay increments writesRetriedIdempotent,
// NOT lost, UNLESS a read-back disproves the replay's own claim.
// ----------------------------------------------------------------------------

describe("correctness.ts — CRITICAL: idempotent replay is not a loss (but is verified, not trusted)", () => {
  it("recordIdempotentReplay with no candidate increments writesRetriedIdempotent only", async () => {
    const tracker = new CorrectnessTracker();
    await tracker.recordIdempotentReplay();
    await tracker.recordIdempotentReplay();
    const snap = tracker.snapshot();
    expect(snap.writesRetriedIdempotent).toBe(2);
    expect(snap.writesAcked).toBe(0);
    expect(snap.lost).toBe(0);
    expect(snap.meterState).toBe("green");
  });

  it("recordIdempotentReplay for a trackable candidate whose read-back CONFIRMS the value stays green", async () => {
    const tracker = new CorrectnessTracker();
    const write = stockWrite();
    const readBack: ReadBackFn = async () => ({ found: true, row: rowFor(write) });
    await tracker.recordIdempotentReplay(write, readBack);
    const snap = tracker.snapshot();
    expect(snap.writesRetriedIdempotent).toBe(1);
    expect(snap.lost).toBe(0);
    expect(snap.meterState).toBe("green");
  });

  it("REGRESSION: recordIdempotentReplay for a candidate whose read-back DISPROVES the replay claim is `lost`, not silently trusted", async () => {
    const tracker = new CorrectnessTracker();
    const write = stockWrite();
    const readBack: ReadBackFn = async () => ({ found: false }); // the "replayed" row is nowhere to be found
    await tracker.recordIdempotentReplay(write, readBack);
    const snap = tracker.snapshot();
    expect(snap.writesRetriedIdempotent).toBe(1); // still counted as a replay (that's what the caller's result claimed)...
    expect(snap.lost).toBe(1); // ...but the read-back proved it wasn't actually there
    expect(snap.meterState).toBe("red");
  });

  it("TrackingTxExecutor classifies a `duplicated: true` mutate result as a replay, and verifies it via the supplied read-back before trusting it", async () => {
    const tracker = new CorrectnessTracker();
    const call: MutateCall = { op: "update", table: "tpcc_stock", partitionKey: stockKey(1, 42), values: { s_quantity: 10 } };
    const matchingRow = { s_key: stockKey(1, 42), s_quantity: 10 };
    const sqlReader = fakeSqlPointReader(new Map([[stockKey(1, 42), matchingRow]]));
    const inner = fakeExecutor({ mutateResult: { rowsAffected: 1, duplicated: true } });
    const exec = new TrackingTxExecutor(inner, tracker, resolveToCatalog0, gatewayReadBack(sqlReader));

    await exec.mutate(1, call);

    const snap = tracker.snapshot();
    expect(snap.writesRetriedIdempotent).toBe(1);
    expect(snap.writesAcked).toBe(0);
    expect(snap.lost).toBe(0);
  });

  it("REGRESSION: TrackingTxExecutor's replay path does NOT stay green when the replayed row is actually missing", async () => {
    const tracker = new CorrectnessTracker();
    const call: MutateCall = { op: "update", table: "tpcc_stock", partitionKey: stockKey(1, 42), values: { s_quantity: 10 } };
    const sqlReader = fakeSqlPointReader(); // empty — row genuinely missing
    const inner = fakeExecutor({ mutateResult: { rowsAffected: 1, duplicated: true } });
    const exec = new TrackingTxExecutor(inner, tracker, resolveToCatalog0, gatewayReadBack(sqlReader));

    await exec.mutate(1, call);

    const snap = tracker.snapshot();
    expect(snap.writesRetriedIdempotent).toBe(1);
    expect(snap.lost).toBe(1);
    expect(snap.meterState).toBe("red");
  });

  it("a TrackingTxExecutor built WITHOUT a replay read-back (the 4th ctor arg omitted) simply records the replay claim, same as recordIdempotentReplay(candidate) with no readBack", async () => {
    const tracker = new CorrectnessTracker();
    const call: MutateCall = { op: "update", table: "tpcc_stock", partitionKey: stockKey(1, 42), values: { s_quantity: 10 } };
    const inner = fakeExecutor({ mutateResult: { rowsAffected: 1, duplicated: true } });
    const exec = new TrackingTxExecutor(inner, tracker, resolveToCatalog0); // 3 args — matches every pre-round-3 call site

    await exec.mutate(1, call);

    const snap = tracker.snapshot();
    expect(snap.writesRetriedIdempotent).toBe(1);
    expect(snap.lost).toBe(0); // no read-back supplied -> the claim is recorded, not disproven
  });

  it("a fresh (non-duplicated) mutate result increments writesAcked, not writesRetriedIdempotent", async () => {
    const tracker = new CorrectnessTracker();
    const inner = fakeExecutor({ mutateResult: { rowsAffected: 1 } });
    const exec = new TrackingTxExecutor(inner, tracker, resolveToCatalog0);

    await exec.mutate(1, { op: "update", table: "tpcc_stock", partitionKey: stockKey(1, 42), values: { s_quantity: 10 } });

    const snap = tracker.snapshot();
    expect(snap.writesAcked).toBe(1);
    expect(snap.writesRetriedIdempotent).toBe(0);
  });
});

// ----------------------------------------------------------------------------
// CRITICAL TEST 3 — a known TPC-C abort increments txAbortedExpected, NOT
// lost. Also REGRESSION: isExpectedAbort is anchored, not a loose substring
// match.
// ----------------------------------------------------------------------------

describe("correctness.ts — CRITICAL: a known/legitimate abort is not a loss", () => {
  it("recordExpectedAbort increments txAbortedExpected only", () => {
    const tracker = new CorrectnessTracker();
    tracker.recordExpectedAbort();
    const snap = tracker.snapshot();
    expect(snap.txAbortedExpected).toBe(1);
    expect(snap.lost).toBe(0);
    expect(snap.writesAcked).toBe(0);
    expect(snap.meterState).toBe("green");
  });

  it("isExpectedAbort recognizes transactions.ts's documented contention-race messages", () => {
    expect(isExpectedAbort("customer 7 balance update did not apply after 5 attempts — persistent contention")).toBe(true);
    expect(isExpectedAbort("stock row s-0001-000042 changed concurrently — remote line's stock update did not apply")).toBe(true);
    expect(isExpectedAbort("New-Order failed on 2/8 line(s) (compensated 6 already-committed line(s)): line 3: boom")).toBe(true);
  });

  it("isExpectedAbort does NOT classify an unrecognized error as expected", () => {
    expect(isExpectedAbort("district 3 not found in warehouse 1's district table-scan")).toBe(false);
    expect(isExpectedAbort("network timeout")).toBe(false);
    expect(isExpectedAbort(undefined)).toBe(false);
  });

  it("REGRESSION: does NOT classify an infra/storage error that merely mentions 'persistent contention' in passing", () => {
    expect(isExpectedAbort("storage backend unavailable while contacting the persistent contention coordinator service")).toBe(false);
  });

  it("REGRESSION: does NOT classify an infra/storage error that merely mentions 'changed concurrently' in passing", () => {
    expect(isExpectedAbort("internal cache invalidation: upstream config changed concurrently with startup, retry the request")).toBe(false);
  });

  it("REGRESSION: does NOT classify a New-Order failure message unless it matches the exact compensation-summary shape", () => {
    expect(isExpectedAbort("New-Order failed catastrophically — infra outage, no compensation attempted")).toBe(false);
  });
});

// ----------------------------------------------------------------------------
// CRITICAL TEST 4 — all-consistent reads keep lost == 0 (GREEN).
// ----------------------------------------------------------------------------

describe("correctness.ts — CRITICAL: consistent reads stay GREEN", () => {
  it("verify() against matching read-backs never increments lost", async () => {
    const tracker = new CorrectnessTracker();
    const writes = [stockWrite({ warehouseId: 1 }), stockWrite({ warehouseId: 2 }), stockWrite({ warehouseId: 3 })];
    for (const w of writes) tracker.recordWriteAcked(w);
    drainAndPromote(tracker);

    const readBack: ReadBackFn = async (write: TrackedWrite): Promise<ReadBackResult> => ({ found: true, row: rowFor(write) });
    const result = await tracker.verify(readBack);

    expect(result.checked).toBe(3);
    expect(result.lostThisPass).toBe(0);
    const snap = tracker.snapshot();
    expect(snap.lost).toBe(0);
    expect(snap.meterState).toBe("green");
  });

  it("a read-back row with EXTRA fields (not part of the acked write) still matches — subset comparison, not full-row equality", async () => {
    const tracker = new CorrectnessTracker();
    const write = stockWrite();
    tracker.recordWriteAcked(write);
    drainAndPromote(tracker);

    const readBack: ReadBackFn = async (): Promise<ReadBackResult> => ({
      found: true,
      row: rowFor(write, { extra_column_untouched_by_this_write: "whatever" }),
    });
    const result = await tracker.verify(readBack);
    expect(result.lostThisPass).toBe(0);
    expect(tracker.snapshot().meterState).toBe("green");
  });

  it("recordWriteAcked on an ALREADY-tracked key refreshes its expected value in place — a later legitimate update never false-reds", async () => {
    const tracker = new CorrectnessTracker();
    const w = stockWrite({ values: { s_quantity: 88, s_ytd: 12, s_order_cnt: 3, s_remote_cnt: 0 } });
    tracker.recordWriteAcked(w);
    drainAndPromote(tracker);

    // A second, later New-Order line legitimately updates the SAME stock row.
    const w2 = stockWrite({ values: { s_quantity: 78, s_ytd: 22, s_order_cnt: 4, s_remote_cnt: 0 } });
    tracker.recordWriteAcked(w2);

    const readBack: ReadBackFn = async (): Promise<ReadBackResult> => ({ found: true, row: rowFor(w2) });
    const result = await tracker.verify(readBack);
    expect(result.lostThisPass).toBe(0);
    expect(tracker.snapshot().meterState).toBe("green");
    expect(tracker.snapshot().writesAcked).toBe(2);
  });

  it("empty tracked set: verify() is a safe no-op, meter stays green", async () => {
    const tracker = new CorrectnessTracker();
    const result = await tracker.verify(async () => ({ found: true, row: {} }));
    expect(result.checked).toBe(0);
    expect(result.lostThisPass).toBe(0);
    expect(tracker.snapshot().meterState).toBe("green");
  });
});

// ----------------------------------------------------------------------------
// snapshot() — trackedKeyCount is the honest scope figure (ROUND 7: a
// COMPLETE count over every distinct key acked this run, not a bounded
// sample size — see ./correctness.ts's header comment, "ROUND 7 — EVICTION
// REMOVED").
// ----------------------------------------------------------------------------

describe("correctness.ts — snapshot(): trackedKeyCount is the honest scope figure", () => {
  it("trackedKeyCount reflects the CURRENT tracked-set size, not the cumulative writesAcked total", () => {
    const tracker = new CorrectnessTracker();
    const writes = [stockWrite({ warehouseId: 1 }), stockWrite({ warehouseId: 2 })];
    for (const w of writes) tracker.recordWriteAcked(w);
    // Not yet promoted: trackedKeyCount is still 0 even though writesAcked is 2.
    expect(tracker.snapshot().trackedKeyCount).toBe(0);
    expect(tracker.snapshot().writesAcked).toBe(2);

    drainAndPromote(tracker);
    expect(tracker.snapshot().trackedKeyCount).toBe(2);
  });

  it("REGRESSION (round 7): trackedKeyCount grows with every DISTINCT key acked — no cap, no eviction, one repeated key never inflates the count", () => {
    const tracker = new CorrectnessTracker();
    for (let i = 0; i < 10; i++) {
      tracker.recordWriteAcked(stockWrite({ warehouseId: i + 1 }));
    }
    drainAndPromote(tracker);
    // Complete over every distinct key acked — not capped at some bounded
    // sample size (the pre-round-7 design would have capped this at
    // DEFAULT_MAX_TRACKED_KEYS, here deliberately fewer than 10 to prove the
    // cap; round 7 has no cap left to prove).
    expect(tracker.snapshot().trackedKeyCount).toBe(10);
    expect(tracker.snapshot().writesAcked).toBe(10);

    // A repeated ack for an ALREADY-tracked key refreshes it in place —
    // still 10 distinct keys, not 11.
    tracker.recordWriteAcked(stockWrite({ warehouseId: 1, values: { s_quantity: 12345, s_ytd: 1, s_order_cnt: 1, s_remote_cnt: 0 } }));
    expect(tracker.snapshot().trackedKeyCount).toBe(10);
    expect(tracker.snapshot().writesAcked).toBe(11);
  });
});

// ----------------------------------------------------------------------------
// Bias mechanism — pickTrackedCandidates prefers keys on migrating vBuckets,
// scoped to ONE catalog at a time.
// ----------------------------------------------------------------------------

describe("correctness.ts — pickTrackedCandidates: migrating-vBucket bias, catalog-scoped", () => {
  const TOTAL_VBUCKETS = 64;
  const CATALOG_ID = "catalog-0";
  const tenantId = "tpcc-w0001";
  const table = "tpcc_stock";

  function candidateFor(itemId: number, catalogShardId: string = CATALOG_ID, ackSeq = itemId): TrackedCandidate {
    const partitionKey = stockKey(1, itemId);
    return {
      write: {
        tenantId,
        table,
        partitionKey,
        warehouseId: 1,
        catalogShardId,
        indexName: "idx_stock_by_item",
        indexValues: { i_id: itemId },
        keyField: "s_key",
        values: { s_quantity: 1 },
      },
      ackSeq,
    };
  }

  it("with no active migration, returns every candidate unchanged (no bias to apply, ROUND 7: no cap either)", () => {
    const vbuckets: VBucketMigrationRow[] = Array.from({ length: TOTAL_VBUCKETS }, (_, v) => ({ vbucket: v, migrationStatus: "none" }));
    const catalog: CatalogVBucketMap = { catalogShardId: CATALOG_ID, totalVBuckets: TOTAL_VBUCKETS, vbuckets };
    const candidates = Array.from({ length: 10 }, (_, i) => candidateFor(i + 1));
    const picked = pickTrackedCandidates(candidates, catalog);
    expect(picked).toHaveLength(10);
    expect(picked).toEqual(candidates);
  });

  it("with an active migration, candidates on a migrating vbucket are strictly preferred (sorted first) over non-migrating ones — ROUND 7: nothing is dropped, only reordered", () => {
    const vbuckets: VBucketMigrationRow[] = Array.from({ length: TOTAL_VBUCKETS }, (_, v) => ({ vbucket: v, migrationStatus: "none" }));

    const candidates = Array.from({ length: 500 }, (_, i) => candidateFor(i + 1));
    const onVbucket0 = candidates.filter((c) => hashKey(`${c.write.tenantId}:${c.write.table}:${c.write.partitionKey}`) % TOTAL_VBUCKETS === 0);
    expect(onVbucket0.length).toBeGreaterThan(0); // sanity: the fixture actually has matches to bias toward
    expect(onVbucket0.length).toBeLessThan(candidates.length); // sanity: not ALL candidates are on vbucket 0

    vbuckets[0] = { vbucket: 0, migrationStatus: "backfilling" };
    const catalog: CatalogVBucketMap = { catalogShardId: CATALOG_ID, totalVBuckets: TOTAL_VBUCKETS, vbuckets };

    const picked = pickTrackedCandidates(candidates, catalog);

    // Nothing dropped — every candidate handed in comes back out (same
    // object references, just reordered).
    expect(picked).toHaveLength(candidates.length);
    for (const c of candidates) expect(picked).toContain(c);
    // The FIRST onVbucket0.length entries are exactly the migrating-vbucket
    // candidates (bias = sort order, not a cap).
    for (const p of picked.slice(0, onVbucket0.length)) {
      expect(hashKey(`${p.write.tenantId}:${p.write.table}:${p.write.partitionKey}`) % TOTAL_VBUCKETS).toBe(0);
    }
    // And every non-migrating candidate is NOT among that leading run.
    for (const p of picked.slice(onVbucket0.length)) {
      expect(hashKey(`${p.write.tenantId}:${p.write.table}:${p.write.partitionKey}`) % TOTAL_VBUCKETS).not.toBe(0);
    }
  });

  it("migratingVBuckets returns exactly the vbuckets whose migrationStatus isn't 'none'", () => {
    const map: VBucketMigrationRow[] = [
      { vbucket: 0, migrationStatus: "none" },
      { vbucket: 1, migrationStatus: "backfilling" },
      { vbucket: 2, migrationStatus: "cutover" },
      { vbucket: 3, migrationStatus: "aborting" },
      { vbucket: 4, migrationStatus: "none" },
    ];
    expect(migratingVBuckets(map)).toEqual(new Set([1, 2, 3]));
  });

  it("REGRESSION (round 7): does NOT drop any candidate even when every one is on a migrating vbucket — there is no cap left to respect", () => {
    const vbuckets: VBucketMigrationRow[] = Array.from({ length: TOTAL_VBUCKETS }, (_, v) => ({ vbucket: v, migrationStatus: "backfilling" }));
    const catalog: CatalogVBucketMap = { catalogShardId: CATALOG_ID, totalVBuckets: TOTAL_VBUCKETS, vbuckets };
    const candidates = Array.from({ length: 20 }, (_, i) => candidateFor(i + 1));
    const picked = pickTrackedCandidates(candidates, catalog);
    expect(picked).toHaveLength(20);
  });

  it("REGRESSION: drops candidates from a DIFFERENT catalog than the one being scored, even if handed in the same array", () => {
    const vbuckets: VBucketMigrationRow[] = Array.from({ length: TOTAL_VBUCKETS }, (_, v) => ({ vbucket: v, migrationStatus: "none" }));
    const catalog: CatalogVBucketMap = { catalogShardId: CATALOG_ID, totalVBuckets: TOTAL_VBUCKETS, vbuckets };
    const inCatalog = candidateFor(1, "catalog-0");
    const crossCatalog = candidateFor(2, "catalog-1");
    const picked = pickTrackedCandidates([inCatalog, crossCatalog], catalog);
    expect(picked).toHaveLength(1);
    expect(picked[0].write.catalogShardId).toBe("catalog-0");
  });

  it("REGRESSION: an entirely cross-catalog candidate pool yields nothing rather than being scored against the wrong map", () => {
    const vbuckets: VBucketMigrationRow[] = Array.from({ length: TOTAL_VBUCKETS }, (_, v) => ({ vbucket: v, migrationStatus: "none" }));
    const catalog: CatalogVBucketMap = { catalogShardId: CATALOG_ID, totalVBuckets: TOTAL_VBUCKETS, vbuckets };
    const candidates = Array.from({ length: 5 }, (_, i) => candidateFor(i + 1, "catalog-99"));
    const picked = pickTrackedCandidates(candidates, catalog);
    expect(picked).toHaveLength(0);
  });

  it("never touches .ackSeq — a candidate's seq survives pickTrackedCandidates unchanged", () => {
    const vbuckets: VBucketMigrationRow[] = Array.from({ length: TOTAL_VBUCKETS }, (_, v) => ({ vbucket: v, migrationStatus: "none" }));
    const catalog: CatalogVBucketMap = { catalogShardId: CATALOG_ID, totalVBuckets: TOTAL_VBUCKETS, vbuckets };
    const candidate = candidateFor(7, CATALOG_ID, 12345);
    const picked = pickTrackedCandidates([candidate], catalog);
    expect(picked[0].ackSeq).toBe(12345);
  });
});

// ----------------------------------------------------------------------------
// trackableWriteFromStockUpdate — only tpcc_stock UPDATEs are trackable.
// ----------------------------------------------------------------------------

describe("correctness.ts — trackableWriteFromStockUpdate", () => {
  it("builds an UnresolvedTrackedWrite from a tpcc_stock update call, recovering (warehouseId, itemId) from the partition key", () => {
    const call: MutateCall = {
      op: "update",
      table: "tpcc_stock",
      partitionKey: stockKey(7, 123),
      values: { s_quantity: 50, s_ytd: 5, s_order_cnt: 1, s_remote_cnt: 0 },
    };
    const write = trackableWriteFromStockUpdate(call);
    expect(write).not.toBeNull();
    expect(write?.warehouseId).toBe(7);
    expect(write?.tenantId).toBe(tenantIdForWarehouse(7));
    expect(write?.indexValues).toEqual({ i_id: 123 });
    expect(write?.keyField).toBe("s_key");
    expect(write?.values).toEqual(call.values);
    // catalogShardId is deliberately absent at this stage — it's resolved
    // later by the caller (see TrackingTxExecutor.observe).
    expect((write as unknown as { catalogShardId?: unknown })?.catalogShardId).toBeUndefined();
  });

  it("returns null for a non-stock table", () => {
    const call: MutateCall = { op: "update", table: "tpcc_customer", partitionKey: "c-0001-01-000001", values: { c_balance: 10 } };
    expect(trackableWriteFromStockUpdate(call)).toBeNull();
  });

  it("returns null for a non-update op on tpcc_stock", () => {
    const call: MutateCall = { op: "insert", table: "tpcc_stock", partitionKey: stockKey(1, 1), values: { s_quantity: 100 } };
    expect(trackableWriteFromStockUpdate(call)).toBeNull();
  });

  it("returns null when values is missing", () => {
    const call: MutateCall = { op: "delete", table: "tpcc_stock", partitionKey: stockKey(1, 1) };
    expect(trackableWriteFromStockUpdate(call)).toBeNull();
  });
});

// ----------------------------------------------------------------------------
// TrackingTxExecutor — transparency: never changes behavior/propagation.
// ----------------------------------------------------------------------------

describe("correctness.ts — TrackingTxExecutor transparency", () => {
  it("propagates a thrown error from the wrapped executor unchanged, and does not record an ack for it", async () => {
    const tracker = new CorrectnessTracker();
    const inner: TxExecutor = {
      mutate: async () => {
        throw new Error("simulated gateway failure");
      },
      tx: async () => ({ committed: true }),
      indexQuery: async () => ({ rows: [] }),
      tableScan: async () => ({ rows: [] }),
    };
    const exec = new TrackingTxExecutor(inner, tracker, resolveToCatalog0);
    await expect(exec.mutate(1, { op: "update", table: "tpcc_stock", partitionKey: stockKey(1, 1), values: { s_quantity: 1 } })).rejects.toThrow(
      "simulated gateway failure",
    );
    expect(tracker.snapshot().writesAcked).toBe(0);
  });

  it("a successful tx() records every mutation in it as a fresh ack (no per-mutation replay signal available)", async () => {
    const tracker = new CorrectnessTracker();
    const inner = fakeExecutor();
    const exec = new TrackingTxExecutor(inner, tracker, resolveToCatalog0);
    await exec.tx(1, [
      { op: "insert", table: "tpcc_order_line", partitionKey: "ol-0001-01-000000001-01", values: { ol_amount: 5 } },
      { op: "update", table: "tpcc_stock", partitionKey: stockKey(1, 1), values: { s_quantity: 90 } },
    ]);
    expect(tracker.snapshot().writesAcked).toBe(2);
    expect(tracker.snapshot().writesRetriedIdempotent).toBe(0);
    // Only the tpcc_stock mutation is verifiable.
    expect(tracker.trackedWrites()).toHaveLength(0); // not promoted yet — still pending
  });

  it("delete calls (e.g. a New-Order marker claim/compensation) are never treated as trackable writes", async () => {
    const tracker = new CorrectnessTracker();
    const inner = fakeExecutor();
    const exec = new TrackingTxExecutor(inner, tracker, resolveToCatalog0);
    await exec.mutate(1, { op: "delete", table: "tpcc_new_order", partitionKey: "no-0001-01-000000001" });
    expect(tracker.snapshot().writesAcked).toBe(1); // still counts as an acked write...
    const drained = tracker.drainPendingCandidates();
    expect(drained).toHaveLength(0); // ...but never becomes a verifiable candidate
  });

  it("REGRESSION: a candidate observed while the catalog resolver can't resolve yet (returns null) is never tracked", async () => {
    const tracker = new CorrectnessTracker();
    const inner = fakeExecutor();
    const exec = new TrackingTxExecutor(inner, tracker, () => null); // e.g. vbucket map not fetched yet
    await exec.mutate(1, { op: "update", table: "tpcc_stock", partitionKey: stockKey(1, 42), values: { s_quantity: 10 } });
    expect(tracker.snapshot().writesAcked).toBe(1); // still a real ack...
    expect(tracker.drainPendingCandidates()).toHaveLength(0); // ...but not trackable until the catalog resolves
  });
});

// ----------------------------------------------------------------------------
// HOLE #2 — gatewayReadBack: a PRECISE primary-key point read-back, not a
// windowed secondary-index scan (design round 3).
// ----------------------------------------------------------------------------

describe("correctness.ts — HOLE #2: gatewayReadBack is a precise point read-back by primary key", () => {
  it("found: true with the row when sqlSelect returns a match", async () => {
    const write = stockWrite();
    const reader = fakeSqlPointReader(new Map([[write.partitionKey, rowFor(write)]]));
    const readBack = gatewayReadBack(reader);
    const result = await readBack(write);
    expect(result.found).toBe(true);
    expect(result.row?.s_quantity).toBe(write.values.s_quantity);
  });

  it("found: false when sqlSelect returns no rows (the row is genuinely, provably missing)", async () => {
    const write = stockWrite();
    const reader = fakeSqlPointReader(); // empty
    const readBack = gatewayReadBack(reader);
    const result = await readBack(write);
    expect(result.found).toBe(false);
    expect(result.row).toBeUndefined();
  });

  it("scopes the point SELECT by the tracked write's own table/tenantId/partitionKey and queries by the primary-key column — a full-identity lookup, not an index scan", async () => {
    const write = stockWrite();
    let captured: { table: string; tenantId: string; partitionKey: string; sql: string; params: unknown[] } | undefined;
    const reader: SqlPointReader = {
      sqlSelect: async (args) => {
        captured = args;
        return { rows: [rowFor(write)] };
      },
    };
    await gatewayReadBack(reader)(write);
    expect(captured?.table).toBe(write.table);
    expect(captured?.tenantId).toBe(write.tenantId);
    expect(captured?.partitionKey).toBe(write.partitionKey);
    expect(captured?.params).toEqual([write.partitionKey]);
    expect(captured?.sql).toContain(write.keyField);
    expect(captured?.sql).toContain(write.table);
  });

  it("REGRESSION: gatewayReadBack finds the exact row even when the backend (buggy or malicious) returns MULTIPLE rows sharing the item id, and is not fooled by a same-field-value duplicate", async () => {
    const write = stockWrite({ warehouseId: 1 }); // s-0001-000042
    // A DIFFERENT warehouse's row, sharing i_id=42 (what idx_stock_by_item
    // would have returned under the OLD design) AND coincidentally sharing
    // the mutated field's value — exactly the false-GREEN scenario a
    // duplicate-accepting index scan was exposed to.
    const duplicateWrongRow = { s_key: stockKey(2, 42), s_quantity: write.values.s_quantity };
    const ownRow = rowFor(write);
    const reader: SqlPointReader = { sqlSelect: async () => ({ rows: [duplicateWrongRow, ownRow] }) };
    const result = await gatewayReadBack(reader)(write);
    expect(result.found).toBe(true);
    expect(result.row?.[write.keyField]).toBe(write.partitionKey); // the RIGHT row, not the first one
    expect(result.row?.s_quantity).toBe(write.values.s_quantity);
  });

  it("REGRESSION: if NONE of the returned rows match this write's own identity, reports missing rather than accepting a look-alike", async () => {
    const write = stockWrite({ warehouseId: 1 });
    const duplicateWrongRow = { s_key: stockKey(2, 42), s_quantity: write.values.s_quantity };
    const reader: SqlPointReader = { sqlSelect: async () => ({ rows: [duplicateWrongRow] }) }; // OUR row is genuinely absent
    const result = await gatewayReadBack(reader)(write);
    expect(result.found).toBe(false);
  });

  it("REGRESSION (end-to-end through CorrectnessTracker): a backend that (incorrectly) returns extra rows alongside the tracked one must NOT false-red the meter", async () => {
    const tracker = new CorrectnessTracker();
    const write = stockWrite({ warehouseId: 1 });
    tracker.recordWriteAcked(write);
    drainAndPromote(tracker);

    const duplicateWrongRow = { s_key: stockKey(2, 42), s_quantity: write.values.s_quantity };
    const reader: SqlPointReader = { sqlSelect: async () => ({ rows: [duplicateWrongRow, rowFor(write)] }) };
    const result = await tracker.verify(gatewayReadBack(reader));

    expect(result.lostThisPass).toBe(0); // the tracked write's OWN row genuinely matches — must stay green
    expect(tracker.snapshot().meterState).toBe("green");
  });
});

// ----------------------------------------------------------------------------
// HOLE #3 — VERIFIED EPOCH (design round 3): a monotonic epoch bumps on any
// tracked-set change; snapshot().verified is true only when the last
// verify() pass covered the CURRENT epoch, checked >=1 key, and lost === 0.
// ----------------------------------------------------------------------------

describe("correctness.ts — HOLE #3: verified epoch — a stale claim never renders as reassuring green", () => {
  it("verified starts false: verify() has never run this tracker's lifetime", () => {
    const tracker = new CorrectnessTracker();
    expect(tracker.snapshot().verified).toBe(false);
    expect(tracker.snapshot().lastVerifyChecked).toBeNull();
  });

  it("REGRESSION: trackedKeyCount can be > 0 while verified is still false — a key can be tracked before the first verify() pass ever runs", () => {
    const tracker = new CorrectnessTracker();
    tracker.recordWriteAcked(stockWrite());
    drainAndPromote(tracker);
    expect(tracker.snapshot().trackedKeyCount).toBe(1);
    expect(tracker.snapshot().verified).toBe(false);
  });

  it("a clean verify() pass over a non-empty tracked set makes verified true", async () => {
    const tracker = new CorrectnessTracker();
    const write = stockWrite();
    tracker.recordWriteAcked(write);
    drainAndPromote(tracker);
    const readBack: ReadBackFn = async () => ({ found: true, row: rowFor(write) });
    await tracker.verify(readBack);
    expect(tracker.snapshot().verified).toBe(true);
  });

  it("an empty-tracked-set verify() pass leaves verified false (0 keys checked doesn't back a claim)", async () => {
    const tracker = new CorrectnessTracker();
    await tracker.verify(async () => ({ found: true, row: {} }));
    expect(tracker.snapshot().verified).toBe(false);
    expect(tracker.snapshot().lastVerifyChecked).toBe(0);
  });

  it("REGRESSION (THE required fix): a tracked-set change AFTER verify() immediately un-verifies the meter, even though lost stays 0", async () => {
    const tracker = new CorrectnessTracker();
    const write = stockWrite();
    tracker.recordWriteAcked(write);
    drainAndPromote(tracker);
    const readBack: ReadBackFn = async () => ({ found: true, row: rowFor(write) });
    await tracker.verify(readBack);
    expect(tracker.snapshot().verified).toBe(true);

    // A NEW write lands on the SAME tracked key (a legitimate later update)
    // — its new value hasn't been read back and compared by any verify()
    // pass yet.
    const updated = stockWrite({ values: { s_quantity: 999, s_ytd: 1, s_order_cnt: 1, s_remote_cnt: 0 } });
    tracker.recordWriteAcked(updated);

    const snap = tracker.snapshot();
    expect(snap.lost).toBe(0); // nothing has been PROVEN lost...
    expect(snap.verified).toBe(false); // ...but the claim is stale: this update was never actually checked

    // Re-verifying against the NEW value restores verified.
    const readBack2: ReadBackFn = async () => ({ found: true, row: rowFor(updated) });
    await tracker.verify(readBack2);
    expect(tracker.snapshot().verified).toBe(true);
  });

  it("REGRESSION: promoting a NEW key into the tracked sample after verify() also un-verifies the meter (a newly-added, never-checked key must not ride along on a stale green)", async () => {
    const tracker = new CorrectnessTracker();
    const w1 = stockWrite({ warehouseId: 1 });
    tracker.recordWriteAcked(w1);
    drainAndPromote(tracker);
    await tracker.verify(async () => ({ found: true, row: rowFor(w1) }));
    expect(tracker.snapshot().verified).toBe(true);

    const w2 = stockWrite({ warehouseId: 2 });
    tracker.recordWriteAcked(w2);
    drainAndPromote(tracker); // promotes w2 into the tracked sample
    expect(tracker.snapshot().verified).toBe(false);
  });

  it("a verify() pass that finds a genuine loss reports verified: false (lost !== 0), even though the epoch matches and a key was checked", async () => {
    const tracker = new CorrectnessTracker();
    const write = stockWrite();
    tracker.recordWriteAcked(write);
    drainAndPromote(tracker);
    await tracker.verify(async () => ({ found: false })); // genuinely missing
    const snap = tracker.snapshot();
    expect(snap.lost).toBe(1);
    expect(snap.meterState).toBe("red");
    expect(snap.verified).toBe(false);
  });

  it("a disproven idempotent-replay claim (which never touches the tracked set/epoch) also flips verified to false on the next snapshot", async () => {
    const tracker = new CorrectnessTracker();
    const write = stockWrite();
    tracker.recordWriteAcked(write);
    drainAndPromote(tracker);
    await tracker.verify(async () => ({ found: true, row: rowFor(write) }));
    expect(tracker.snapshot().verified).toBe(true);

    // A completely separate key's replay claim is disproven — doesn't touch
    // `tracked`/epoch at all, but DOES bump `lost`, which verified's own
    // formula checks independently of the epoch match.
    const otherWrite = stockWrite({ warehouseId: 99 });
    await tracker.recordIdempotentReplay(otherWrite, async () => ({ found: false }));
    expect(tracker.snapshot().lost).toBe(1);
    expect(tracker.snapshot().verified).toBe(false);
  });
});

// ----------------------------------------------------------------------------
// ROUND 5 (Codex re-review of the round-4 redesign) — closing the four
// remaining holes: a high-water mark for finding #1/#6 (stale drained
// candidate survives pending eviction), ackSeq validation for finding #2
// (fabricated ackSeq beats real acks), a race-safe verify() for finding #3
// (false-RED racing a legitimate refresh), and notifyClusterChanged() for
// finding #4 (verified survives a reshard that didn't touch the tracked
// set). See correctness.ts's header comment, "design round 4", for the full
// writeup of each fix.
// ----------------------------------------------------------------------------

describe("correctness.ts — ROUND 5 finding #1/#6: a high-water mark stops a stale drained candidate from ever winning", () => {
  it("v1 acked+drained (held by caller) -> v2 acked for the SAME key -> v2 drained+promoted normally elsewhere -> promoting the STALE drained v1 afterward is refused, not silently installed", () => {
    // ROUND 7 note: this race no longer needs a bounded pending buffer to
    // manufacture — with eviction gone, the SAME hole (a caller holding a
    // stale drained candidate out of order) is reproduced directly: v2
    // reaches `tracked` through the normal drain/promote pipeline, and v1
    // (drained earlier, held separately) is promoted afterward.
    const tracker = new CorrectnessTracker();
    const v1 = stockWrite({ warehouseId: 1, values: { s_quantity: 1 } });
    const v2 = stockWrite({ warehouseId: 1, values: { s_quantity: 2 } }); // SAME row as v1, strictly newer ack

    tracker.recordWriteAcked(v1);
    const drainedV1 = tracker.drainPendingCandidates(); // caller now holds [{write: v1, ackSeq}]; pendingCandidates is empty
    expect(drainedV1).toHaveLength(1);

    tracker.recordWriteAcked(v2); // same key as v1, not yet tracked -> lands in pendingCandidates
    drainAndPromote(tracker); // v2 reaches `tracked` normally — hwm[key] is now v2's ackSeq
    expect(tracker.trackedWrites().find((w) => w.warehouseId === 1)?.values).toEqual(v2.values);

    // The caller now (out of order) promotes its STALE drained v1 snapshot.
    // `pendingCandidates` has no memory of v1 or v2 left (both already
    // drained). With `tracked` never evicting (round 7), the plain "never
    // regress" comparison against v2's already-tracked, higher-ackSeq entry
    // is ENOUGH on its own to refuse v1 here — but the high-water-mark check
    // refuses it too (v1's ackSeq !== hwm[key], which is now v2's), so this
    // stays green either way. See the SECONDARY FIX 1 describe block below
    // for the narrower case where `highWaterMark` is the ONLY thing standing
    // between a stale/mismatched candidate and a silent overwrite.
    tracker.promoteToTracked(drainedV1);
    expect(tracker.trackedWrites().find((w) => w.warehouseId === 1)?.values).toEqual(v2.values);
  });

  it("the SAME stale-drain race, but a later, fresh re-ack for the SAME row DOES get tracked — the high-water mark blocks the stale write, it doesn't just blackhole the key forever", () => {
    const tracker = new CorrectnessTracker();
    const v1 = stockWrite({ warehouseId: 1, values: { s_quantity: 1 } });
    const v2 = stockWrite({ warehouseId: 1, values: { s_quantity: 2 } });

    tracker.recordWriteAcked(v1);
    const drainedV1 = tracker.drainPendingCandidates();
    tracker.recordWriteAcked(v2);
    drainAndPromote(tracker); // v2 tracked normally

    tracker.promoteToTracked(drainedV1); // stale v1 refused (see previous test)
    expect(tracker.trackedWrites().find((w) => w.warehouseId === 1)?.values).toEqual(v2.values);

    // A later, fresh ack for the SAME row (e.g. the load-driver's own
    // re-verification traffic) is tracked normally — the high-water mark
    // only refuses candidates that DON'T match it, never a genuinely fresh
    // one that advances it.
    const v3 = stockWrite({ warehouseId: 1, values: { s_quantity: 3 } });
    tracker.recordWriteAcked(v3); // already tracked -> refreshed in place directly
    expect(tracker.trackedWrites().find((w) => w.warehouseId === 1)?.values).toEqual(v3.values);
  });
});

describe("correctness.ts — SECONDARY FIX 1 (round 7, Codex round-6 finding): promoteToTracked refuses a genuinely tracker-minted ackSeq that was minted for a DIFFERENT key", () => {
  it("REGRESSION: a candidate for key A carrying key B's real, in-range, tracker-minted ackSeq is refused — not merely a candidate BEHIND its own key's high-water mark", () => {
    const tracker = new CorrectnessTracker();
    const keyA = stockWrite({ warehouseId: 1, values: { s_quantity: 1 } });
    const keyB = stockWrite({ warehouseId: 2, values: { s_quantity: 1 } });
    tracker.recordWriteAcked(keyA); // ackSeq 0, hwm[A] = 0
    tracker.recordWriteAcked(keyB); // ackSeq 1, hwm[B] = 1
    drainAndPromote(tracker);
    expect(tracker.trackedWrites().find((w) => w.warehouseId === 1)?.values).toEqual(keyA.values);
    expect(tracker.trackedWrites().find((w) => w.warehouseId === 2)?.values).toEqual(keyB.values);

    // A hostile/buggy candidate FOR ROW A, but carrying row B's genuinely
    // tracker-minted ackSeq (1) — a real, in-range, integer value, so it
    // passes the fabrication/range check untouched. It was never actually
    // minted for row A's identity at all.
    const borrowed: TrackedCandidate = { write: stockWrite({ warehouseId: 1, values: { s_quantity: 99999 } }), ackSeq: 1 };
    tracker.promoteToTracked([borrowed]);

    // REVERT-TO-CONFIRM-RED (for this being a false-GREEN-style API-misuse
    // hole, not a loss-detection one): reverting promoteToTracked's guard
    // back to the round-5 shape (`toPromote.ackSeq < hwm` -> refuse, ELSE
    // accept) makes this assertion fail — 1 is NOT < hwm[A] (0), so the old
    // guard would let it through, and it would then win the "never regress"
    // comparison against A's real ackSeq-0 entry (0 >= 1 is false), silently
    // overwriting A's real value with the borrowed candidate's corrupted
    // one. The tightened `toPromote.ackSeq !== hwm` guard refuses it:
    // 1 !== hwm[A] (0).
    expect(tracker.trackedWrites().find((w) => w.warehouseId === 1)?.values).toEqual(keyA.values);
  });
});

describe("correctness.ts — CODEX ROUND 7 finding: promoteToTracked refuses a candidate for a key with NO high-water mark at all, not just a mismatched one", () => {
  it("REGRESSION: a candidate for a NEVER-acked key, carrying some OTHER key's real, in-range, tracker-minted ackSeq, is refused — hwm===undefined must never mean 'anything goes'", () => {
    const tracker = new CorrectnessTracker();
    const keyB = stockWrite({ warehouseId: 2, values: { s_quantity: 1 } });
    tracker.recordWriteAcked(keyB); // ackSeq 0, hwm[B] = 0 — the ONLY key this tracker instance has ever acked
    drainAndPromote(tracker);
    expect(tracker.trackedWrites()).toHaveLength(1);

    // A fabricated candidate for key A — a row this tracker has NEVER acked
    // a write for (highWaterMark has NO entry for A's id at all) — carrying
    // key B's real, in-range, genuinely tracker-minted ackSeq (0). It passes
    // the fabrication/range check (2) untouched: 0 is a real, non-negative
    // integer strictly less than nextSeq (1).
    const keyA = stockWrite({ warehouseId: 1, values: { s_quantity: 999999 } });
    const fabricated: TrackedCandidate = { write: keyA, ackSeq: 0 };
    tracker.promoteToTracked([fabricated]);

    // REVERT-TO-CONFIRM-RED (for this being a false-GREEN-style API-misuse
    // hole, not a loss-detection one): reverting promoteToTracked's guard
    // back to the round-7-original shape (`hwm !== undefined &&
    // ackSeq !== hwm` -> refuse) makes this assertion fail. Under that
    // guard, `hwm.get(idA)` is `undefined` (key A has never been acked), so
    // the `&&` short-circuits to false and the candidate is NOT refused
    // here — it falls through to the "never regress" comparison, where
    // `existing` (tracked.get(idA)) is ALSO undefined (key A has never been
    // tracked either), so that check passes trivially too, and the
    // fabricated candidate gets tracked unconditionally: key A would
    // incorrectly appear in trackedWrites() carrying the fabricated
    // s_quantity: 999999 value, and the next verify() pass would read back
    // key A's REAL row (which this tracker never wrote and has no
    // relationship to 999999) and find it mismatched — a false RED on a row
    // this tracker never legitimately observed a write for (Codex round 7's
    // exact finding). The tightened `hwm === undefined || ackSeq !== hwm`
    // guard refuses this outright, on its first disjunct alone.
    expect(tracker.trackedWrites()).toHaveLength(1);
    expect(tracker.trackedWrites().find((w) => w.warehouseId === 1)).toBeUndefined();
    expect(tracker.trackedWrites().find((w) => w.warehouseId === 2)?.values).toEqual(keyB.values);
  });

  it("REVERT-TO-CONFIRM-RED, end to end: a verify() pass never goes RED over a fabricated/borrowed candidate, because it was never tracked in the first place", async () => {
    const tracker = new CorrectnessTracker();
    const keyB = stockWrite({ warehouseId: 2 });
    tracker.recordWriteAcked(keyB);
    drainAndPromote(tracker);

    const keyA = stockWrite({ warehouseId: 1, values: { s_quantity: 999999 } });
    tracker.promoteToTracked([{ write: keyA, ackSeq: 0 }]); // fabricated/borrowed — refused by the guard above

    // A readBack that would report key A's row as flatly missing if verify()
    // ever actually asked about it — if the guard regressed and key A got
    // tracked, this readBack proves it: `checked` would be 2 (not 1) and
    // `lostThisPass` would be 1 (not 0), turning the meter RED for a row
    // this tracker never legitimately acked.
    const readBack: ReadBackFn = async (write) => {
      if (write.warehouseId === 1) return { found: false }; // key A: must never even be asked about
      return { found: true, row: { s_key: write.partitionKey, ...write.values } };
    };
    const result = await tracker.verify(readBack);
    expect(result.checked).toBe(1); // only key B — the one genuinely tracked key
    expect(result.lostThisPass).toBe(0);
    expect(tracker.snapshot().lost).toBe(0);
    expect(tracker.snapshot().meterState).toBe("green");
  });
});

describe("correctness.ts — ROUND 5 finding #2: promoteToTracked refuses a fabricated (non-tracker-minted) ackSeq", () => {
  it("Infinity / NaN / MAX_SAFE_INTEGER / a non-integer / a negative ackSeq are all refused — never tracked, never beat a real entry", () => {
    const tracker = new CorrectnessTracker();
    const real = stockWrite({ values: { s_quantity: 5 } });
    tracker.recordWriteAcked(real);
    drainAndPromote(tracker); // real entry tracked with a genuine, tracker-minted ackSeq (0)
    expect(tracker.trackedWrites()).toHaveLength(1);

    const fabricatedAckSeqs = [Infinity, -Infinity, NaN, Number.MAX_SAFE_INTEGER, 1.5, -1];
    for (const ackSeq of fabricatedAckSeqs) {
      const fabricated: TrackedCandidate = { write: stockWrite({ values: { s_quantity: 999 } }), ackSeq };
      tracker.promoteToTracked([fabricated]);
    }
    // None of the fabricated candidates displaced the real one.
    expect(tracker.trackedWrites()).toHaveLength(1);
    expect(tracker.trackedWrites()[0].values).toEqual(real.values);
  });

  it("a fabricated ackSeq for a BRAND NEW key (never acked at all via recordWriteAcked) is refused outright, not tracked", () => {
    const tracker = new CorrectnessTracker();
    // nextSeq is still 0 (nothing has ever been acked) -> ANY ackSeq,
    // including 0, is >= nextSeq and therefore cannot be a real, already
    // tracker-minted value.
    const fabricated: TrackedCandidate = { write: stockWrite(), ackSeq: 0 };
    tracker.promoteToTracked([fabricated]);
    expect(tracker.trackedWrites()).toHaveLength(0);
  });

  it("a genuinely-minted ackSeq (obtained legitimately via recordWriteAcked/drainPendingCandidates) is still accepted normally — the guard only rejects fabrication, not real traffic", () => {
    const tracker = new CorrectnessTracker();
    const write = stockWrite();
    tracker.recordWriteAcked(write);
    const drained = tracker.drainPendingCandidates();
    tracker.promoteToTracked(drained);
    expect(tracker.trackedWrites()).toHaveLength(1);
    expect(tracker.trackedWrites()[0].values).toEqual(write.values);
  });
});

describe("correctness.ts — ROUND 5 finding #3: verify() does not false-RED a key that was legitimately refreshed while its read-back was in flight", () => {
  it("a same-key refresh landing mid-read-back is not counted as a spurious loss", async () => {
    const tracker = new CorrectnessTracker();
    const v1 = stockWrite({ values: { s_quantity: 1 } });
    tracker.recordWriteAcked(v1);
    drainAndPromote(tracker);

    const v2 = stockWrite({ values: { s_quantity: 2 } });
    let refreshed = false;
    const readBack: ReadBackFn = async (write) => {
      if (!refreshed) {
        refreshed = true;
        // A legitimate NEWER ack for this exact row lands WHILE this
        // read-back is "in flight" (simulated synchronously here — a real
        // gateway race would have this happen during the actual network
        // round trip). This refreshes `tracked` in place and bumps epoch.
        tracker.recordWriteAcked(v2);
      }
      // The read-back reflects the row's ACTUAL current state (v2's value)
      // — correct behavior for a real gateway — but that's stale relative
      // to `write`/`entry`, which verify() snapshotted (as v1) before this
      // read-back started.
      return { found: true, row: rowFor(v2) };
    };

    const result = await tracker.verify(readBack);
    expect(result.lostThisPass).toBe(0);
    expect(tracker.snapshot().lost).toBe(0);
    expect(tracker.snapshot().meterState).toBe("green");
    // Correctly reflects that this pass did NOT genuinely cover the
    // refreshed key (recordWriteAcked's own epoch bump already un-verifies
    // this — HOLE #3 from design round 3).
    expect(tracker.snapshot().verified).toBe(false);
  });

  it("a GENUINE loss (no concurrent refresh) still counts — this fix only suppresses the specific refresh-race false positive, never a real one", async () => {
    const tracker = new CorrectnessTracker();
    const write = stockWrite();
    tracker.recordWriteAcked(write);
    drainAndPromote(tracker);

    const readBack: ReadBackFn = async () => ({ found: false }); // genuinely missing, nothing refreshed it
    const result = await tracker.verify(readBack);
    expect(result.lostThisPass).toBe(1);
    expect(tracker.snapshot().lost).toBe(1);
    expect(tracker.snapshot().meterState).toBe("red");
  });

  // ROUND 6's "a key evicted for CAPACITY (no newer ack) while its own
  // read-back was in flight IS a genuine loss" scenario — this file's
  // then-residual false-green — is RETIRED here: round 7 removed eviction
  // entirely, so "evicted for capacity, no newer ack, read-back in flight"
  // is no longer a reachable state at all (there is no eviction to race).
  // The stronger claim this scenario used to guard — that a superseding
  // write can't be silently dropped by capacity bookkeeping before it's
  // ever verified — is now proven directly by the top-level "ROUND 7 —
  // EVICTION REMOVED" describe block above, which floods hundreds of other
  // keys through the tracker (far more than the old default maxTracked)
  // and confirms the superseding value is STILL tracked and its loss is
  // STILL caught, with no eviction-mid-read race needed to construct the
  // scenario at all.

  it("ROUND 7: a genuine newer ack superseding a key mid-read-back is still correctly skipped (not a loss), even amid a flood of unrelated keys that (pre-round-7) would also have evicted it", async () => {
    // The companion case to the primary round-7 regression above: this time
    // a REAL newer ack for the SAME row (K) lands before the flood of other
    // keys, so highWaterMark[K] genuinely advances past what this pass
    // snapshotted, and the mismatch must stay suppressed exactly like the
    // plain same-key-refresh case (ROUND 5 finding #3) — round 7 doesn't
    // change this outcome, it just removes the (now-impossible) alternative
    // where K could ALSO have been evicted from `tracked` entirely.
    const tracker = new CorrectnessTracker();
    const v1 = stockWrite({ warehouseId: 1, values: { s_quantity: 1 } }); // K @ ackSeq 0
    tracker.recordWriteAcked(v1);
    drainAndPromote(tracker);
    expect(tracker.trackedWrites()).toHaveLength(1);

    let step = 0;
    const readBack: ReadBackFn = async () => {
      if (step === 0) {
        step = 1;
        // A genuine NEWER ack for the SAME row K lands mid-read-back — K is
        // already tracked, so this refreshes it in place and bumps
        // highWaterMark[K] past what this pass snapshotted.
        const v2 = stockWrite({ warehouseId: 1, values: { s_quantity: 2 } });
        tracker.recordWriteAcked(v2);
        // A flood of OTHER keys is acked and promoted too — under the old
        // (pre-round-7) design this could have evicted K from `tracked`
        // entirely; with eviction gone, K simply stays in `tracked` (at its
        // newest value) alongside all of them.
        for (let i = 0; i < 500; i++) {
          tracker.recordWriteAcked(stockWrite({ warehouseId: 1000 + i }));
        }
        drainAndPromote(tracker);
      }
      // Even a (buggy/adversarial) missing read-back for K's ORIGINAL
      // snapshot (v1) must not count — a real newer value (v2) superseded
      // it.
      return { found: false };
    };

    const result = await tracker.verify(readBack);
    expect(result.lostThisPass).toBe(0);
    expect(tracker.snapshot().lost).toBe(0);
    expect(tracker.snapshot().meterState).toBe("green");
  });
});

describe("correctness.ts — ROUND 5 finding #6: lastVerifyChecked reflects what THIS PASS actually read back, not tracked.size sampled after the loop", () => {
  it("a promotion that lands mid-pass (after this pass already snapshotted its keys) is not counted in checked/lastVerifyChecked", async () => {
    const tracker = new CorrectnessTracker();
    const w1 = stockWrite({ warehouseId: 1 });
    const w2 = stockWrite({ warehouseId: 2 });
    tracker.recordWriteAcked(w1);
    tracker.recordWriteAcked(w2);
    drainAndPromote(tracker);
    expect(tracker.trackedWrites()).toHaveLength(2);

    let calls = 0;
    const readBack: ReadBackFn = async (write) => {
      calls += 1;
      if (calls === 1) {
        // While the FIRST key's read-back is in flight, a brand-new key is
        // acked and promoted — this verify() pass never actually reads
        // THAT key back, so it must not inflate `checked`.
        const w3 = stockWrite({ warehouseId: 3 });
        tracker.recordWriteAcked(w3);
        drainAndPromote(tracker);
      }
      return { found: true, row: rowFor(write) };
    };

    const result = await tracker.verify(readBack);
    expect(result.checked).toBe(2); // exactly the 2 keys this pass snapshotted and actually read back
    expect(tracker.snapshot().lastVerifyChecked).toBe(2);
    expect(tracker.trackedWrites()).toHaveLength(3); // the new key IS tracked now...
    expect(tracker.snapshot().verified).toBe(false); // ...but this pass never verified it
  });
});

describe("correctness.ts — ROUND 5 finding #4: notifyClusterChanged() un-verifies the meter across an external event the tracked set never observed", () => {
  it("bumps verified to false even though nothing about the tracked set/values changed, and a fresh verify() pass restores it", async () => {
    const tracker = new CorrectnessTracker();
    const write = stockWrite();
    tracker.recordWriteAcked(write);
    drainAndPromote(tracker);
    const readBack: ReadBackFn = async () => ({ found: true, row: rowFor(write) });
    await tracker.verify(readBack);
    expect(tracker.snapshot().verified).toBe(true);

    // The load-driver observed migration activity in the live vbucket map
    // (see load-driver.ts's runTick wiring) and calls this — nothing about
    // CorrectnessTracker's OWN bookkeeping changed.
    tracker.notifyClusterChanged();
    const snap = tracker.snapshot();
    expect(snap.verified).toBe(false);
    expect(snap.lost).toBe(0); // still nothing PROVEN lost...
    expect(snap.meterState).toBe("green"); // ...meterState is a pure function of `lost`, unaffected by verified

    // A fresh verify() pass, with no further invalidation, re-earns
    // verified: true — never permanently wedged.
    await tracker.verify(readBack);
    expect(tracker.snapshot().verified).toBe(true);
  });

  it("repeated invalidation (simulating an ongoing multi-tick reshard) keeps verified false across every tick it's called, not just once", async () => {
    const tracker = new CorrectnessTracker();
    const write = stockWrite();
    tracker.recordWriteAcked(write);
    drainAndPromote(tracker);
    const readBack: ReadBackFn = async () => ({ found: true, row: rowFor(write) });

    await tracker.verify(readBack);
    expect(tracker.snapshot().verified).toBe(true);

    for (let tick = 0; tick < 5; tick++) {
      tracker.notifyClusterChanged();
      expect(tracker.snapshot().verified).toBe(false);
    }

    // The (simulated) reshard finishes; no further invalidation -> the next
    // verify() pass over the still-consistent post-reshard state restores
    // verified.
    await tracker.verify(readBack);
    expect(tracker.snapshot().verified).toBe(true);
  });

  it("RED still wins: a genuine loss during an active invalidation window still reports meterState red, even though verified is also false", async () => {
    const tracker = new CorrectnessTracker();
    const write = stockWrite();
    tracker.recordWriteAcked(write);
    drainAndPromote(tracker);
    tracker.notifyClusterChanged(); // reshard in progress

    await tracker.verify(async () => ({ found: false })); // genuinely missing
    const snap = tracker.snapshot();
    expect(snap.lost).toBe(1);
    expect(snap.meterState).toBe("red");
    expect(snap.verified).toBe(false);
  });
});

// ----------------------------------------------------------------------------
// emptyCorrectnessCounters / meterStateFor — small pure helpers.
// ----------------------------------------------------------------------------

describe("correctness.ts — small helpers", () => {
  it("emptyCorrectnessCounters starts everything at zero, meter green", () => {
    const counters = emptyCorrectnessCounters();
    expect(counters).toEqual({ writesAcked: 0, writesRetriedIdempotent: 0, txAbortedExpected: 0, lost: 0 });
    expect(meterStateFor(counters)).toBe("green");
  });

  it("meterStateFor is red iff lost > 0, regardless of every other counter", () => {
    expect(meterStateFor({ writesAcked: 10_000, writesRetriedIdempotent: 500, txAbortedExpected: 300, lost: 0 })).toBe("green");
    expect(meterStateFor({ writesAcked: 0, writesRetriedIdempotent: 0, txAbortedExpected: 0, lost: 1 })).toBe("red");
  });
});
