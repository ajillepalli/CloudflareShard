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
  type ReadBackFn,
  type ReadBackResult,
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
    indexName: "idx_stock_by_item",
    indexValues: { i_id: itemId },
    values: { s_quantity: 88, s_ytd: 12, s_order_cnt: 3, s_remote_cnt: 0 },
    ...overrides,
  };
}

/** A fake TxExecutor whose mutate() always succeeds with the given
 * rowsAffected/duplicated shape — enough to exercise TrackingTxExecutor
 * without any network. indexQuery/tableScan are wired to a caller-supplied
 * in-memory row table for gatewayReadBack tests. */
function fakeExecutor(opts?: {
  mutateResult?: MutateResult;
  rowsByIndexValue?: Map<string, Record<string, unknown>>;
}): TxExecutor {
  const rows = opts?.rowsByIndexValue ?? new Map<string, Record<string, unknown>>();
  return {
    mutate: async (_warehouseId: number, _call: MutateCall): Promise<MutateResult> => opts?.mutateResult ?? { rowsAffected: 1 },
    tx: async () => ({ committed: true }),
    indexQuery: async (_warehouseId: number, _table: string, _indexName: string, values: Record<string, unknown>): Promise<QueryResult> => {
      const key = JSON.stringify(values);
      const row = rows.get(key);
      return { rows: row ? [row] : [] };
    },
    tableScan: async (): Promise<QueryResult> => ({ rows: [] }),
  };
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
    tracker.promoteToTracked(tracker.drainPendingCandidates());
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
    tracker.promoteToTracked(tracker.drainPendingCandidates());

    // A stale value landed instead of the acked one (e.g. a migration
    // cutover that silently reverted to a pre-migration snapshot).
    const staleReadBack: ReadBackFn = async (): Promise<ReadBackResult> => ({
      found: true,
      row: { s_quantity: 91, s_ytd: 12, s_order_cnt: 3, s_remote_cnt: 0 }, // s_quantity wrong
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
    tracker.promoteToTracked(tracker.drainPendingCandidates());

    const readBack: ReadBackFn = async (write: TrackedWrite): Promise<ReadBackResult> => {
      if (write.warehouseId === 1) return { found: true, row: write.values };
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
// CRITICAL TEST 2 — idempotent replay increments writesRetriedIdempotent,
// NOT lost.
// ----------------------------------------------------------------------------

describe("correctness.ts — CRITICAL: idempotent replay is not a loss", () => {
  it("recordIdempotentReplay increments writesRetriedIdempotent only", () => {
    const tracker = new CorrectnessTracker();
    tracker.recordIdempotentReplay();
    tracker.recordIdempotentReplay();
    const snap = tracker.snapshot();
    expect(snap.writesRetriedIdempotent).toBe(2);
    expect(snap.writesAcked).toBe(0);
    expect(snap.lost).toBe(0);
    expect(snap.meterState).toBe("green");
  });

  it("TrackingTxExecutor classifies a `duplicated: true` mutate result as a replay, not a fresh ack", async () => {
    const tracker = new CorrectnessTracker();
    const inner = fakeExecutor({ mutateResult: { rowsAffected: 1, duplicated: true } });
    const exec = new TrackingTxExecutor(inner, tracker);

    await exec.mutate(1, { op: "update", table: "tpcc_stock", partitionKey: stockKey(1, 42), values: { s_quantity: 10 } });

    const snap = tracker.snapshot();
    expect(snap.writesRetriedIdempotent).toBe(1);
    expect(snap.writesAcked).toBe(0);
    expect(snap.lost).toBe(0);
  });

  it("a fresh (non-duplicated) mutate result increments writesAcked, not writesRetriedIdempotent", async () => {
    const tracker = new CorrectnessTracker();
    const inner = fakeExecutor({ mutateResult: { rowsAffected: 1 } });
    const exec = new TrackingTxExecutor(inner, tracker);

    await exec.mutate(1, { op: "update", table: "tpcc_stock", partitionKey: stockKey(1, 42), values: { s_quantity: 10 } });

    const snap = tracker.snapshot();
    expect(snap.writesAcked).toBe(1);
    expect(snap.writesRetriedIdempotent).toBe(0);
  });
});

// ----------------------------------------------------------------------------
// CRITICAL TEST 3 — a known TPC-C abort increments txAbortedExpected, NOT
// lost.
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
});

// ----------------------------------------------------------------------------
// CRITICAL TEST 4 — all-consistent reads keep lost == 0 (GREEN).
// ----------------------------------------------------------------------------

describe("correctness.ts — CRITICAL: consistent reads stay GREEN", () => {
  it("verify() against matching read-backs never increments lost", async () => {
    const tracker = new CorrectnessTracker();
    const writes = [stockWrite({ warehouseId: 1 }), stockWrite({ warehouseId: 2 }), stockWrite({ warehouseId: 3 })];
    for (const w of writes) tracker.recordWriteAcked(w);
    tracker.promoteToTracked(tracker.drainPendingCandidates());

    const readBack: ReadBackFn = async (write: TrackedWrite): Promise<ReadBackResult> => ({ found: true, row: { ...write.values } });
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
    tracker.promoteToTracked(tracker.drainPendingCandidates());

    const readBack: ReadBackFn = async (): Promise<ReadBackResult> => ({
      found: true,
      row: { ...write.values, s_key: stockKey(1, 42), extra_column_untouched_by_this_write: "whatever" },
    });
    const result = await tracker.verify(readBack);
    expect(result.lostThisPass).toBe(0);
    expect(tracker.snapshot().meterState).toBe("green");
  });

  it("recordWriteAcked on an ALREADY-tracked key refreshes its expected value in place — a later legitimate update never false-reds", async () => {
    const tracker = new CorrectnessTracker();
    const w = stockWrite({ values: { s_quantity: 88, s_ytd: 12, s_order_cnt: 3, s_remote_cnt: 0 } });
    tracker.recordWriteAcked(w);
    tracker.promoteToTracked(tracker.drainPendingCandidates());

    // A second, later New-Order line legitimately updates the SAME stock row.
    const w2 = stockWrite({ values: { s_quantity: 78, s_ytd: 22, s_order_cnt: 4, s_remote_cnt: 0 } });
    tracker.recordWriteAcked(w2);

    const readBack: ReadBackFn = async (): Promise<ReadBackResult> => ({ found: true, row: { ...w2.values } });
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
// Bias mechanism — pickTrackedCandidates prefers keys on migrating vBuckets.
// ----------------------------------------------------------------------------

describe("correctness.ts — pickTrackedCandidates: migrating-vBucket bias", () => {
  const TOTAL_VBUCKETS = 64;
  const tenantId = "tpcc-w0001";
  const table = "tpcc_stock";

  function candidateFor(itemId: number): TrackedWrite {
    const partitionKey = stockKey(1, itemId);
    return {
      tenantId,
      table,
      partitionKey,
      warehouseId: 1,
      indexName: "idx_stock_by_item",
      indexValues: { i_id: itemId },
      values: { s_quantity: 1 },
    };
  }

  it("with no active migration, just caps the pool (no bias to apply)", () => {
    const vbucketMap: VBucketMigrationRow[] = Array.from({ length: TOTAL_VBUCKETS }, (_, v) => ({ vbucket: v, migrationStatus: "none" }));
    const candidates = Array.from({ length: 10 }, (_, i) => candidateFor(i + 1));
    const picked = pickTrackedCandidates(candidates, vbucketMap, TOTAL_VBUCKETS, 5);
    expect(picked).toHaveLength(5);
    expect(picked).toEqual(candidates.slice(0, 5));
  });

  it("with an active migration, candidates on a migrating vbucket are strictly preferred over non-migrating ones", () => {
    // Build a map where exactly one vbucket is "backfilling" (mid-migration).
    const vbucketMap: VBucketMigrationRow[] = Array.from({ length: TOTAL_VBUCKETS }, (_, v) => ({ vbucket: v, migrationStatus: "none" }));

    // Generate a wide pool of item-id candidates and find which ones
    // currently hash onto vbucket 0 vs some other vbucket, using the exact
    // same formula pickTrackedCandidates uses internally.
    const candidates = Array.from({ length: 500 }, (_, i) => candidateFor(i + 1));
    const onVbucket0 = candidates.filter((c) => hashKey(`${c.tenantId}:${c.table}:${c.partitionKey}`) % TOTAL_VBUCKETS === 0);
    expect(onVbucket0.length).toBeGreaterThan(0); // sanity: the fixture actually has matches to bias toward

    vbucketMap[0] = { vbucket: 0, migrationStatus: "backfilling" };

    const maxTracked = onVbucket0.length; // exactly enough room for the migrating-vbucket set
    const picked = pickTrackedCandidates(candidates, vbucketMap, TOTAL_VBUCKETS, maxTracked);

    expect(picked).toHaveLength(maxTracked);
    // Every picked candidate must actually be on the migrating vbucket —
    // the bias isn't just "sometimes prefers", it's a strict partition
    // (migrating-vbucket candidates exhaust the budget before any other
    // candidate is considered).
    for (const p of picked) {
      expect(hashKey(`${p.tenantId}:${p.table}:${p.partitionKey}`) % TOTAL_VBUCKETS).toBe(0);
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

  it("respects maxTracked even when every candidate is on a migrating vbucket", () => {
    const vbucketMap: VBucketMigrationRow[] = Array.from({ length: TOTAL_VBUCKETS }, (_, v) => ({ vbucket: v, migrationStatus: "backfilling" }));
    const candidates = Array.from({ length: 20 }, (_, i) => candidateFor(i + 1));
    const picked = pickTrackedCandidates(candidates, vbucketMap, TOTAL_VBUCKETS, 3);
    expect(picked).toHaveLength(3);
  });
});

// ----------------------------------------------------------------------------
// trackableWriteFromStockUpdate — only tpcc_stock UPDATEs are trackable.
// ----------------------------------------------------------------------------

describe("correctness.ts — trackableWriteFromStockUpdate", () => {
  it("builds a TrackedWrite from a tpcc_stock update call, recovering (warehouseId, itemId) from the partition key", () => {
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
    expect(write?.values).toEqual(call.values);
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
    const exec = new TrackingTxExecutor(inner, tracker);
    await expect(exec.mutate(1, { op: "update", table: "tpcc_stock", partitionKey: stockKey(1, 1), values: { s_quantity: 1 } })).rejects.toThrow(
      "simulated gateway failure",
    );
    expect(tracker.snapshot().writesAcked).toBe(0);
  });

  it("a successful tx() records every mutation in it as a fresh ack (no per-mutation replay signal available)", async () => {
    const tracker = new CorrectnessTracker();
    const inner = fakeExecutor();
    const exec = new TrackingTxExecutor(inner, tracker);
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
    const exec = new TrackingTxExecutor(inner, tracker);
    await exec.mutate(1, { op: "delete", table: "tpcc_new_order", partitionKey: "no-0001-01-000000001" });
    expect(tracker.snapshot().writesAcked).toBe(1); // still counts as an acked write...
    const drained = tracker.drainPendingCandidates();
    expect(drained).toHaveLength(0); // ...but never becomes a verifiable candidate
  });
});

// ----------------------------------------------------------------------------
// gatewayReadBack — the real adapter, exercised against a fake TxExecutor
// (a live cluster is what the pending live run will exercise this against).
// ----------------------------------------------------------------------------

describe("correctness.ts — gatewayReadBack", () => {
  it("found: true with the row when indexQuery returns a match", async () => {
    const write = stockWrite();
    const rows = new Map<string, Record<string, unknown>>([[JSON.stringify(write.indexValues), { ...write.values, s_key: write.partitionKey }]]);
    const exec = fakeExecutor({ rowsByIndexValue: rows });
    const readBack = gatewayReadBack(exec);
    const result = await readBack(write);
    expect(result.found).toBe(true);
    expect(result.row?.s_quantity).toBe(write.values.s_quantity);
  });

  it("found: false when indexQuery returns no rows (the row is genuinely missing)", async () => {
    const write = stockWrite();
    const exec = fakeExecutor(); // empty row table
    const readBack = gatewayReadBack(exec);
    const result = await readBack(write);
    expect(result.found).toBe(false);
    expect(result.row).toBeUndefined();
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
