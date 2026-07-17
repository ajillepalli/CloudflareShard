import { describe, expect, it } from "vitest";
import { deriveChecksumStatus, initialChecksumTrackingState, TopologyAggregator, type ChecksumTrackingState } from "./aggregator";

/** Minimal vbucket-map row shape deriveChecksumStatus reads — mirrors
 * aggregator.ts's own VbucketMapRow (only the two fields this function
 * actually uses). */
function row(migrationStatus: string, cutoverStartedAt: string | null = null) {
  return { migrationStatus, cutoverStartedAt };
}

function catalogsOf(...rows: ReturnType<typeof row>[]) {
  return [{ vbuckets: rows }];
}

const NOW = Date.parse("2026-07-16T12:00:00.000Z");

describe("aggregator.ts — deriveChecksumStatus: honest checksum labeling", () => {
  it("no migration anywhere, never observed one -> idle", () => {
    const { status } = deriveChecksumStatus(catalogsOf(row("none"), row("none")), initialChecksumTrackingState(), NOW);
    expect(status).toEqual({ label: "idle", state: "idle" });
  });

  it("a vbucket backfilling -> backfilling… (not idle, not verifying — checksum hasn't started yet)", () => {
    const { status } = deriveChecksumStatus(catalogsOf(row("backfilling"), row("none")), initialChecksumTrackingState(), NOW);
    expect(status).toEqual({ label: "backfilling…", state: "backfilling" });
  });

  it("a vbucket in cutover, just started -> verifying…", () => {
    const cutoverStartedAt = new Date(NOW - 2000).toISOString();
    const { status } = deriveChecksumStatus(catalogsOf(row("cutover", cutoverStartedAt)), initialChecksumTrackingState(), NOW);
    expect(status).toEqual({ label: "verifying…", state: "verifying" });
  });

  it("a vbucket in cutover for a long time -> stalled, not stuck showing verifying… forever", () => {
    const cutoverStartedAt = new Date(NOW - 60_000).toISOString();
    const { status } = deriveChecksumStatus(catalogsOf(row("cutover", cutoverStartedAt)), initialChecksumTrackingState(), NOW);
    expect(status.state).toBe("stalled");
    expect(status.label).toMatch(/stalled/i);
  });

  it("a vbucket aborting -> aborting…", () => {
    const { status } = deriveChecksumStatus(catalogsOf(row("aborting")), initialChecksumTrackingState(), NOW);
    expect(status).toEqual({ label: "aborting…", state: "aborting" });
  });

  it("a full migration lifecycle: idle -> backfilling -> cutover -> none reports 'cutover verified' (last-known), NOT a fabricated permanent OK", () => {
    let tracking: ChecksumTrackingState = initialChecksumTrackingState();

    let result = deriveChecksumStatus(catalogsOf(row("none")), tracking, NOW);
    expect(result.status.state).toBe("idle");
    tracking = result.nextTracking;

    result = deriveChecksumStatus(catalogsOf(row("backfilling")), tracking, NOW + 1000);
    expect(result.status.state).toBe("backfilling");
    tracking = result.nextTracking;

    result = deriveChecksumStatus(catalogsOf(row("cutover", new Date(NOW + 2000).toISOString())), tracking, NOW + 2000);
    expect(result.status.state).toBe("verifying");
    tracking = result.nextTracking;

    // cutover committed: the row flips back to "none" (shardId updated,
    // target cleared) — this is what a REAL successful migration looks like
    // from the vbucket map's point of view.
    result = deriveChecksumStatus(catalogsOf(row("none")), tracking, NOW + 3000);
    expect(result.status).toEqual({ label: "cutover verified", state: "verified" });
    tracking = result.nextTracking;

    // and it STAYS "cutover verified" (last-known) on subsequent idle ticks
    // — not a one-tick flash back to "idle".
    result = deriveChecksumStatus(catalogsOf(row("none")), tracking, NOW + 4000);
    expect(result.status).toEqual({ label: "cutover verified", state: "verified" });
  });

  it("a migration that gets ABORTED (backfilling -> aborting -> none) reports 'aborted', never 'cutover verified'", () => {
    let tracking: ChecksumTrackingState = initialChecksumTrackingState();

    let result = deriveChecksumStatus(catalogsOf(row("backfilling")), tracking, NOW);
    tracking = result.nextTracking;

    result = deriveChecksumStatus(catalogsOf(row("aborting")), tracking, NOW + 1000);
    expect(result.status.state).toBe("aborting");
    tracking = result.nextTracking;

    result = deriveChecksumStatus(catalogsOf(row("none")), tracking, NOW + 2000);
    expect(result.status).toEqual({ label: "aborted", state: "aborted" });
  });

  it("a migration that reaches CUTOVER then gets aborted (cutover -> aborting -> none) reports 'aborted', not 'cutover verified'", () => {
    let tracking: ChecksumTrackingState = initialChecksumTrackingState();

    let result = deriveChecksumStatus(catalogsOf(row("cutover", new Date(NOW).toISOString())), tracking, NOW);
    tracking = result.nextTracking;

    result = deriveChecksumStatus(catalogsOf(row("aborting")), tracking, NOW + 1000);
    tracking = result.nextTracking;

    result = deriveChecksumStatus(catalogsOf(row("none")), tracking, NOW + 2000);
    expect(result.status).toEqual({ label: "aborted", state: "aborted" });
  });

  it("a SECOND migration cycle after a verified one resets tracking — a later abort doesn't retroactively taint the earlier verified outcome, and vice versa", () => {
    let tracking: ChecksumTrackingState = initialChecksumTrackingState();

    // First cycle: clean cutover.
    let result = deriveChecksumStatus(catalogsOf(row("cutover", new Date(NOW).toISOString())), tracking, NOW);
    tracking = result.nextTracking;
    result = deriveChecksumStatus(catalogsOf(row("none")), tracking, NOW + 1000);
    expect(result.status.state).toBe("verified");
    tracking = result.nextTracking;

    // Second cycle starts fresh and gets aborted.
    result = deriveChecksumStatus(catalogsOf(row("backfilling")), tracking, NOW + 2000);
    tracking = result.nextTracking;
    result = deriveChecksumStatus(catalogsOf(row("aborting")), tracking, NOW + 3000);
    tracking = result.nextTracking;
    result = deriveChecksumStatus(catalogsOf(row("none")), tracking, NOW + 4000);
    expect(result.status).toEqual({ label: "aborted", state: "aborted" });
  });

  it("aborting takes priority over cutover/backfilling when multiple rows disagree in one tick", () => {
    const { status } = deriveChecksumStatus(catalogsOf(row("cutover", new Date(NOW).toISOString()), row("aborting"), row("backfilling")), initialChecksumTrackingState(), NOW);
    expect(status.state).toBe("aborting");
  });

  it("cutover takes priority over backfilling when multiple rows disagree in one tick", () => {
    const { status } = deriveChecksumStatus(catalogsOf(row("backfilling"), row("cutover", new Date(NOW).toISOString())), initialChecksumTrackingState(), NOW);
    expect(status.state).toBe("verifying");
  });

  it("multiple catalogs are scanned together, not just the first", () => {
    const catalogs = [{ vbuckets: [row("none")] }, { vbuckets: [row("aborting")] }];
    const { status } = deriveChecksumStatus(catalogs, initialChecksumTrackingState(), NOW);
    expect(status.state).toBe("aborting");
  });
});

// ----------------------------------------------------------------------------
// Design round 3, point 3 — the "is this genuinely verified right now"
// invariant used to be reconstructed HERE, in aggregator.ts, from raw
// trackedKeyCount/lastVerifyChecked figures (deriveScoreboardVerified). That
// reconstruction is gone: CorrectnessTracker.snapshot().verified is now
// computed exactly once, inside ./load/correctness.ts (the one place that
// actually knows whether the tracked SET has changed since the last
// verify() pass via its own epoch counter), and aggregator.ts's
// mergeScoreboard just forwards it — see that file's own comment. The
// boundary-case coverage that used to live here now lives in
// correctness.test.ts, next to the invariant it's actually testing.
// ----------------------------------------------------------------------------

// ----------------------------------------------------------------------------
// Pre-PR review Fix 2 (P2) — SSE backpressure wedge. broadcast()/safeWrite()
// used to `await writer.write(payload)` with no bound: a subscriber that's
// CONNECTED BUT NOT READING (a full TCP receive window — the write() promise
// never settles, per the Streams backpressure contract) made that await pend
// forever. Because runTick() holds `this.polling = true` across the awaited
// broadcast() and only calls scheduleNextTick() after it resolves, ONE such
// stalled subscriber froze the entire singleton poller — no snapshots for
// ANY tab, no future alarm reschedule. That defeats the "one shared poller,
// O(1) in viewers" availability promise this whole file's header comment
// rests on.
//
// These tests exercise TopologyAggregator's private broadcast()/subscribers
// directly (no DurableObjectState/Miniflare machinery needed — broadcast()
// and safeWrite() never touch state.storage or env, only the in-memory
// `subscribers` set and each writer's own write()/close()), via a fake
// DurableObjectState/Env and a stub WritableStreamDefaultWriter whose
// write() intentionally never resolves. The constructor's third
// (test-only) `writeTimeoutMs` argument keeps this fast and deterministic —
// real instantiations (the Workers runtime always calls `new
// TopologyAggregator(state, env)`, exactly two args) get the production
// SUBSCRIBER_WRITE_TIMEOUT_MS default instead.
// ----------------------------------------------------------------------------

/** Fake DurableObjectState — broadcast()/safeWrite() never call
 * state.storage, so only a shape TypeScript accepts is needed, not a working
 * implementation. */
function fakeState(): DurableObjectState {
  return {
    storage: {
      setAlarm: async () => {},
    },
  } as unknown as DurableObjectState;
}

/** A WritableStreamDefaultWriter stub whose write() never settles — models a
 * browser tab that's connected but not reading (full receive-window
 * backpressure), the exact condition the old unbounded `await writer.write()`
 * couldn't survive. close() resolves immediately since safeWrite's
 * best-effort teardown call to it is fire-and-forget either way. */
function stalledWriter(): WritableStreamDefaultWriter<Uint8Array> {
  return {
    write: () => new Promise<void>(() => {}),
    close: async () => {},
  } as unknown as WritableStreamDefaultWriter<Uint8Array>;
}

/** A WritableStreamDefaultWriter stub that resolves immediately and records
 * every chunk it was asked to write — a healthy subscriber, used to prove a
 * stalled sibling doesn't starve it. */
function recordingWriter(): { writer: WritableStreamDefaultWriter<Uint8Array>; written: Uint8Array[] } {
  const written: Uint8Array[] = [];
  const writer = {
    write: async (chunk: Uint8Array) => {
      written.push(chunk);
    },
    close: async () => {},
  } as unknown as WritableStreamDefaultWriter<Uint8Array>;
  return { writer, written };
}

describe("aggregator.ts — TopologyAggregator SSE backpressure (pre-PR review Fix 2)", () => {
  // Test-only write timeout — short so the test doesn't burn real seconds
  // waiting out the production SUBSCRIBER_WRITE_TIMEOUT_MS, but long enough
  // that recordingWriter's immediate resolution is unambiguously "fast, not
  // a race with the timer".
  const TEST_WRITE_TIMEOUT_MS = 50;

  it("a stalled subscriber does not wedge broadcast(): the call still completes within the timeout, the stalled subscriber is dropped, and OTHER subscribers still receive the snapshot", async () => {
    const aggregator = new TopologyAggregator(fakeState(), {} as never, TEST_WRITE_TIMEOUT_MS);
    // subscribers/broadcast are private — reaching in here directly is
    // exactly the "drive TopologyAggregator's own fan-out logic, not a
    // reimplementation of it" this regression test needs; there is no public
    // seam for this that doesn't also require a real DurableObjectState.
    const aggregatorAny = aggregator as unknown as {
      subscribers: Set<WritableStreamDefaultWriter<Uint8Array>>;
      broadcast(payload: Uint8Array): Promise<void>;
    };

    const stalled = stalledWriter();
    const { writer: healthy, written: healthyWritten } = recordingWriter();
    aggregatorAny.subscribers.add(stalled);
    aggregatorAny.subscribers.add(healthy);

    const payload = new TextEncoder().encode("event: snapshot\ndata: {}\n\n");

    const start = Date.now();
    await aggregatorAny.broadcast(payload);
    const elapsedMs = Date.now() - start;

    // The old code hung this await forever (see the revert-to-confirm note
    // below) — completing at all, well under the timeout's own budget, is
    // the core regression check.
    expect(elapsedMs).toBeLessThan(TEST_WRITE_TIMEOUT_MS * 4);
    // The stalled subscriber is treated exactly like a write-error
    // subscriber: dropped so it can't wedge a future tick either.
    expect(aggregatorAny.subscribers.has(stalled)).toBe(false);
    // The healthy subscriber was NOT starved by its stalled sibling — it got
    // the snapshot on this same broadcast, and is still registered.
    expect(aggregatorAny.subscribers.has(healthy)).toBe(true);
    expect(healthyWritten).toHaveLength(1);
    expect(healthyWritten[0]).toEqual(payload);
  });

  it("revert-to-confirm: without a bound, a single stalled writer.write() hangs the whole broadcast (documents why the timeout in safeWrite is load-bearing, not just belt-and-suspenders)", async () => {
    // This does NOT call TopologyAggregator at all — it reproduces the OLD
    // safeWrite behavior (`await writer.write(payload)`, no race) directly,
    // as a standalone regression anchor. If aggregator.ts's safeWrite ever
    // regresses back to an unbounded await, THIS test still documents why
    // that's broken; it is deliberately independent of aggregator.ts's
    // current implementation so it can't be accidentally fixed by editing
    // the wrong function.
    async function oldUnboundedSafeWrite(writer: WritableStreamDefaultWriter<Uint8Array>, payload: Uint8Array): Promise<void> {
      await writer.write(payload);
    }

    const stalled = stalledWriter();
    const payload = new TextEncoder().encode("event: snapshot\ndata: {}\n\n");

    const raced = await Promise.race([
      oldUnboundedSafeWrite(stalled, payload).then(() => "resolved" as const),
      new Promise<"timed-out">((resolve) => setTimeout(() => resolve("timed-out"), 200)),
    ]);

    // The pre-fix behavior: still pending 200ms later — i.e. it hangs, which
    // is exactly the singleton-poller wedge this task fixes. If this
    // assertion ever fails, stalledWriter() stopped modeling a stalled
    // writer (not that the bug got fixed — the fix lives in
    // aggregator.ts's safeWrite/writeWithTimeout, exercised by the test
    // above).
    expect(raced).toBe("timed-out");
  });
});
