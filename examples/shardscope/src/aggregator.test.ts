import { describe, expect, it } from "vitest";
import { deriveChecksumStatus, initialChecksumTrackingState, type ChecksumTrackingState } from "./aggregator";

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
