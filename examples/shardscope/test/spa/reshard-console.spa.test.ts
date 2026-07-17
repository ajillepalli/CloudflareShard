import { afterEach, describe, expect, it } from "vitest";
import { bootApp, type Harness } from "./helpers/domHarness";

/** Enters the Reshard room and lets its entry-triggered
 * /api/reshard/lock-status poll settle (see room-switching.spa.test.ts for
 * the same sequencing). refreshReshardPickers() (called synchronously on
 * entry, off the ?demo=1 embedded sample snapshot) is what seeds the
 * catalog/vbucket/shard <select> defaults every test below relies on. */
async function enterReshardRoom(harness: Harness) {
  harness.hook("rail-reshard")!.click();
  await harness.flush();
}

/** Leaves the room so app.js's own setActiveRoom -> stopReshardPolling()
 * clears the 1.5s poll interval before the test ends, rather than relying on
 * window.close() to sweep it up. */
function leaveReshardRoom(harness: Harness) {
  harness.hook("rail-topology")!.click();
}

describe("Shardscope SPA — Reshard console ops", () => {
  let harness: Harness | null = null;

  afterEach(() => {
    harness?.cleanup();
    harness = null;
  });

  it("entering the room polls /api/reshard/lock-status and renders a held lock's status via textContent", async () => {
    harness = bootApp({
      routes: {
        "/api/reshard/lock-status": {
          status: 200,
          body: { held: true, operationId: "op-77", operationType: "migrate-vbucket", acquiredAt: new Date().toISOString(), expired: false },
        },
      },
    });

    await enterReshardRoom(harness);

    expect(harness.calls.some((c) => c.pathname === "/api/reshard/lock-status" && c.method === "GET")).toBe(true);
    expect(harness.hook("lock-state")!.textContent).toBe("held");
    expect(harness.hook("lock-state")!.className).toContain("held");
    expect(harness.hook("lock-detail")!.textContent).toContain("migrate-vbucket");
    expect(harness.hook("lock-detail")!.textContent).toContain("op-77");
    expect((harness.hook("lock-release-btn") as HTMLElement).hidden).toBe(false);

    leaveReshardRoom(harness);
  });

  it("Split: submits /api/reshard/split with the picker's selection, then polls migrate-status and renders the op card via textContent", async () => {
    harness = bootApp({
      routes: {
        "/api/reshard/lock-status": { status: 200, body: { held: false } },
        "/api/reshard/split": { status: 200, body: { toShard: "shard-9" } },
        "/api/reshard/migrate-status": { status: 200, body: { status: "backfilling", toShard: "shard-9", rowsCopied: 120 } },
      },
    });

    await enterReshardRoom(harness);

    // Split is the default-selected op tab; catalog-0 / vbucket 0 (owned by
    // shard-0 in buildSampleSnapshot()) are the pickers' default selections —
    // refreshReshardPickers() seeded them off the ?demo=1 sample snapshot.
    const form = harness.hook("op-form-split") as HTMLFormElement;
    form.dispatchEvent(new harness.window.Event("submit", { bubbles: true, cancelable: true }));
    await harness.flush();

    const splitCall = harness.calls.find((c) => c.pathname === "/api/reshard/split");
    expect(splitCall).toBeDefined();
    expect(splitCall!.method).toBe("POST");
    expect(splitCall!.body).toEqual({ catalogShardId: "catalog-0", vbucket: 0 });

    // handleSplitSubmit's .then sets activeOp + immediately calls
    // pollActiveOp(), which fires this GET.
    const statusCall = harness.calls.find((c) => c.pathname === "/api/reshard/migrate-status");
    expect(statusCall).toBeDefined();
    expect(statusCall!.method).toBe("GET");

    expect((harness.hook("op-card") as HTMLElement).hidden).toBe(false);
    expect(harness.hook("op-card-name")!.textContent).toBe("migrate-vbucket · backfilling");
    expect(harness.hook("op-card-detail")!.textContent).toContain("shard-9");
    expect(harness.hook("op-card-detail")!.textContent).toContain("120 rows copied");
    expect((harness.hook("op-abort-btn") as HTMLElement).hidden).toBe(false);

    leaveReshardRoom(harness);
  });

  it("Migrate: switching tabs submits /api/reshard/migrate with the picker's selection and renders the op card", async () => {
    harness = bootApp({
      routes: {
        "/api/reshard/lock-status": { status: 200, body: { held: false } },
        "/api/reshard/migrate": { status: 200, body: { toShard: "shard-2" } },
        "/api/reshard/migrate-status": { status: 200, body: { status: "starting", toShard: "shard-2" } },
      },
    });

    await enterReshardRoom(harness);
    harness.hook("op-tab-migrate")!.click();
    expect(harness.hook("op-tab-migrate")!.classList.contains("selected")).toBe(true);
    expect((harness.hook("op-form-migrate") as HTMLElement).hidden).toBe(false);
    expect((harness.hook("op-form-split") as HTMLElement).hidden).toBe(true);

    const form = harness.hook("op-form-migrate") as HTMLFormElement;
    form.dispatchEvent(new harness.window.Event("submit", { bubbles: true, cancelable: true }));
    await harness.flush();

    const migrateCall = harness.calls.find((c) => c.pathname === "/api/reshard/migrate");
    expect(migrateCall).toBeDefined();
    expect(migrateCall!.method).toBe("POST");
    // migrate-target's default selection is the "auto (new shard)" placeholder
    // (value "") -> targetShardId is left undefined, dropped by JSON.stringify.
    expect(migrateCall!.body).toEqual({ catalogShardId: "catalog-0", vbucket: 0 });

    expect((harness.hook("op-card") as HTMLElement).hidden).toBe(false);
    expect(harness.hook("op-card-name")!.textContent).toBe("migrate-vbucket · starting");

    leaveReshardRoom(harness);
  });

  it("Drain: switching tabs submits /api/reshard/drain with the picker's selection, then polls drain-status", async () => {
    harness = bootApp({
      routes: {
        "/api/reshard/lock-status": { status: 200, body: { held: false } },
        "/api/reshard/drain": { status: 200, body: {} },
        "/api/reshard/drain-status": { status: 200, body: { status: "evacuating", vbucketsRemaining: 3, ringsRemaining: 1 } },
      },
    });

    await enterReshardRoom(harness);
    harness.hook("op-tab-drain")!.click();
    expect((harness.hook("op-form-drain") as HTMLElement).hidden).toBe(false);

    const form = harness.hook("op-form-drain") as HTMLFormElement;
    form.dispatchEvent(new harness.window.Event("submit", { bubbles: true, cancelable: true }));
    await harness.flush();

    const drainCall = harness.calls.find((c) => c.pathname === "/api/reshard/drain");
    expect(drainCall).toBeDefined();
    expect(drainCall!.method).toBe("POST");
    // drain-shard-select's default is the first entry of shardIdsForCatalog("catalog-0"), sorted: "shard-0".
    expect(drainCall!.body).toEqual({ catalogShardId: "catalog-0", shardId: "shard-0" });

    const statusCall = harness.calls.find((c) => c.pathname === "/api/reshard/drain-status");
    expect(statusCall).toBeDefined();

    expect((harness.hook("op-card") as HTMLElement).hidden).toBe(false);
    expect(harness.hook("op-card-name")!.textContent).toBe("drain-shard · evacuating");
    expect(harness.hook("op-card-detail")!.textContent).toContain("shard-0");
    expect(harness.hook("op-card-detail")!.textContent).toContain("3 vbucket(s) left");
    expect(harness.hook("op-card-detail")!.textContent).toContain("1 ring(s) left");
    // Drain has no Abort control (only a migrate op does).
    expect((harness.hook("op-abort-btn") as HTMLElement).hidden).toBe(true);

    leaveReshardRoom(harness);
  });

  it("Abort: aborting an in-flight migrate fires /api/reshard/migrate-abort with the op's identity and clears the op card", async () => {
    harness = bootApp({
      routes: {
        "/api/reshard/lock-status": { status: 200, body: { held: false } },
        "/api/reshard/split": { status: 200, body: { toShard: "shard-9" } },
        "/api/reshard/migrate-status": { status: 200, body: { status: "backfilling", toShard: "shard-9" } },
        "/api/reshard/migrate-abort": { status: 200, body: { ok: true, status: "aborted" } },
      },
    });

    await enterReshardRoom(harness);
    const splitForm = harness.hook("op-form-split") as HTMLFormElement;
    splitForm.dispatchEvent(new harness.window.Event("submit", { bubbles: true, cancelable: true }));
    await harness.flush();
    expect((harness.hook("op-card") as HTMLElement).hidden).toBe(false);

    harness.hook("op-abort-btn")!.click();
    await harness.flush();

    const abortCall = harness.calls.find((c) => c.pathname === "/api/reshard/migrate-abort");
    expect(abortCall).toBeDefined();
    expect(abortCall!.method).toBe("POST");
    expect(abortCall!.body).toEqual({ catalogShardId: "catalog-0", vbucket: 0 });

    // handleAbortClick's .then clears activeOp and calls renderOpCard(null).
    expect((harness.hook("op-card") as HTMLElement).hidden).toBe(true);

    leaveReshardRoom(harness);
  });

  it("Force-release lock: requires a two-step confirm, then fires /api/reshard/force-release-lock with the held operationId", async () => {
    harness = bootApp({
      routes: {
        "/api/reshard/lock-status": {
          status: 200,
          body: { held: true, operationId: "op-77", operationType: "drain-shard", acquiredAt: new Date().toISOString(), expired: false },
        },
        "/api/reshard/force-release-lock": { status: 200, body: { released: true } },
      },
    });

    await enterReshardRoom(harness);
    const releaseBtn = harness.hook("lock-release-btn") as HTMLButtonElement;
    expect(releaseBtn.hidden).toBe(false);

    // First click only arms the confirm — no request fired yet.
    releaseBtn.click();
    expect(releaseBtn.classList.contains("confirming")).toBe(true);
    expect(releaseBtn.textContent).toBe("Confirm force-release?");
    expect(harness.calls.some((c) => c.pathname === "/api/reshard/force-release-lock")).toBe(false);

    // Second click (within the confirm window) actually fires it.
    releaseBtn.click();
    await harness.flush();

    const releaseCall = harness.calls.find((c) => c.pathname === "/api/reshard/force-release-lock");
    expect(releaseCall).toBeDefined();
    expect(releaseCall!.method).toBe("POST");
    expect(releaseCall!.body).toEqual({ operationId: "op-77" });
    // resetLockReleaseButton runs in .finally regardless of outcome.
    expect(releaseBtn.classList.contains("confirming")).toBe(false);
    expect(releaseBtn.textContent).toBe("Force-release lock");
    expect(releaseBtn.disabled).toBe(false);

    leaveReshardRoom(harness);
  });
});
