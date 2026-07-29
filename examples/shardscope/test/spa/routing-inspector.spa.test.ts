import { afterEach, describe, expect, it } from "vitest";
import { bootApp, type Harness } from "./helpers/domHarness";
import { liveSnapshotFixture } from "./helpers/liveReshardBoot";

describe("Shardscope SPA — Playground Routing Inspector", () => {
  let harness: Harness | null = null;

  afterEach(() => {
    harness?.cleanup();
    harness = null;
  });

  it("resolves via /api/play/route-inspect and renders the summary via textContent", async () => {
    // GitHub #53: playFetch (every /api/play/* call, including route-inspect)
    // is now demo-mode-gated, same as reshardFetch -- the Playground room is
    // live-only, so this test must boot live. ownerShardId is "shard-0" to
    // match liveSnapshotFixture's own topology (shard-0/shard-1), so the
    // highlight actually resolves against a real rendered shard below.
    harness = bootApp({
      search: "",
      routes: {
        "/api/load/status": { status: 200 },
        "/api/play/route-inspect": {
          status: 200,
          body: {
            tenantId: "tenant-warehouse-1",
            catalogShardId: "catalog-0",
            vbucket: 5,
            totalVBuckets: 64,
            catalogShardCount: 2,
            ownerShardId: "shard-0",
          },
        },
      },
    });
    await harness.flush();
    harness.dispatchServerEvent("snapshot", liveSnapshotFixture());
    await harness.flush();

    harness.hook("rail-play")!.click();

    const form = harness.hook("play-route-form") as HTMLFormElement;
    const key = harness.hook("play-route-key") as HTMLInputElement;
    key.value = "s-0001-000001";
    form.dispatchEvent(new harness.window.Event("submit", { bubbles: true, cancelable: true }));
    await harness.flush();

    const call = harness.calls.find((c) => c.pathname === "/api/play/route-inspect");
    expect(call).toBeDefined();
    expect(call!.method).toBe("POST");
    expect(call!.body).toMatchObject({
      warehouseId: 1, // first PLAYGROUND_WAREHOUSE_IDS entry — the form's default selection
      table: "tpcc_warehouse",
      partitionKey: "s-0001-000001",
    });

    const resultEl = harness.hook("play-route-result") as HTMLElement;
    expect(resultEl.hidden).toBe(false);
    expect(resultEl.className).toContain("ok");
    expect(resultEl.textContent).toContain("shard-0");

    const summaryEl = harness.hook("play-route-summary") as HTMLElement;
    expect(summaryEl.hidden).toBe(false);
    expect(summaryEl.textContent).toContain("catalog-0");
    expect(summaryEl.textContent).toContain("shard-0");
    expect(summaryEl.textContent).toContain("5 / 64");

    // Honesty branch: booted live with shard-0 actually present in the
    // pushed snapshot (see liveSnapshotFixture), so describeHighlightState()
    // takes its real "found it, highlighting" branch rather than fabricating
    // a spotlight in a topology that was never actually rendered (see
    // app.js's describeHighlightState doc comment). The "mode !== live"
    // honesty branch this test used to exercise via a demo-mode boot is no
    // longer reachable that way post-#53 (a demo-mode route-inspect call can
    // no longer succeed at all) — it's still reachable via a live session
    // whose connection later degrades while a highlight is active, which
    // isn't covered here.
    expect(summaryEl.textContent).toContain("Highlighted");
  });

  it("renders a failed resolve honestly and never fabricates a summary", async () => {
    // GitHub #53: booted live for the same reason as the test above — a
    // demo-mode boot would now short-circuit before ever reaching the
    // mocked 400 route.
    harness = bootApp({
      search: "",
      routes: {
        "/api/load/status": { status: 200 },
        "/api/play/route-inspect": { status: 400, body: { error: "unknown table" } },
      },
    });
    await harness.flush();
    harness.dispatchServerEvent("snapshot", liveSnapshotFixture());
    await harness.flush();

    harness.hook("rail-play")!.click();
    const form = harness.hook("play-route-form") as HTMLFormElement;
    form.dispatchEvent(new harness.window.Event("submit", { bubbles: true, cancelable: true }));
    await harness.flush();

    const resultEl = harness.hook("play-route-result") as HTMLElement;
    expect(resultEl.hidden).toBe(false);
    expect(resultEl.className).toContain("err");
    expect(resultEl.textContent).toContain("unknown table");

    const summaryEl = harness.hook("play-route-summary") as HTMLElement;
    expect(summaryEl.hidden).toBe(true);
  });
});
