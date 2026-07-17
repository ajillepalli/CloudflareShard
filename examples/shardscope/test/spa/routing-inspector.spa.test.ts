import { afterEach, describe, expect, it } from "vitest";
import { bootApp, type Harness } from "./helpers/domHarness";

describe("Shardscope SPA — Playground Routing Inspector", () => {
  let harness: Harness | null = null;

  afterEach(() => {
    harness?.cleanup();
    harness = null;
  });

  it("resolves via /api/play/route-inspect and renders the summary via textContent", async () => {
    harness = bootApp({
      routes: {
        "/api/play/route-inspect": {
          status: 200,
          body: {
            tenantId: "tenant-warehouse-1",
            catalogShardId: "catalog-0",
            vbucket: 5,
            totalVBuckets: 64,
            catalogShardCount: 2,
            ownerShardId: "shard-3",
          },
        },
      },
    });

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
    expect(resultEl.textContent).toContain("shard-3");

    const summaryEl = harness.hook("play-route-summary") as HTMLElement;
    expect(summaryEl.hidden).toBe(false);
    expect(summaryEl.textContent).toContain("catalog-0");
    expect(summaryEl.textContent).toContain("shard-3");
    expect(summaryEl.textContent).toContain("5 / 64");

    // Honesty branch: boot()'s default is ?demo=1, so mode !== "live" —
    // describeHighlightState() must say it's skipping the canvas highlight
    // rather than silently pretending to spotlight a shard in a fabricated
    // topology (see app.js's describeHighlightState doc comment).
    expect(summaryEl.textContent).toContain("sample/demo data");
  });

  it("renders a failed resolve honestly and never fabricates a summary", async () => {
    harness = bootApp({
      routes: {
        "/api/play/route-inspect": { status: 400, body: { error: "unknown table" } },
      },
    });

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
