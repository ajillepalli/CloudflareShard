import { afterEach, describe, expect, it } from "vitest";
import { bootApp, type Harness } from "./helpers/domHarness";
import { liveSnapshotFixture } from "./helpers/liveReshardBoot";

describe("Shardscope SPA — App room", () => {
  let harness: Harness | null = null;

  afterEach(() => {
    harness?.cleanup();
    harness = null;
  });

  it("switching tenant fires /api/play/table-scan and renders rows via textContent, escaping any HTML in them (XSS regression guard)", async () => {
    // GitHub #53: playFetch (every /api/play/* call, including table-scan)
    // is now demo-mode-gated, same as reshardFetch -- a tenant switch
    // (handleAppWarehouseChange -> loadAppData) used to be the one App-room
    // path NOT gated by mode (a real gap this test previously relied on to
    // exercise a live read without booting live), but that gap is exactly
    // what #53 closed. Boot live here instead.
    const maliciousCredit = '<img src=x onerror="window.__xss_fired = true">';
    harness = bootApp({
      search: "",
      routes: {
        "/api/load/status": { status: 200 },
        "/api/play/table-scan": ({ body }) => {
          const { table } = body as { table: string };
          if (table === "tpcc_customer") {
            return {
              status: 200,
              body: { rows: [{ c_id: 7, c_first: "Ada", c_last: "Lovelace", c_credit: maliciousCredit, c_balance: -12.5 }] },
            };
          }
          if (table === "tpcc_stock") {
            return { status: 200, body: { rows: [{ i_id: 42, s_quantity: 88, s_ytd: 10, s_order_cnt: 3 }] } };
          }
          return { status: 200, body: { rows: [] } };
        },
      },
    });
    await harness.flush();
    harness.dispatchServerEvent("snapshot", liveSnapshotFixture());
    await harness.flush();

    harness.hook("rail-app")!.click(); // enters the App room — live mode fires an initial scan for the default warehouse
    await harness.flush();

    const warehouseSelect = harness.hook("app-warehouse") as HTMLSelectElement;
    warehouseSelect.value = "2";
    warehouseSelect.dispatchEvent(new harness.window.Event("change", { bubbles: true }));
    await harness.flush();

    // Isolate the SWITCH's own calls (warehouseId: 2) from the room-entry
    // scan already fired for the default warehouse above.
    const scanCalls = harness.calls.filter(
      (c) => c.pathname === "/api/play/table-scan" && (c.body as { warehouseId?: number }).warehouseId === 2,
    );
    expect(scanCalls).toHaveLength(2);
    expect(scanCalls).toContainEqual({
      pathname: "/api/play/table-scan",
      method: "POST",
      body: { warehouseId: 2, table: "tpcc_customer", limit: 5 },
    });
    expect(scanCalls).toContainEqual({
      pathname: "/api/play/table-scan",
      method: "POST",
      body: { warehouseId: 2, table: "tpcc_stock", limit: 5 },
    });

    const customersTable = harness.hook("app-customers-table") as HTMLElement;
    expect(harness.hook("app-customers-sub")!.textContent).toBe("1 row");
    // Rendered via renderAppTable's createElement/textContent discipline —
    // the malicious c_credit value shows up as literal text in a <td>...
    expect(customersTable.textContent).toContain(maliciousCredit);
    // ...and was never parsed as markup: no <img> element materialized.
    expect(customersTable.querySelector("img")).toBeNull();

    const stockTable = harness.hook("app-stock-table") as HTMLElement;
    expect(harness.hook("app-stock-sub")!.textContent).toBe("1 row");
    expect(stockTable.textContent).toContain("88");
  });
});
