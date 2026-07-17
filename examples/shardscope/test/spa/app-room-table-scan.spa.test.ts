import { afterEach, describe, expect, it } from "vitest";
import { bootApp, type Harness } from "./helpers/domHarness";

describe("Shardscope SPA — App room", () => {
  let harness: Harness | null = null;

  afterEach(() => {
    harness?.cleanup();
    harness = null;
  });

  it("switching tenant fires /api/play/table-scan and renders rows via textContent, escaping any HTML in them (XSS regression guard)", async () => {
    // ?demo=1 deliberately skips the App room's *initial-entry* read (see
    // app.js's startAppRoom: "mode === 'demo' -> render an honest 'no live
    // read' state instead of firing a request") — but a tenant switch
    // (handleAppWarehouseChange -> loadAppData) is NOT gated by mode, so
    // it's the reliable way to exercise the real table-scan read path
    // without needing to fake the live/gate flow.
    const maliciousCredit = '<img src=x onerror="window.__xss_fired = true">';
    harness = bootApp({
      routes: {
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

    harness.hook("rail-app")!.click(); // enters the App room (demo mode: renders "demo mode" placeholders, no fetch yet)
    await harness.flush();
    expect(harness.calls.some((c) => c.pathname === "/api/play/table-scan")).toBe(false);

    const warehouseSelect = harness.hook("app-warehouse") as HTMLSelectElement;
    warehouseSelect.value = "2";
    warehouseSelect.dispatchEvent(new harness.window.Event("change", { bubbles: true }));
    await harness.flush();

    const scanCalls = harness.calls.filter((c) => c.pathname === "/api/play/table-scan");
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
