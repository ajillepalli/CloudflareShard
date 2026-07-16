// Loads the tenants/reference-data file written by `seed` and builds the
// in-memory "world" the benchmark driver and transaction implementations
// share: one TenantClient per warehouse (reusing its cached token, never
// re-registering), plus the immutable reference data (item catalog, district
// tax rates) cached client-side per the design doc's documented simplification
// (real TPC-C mandates fresh reads of these fields on every transaction).

import { readFile } from "node:fs/promises";
import { TenantClient } from "./client.mjs";

export async function loadWorld(tenantsFile, baseUrlOverride) {
  const raw = await readFile(tenantsFile, "utf8");
  const data = JSON.parse(raw);
  const baseUrl = baseUrlOverride || data.baseUrl;
  if (!baseUrl) {
    throw new Error(`No base URL in ${tenantsFile} and none provided via --base-url.`);
  }

  const itemByI_id = new Map(data.items.map((it) => [it.i_id, it]));

  const warehouses = data.warehouses.map((w) => ({
    warehouseId: w.warehouseId,
    tenantId: w.tenantId,
    wTax: w.wTax,
    districts: w.districts, // [{ d_id, d_tax }]
    client: new TenantClient(baseUrl, w.token, w.tenantId),
  }));

  const warehouseById = new Map(warehouses.map((w) => [w.warehouseId, w]));

  return {
    baseUrl,
    config: data.config,
    items: data.items,
    itemByI_id,
    warehouses,
    warehouseById,
    randomWarehouse() {
      return warehouses[Math.floor(Math.random() * warehouses.length)];
    },
    randomDistrictId() {
      return 1 + Math.floor(Math.random() * data.config.districtsPerWarehouse);
    },
    randomCustomerId() {
      return 1 + Math.floor(Math.random() * data.config.customersPerDistrict);
    },
    randomItemId() {
      return 1 + Math.floor(Math.random() * data.config.items);
    },
  };
}
