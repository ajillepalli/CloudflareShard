#!/usr/bin/env node
// CLI entrypoint: `seed` (data generator) and `benchmark` (load driver)
// subcommands, against a running CloudflareShard deployment (local
// `wrangler dev` or a live Worker) over the existing HTTP API. Plain Node
// CLI script -- no wrangler needed to run this package itself.
//
// Usage:
//   node src/run.mjs seed --base-url <url> --admin-token <token> \
//     [--warehouses N] [--districts-per-warehouse N] [--customers-per-district N] \
//     [--items N] [--tenants-file path] [--seed-orders]
//
//   node src/run.mjs benchmark --base-url <url> [--tenants-file path] \
//     --duration <seconds> [--concurrency N]

import { seed } from "./generate.mjs";
import { loadWorld } from "./world.mjs";
import { runOneTransaction } from "./transactions.mjs";

function parseFlags(argv) {
  const flags = {};
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (!arg.startsWith("--")) continue;
    const key = arg.slice(2);
    const next = argv[i + 1];
    if (next === undefined || next.startsWith("--")) {
      flags[key] = true;
    } else {
      flags[key] = next;
      i++;
    }
  }
  return flags;
}

function intFlag(flags, name, fallback) {
  if (flags[name] === undefined) return fallback;
  const n = parseInt(flags[name], 10);
  if (Number.isNaN(n)) throw new Error(`--${name} must be an integer, got ${flags[name]}`);
  return n;
}

function floatFlag(flags, name, fallback) {
  if (flags[name] === undefined) return fallback;
  const n = parseFloat(flags[name]);
  if (Number.isNaN(n)) throw new Error(`--${name} must be a number, got ${flags[name]}`);
  return n;
}

async function runSeedCmd(flags) {
  const baseUrl = flags["base-url"];
  const adminToken = flags["admin-token"];
  if (!baseUrl || typeof baseUrl !== "string" || !adminToken || typeof adminToken !== "string") {
    console.error("Usage: node src/run.mjs seed --base-url <url> --admin-token <token> [options]");
    process.exit(1);
  }

  await seed({
    baseUrl,
    adminToken,
    warehouses: intFlag(flags, "warehouses", 2),
    districtsPerWarehouse: intFlag(flags, "districts-per-warehouse", 10),
    customersPerDistrict: intFlag(flags, "customers-per-district", 100),
    items: intFlag(flags, "items", 200),
    tenantsFile: flags["tenants-file"] || ".tpcc-tenants.json",
    // Off by default: seeding pre-existing orders (verified independently --
    // see generate.mjs) is by far the most expensive part of seeding (each
    // order contributes 5-15 extra order_line rows, dwarfing every other
    // table's row count) and also makes idx_order_line_by_order's
    // single-pass synchronous backfill proportionally slower -- at this
    // command's own defaults it pushed total seed time past several minutes,
    // defeating the "fast demo seeding" goal these reduced defaults exist
    // for. Opt in with --seed-orders if you want Delivery to have real
    // undelivered orders to act on from the very first benchmark run rather
    // than waiting for New-Order transactions to create some during the run.
    seedOrders: Boolean(flags["seed-orders"]),
    startWarehouse: intFlag(flags, "start-warehouse", 1),
  });
}

function percentile(sortedAscending, p) {
  if (sortedAscending.length === 0) return null;
  const idx = Math.min(sortedAscending.length - 1, Math.floor((p / 100) * sortedAscending.length));
  return sortedAscending[idx];
}

function fmtMs(v) {
  return v === null ? "n/a".padStart(8) : v.toFixed(1).padStart(8);
}

function printReport(records, durationSeconds) {
  const total = records.length;
  const byType = new Map();
  for (const r of records) {
    if (!byType.has(r.type)) byType.set(r.type, []);
    byType.get(r.type).push(r);
  }

  // tpmC-equivalent: real TPC-C's actual throughput metric is specifically
  // successful New-Order transactions per minute, not total transaction
  // count.
  const newOrderOk = (byType.get("new-order") || []).filter((r) => r.ok).length;
  const tpmC = (newOrderOk / durationSeconds) * 60;

  console.log("\n=== TPC-C-derived benchmark report (CloudflareShard, issue #16) ===");
  console.log(`Duration: ${durationSeconds}s`);
  console.log(`Total transactions attempted: ${total}`);
  console.log(`tpmC-equivalent (successful New-Order transactions / minute): ${tpmC.toFixed(2)}`);

  console.log("\nPer-transaction-type latency (ms, successful attempts only) and outcome counts:");
  console.log("type            n     ok    err      p50      p95      p99");
  for (const [type, recs] of byType) {
    const ok = recs.filter((r) => r.ok);
    const err = recs.filter((r) => !r.ok);
    const lat = ok.map((r) => r.latencyMs).sort((a, b) => a - b);
    const p50 = percentile(lat, 50);
    const p95 = percentile(lat, 95);
    const p99 = percentile(lat, 99);
    console.log(
      `${type.padEnd(14)} ${String(recs.length).padStart(5)} ${String(ok.length).padStart(5)} ${String(err.length).padStart(5)}  ${fmtMs(p50)} ${fmtMs(p95)} ${fmtMs(p99)}`,
    );
  }

  const errors = records.filter((r) => !r.ok);
  if (errors.length > 0) {
    console.log(`\n${errors.length} failed transaction attempt(s). Distinct error samples (up to 10):`);
    const seen = new Set();
    for (const e of errors) {
      const key = `${e.type}: ${e.error}`;
      if (seen.has(key)) continue;
      seen.add(key);
      console.log(`  [${e.type}] ${e.error}`);
      if (seen.size >= 10) break;
    }
  }
}

async function runBenchmarkCmd(flags) {
  const tenantsFile = flags["tenants-file"] || ".tpcc-tenants.json";
  const baseUrlOverride = typeof flags["base-url"] === "string" ? flags["base-url"] : undefined;
  const duration = floatFlag(flags, "duration", undefined);
  if (!duration || duration <= 0) {
    console.error("Usage: node src/run.mjs benchmark --base-url <url> [--tenants-file path] --duration <seconds> [--concurrency N]");
    process.exit(1);
  }
  const concurrency = intFlag(flags, "concurrency", 10);

  const world = await loadWorld(tenantsFile, baseUrlOverride);
  console.log(
    `Loaded ${world.warehouses.length} warehouse(s), ${world.items.length} cached items from ${tenantsFile}. Running for ${duration}s at concurrency ${concurrency} against ${world.baseUrl}...`,
  );

  const records = [];
  const deadline = Date.now() + duration * 1000;

  async function worker() {
    while (Date.now() < deadline) {
      records.push(await runOneTransaction(world));
    }
  }

  const workers = Array.from({ length: concurrency }, () => worker());
  await Promise.all(workers);

  printReport(records, duration);
}

async function main() {
  const [subcommand, ...rest] = process.argv.slice(2);
  const flags = parseFlags(rest);

  if (subcommand === "seed") {
    await runSeedCmd(flags);
  } else if (subcommand === "benchmark") {
    await runBenchmarkCmd(flags);
  } else {
    console.error("Usage: node src/run.mjs <seed|benchmark> [options]");
    console.error("  seed:      --base-url <url> --admin-token <token> [--warehouses N] [--districts-per-warehouse N] [--customers-per-district N] [--items N] [--tenants-file path] [--seed-orders]");
    console.error("  benchmark: --base-url <url> [--tenants-file path] --duration <seconds> [--concurrency N]");
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err instanceof Error ? err.stack || err.message : err);
  process.exit(1);
});
