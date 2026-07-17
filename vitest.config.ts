import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { configDefaults, defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [
    cloudflareTest({
      wrangler: {
        configPath: "./wrangler.toml",
      },
      miniflare: {
        bindings: {
          ADMIN_TOKEN: "test-admin-token",
        },
      },
    }),
  ],
  test: {
    // Milestone 3 added heavyweight end-to-end migration/drain tests to
    // index.test.ts, and per-request latency in the workers pool grows
    // cumulatively over a long test file's lifetime (measured: ~40ms per
    // gateway call early in the file vs ~400ms by its end — an environment
    // property, not a product regression). The review-response commits added
    // more multi-round-trip tests, so a late-file test doing ~20 gateway
    // calls sits near an aggressive per-test budget; 30s gives honest
    // headroom (a genuine hang still blows past it — the migration tests
    // that legitimately need longer set their own explicit larger timeouts).
    testTimeout: 30000,
    // Same cumulative-latency reason for setup/teardown hooks (some fan out
    // list-tables + per-shard cleanup).
    hookTimeout: 30000,
    // The Shardscope SPA smoke tests run under jsdom (see vitest.spa.config.ts
    // + `npm run test:spa`) — they can't load in this workers-pool/Miniflare
    // environment, so exclude them here rather than letting this config try
    // (and fail) to run them.
    exclude: [...configDefaults.exclude, "**/*.spa.test.ts"],
  },
});
