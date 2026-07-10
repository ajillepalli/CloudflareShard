import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vitest/config";

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
    // property, not a product regression). Tests late in the file were
    // sitting within ~5% of vitest's default 5s budget before M3; the
    // longer file pushed the last one over. 15s keeps honest headroom
    // without masking a genuine hang.
    testTimeout: 15000,
  },
});
