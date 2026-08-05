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
        // Root transaction tests exercise the production RPC seam without
        // duplicating JournalManifestDO storage tests from its own package.
        // Cloudflare's Vitest pool requires auxiliary Workers to be JavaScript.
        workers: [
          {
            name: "test-control-plane",
            modules: true,
            script: `
              import { WorkerEntrypoint } from "cloudflare:workers";
              export default class TestControlPlane extends WorkerEntrypoint {
                async checkManifestAdmission() {
                  return { ok: true, status: "ready", circuit_policy: { failure_threshold: 3, failure_window_ms: 30000, maximum_cooldown_ms: 300000 } };
                }
                async registerManifest(registration) {
                  return { ok: true, status: "registered", http_status: 200, record_hash: registration.record_hash, quarantined: false };
                }
                async releaseManifestRetention() {
                  return { ok: true, status: "released" };
                }
                async fetch() { return new Response("Not Found", { status: 404 }); }
              }
            `,
          },
        ],
        serviceBindings: {
          CONTROL_PLANE: "test-control-plane",
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
    // Local agent worktrees can contain complete sibling checkouts. Excluding
    // them keeps the root gate scoped to this checkout and prevents duplicate,
    // stale suites from being reported as failures of the active branch.
    exclude: [
      ...configDefaults.exclude,
      "**/*.spa.test.ts",
      ".claude/**",
      "client/**",
      "packages/**",
      "examples/cloudflareshard-oltp/**",
      "workers/control-plane/**",
    ],
  },
});
