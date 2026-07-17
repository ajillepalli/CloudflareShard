import { defineConfig } from "vitest/config";

// Separate config for Shardscope's frontend SPA (public/app.js) smoke tests.
//
// The root vitest.config.ts runs everything through
// @cloudflare/vitest-pool-workers (Miniflare) — a Worker runtime, not a
// browser DOM, so it can't load a plain <script> that expects `document`,
// `fetch`-as-a-global, `EventSource`, etc. These SPA tests need a real DOM
// (jsdom), so they get their own vitest invocation (`npm run test:spa`)
// with NO cloudflareTest plugin, isolated to examples/shardscope/test/spa.
//
// Root vitest.config.ts explicitly excludes **/*.spa.test.ts so the
// workers-pool run never attempts to load these files, and this config's
// `include` is scoped narrowly so it never picks up the 852 workers-pool
// tests either — the two suites are fully partitioned by filename pattern
// and never run in the same process.
export default defineConfig({
  test: {
    environment: "jsdom",
    include: ["examples/shardscope/test/spa/**/*.spa.test.ts"],
  },
});
