#!/usr/bin/env node
// Compatibility wrapper for the former standalone live verifier. The actual
// flow now lives in the tested CLI/domain implementation, so this script can
// no longer drift into a destructive one-shard path.
//
// Build first: npm run build
// Run only against a disposable target:
//   CLOUDFLARESHARD_URL=... CLOUDFLARESHARD_ADMIN_TOKEN=... \
//   CLOUDFLARESHARD_DISPOSABLE_TARGET=true node scripts/verify-live.mjs

import { run } from "../dist/cli.js";

if (process.env.CLOUDFLARESHARD_DISPOSABLE_TARGET !== "true") {
  console.error(
    "Refusing to mutate the target. Set CLOUDFLARESHARD_DISPOSABLE_TARGET=true only for a disposable deployment you intend to tear down.",
  );
  process.exitCode = 1;
} else {
  process.exitCode = await run(["verify", "--disposable-target"]);
}
