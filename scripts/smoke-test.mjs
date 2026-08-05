#!/usr/bin/env node
// Post-deploy compatibility wrapper. Verification is implemented once in the
// built client CLI and requires an explicit disposable-target acknowledgement
// because the current API cannot remove the isolated table it creates.
//
// Usage:
//   cd client && npm run build && cd ..
//   node scripts/smoke-test.mjs <base-url> <admin-token> --disposable-target

import { run } from "../client/dist/cli.js";

const [baseUrl, adminToken, acknowledgement] = process.argv.slice(2);
if (!baseUrl || !adminToken || acknowledgement !== "--disposable-target") {
  console.error("Usage: node scripts/smoke-test.mjs <base-url> <admin-token> --disposable-target");
  process.exitCode = 1;
} else {
  process.exitCode = await run([
    "verify",
    "--url",
    baseUrl,
    "--token",
    adminToken,
    "--disposable-target",
  ]);
}
