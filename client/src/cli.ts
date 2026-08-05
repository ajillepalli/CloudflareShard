#!/usr/bin/env node
import { parseArgs } from "node:util";
import { pathToFileURL } from "node:url";
import { realpathSync } from "node:fs";
import { CloudflareShardAdminClient } from "./admin-client.js";
import { CloudflareShardClient } from "./client.js";
import { CloudflareShardError } from "./errors.js";
import { exitCodeFor, renderHuman, renderJson, type OnboardingOutput } from "./onboarding-output.js";
import { runDoctor, runVerify, sanitizeOnboardingText } from "./onboarding.js";
import { writeReceipt } from "./receipt.js";

const ADMIN_COMMANDS = [
  "init",
  "create-table",
  "register-table",
  "register-tenant",
  "create-index",
  "create-index-status",
  "status",
  "shard-stats",
  "list-tables",
  "list-indexes",
] as const;
const ONBOARDING_COMMANDS = ["doctor", "verify"] as const;
const COMMANDS = [...ONBOARDING_COMMANDS, ...ADMIN_COMMANDS] as const;
type Command = (typeof COMMANDS)[number];
type AdminCommand = (typeof ADMIN_COMMANDS)[number];

export function usage(): string {
  return `cloudflareshard <command> [options]

Commands:
  doctor [--output human|json]
  verify --disposable-target [--output human|json] [--receipt-dir PATH]
  init [--num-shards N] [--total-vbuckets N] [--force]
  create-table --table NAME --schema "CREATE TABLE ..." --partition-key-column COL
  register-table --table NAME --partition-key-column COL
  register-tenant --tenant-id ID [--rotate]
  create-index --index-name NAME --table NAME --columns col1,col2
  create-index-status --index-name NAME
  status
  shard-stats --shard-id ID
  list-tables
  list-indexes

Connection (required, via flags or env vars):
  --url URL         or CLOUDFLARESHARD_URL       e.g. http://127.0.0.1:8787
  --token TOKEN      or CLOUDFLARESHARD_ADMIN_TOKEN   admin bearer token

Onboarding output:
  --output MODE      human or json (default: human on a TTY, json otherwise)
  --json             shorthand for --output json
  --receipt-dir PATH receipt directory (default: .cloudflareshard/receipts)
  Exit codes         0 success, 2 invalid/unsafe input, 3 prerequisite failure,
                     4 verification failure, 5 pending reconciliation

Examples:
  cloudflareshard doctor
  cloudflareshard verify --disposable-target
  cloudflareshard init --num-shards 4 --total-vbuckets 256
  cloudflareshard create-table --table events --schema "CREATE TABLE events (id TEXT PRIMARY KEY, body TEXT)" --partition-key-column id
  cloudflareshard status
`;
}

export function isCommand(value: string | undefined): value is Command {
  return !!value && (COMMANDS as readonly string[]).includes(value);
}

export async function run(argv: string[]): Promise<number> {
  const command = argv[0];
  if (!command || command === "--help" || command === "-h") {
    process.stdout.write(usage());
    return command ? 0 : 1;
  }
  if (!isCommand(command)) {
    process.stderr.write(`Unknown command: ${command}\n\n${usage()}`);
    return 1;
  }

  const { values } = parseArgs({
    args: argv.slice(1),
    options: {
      url: { type: "string" },
      token: { type: "string" },
      table: { type: "string" },
      schema: { type: "string" },
      "partition-key-column": { type: "string" },
      "tenant-id": { type: "string" },
      rotate: { type: "boolean" },
      "index-name": { type: "string" },
      columns: { type: "string" },
      "shard-id": { type: "string" },
      "num-shards": { type: "string" },
      "total-vbuckets": { type: "string" },
      force: { type: "boolean" },
      output: { type: "string" },
      json: { type: "boolean" },
      "receipt-dir": { type: "string" },
      "disposable-target": { type: "boolean" },
      "max-route-probes": { type: "string" },
    },
    allowPositionals: false,
  });

  const url = values.url ?? process.env.CLOUDFLARESHARD_URL;
  const token = values.token ?? process.env.CLOUDFLARESHARD_ADMIN_TOKEN;
  if (!url || !token) {
    process.stderr.write("Missing connection info: pass --url/--token or set CLOUDFLARESHARD_URL/CLOUDFLARESHARD_ADMIN_TOKEN.\n");
    return 3;
  }
  const client = new CloudflareShardAdminClient({ baseUrl: url, token });

  if (command === "doctor" || command === "verify") {
    if (command === "verify" && values["disposable-target"] !== true) {
      process.stderr.write("verify mutates and retains isolated resources. Re-run with --disposable-target only against a target you intend to tear down.\n");
      return 2;
    }
    const requestedOutput = values.json === true ? "json" : values.output;
    if (requestedOutput !== undefined && requestedOutput !== "human" && requestedOutput !== "json") {
      process.stderr.write("--output must be 'human' or 'json'.\n");
      return 2;
    }
    const outputMode = requestedOutput ?? (process.stdout.isTTY ? "human" : "json");
    const maxRouteProbes = values["max-route-probes"] === undefined ? undefined : Number(values["max-route-probes"]);
    if (maxRouteProbes !== undefined && (!Number.isInteger(maxRouteProbes) || maxRouteProbes < 2 || maxRouteProbes > 1000)) {
      process.stderr.write("--max-route-probes must be an integer from 2 through 1000.\n");
      return 2;
    }
    const result = command === "doctor"
      ? await runDoctor(client)
      : await runVerify(
          client,
          (tenantToken) => new CloudflareShardClient({ baseUrl: url, token: tenantToken }),
          { maxRouteProbes },
        );
    const color = outputMode === "human" && process.stdout.isTTY === true && process.env.NO_COLOR === undefined;
    let output: OnboardingOutput;
    try {
      const receipt = await writeReceipt(result, url, {
        directory: typeof values["receipt-dir"] === "string" ? values["receipt-dir"] : undefined,
        secrets: [token],
      });
      output = {
        schemaVersion: "cloudflareshard.cli-output.v1",
        result,
        receipt,
      };
    } catch (error) {
      output = {
        schemaVersion: "cloudflareshard.cli-output.v1",
        result,
        receipt: null,
        failure: {
          code: "RECEIPT_WRITE_FAILED",
          category: "PREREQUISITE",
          message: sanitizeOnboardingText(`Receipt could not be persisted: ${error instanceof Error ? error.message : String(error)}`),
        },
      };
      process.stdout.write(outputMode === "json" ? renderJson(output) : renderHuman(output, { width: process.stdout.columns, color }));
      return 3;
    }
    process.stdout.write(
      outputMode === "json"
        ? renderJson(output)
        : renderHuman(output, { width: process.stdout.columns, color }),
    );
    return exitCodeFor(result);
  }

  const result = await dispatch(client, command, values);
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  return 0;
}

export function requireFlag(values: Record<string, unknown>, flag: string): string {
  const value = values[flag];
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`Missing required flag: --${flag}`);
  }
  return value;
}

export async function dispatch(client: CloudflareShardAdminClient, command: AdminCommand, values: Record<string, unknown>): Promise<unknown> {
  switch (command) {
    case "init":
      return client.init({
        numShards: values["num-shards"] ? Number(values["num-shards"]) : undefined,
        totalVBuckets: values["total-vbuckets"] ? Number(values["total-vbuckets"]) : undefined,
        force: values.force === true,
      });
    case "create-table":
      return client.createTable({
        table: requireFlag(values, "table"),
        schema: requireFlag(values, "schema"),
        partitionKeyColumn: requireFlag(values, "partition-key-column"),
      });
    case "register-table":
      return client.registerTable({
        table: requireFlag(values, "table"),
        partitionKeyColumn: requireFlag(values, "partition-key-column"),
      });
    case "register-tenant":
      return client.registerTenant({
        tenantId: requireFlag(values, "tenant-id"),
        rotate: values.rotate === true,
      });
    case "create-index":
      return client.createIndex({
        indexName: requireFlag(values, "index-name"),
        table: requireFlag(values, "table"),
        columns: requireFlag(values, "columns").split(",").map((c) => c.trim()),
      });
    case "create-index-status":
      return client.createIndexStatus(requireFlag(values, "index-name"));
    case "status":
      return client.status();
    case "shard-stats":
      return client.shardStats(requireFlag(values, "shard-id"));
    case "list-tables":
      return client.listTables();
    case "list-indexes":
      return client.listIndexes();
  }
}

// Only auto-run when this file is executed directly (the published `bin`
// entry point) -- not when imported, e.g. by tests exercising dispatch()
// directly against a mocked client.
//
// Codex review: npm's installed CLI runs through a node_modules/.bin
// symlink on POSIX -- import.meta.url resolves to this file's REAL path,
// but process.argv[1] is the symlink path Node was invoked with, so a bare
// string comparison never matches and the CLI would silently no-op for
// every real npm install. realpathSync resolves argv[1] through the
// symlink first so both sides compare the same real path. Wrapped in
// try/catch (falling back to isMain: false) so a process.argv[1] that
// doesn't resolve to a real file -- unlikely, but conceivable for some
// non-standard invocation -- fails safe instead of throwing on import.
function resolveIsMain(): boolean {
  if (!process.argv[1]) return false;
  try {
    return import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href;
  } catch {
    return false;
  }
}
const isMain = resolveIsMain();
if (isMain) {
  run(process.argv.slice(2))
    .then((code) => {
      process.exitCode = code;
    })
    .catch((error: unknown) => {
      if (error instanceof CloudflareShardError) {
        process.stderr.write(`Error${error.code ? ` [${error.code}]` : ""}: ${error.message}\n`);
        if (error.fix) process.stderr.write(`Fix: ${error.fix}\n`);
      } else {
        process.stderr.write(`Error: ${error instanceof Error ? error.message : String(error)}\n`);
      }
      process.exitCode = 1;
    });
}
