#!/usr/bin/env node
import { parseArgs } from "node:util";
import { pathToFileURL } from "node:url";
import { CloudflareShardAdminClient } from "./admin-client.js";
import { CloudflareShardError } from "./errors.js";

const COMMANDS = [
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
type Command = (typeof COMMANDS)[number];

export function usage(): string {
  return `cloudflareshard <command> [options]

Commands:
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

Examples:
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
    },
    allowPositionals: false,
  });

  const url = values.url ?? process.env.CLOUDFLARESHARD_URL;
  const token = values.token ?? process.env.CLOUDFLARESHARD_ADMIN_TOKEN;
  if (!url || !token) {
    process.stderr.write("Missing connection info: pass --url/--token or set CLOUDFLARESHARD_URL/CLOUDFLARESHARD_ADMIN_TOKEN.\n");
    return 1;
  }
  const client = new CloudflareShardAdminClient({ baseUrl: url, token });

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

export async function dispatch(client: CloudflareShardAdminClient, command: Command, values: Record<string, unknown>): Promise<unknown> {
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
const isMain = !!process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
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
