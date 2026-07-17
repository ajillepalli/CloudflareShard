/** build.ts — Shardscope's "Build on it" panel backend (the watch->build
 * bridge). A developer who watched the demo can download a REAL, runnable
 * CloudflareShard starter repo: a small multi-tenant "inventory" app,
 * generated here as a set of files, then zipped for download by
 * src/index.ts's /api/build/* routes.
 *
 * ============================================================================
 * ACCURACY CONTRACT — read this before touching any of the string constants
 * below.
 * ============================================================================
 * Every generated file is cross-checked against the REAL CloudflareShard API,
 * not invented:
 *   - The generated `wrangler.toml`'s `[[services]]` block is copied from
 *     `examples/rpc-consumer/wrangler.toml` (the canonical service-binding
 *     starter) — same binding name (SHARD_API), same `service`
 *     ("cloudflare-shard-mvp"), same `entrypoint` ("CloudflareShardRpc").
 *   - The generated `src/index.ts`'s `ShardApiBinding` interface hand-mirrors
 *     `CloudflareShardRpc` (repo-root `src/index.ts`) exactly the way
 *     `examples/rpc-consumer/src/index.ts` and this app's own
 *     `src/env.d.ts` already do: `mutate`/`tx`/`tableScan` take a tenant
 *     bearer token as their first argument and a body shape matching
 *     `StructuredMutation`/`StructuredOperation` (repo-root
 *     `src/structured-op.ts`); `adminCreateTable`/`adminRegisterTenant` take
 *     `adminToken` explicitly and a body shape matching the real
 *     `adminCreateTableCore`/`adminRegisterTenantCore` handlers and the
 *     root README's own "Register a logical table" / "Register a tenant"
 *     walkthrough (`{table, schema, partitionKeyColumn}` /
 *     `{tenantId, rotate?}`).
 *   - The generated `schema.sql`'s `inventory_items` table uses a TEXT
 *     PRIMARY KEY column (`id`) with no other columns needed for tenant
 *     isolation — matching the demo's own TPC-C tables (e.g. `tpcc_stock`'s
 *     `s_key TEXT PRIMARY KEY`, see `src/load/reshard.integration.test.ts`),
 *     which carry no physical `tenant_id` column either; CloudflareShard
 *     tracks row ownership internally via `__cf_row_owners` (see the root
 *     README's "Tenant-scoped table scan" section).
 *
 * SECURITY: no secret ever appears in a generated file. The demo's
 * ADMIN_TOKEN and SHARDSCOPE_GATE_TOKEN, and every tenant token this Worker
 * holds, are server-side-only values (see src/gate.ts's header comment) —
 * `generateScaffoldFiles()` below is a pure function with NO access to `Env`,
 * so it is structurally incapable of embedding one. The generated starter's
 * own `src/index.ts` takes `adminToken`/`tenantToken` as explicit
 * request-body parameters on its setup/app routes (the same "explicit
 * argument, checked on the other side of the binding" pattern
 * `examples/rpc-consumer`'s `/demo/*` routes already use for `tenantToken`),
 * so the developer supplies their OWN tokens when they run it — never a
 * baked-in one. build.test.ts asserts no known secret substring appears in
 * any generated file's content as a regression guard.
 * ============================================================================
 */

export interface ScaffoldFile {
  path: string;
  content: string;
}

const WRANGLER_TOML = `name = "cloudflareshard-inventory-starter"
main = "src/index.ts"
compatibility_date = "2026-07-14"

# Service binding to CloudflareShard's RPC entrypoint. This is what "no HTTP,
# no bearer-token header to construct" actually looks like: the security
# boundary is this binding existing in this Worker's own config, not a
# request-level credential — admin/tenant tokens are still passed as explicit
# method arguments and checked internally by CloudflareShardRpc, exactly as
# an HTTP caller's Authorization header would be. Copied from
# examples/rpc-consumer/wrangler.toml in the CloudflareShard repo — the
# canonical service-binding starter this scaffold mirrors.
#
# Locally (\`wrangler dev\`), this resolves via Wrangler's local dev registry
# as long as the main cloudflare-shard-mvp Worker is ALSO running
# \`wrangler dev\` on the same machine — no extra config needed. Deployed,
# both Workers just need to exist in the same Cloudflare account under these
# names (rename \`service\` below if your cloudflare-shard-mvp deployment uses
# a different Worker name).
[[services]]
binding = "SHARD_API"
service = "cloudflare-shard-mvp"
entrypoint = "CloudflareShardRpc"

# No secrets are baked into this file, or anywhere else in this download.
# This starter's routes take adminToken/tenantToken as explicit request-body
# parameters (see src/index.ts) so nothing sensitive ships in the zip you
# just downloaded — see README.md's "What this starter deliberately leaves
# out" section for how you'd move a token into \`wrangler secret put\` once
# you're past "hello world".
`;

const PACKAGE_JSON = `{
  "name": "cloudflareshard-inventory-starter",
  "version": "1.0.0",
  "private": true,
  "type": "module",
  "description": "A real, runnable multi-tenant inventory app on CloudflareShard, service-bound to cloudflare-shard-mvp's CloudflareShardRpc entrypoint — generated by Shardscope's \\"Build on it\\" panel.",
  "scripts": {
    "dev": "wrangler dev",
    "deploy": "wrangler deploy",
    "typecheck": "tsc --noEmit"
  },
  "devDependencies": {
    "@cloudflare/workers-types": "^4.20260610.0",
    "typescript": "^5.9.2",
    "wrangler": "^4.24.0"
  }
}
`;

const TSCONFIG_JSON = `{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ES2022",
    "moduleResolution": "Bundler",
    "strict": true,
    "skipLibCheck": true,
    "types": ["@cloudflare/workers-types"]
  },
  "include": ["src/**/*.ts"]
}
`;

const SCHEMA_SQL = `-- inventory_items: this starter's one demo table. \`id\` is a TEXT PRIMARY
-- KEY — CloudflareShard's /admin/create-table (see README.md's "Setup"
-- section) verifies it's UNIQUE, TEXT/BLOB-affinity, and BINARY-collated
-- automatically, which is what makes this table eligible for
-- /v1/table-scan (see the main CloudflareShard repo's README, "Tenant-
-- scoped table scan"). No physical tenant_id column is needed: CloudflareShard
-- tracks row ownership internally per (tenantId, table, partitionKey) via
-- __cf_row_owners, the same convention this demo's own TPC-C tables use
-- (e.g. tpcc_stock's s_key TEXT PRIMARY KEY carries no tenant_id column
-- either).
--
-- This file is a reference copy of the schema this starter's own
-- POST /setup/table route sends to CloudflareShardRpc's adminCreateTable —
-- CloudflareShard applies schema changes only through that admin call (or
-- /admin/register-table for an already-existing table), never through
-- /v1/sql (its deny-list blocks CREATE/DROP/ALTER).
CREATE TABLE inventory_items (
  id TEXT PRIMARY KEY,
  sku TEXT,
  name TEXT,
  quantity INTEGER
);
`;

const SRC_INDEX_TS = `/** Starter Worker for a small multi-tenant "inventory" app on
 * CloudflareShard — generated by Shardscope's "Build on it" panel
 * (examples/shardscope/src/build.ts in the CloudflareShard repo). Every
 * route below calls a REAL CloudflareShardRpc method with its REAL argument
 * shape (mirrored from the CloudflareShard repo's
 * examples/rpc-consumer/src/index.ts — the canonical service-binding
 * starter — and repo-root src/index.ts's CloudflareShardRpc class).
 *
 * A real consumer wouldn't have access to CloudflareShardRpc's internal
 * TypeScript types (a separate npm package/repo, same as rpc-consumer) — so,
 * like rpc-consumer, this interface is the documented RPC contract,
 * hand-mirrored, not an import.
 *
 * NO SECRETS ARE BAKED INTO THIS FILE. adminToken/tenantToken are taken as
 * explicit request-body parameters on the routes below, exactly the way
 * rpc-consumer's own /demo/* routes take tenantToken — see README.md for how
 * to get your own tokens, and for how you'd move them into
 * \`wrangler secret put\` + env.ADMIN_TOKEN/env.TENANT_TOKEN once you're past
 * the "hello world" stage (kept out of scope here so this download can never
 * contain a live credential).
 */
export interface ShardApiBinding {
  mutate(
    tenantToken: string,
    body: {
      op: "insert" | "update" | "delete" | "upsert";
      table: string;
      tenantId: string;
      partitionKey: string;
      values?: Record<string, unknown>;
      where?: Record<string, unknown>;
      requestId?: string;
    },
  ): Promise<{ ok: true; rowsAffected: number }>;
  tx(
    tenantToken: string,
    body: {
      mutations: Array<{
        op: "insert" | "update" | "delete" | "upsert";
        table: string;
        tenantId: string;
        partitionKey: string;
        values?: Record<string, unknown>;
        where?: Record<string, unknown>;
      }>;
      requestId?: string;
    },
  ): Promise<unknown>;
  tableScan(
    tenantToken: string,
    body: { tenantId: string; table: string; limit?: number; cursor?: string | null },
  ): Promise<{
    rows: Array<Record<string, unknown>>;
    nextCursor?: string;
    provenance: { complete: boolean; fix?: string };
    scan: { catalogShardId: string; shardCount: number; successCount: number; scanMs: number };
  }>;
  // Admin methods — every one takes adminToken explicitly, just like an HTTP
  // call to the equivalent /admin/* route needs
  // \`Authorization: Bearer <ADMIN_TOKEN>\`.
  adminCreateTable(adminToken: string, body: { table: string; schema: string; partitionKeyColumn: string }): Promise<unknown>;
  adminRegisterTenant(adminToken: string, body: { tenantId: string; rotate?: boolean }): Promise<unknown>;
}

export interface Env {
  SHARD_API: ShardApiBinding;
}

const TABLE = "inventory_items";

const json = (data: unknown, status = 200): Response =>
  new Response(JSON.stringify(data, null, 2), { status, headers: { "content-type": "application/json" } });

function errorJson(err: unknown, status = 502): Response {
  const message = err instanceof Error ? err.message : String(err);
  return json({ error: message }, status);
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/health") {
      return json({ ok: true, service: "cloudflareshard-inventory-starter" });
    }

    // ---- One-time setup — run these against YOUR OWN running cluster (see
    // README.md's "Setup" section). Neither call is gated by this Worker
    // itself: adminToken is the same ADMIN_TOKEN your cloudflare-shard-mvp
    // deployment already checks server-side, so it's never trusted or
    // stored here — the same "explicit argument, checked on the other side
    // of the binding" model rpc-consumer's own /demo/admin-* routes use. ----

    // POST /setup/table: creates this app's one demo table via
    // adminCreateTable — CloudflareShardRpc's RPC counterpart of
    // /admin/create-table. Run ONCE per cluster (a second call errors — the
    // table already exists; that's expected, see README.md).
    if (request.method === "POST" && url.pathname === "/setup/table") {
      const body = (await request.json()) as { adminToken: string };
      try {
        const result = await env.SHARD_API.adminCreateTable(body.adminToken, {
          table: TABLE,
          schema: \`CREATE TABLE \${TABLE} (id TEXT PRIMARY KEY, sku TEXT, name TEXT, quantity INTEGER)\`,
          partitionKeyColumn: "id",
        });
        return json({ result });
      } catch (err) {
        return errorJson(err);
      }
    }

    // POST /setup/tenant: registers one tenant (a customer/org/environment
    // of YOUR app) via adminRegisterTenant and returns its bearer token —
    // returned ONLY this once, exactly like the real /admin/register-tenant
    // route (see README.md). Run once per tenant you want to onboard.
    if (request.method === "POST" && url.pathname === "/setup/tenant") {
      const body = (await request.json()) as { adminToken: string; tenantId: string };
      try {
        const result = (await env.SHARD_API.adminRegisterTenant(body.adminToken, { tenantId: body.tenantId })) as {
          ok: true;
          tenantId: string;
          token: string;
        };
        return json({ tenantId: result.tenantId, tenantToken: result.token });
      } catch (err) {
        return errorJson(err);
      }
    }

    // ---- App routes — what a real caller of THIS app hits. Each one takes
    // \`tenantToken\` in the body: this starter has no auth layer of its own
    // (a deliberate scope cut for a "hello world" — see README.md), so it
    // trusts whatever tenantToken the caller presents, the same way it's
    // ultimately CloudflareShardRpc's job (not this Worker's) to check it's
    // valid. A real app would authenticate its own users first and look up
    // (or mint) their tenantToken server-side, never take it as an
    // untrusted client input the way this demo does. ----

    // POST /items: insert one row via RPC mutate() — no HTTP request, no
    // Authorization header built anywhere in this Worker's own code.
    if (request.method === "POST" && url.pathname === "/items") {
      const body = (await request.json()) as {
        tenantToken: string;
        tenantId: string;
        id: string;
        sku: string;
        name: string;
        quantity: number;
      };
      try {
        const result = await env.SHARD_API.mutate(body.tenantToken, {
          op: "insert",
          table: TABLE,
          tenantId: body.tenantId,
          partitionKey: body.id,
          values: { id: body.id, sku: body.sku, name: body.name, quantity: body.quantity },
        });
        return json(result);
      } catch (err) {
        return errorJson(err);
      }
    }

    // POST /items/transfer: move \`amount\` units from one item to another,
    // atomically, via RPC tx() — CloudflareShard's cross-row two-phase
    // commit. Both mutations commit, or neither does.
    // fromExpectedQuantity/toExpectedQuantity are optimistic-concurrency
    // guards (the same "where = the value you last read" pattern
    // Shardscope's own dashboard uses for its Restock action) — a
    // concurrent writer changing either row between your read and this call
    // makes the whole tx reject rather than silently overwrite it.
    if (request.method === "POST" && url.pathname === "/items/transfer") {
      const body = (await request.json()) as {
        tenantToken: string;
        tenantId: string;
        fromId: string;
        fromExpectedQuantity: number;
        toId: string;
        toExpectedQuantity: number;
        amount: number;
      };
      try {
        const result = await env.SHARD_API.tx(body.tenantToken, {
          mutations: [
            {
              op: "update",
              table: TABLE,
              tenantId: body.tenantId,
              partitionKey: body.fromId,
              values: { quantity: body.fromExpectedQuantity - body.amount },
              where: { quantity: body.fromExpectedQuantity },
            },
            {
              op: "update",
              table: TABLE,
              tenantId: body.tenantId,
              partitionKey: body.toId,
              values: { quantity: body.toExpectedQuantity + body.amount },
              where: { quantity: body.toExpectedQuantity },
            },
          ],
          requestId: crypto.randomUUID(),
        });
        return json(result);
      } catch (err) {
        return errorJson(err);
      }
    }

    // GET /items: list this tenant's own rows via RPC tableScan() —
    // cursor-paginated, no arbitrary filter (see README.md and the main
    // CloudflareShard repo's README's "Tenant-scoped table scan" section).
    if (request.method === "GET" && url.pathname === "/items") {
      const tenantToken = url.searchParams.get("tenantToken");
      const tenantId = url.searchParams.get("tenantId");
      const cursor = url.searchParams.get("cursor");
      if (!tenantToken || !tenantId) {
        return json({ error: "Missing required query params: tenantToken, tenantId." }, 400);
      }
      try {
        const result = await env.SHARD_API.tableScan(tenantToken, { tenantId, table: TABLE, limit: 20, cursor });
        return json(result);
      } catch (err) {
        return errorJson(err);
      }
    }

    return json({ error: \`Unknown route: \${url.pathname}\` }, 404);
  },
};
`;

const README_MD = `# cloudflareshard-inventory-starter

A REAL, runnable CloudflareShard app — generated by Shardscope's "Build on
it" panel from the exact pattern the demo showed you: a Worker holding a
service binding straight into cloudflare-shard-mvp's \`CloudflareShardRpc\`
entrypoint (see the CloudflareShard project's \`examples/rpc-consumer/\` for
the canonical version of this pattern — this scaffold mirrors it).

**No secrets are baked into this download.** \`adminToken\`/\`tenantToken\` are
supplied as explicit request parameters when you call the setup/app routes
below — see "What this starter deliberately leaves out" at the bottom.

## How the binding works

\`wrangler.toml\` declares:

\`\`\`toml
[[services]]
binding = "SHARD_API"
service = "cloudflare-shard-mvp"
entrypoint = "CloudflareShardRpc"
\`\`\`

\`entrypoint\` names the specific exported class in the main CloudflareShard
Worker's \`src/index.ts\` (\`CloudflareShardRpc\`, a \`WorkerEntrypoint\`
subclass) to bind to. The security boundary is this binding existing in
this Worker's own config — there's no per-request credential the way an
HTTP call needs a bearer token. The token is still required and checked on
the other side of the binding, exactly as it would be over HTTP.

## Running it locally

You need **two** \`wrangler dev\` processes running at once — one for the
main CloudflareShard Worker, one for this app (identical to
\`examples/rpc-consumer\`'s own instructions in the CloudflareShard repo):

\`\`\`bash
# Terminal 1, from your cloudflare-shard-mvp checkout:
npm run dev   # defaults to http://localhost:8787

# Terminal 2, from this directory:
npm install
npm run dev -- --port 8788
\`\`\`

Wrangler's local dev registry connects the two automatically — you'll see
\`env.SHARD_API (cloudflare-shard-mvp#CloudflareShardRpc) [connected]\` in
this Worker's dev server output once both are up. If it says
\`[not connected]\`, start the main Worker's dev server first.

## Setup

1. If your cluster isn't initialized yet, do that once against the MAIN
   Worker directly (not this starter) — see the CloudflareShard root
   README's "Initialize cluster metadata and shard map":

\`\`\`bash
curl -X POST http://localhost:8787/admin/init \\
  -H "content-type: application/json" \\
  -H "authorization: Bearer $ADMIN_TOKEN" \\
  -d '{"numShards":4,"totalVBuckets":256}'
\`\`\`

2. Create this app's one table (run once per cluster):

\`\`\`bash
curl -X POST http://localhost:8788/setup/table \\
  -H "content-type: application/json" \\
  -d "{\\"adminToken\\":\\"$ADMIN_TOKEN\\"}"
\`\`\`

3. Register a tenant (run once per tenant — a customer, org, or
   environment of your app):

\`\`\`bash
curl -X POST http://localhost:8788/setup/tenant \\
  -H "content-type: application/json" \\
  -d "{\\"adminToken\\":\\"$ADMIN_TOKEN\\",\\"tenantId\\":\\"acme-corp\\"}"
\`\`\`

Response: \`{"tenantId":"acme-corp","tenantToken":"<save this>"}\` — returned
only this once, exactly like the real \`/admin/register-tenant\` route.
Export it:

\`\`\`bash
export TENANT_TOKEN=<token from the response above>
\`\`\`

## Using the app

\`\`\`bash
# Add an item
curl -X POST http://localhost:8788/items \\
  -H "content-type: application/json" \\
  -d "{\\"tenantToken\\":\\"$TENANT_TOKEN\\",\\"tenantId\\":\\"acme-corp\\",\\"id\\":\\"sku-001\\",\\"sku\\":\\"WIDGET-A\\",\\"name\\":\\"Widget A\\",\\"quantity\\":100}"

# Add a second item (the transfer below moves stock between the two)
curl -X POST http://localhost:8788/items \\
  -H "content-type: application/json" \\
  -d "{\\"tenantToken\\":\\"$TENANT_TOKEN\\",\\"tenantId\\":\\"acme-corp\\",\\"id\\":\\"sku-002\\",\\"sku\\":\\"WIDGET-B\\",\\"name\\":\\"Widget B\\",\\"quantity\\":0}"

# List this tenant's items
curl "http://localhost:8788/items?tenantToken=$TENANT_TOKEN&tenantId=acme-corp"

# Move 10 units from sku-001 to sku-002, atomically (a real two-phase commit)
curl -X POST http://localhost:8788/items/transfer \\
  -H "content-type: application/json" \\
  -d "{\\"tenantToken\\":\\"$TENANT_TOKEN\\",\\"tenantId\\":\\"acme-corp\\",\\"fromId\\":\\"sku-001\\",\\"fromExpectedQuantity\\":100,\\"toId\\":\\"sku-002\\",\\"toExpectedQuantity\\":0,\\"amount\\":10}"
\`\`\`

## What this starter deliberately leaves out

- **No auth layer of its own.** Every route trusts whatever \`tenantToken\`
  the caller presents — a real app authenticates its OWN users first and
  looks up (or mints) their tenantToken server-side, never accepts one as a
  raw client input the way this demo does for brevity.
- **No secondary index / \`/v1/index-query\` route.** This starter only
  exercises \`mutate\`, \`tx\`, and \`tableScan\` — see the CloudflareShard root
  README's "Tenant-scoped table scan" section, and Shardscope's Playground
  room (\`/api/play/index-query\`), for a live example of index-query if you
  need exact-tuple lookups.
- **Tokens as request params, not \`wrangler secret\`.** Once you're past
  "hello world", move \`ADMIN_TOKEN\` into \`wrangler secret put ADMIN_TOKEN\`
  and read it from \`env.ADMIN_TOKEN\` (add it to the \`Env\` interface in
  \`src/index.ts\`) instead of passing it on every setup call — the same
  pattern Shardscope's own dashboard Worker uses server-side (see its
  \`src/env.d.ts\`).

## Deployed (not just local dev)

Once both Workers are deployed to the same Cloudflare account, the service
binding resolves automatically — rename \`service\` in \`wrangler.toml\` if your
\`cloudflare-shard-mvp\` deployment uses a different Worker name.
`;

/** Generates the starter repo's file set. Pure — no Env access, no I/O — so
 * it can never embed a live secret (see this file's header comment) and is
 * trivially unit-testable. */
export function generateScaffoldFiles(): ScaffoldFile[] {
  return [
    { path: "wrangler.toml", content: WRANGLER_TOML },
    { path: "package.json", content: PACKAGE_JSON },
    { path: "tsconfig.json", content: TSCONFIG_JSON },
    { path: "schema.sql", content: SCHEMA_SQL },
    { path: "src/index.ts", content: SRC_INDEX_TS },
    { path: "README.md", content: README_MD },
  ];
}

// ============================================================================
// Dependency-free ZIP writer — "stored" (no compression) entries only. Small
// text files, so the size cost of skipping DEFLATE is negligible, and it
// keeps this file free of any external dependency for something as
// fiddly-to-get-subtly-wrong as compression. Format: local file header +
// raw bytes per entry, then one central directory record per entry, then a
// single end-of-central-directory record — the same three-section
// structure every ZIP reader (Windows Explorer, macOS Archive Utility,
// `unzip`, `zipfile` in Python, JSZip in a browser) expects. Verified
// end-to-end in build.test.ts (round-tripped through Node's built-in `zlib`
// gunzip-adjacent `DecompressionStream`... actually via a hand-rolled
// central-directory parse, since Workers/Node have no built-in zip reader —
// see that file for exactly what's asserted).
// ============================================================================

const CRC32_TABLE: number[] = (() => {
  const table = new Array<number>(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) {
      c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    }
    table[n] = c >>> 0;
  }
  return table;
})();

/** Standard ZIP/PNG CRC-32 (polynomial 0xEDB88320), computed over raw bytes.
 * Exported for direct unit testing against a couple of known vectors. */
export function crc32(bytes: Uint8Array): number {
  let crc = 0xffffffff;
  for (let i = 0; i < bytes.length; i++) {
    crc = CRC32_TABLE[(crc ^ bytes[i]) & 0xff] ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

// MS-DOS date/time encoding ZIP's local/central headers require. A fixed,
// arbitrary timestamp (2026-01-01 00:00:00) is used for every entry —
// deterministic output makes this generator's bytes reproducible/testable,
// and a starter repo's file mtimes carry no meaningful information anyway.
const DOS_TIME = 0;
const DOS_DATE = ((2026 - 1980) << 9) | (1 << 5) | 1;

function writeUint16LE(view: DataView, offset: number, value: number): void {
  view.setUint16(offset, value, true);
}
function writeUint32LE(view: DataView, offset: number, value: number): void {
  view.setUint32(offset, value >>> 0, true);
}

/** Builds a valid, dependency-free "stored" (uncompressed) ZIP archive from
 * `files`. Every entry uses forward-slash paths (the ZIP spec's required
 * separator, regardless of host OS) and UTF-8 encoded content/names — no
 * entry needs the UTF-8 filename flag's language-encoding bit in practice
 * here since every generated path is plain ASCII, but content is encoded as
 * UTF-8 regardless. */
export function buildZip(files: ScaffoldFile[]): Uint8Array {
  const encoder = new TextEncoder();
  const localParts: Uint8Array[] = [];
  const centralParts: Uint8Array[] = [];
  let offset = 0;

  for (const file of files) {
    const nameBytes = encoder.encode(file.path.replace(/\\/g, "/"));
    const contentBytes = encoder.encode(file.content);
    const crc = crc32(contentBytes);

    // Local file header (30 bytes fixed) + name + content.
    const local = new Uint8Array(30 + nameBytes.length + contentBytes.length);
    const localView = new DataView(local.buffer);
    writeUint32LE(localView, 0, 0x04034b50); // local file header signature
    writeUint16LE(localView, 4, 20); // version needed to extract (2.0)
    writeUint16LE(localView, 6, 0); // general purpose bit flag
    writeUint16LE(localView, 8, 0); // compression method: 0 = stored
    writeUint16LE(localView, 10, DOS_TIME);
    writeUint16LE(localView, 12, DOS_DATE);
    writeUint32LE(localView, 14, crc);
    writeUint32LE(localView, 18, contentBytes.length); // compressed size
    writeUint32LE(localView, 22, contentBytes.length); // uncompressed size
    writeUint16LE(localView, 26, nameBytes.length);
    writeUint16LE(localView, 28, 0); // extra field length
    local.set(nameBytes, 30);
    local.set(contentBytes, 30 + nameBytes.length);
    localParts.push(local);

    // Central directory header (46 bytes fixed) + name.
    const central = new Uint8Array(46 + nameBytes.length);
    const centralView = new DataView(central.buffer);
    writeUint32LE(centralView, 0, 0x02014b50); // central directory header signature
    writeUint16LE(centralView, 4, 20); // version made by
    writeUint16LE(centralView, 6, 20); // version needed to extract
    writeUint16LE(centralView, 8, 0); // general purpose bit flag
    writeUint16LE(centralView, 10, 0); // compression method: 0 = stored
    writeUint16LE(centralView, 12, DOS_TIME);
    writeUint16LE(centralView, 14, DOS_DATE);
    writeUint32LE(centralView, 16, crc);
    writeUint32LE(centralView, 20, contentBytes.length); // compressed size
    writeUint32LE(centralView, 24, contentBytes.length); // uncompressed size
    writeUint16LE(centralView, 28, nameBytes.length);
    writeUint16LE(centralView, 30, 0); // extra field length
    writeUint16LE(centralView, 32, 0); // file comment length
    writeUint16LE(centralView, 34, 0); // disk number start
    writeUint16LE(centralView, 36, 0); // internal file attributes
    writeUint32LE(centralView, 38, 0); // external file attributes
    writeUint32LE(centralView, 42, offset); // relative offset of local header
    central.set(nameBytes, 46);
    centralParts.push(central);

    offset += local.length;
  }

  const centralDirSize = centralParts.reduce((sum, part) => sum + part.length, 0);
  const centralDirOffset = offset;

  const end = new Uint8Array(22);
  const endView = new DataView(end.buffer);
  writeUint32LE(endView, 0, 0x06054b50); // end of central directory signature
  writeUint16LE(endView, 4, 0); // disk number
  writeUint16LE(endView, 6, 0); // disk with central directory
  writeUint16LE(endView, 8, files.length); // entries on this disk
  writeUint16LE(endView, 10, files.length); // total entries
  writeUint32LE(endView, 12, centralDirSize);
  writeUint32LE(endView, 16, centralDirOffset);
  writeUint16LE(endView, 20, 0); // comment length

  const totalSize = offset + centralDirSize + end.length;
  const result = new Uint8Array(totalSize);
  let pos = 0;
  for (const part of localParts) {
    result.set(part, pos);
    pos += part.length;
  }
  for (const part of centralParts) {
    result.set(part, pos);
    pos += part.length;
  }
  result.set(end, pos);
  return result;
}
