# Building a real application on CloudflareShard

This guide is for a developer evaluating CloudflareShard as the backend for a
real application — a multi-tenant SaaS product, most likely — and then
actually getting productive with it. It assumes real software engineering
competence and zero prior knowledge of this specific project. It is denser
than the other guides in `docs/guides/`; the payoff is that by the end you
should be able to design a schema, pick the right write path for each
operation, and know exactly where the sharp edges are before you hit them in
production.

Everything here is a companion to, not a replacement for,
[`docs/SPEC.md`](../SPEC.md) (the canonical protocol reference) and
[`client/README.md`](../../client/README.md) (the SDK/CLI reference). Where
this guide says "see SPEC §N", that section has the authoritative
request/response shape.

## 1. Mental model

CloudflareShard gives you one logical SQL database that is physically spread
across many Cloudflare Durable Objects. To use it well you need five
concepts, all defined precisely in SPEC §3–4.

**Tenant.** The unit of isolation. Every row you write belongs to exactly one
`tenantId`. A tenant authenticates with its own bearer token (from
`POST /admin/register-tenant`), structurally separate from the operator's
`ADMIN_TOKEN`. In this project's current self-hosted distribution model, the
deploying developer holds both the operator role and every tenant role — but
the two are kept structurally distinct in the code (SPEC §14), which is what
lets you build a genuinely multi-tenant app on top: each of your customers
can map to a `tenantId` with its own token, and the platform enforces that a
tenant's mutations and reads never cross into another tenant's rows.

**Table.** A logical table name registered once cluster-wide (`table_rules`
in the catalog schema, SPEC §5), with a required `partition_key_column` — the
column CloudflareShard uses to route every row. The physical `CREATE TABLE`
DDL is pushed identically to every shard.

**Partition key.** The value (usually your row's natural ID — an order ID, a
user ID) that determines which physical shard a row lives on. You supply it
explicitly on every write; there is no auto-increment or server-chosen key.

**Vbucket (virtual bucket).** The actual unit of data movement and ownership.
Routing never maps a row directly to a shard — it maps a row to one of a
fixed number of vbuckets, and the catalog separately maps each vbucket to
exactly one physical shard at a time (SPEC §4–5, `vbucket_map`). This
indirection is what makes online resharding possible: moving data between
shards just means changing which shard a vbucket points at, not touching
every row that happens to hash into it. `totalVBuckets` is set once at
`/admin/init` (default 1024, clamped 64–65536) and never changes — only the
vbucket→shard mapping does.

**Routing.** For any (`tenantId`, `table`, `partitionKey`) triple:

```
vbucket  = hash(`${tenantId}:${table}:${partitionKey}`) mod totalVBuckets
shardId  = vbucket_map[vbucket]   // looked up from the catalog, changes over time
```

The hash is FNV-1a 32-bit over the composite string (SPEC §4). Rebalancing
(split/drain, SPEC §11) only ever changes `vbucket_map` entries — the hash
function itself never changes, so the vbucket a given row belongs to is
permanent even as the shard that owns that vbucket moves.

**Catalog shard.** The control plane (table registry, vbucket map, tenant
auth, indexes) is itself sharded, across a fixed set of catalog shards
(`catalog-0`, `catalog-1`, ... — count set by `CATALOG_SHARD_COUNT`, default
4). A tenant's catalog shard is `hash(tenantId) mod CATALOG_SHARD_COUNT` — no
lookup step, which avoids the bootstrapping problem of the metadata store
needing to shard itself recursively (SPEC §4). You mostly don't need to think
about this day to day; it matters when you call a `catalogShardId`-scoped
admin route like `splitVbucket` or `drainShard`.

Put together: a write for `(tenantId="acme", table="invoices",
partitionKey="inv-9137")` deterministically resolves to one vbucket, and that
vbucket is owned by exactly one physical `ShardDO` right now. Two rows with
different partition keys can land on different shards even for the same
tenant and table — that's the whole point (horizontal scale) — but a given
row is always single-shard, which is what lets CloudflareShard give you real
ACID semantics on a per-row/per-partition-key basis without a distributed
transaction, most of the time (see §2 below for when you do need one).

## 2. The write and read paths, and when to use each

There are three ways to write and two ways to read. Pick based on the shape
of the operation, not habit.

### `/v1/mutate` — single-row, tenant-scoped, idempotent

The workhorse. One `StructuredMutation` (`insert`/`update`/`delete`/`upsert`)
against one partition key, executed with strong local ACID semantics on its
one shard. Row ownership is structural, not conditional: the partition-key
predicate is always ANDed into `update`/`delete`'s `WHERE` clause and
force-set into `insert`/`upsert`'s `values`, so there is no way for a mutation
to touch more than the one row/partition it names, even if you pass a
permissive `where` (SPEC §7).

Idempotent via `requestId` (client-generated if you omit it): a retried call
with the same `requestId` and the same `(sql, params)` gets back the original
result instead of re-executing; a retried call with the same `requestId` but
*different* params is rejected 409 rather than silently doing the wrong thing
(SPEC §9). This is what makes it safe to retry a `/v1/mutate` call after a
network timeout without double-applying it.

```ts
const result = await tenant.insert("events", "acme", "e1", {
  user_id: "user-1",
  body: "hello",
});
// result.rowsAffected === 1
```

Use `/v1/mutate` for anything that's naturally scoped to one partition key —
which, if your schema is well-designed, is most of your write traffic.

### `/v1/tx` — cross-shard atomic 2PC

For a write that must span more than one partition key (and therefore
potentially more than one shard) atomically — e.g. "debit this account,
credit that one" or "insert an order header and its first line item in one
commit." `/v1/tx` drives a real two-phase commit across every shard the
mutation set touches (`CoordinatorDO`, SPEC §10): prepare fans out to every
participant, and either every participant commits or the whole thing aborts.

Constraints (SPEC §7, §10):
- 1 or more `StructuredMutation`s, capped at **8 distinct
  `(tenantId, table, partitionKey)` rows** per call — not 8 shards; several
  of those 8 rows can happen to live on the same shard, which still only
  counts once against your actual shard-participant count but each still
  counts toward the 8-row cap.
- Every mutation in the batch must share the same `tenantId` — `/v1/tx` has
  no concept of a cross-tenant transaction.
- `requestId` is **required** here (unlike `/v1/mutate`, where it's
  optional) — it's the whole transaction's idempotency key, not a per-write
  convenience. The SDK fills one in for you if you don't pass one.

```ts
await tenant.tx([
  { op: "insert", table: "events", tenantId: "acme", partitionKey: "e2", values: { user_id: "user-1", body: "a" } },
  { op: "insert", table: "events", tenantId: "acme", partitionKey: "e3", values: { user_id: "user-1", body: "b" } },
]);
```

**Optimistic concurrency inside `/v1/tx` is now real — read this carefully if
you're evaluating this project.** A `StructuredMutation`'s optional `where`
lets you attach a compare-and-swap guard to an `update`/`delete` (e.g. "only
apply this if `balance` still equals the value I read"). As of the fix that
landed most recently on `main`, a guard that matches zero rows correctly
**aborts the entire transaction** with a retryable `409` — every participant
that had already prepared is rolled back, and your application code gets a
real signal that its write never landed. Before this fix, a zero-match guard
inside `/v1/tx` was silently applied as a no-op while the transaction still
reported `committed` — because `/v1/tx` doesn't return a per-mutation
`rowsAffected` the way `/v1/mutate` does, that failure was completely
invisible to the caller. If you're building anything that does CAS-style
updates inside a multi-row transaction (a hot inventory counter touched
alongside an order-line insert, for example), you need the fixed behavior —
don't build around the old one. See §4 below for exactly what the error looks
like and how to handle it.

### `/v1/sql` — admin-only escape hatch

Raw SQL against one partition, gated on `ADMIN_TOKEN`. **Not available to
tenant code, at all** — this used to have a trust-based tenant path, and it
was removed (SPEC §14) because the per-tenant write guard against a
passthrough SQL string proved structurally unwinnable (it leaked six
different ways: mixed case, inter-token comments, schema-qualifiers, quoted
identifiers...), and there is no safe tenant `SELECT` over arbitrary SQL —
base rows carry no physical `tenant_id` column, so a raw partition-scoped
read could return another tenant's rows that hash into the same vbucket.
Treat `/v1/sql` as an operator/debugging tool only, not part of your
application's write or read path. Even for the operator, a mutation whose
write *target* is an internal bookkeeping table (`applied_requests`,
`__cf_*`, etc.) is rejected 403.

### Reads: `/v1/index-query` and `/v1/table-scan`

**`/v1/index-query`** — exact full-tuple secondary-index lookup. You register
an index on one or more columns (`POST /admin/create-index`), and query it
with a value for *every* covered column (leftmost-prefix lookups are not
supported — SPEC §7). This is the fast path for "find the row(s) where
`user_id = X`" — the kind of query you'd otherwise need a table scan for.

```ts
await admin.createIndex({ indexName: "events_by_user", table: "events", columns: ["user_id"] });
await admin.waitForIndexReady("events_by_user");

const { rows } = await tenant.indexQuery({
  table: "events",
  indexName: "events_by_user",
  tenantId: "acme",
  values: { user_id: "user-1" },
});
```

**`/v1/table-scan`** — a tenant-scoped, cursor-paged scan of a tenant's own
rows in a table, with **no arbitrary column filtering**: the query is
mechanically constructed from `table + tenantId + cursor + limit` only, the
same safe-by-construction discipline `/v1/mutate` uses for writes (SPEC §7).
Default page size 100, hard max 500.

```ts
for await (const page of tenant.tableScanAll({ tenantId: "acme", table: "events" })) {
  console.log(page.length, "rows in this page");
}
```

Two things worth knowing up front about table-scan: it requires the table's
partition key to be verified unique and BINARY-collated
(`table_rules.partition_key_unique`, computed automatically at
`/admin/create-table` time — you don't set this yourself) or it rejects 409
`PARTITION_KEY_NOT_UNIQUE`; and it can return duplicate rows during an active
vbucket migration (see §5). Neither of these should surprise you if you
create tables the normal way (`admin.createTable`) and read §5 before you
rely on scan results for anything migration-sensitive.

## 3. Getting started

### Deploy a cluster

Don't hand-roll this — the reference doc's
[Deploy your own cluster](../REFERENCE.md#deploy-your-own-cluster) section
has the one-click Deploy-to-Cloudflare flow, the `ADMIN_TOKEN` secret setup,
and cost/teardown details (Durable Objects require Workers Paid, and
everything is billed to your own account). Follow that, then come back here
once you have a Worker URL and an `ADMIN_TOKEN`.

### Initialize topology

```ts
import { CloudflareShardAdminClient, CloudflareShardClient } from "cloudflare-shard-client";

const baseUrl = "https://<your-worker>.workers.dev";
const admin = new CloudflareShardAdminClient({ baseUrl, token: process.env.ADMIN_TOKEN! });

await admin.init({ numShards: 4, totalVBuckets: 256 });
```

`numShards` and `totalVBuckets` are cluster-wide constants set once. Why
`totalVBuckets` matters even though you'll probably never touch it again:
it's the resolution of your future resharding — every vbucket eventually
moves as a unit, so more vbuckets means finer-grained rebalancing later at
the cost of a bit more bookkeeping now. 256–1024 is a reasonable range for
most applications; you don't need thousands unless you're planning for a
very large number of physical shards eventually. `numShards` is how many
physical `ShardDO`s exist right now — you can add more later via
`splitVbucket`/`migrateVbucket`, so don't over-provision this at day one.

### Register a table

```ts
await admin.createTable({
  table: "invoices",
  schema: "CREATE TABLE invoices (id TEXT PRIMARY KEY, tenant_ref TEXT, amount_cents INTEGER, status TEXT, created_at TEXT)",
  partitionKeyColumn: "id",
});
```

Why `partitionKeyColumn` is mandatory and not inferred: it's the column that
lets `/v1/mutate` and `/v1/tx` structurally enforce that a mutation only ever
touches the one row/partition it claims to touch (SPEC §7) — without a
declared partition key column, the platform can't inject that predicate, and
routing itself has nothing to hash on. Choose it like you'd choose a
partition key in any sharded system: a value with high cardinality relative
to your access patterns, ideally the same ID you already look rows up by.
Two things to know before you commit to one: `createTable` rejects a schema
containing `IF NOT EXISTS` (400 `SCHEMA_IF_NOT_EXISTS_NOT_ALLOWED` — it needs
the DDL push to genuinely apply everywhere it claims to, see SPEC §7), and
repointing an already-configured partition key column later is refused
outright (409 `PARTITION_KEY_ALREADY_SET`) because any existing row
provenance was written keyed by the old column's values — get this right
before you have real data in the table.

### Register a tenant

```ts
const { token } = await admin.registerTenant({ tenantId: "acme" });
const tenant = new CloudflareShardClient({ baseUrl, token });
```

The returned `token` is plaintext, returned exactly once — store it (e.g. as
a secret associated with that customer/tenant in your own system). This is
the credential your application uses on behalf of that tenant from here on;
`ADMIN_TOKEN` should never be embedded in tenant-facing code paths.

### Write your first rows

```ts
await tenant.insert("invoices", "acme", "inv-1001", {
  tenant_ref: "acme",
  amount_cents: 129900,
  status: "open",
  created_at: new Date().toISOString(),
});
```

### Run a cross-shard transaction

```ts
await tenant.tx([
  { op: "update", table: "accounts", tenantId: "acme", partitionKey: "acct-main", values: { balance_cents: 4870100 }, where: { balance_cents: 5000000 } },
  { op: "insert", table: "invoices", tenantId: "acme", partitionKey: "inv-1002", values: { tenant_ref: "acme", amount_cents: 129900, status: "open", created_at: new Date().toISOString() } },
]);
```

Two independent partition keys (`acct-main`, `inv-1002`) very likely hash to
two different vbuckets and therefore, in general, two different physical
shards — `/v1/tx` is what lets you commit both atomically anyway. If you find
yourself reaching for `/v1/tx` on *every* write, it's worth reconsidering
your partition key choice: the fast, cheap, strongly-consistent path is
`/v1/mutate` against a single partition key, and `/v1/tx` exists for the
genuinely cross-partition cases, not as your default write path.

## 4. Gotchas you need to know before you build on this

**No unique-index enforcement.** A registered secondary index does not
reject a write that would create a duplicate value — `__cf_indexes` uses
`INSERT OR REPLACE` with no constraint check, and this was deliberately
scoped out (TODOS.md's "Unique-index support" item; real enforcement needs
either a `UNIQUE` constraint at the index-shard level or explicit
pre-check-plus-lock coordination to close a race between two concurrent
writers both claiming to be first — genuine design work, not a small
addition). If your schema needs "email must be unique per tenant," you must
enforce it yourself at the application layer (e.g. a compare-and-swap insert
against a partition key derived from the value you need unique, so SQLite's
own `PRIMARY KEY` does the enforcing) — don't rely on a registered index for
this.

**`/v1/tx` is capped at 8 distinct participant rows.** Design transactions
that stay under this deliberately; if a single logical operation needs more
than 8 distinct `(tenantId, table, partitionKey)` rows atomically, you likely
need to restructure it (e.g. split it into a small atomic "commit point" plus
asynchronous fan-out) rather than expect the cap to move.

**`/v1/scatter` and `/v1/table-scan` can return duplicate — never missing —
rows during an active vbucket-migration window.** This is a resharding
side-effect (see §5) and a documented limitation, not a bug: during the
`backfilling` phase of a migration, a row can transiently exist on both the
source and target shard, and a scan spanning both can see it twice before
returning it once from the source and once from the target. If your
application logic does anything non-idempotent with scanned rows (e.g.
"charge this customer for every row you see"), make sure it dedupes by
partition key, since resharding can happen at any time without your
application being involved.

**Raw `/v1/sql` is admin-only — there is no per-tenant SQL access, full
stop.** This was covered in §2, but it bears repeating here because it's the
single most common assumption a developer coming from a normal SQL database
brings: you cannot give a tenant's code raw SQL access to "just this
tenant's rows," because the platform has no safe way to enforce that
boundary over arbitrary SQL. Build your application entirely on
`/v1/mutate`, `/v1/tx`, `/v1/index-query`, and `/v1/table-scan`.

**Compare-and-swap via `where` — now genuinely safe, but you must check the
result.** Both write paths support a `where` guard for optimistic
concurrency, and both now fail loudly on a stale guard instead of silently
dropping your write:

- `/v1/mutate`: a `where` that matches zero rows returns `{ok: true,
  rowsAffected: 0}` — a normal, successful HTTP response. **You must check
  `rowsAffected` yourself**; the call does not throw.
- `/v1/tx`: a `where` that matches zero rows on any participant aborts the
  *entire* transaction with `409 TX_ABORTED`, whose `details` carry the
  specific participant's `409 TX_PARTICIPANT_GUARD_MISMATCH` (message: "Row
  `<table>:<partitionKey>` changed concurrently — its WHERE guard no longer
  matched any row.", fix: "Re-read the row's current state and retry with
  fresh values."). This is a real fix, not the original behavior — an
  earlier version of `/v1/tx` applied a zero-match guard as a silent no-op
  while still reporting the transaction `committed`, with no way for the
  caller to know its own write never landed. If you're building on this
  project today, you get the corrected behavior; just make sure your retry
  logic actually handles the 409 rather than assuming `/v1/tx` always either
  fully applies or throws before doing anything.

A correct retry loop for each:

```ts
import { CloudflareShardError } from "cloudflare-shard-client";

// /v1/mutate: check rowsAffected, retry against fresh state.
async function creditBalance(tenant: CloudflareShardClient, tenantId: string, acctKey: string, delta: number) {
  for (let attempt = 0; attempt < 5; attempt++) {
    const [current] = (await tenant.indexQuery({ table: "accounts", indexName: "accounts_by_key", tenantId, values: { key: acctKey } })).rows;
    const result = await tenant.update(
      "accounts",
      tenantId,
      acctKey,
      { balance_cents: (current.balance_cents as number) + delta },
      { balance_cents: current.balance_cents }, // guard: only if unchanged since our read
    );
    if (result.rowsAffected > 0) return;
    // rowsAffected === 0 — someone else updated this row between our read and our write. Loop and retry.
  }
  throw new Error(`gave up updating ${acctKey} after 5 attempts — persistent contention`);
}

// /v1/tx: catch the abort, re-read fresh state, retry the whole transaction.
// readAccountBalance() below is application code -- some read of your own
// (an indexQuery or tableScan against the "accounts" table), not an SDK call.
async function transferAtomically(tenant: CloudflareShardClient, tenantId: string, fromKey: string, toKey: string, amount: number) {
  for (let attempt = 0; attempt < 5; attempt++) {
    const fromBalance = await readAccountBalance(tenant, tenantId, fromKey);
    const toBalance = await readAccountBalance(tenant, tenantId, toKey);
    try {
      await tenant.tx([
        { op: "update", table: "accounts", tenantId, partitionKey: fromKey, values: { balance_cents: fromBalance - amount }, where: { balance_cents: fromBalance } },
        { op: "update", table: "accounts", tenantId, partitionKey: toKey, values: { balance_cents: toBalance + amount }, where: { balance_cents: toBalance } },
      ]);
      return;
    } catch (err) {
      if (err instanceof CloudflareShardError && err.code === "TX_ABORTED") {
        continue; // stale guard on one participant (or another failure) -- re-read both rows and retry the whole tx
      }
      throw err;
    }
  }
  throw new Error(`gave up transferring ${fromKey} -> ${toKey} after 5 attempts`);
}
```

## 5. Online resharding — what a builder needs to know

Splitting a shard, draining a shard, and migrating a vbucket
(`/admin/split-vbucket`, `/admin/drain-shard`, `/admin/migrate-vbucket`) are
operator actions, not something your application code triggers or needs to
be aware of most of the time. The operator-level detail — the backfill /
fenced-cutover algorithm, drain evacuation, the topology lock — lives in
`docs/guides/operator.md`.

The one thing you as a builder should carry forward from §4 above: while a
migration is in flight, `/v1/scatter` and `/v1/table-scan` can return
duplicate rows (never missing ones) for the vbucket being moved. This is
inherent to the dual-write backfill design (SPEC §11) — writes keep landing
on the source shard as authoritative while the catalog copies rows to the
target in the background, so a scan spanning both can transiently observe a
row twice. It only affects scan/fan-out reads, never `/v1/mutate`,
`/v1/tx`, or `/v1/index-query` (which always resolve to the current
authoritative shard). If your application-level idempotency already dedupes
by partition key wherever it consumes scan results — which it should, given
the guidance in §4 — this requires no special handling; it's called out here
so you know it's expected behavior during a migration window, not corruption.

## 6. When not to use this

Be honest with yourself about the shape of your workload before committing:

- **No cross-tenant analytics without app-level work.** "Total orders across
  all tenants yesterday" has no built-in query path — `/v1/scatter` (admin
  bearer only, full-cluster SQL fan-out) plus your own aggregation code is
  the only route today (TODOS.md documents this as an intentional design
  tradeoff, not a gap waiting to be filled: per-tenant/per-shard isolation
  is the point of the architecture). If your product's core value is
  cross-tenant analytics, plan on exporting to a real OLAP store rather than
  building that path on top of CloudflareShard directly.
- **No arbitrary column filtering on `/v1/table-scan`.** It's `table +
  tenantId + cursor + limit`, full stop — if you need "give me this
  tenant's rows where `status = 'open' AND amount > 100'`, you register a
  secondary index for the columns you actually filter on and use
  `/v1/index-query`, or you scan and filter client-side. There's no
  server-side predicate escape hatch for tenant code (raw `/v1/sql` is
  admin-only, per §2).
- **This is young software without a large production track record.** The
  most substantial live evidence so far is a TPC-C-style benchmark
  (`examples/tpc-c-benchmark`, and shardscope's load-testing panel) run
  against a real deployment — the largest documented run so far is 24
  warehouses, ~57,500 rows, 180 seconds at 30-concurrency (TODOS.md). That's
  a real signal, not a synthetic unit test, but it's a modest-scale, short-duration
  run — not evidence of long-running production behavior at real scale.
  Treat this as infrastructure you can evaluate seriously and build a real
  application on, not as something with years of production hardening
  behind it.

## Further reading

- [`docs/SPEC.md`](../SPEC.md) — the full protocol/architecture reference:
  every route's exact request/response shape, the routing algorithm,
  idempotency contract, transaction semantics, and the security/multi-tenancy
  model.
- [`client/README.md`](../../client/README.md) — the full SDK/CLI reference,
  including error handling (`CloudflareShardError`) and what's covered vs.
  not.
- [`examples/rpc-consumer/README.md`](../../examples/rpc-consumer/README.md)
  — calling this API from another Cloudflare Worker over a Durable Object
  RPC / service binding instead of HTTP, if your consumer is itself a
  Worker in the same account.
- [`TODOS.md`](../../TODOS.md) — open roadmap items (unique-index support,
  automatic split heuristics, cross-tenant analytics) and the history of
  what's already been resolved.
- `docs/guides/operator.md` — operator-level detail on splitting, draining,
  and migrating shards, if you end up running this cluster yourself.
