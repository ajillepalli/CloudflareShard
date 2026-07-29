# CloudflareShard MVP

A self-hosted, sharded SQL layer built entirely on Cloudflare Workers + Durable
Objects (SQLite-backed) — no external database, no KV/D1/R2. One logical SQL
endpoint, tenant-scoped writes, cross-shard 2PC transactions, secondary
indexes, and online resharding (split/drain with a zero-downtime,
checksum-verified cutover).

**[Try the live demo](https://cloudflare-shard-shardscope.ananth-jillepalli.workers.dev/?demo=1)**
— a real dashboard (Shardscope) that visualizes topology, drives live load,
splits/drains/migrates shards under fire, and runs chaos attacks while a
correctness meter proves nothing was lost. Sample-data mode simulates the
topology/load walkthrough client-side — free, safe, no login needed; driving
a real reshard or chaos attack needs your own live cluster. New here? Start
with the [end-user guide](docs/guides/end-user.md).

## Who this is for

- **Building an app on it?** → [Infrastructure builder guide](docs/guides/infrastructure-builder.md)
- **Running a cluster in production?** → [Operator guide](docs/guides/operator.md)
- **Evaluating the technology/architecture bet?** → [Investor guide](docs/guides/investor.md)
- **Just exploring?** → [End-user guide](docs/guides/end-user.md)

## Prerequisites

- Node.js 20+
- Cloudflare account + Wrangler authentication

## Setup

```powershell
git clone https://github.com/ajillepalli/CloudflareShard.git
cd CloudflareShard
npm install
```

## Run locally

```powershell
npm run dev
```

## Deploy

```powershell
npm run deploy
```

Want to deploy your **own** cluster to your own Cloudflare account with a
single click instead? See [Deploy your own cluster](docs/REFERENCE.md#deploy-your-own-cluster)
in the reference doc.

## Quick API example

`client/` is a typed TypeScript SDK + CLI wrapping the whole HTTP API —
recommended over hand-writing `fetch()`/`curl` calls. Full reference:
[`client/README.md`](client/README.md).

```ts
import { CloudflareShardAdminClient, CloudflareShardClient } from "cloudflare-shard-client";

const admin = new CloudflareShardAdminClient({ baseUrl: "http://127.0.0.1:8787", token: process.env.ADMIN_TOKEN! });
await admin.init({ numShards: 4, totalVBuckets: 256 });
await admin.createTable({
  table: "events",
  schema: "CREATE TABLE events (id TEXT PRIMARY KEY, user_id TEXT, body TEXT)",
  partitionKeyColumn: "id",
});

const { token } = await admin.registerTenant({ tenantId: "t1" });
const tenant = new CloudflareShardClient({ baseUrl: "http://127.0.0.1:8787", token });
await tenant.insert("events", "t1", "e1", { user_id: "user-1", body: "hello" });
```

For the full walkthrough (transactions, resharding, the CLI, and screenshots
of a real live run) see [`docs/REFERENCE.md`](docs/REFERENCE.md#full-api-walkthrough).

## Learn more

- **[docs/REFERENCE.md](docs/REFERENCE.md)** — deploy-your-own-cluster details, the
  full API walkthrough, catalog sharding, tenant authorization, RPC access,
  known limitations, and observability.
- **[docs/SPEC.md](docs/SPEC.md)** — the canonical protocol/architecture reference: schemas,
  every HTTP route's request/response shape, the routing algorithm, write-idempotency contract,
  transaction semantics, rebalancing/split/drain, and the security/multi-tenancy model.
- **[docs/guides/](docs/guides/)** — perspective guides: [end-user](docs/guides/end-user.md),
  [infrastructure builder](docs/guides/infrastructure-builder.md),
  [operator](docs/guides/operator.md), [investor](docs/guides/investor.md).
- **[client/README.md](client/README.md)** — the typed TypeScript SDK + CLI reference.
- **[examples/rpc-consumer/README.md](examples/rpc-consumer/README.md)** — calling this API over
  a Durable Object RPC / Worker service binding instead of HTTP.
- **[examples/shardscope/README.md](examples/shardscope/README.md)** — the live mission-control
  dashboard's own documentation.
- **[TODOS.md](TODOS.md)** — the open roadmap and resolved milestones.
- **[CHANGELOG.md](CHANGELOG.md)** — release history.

## License

Apache-2.0 — see `LICENSE`.
