<p align="center">
  <img src="docs/images/logo.png" alt="CloudflareShard" width="600" />
</p>

<p align="center">
  <a href="https://cloudflare-shard-shardscope.ajill.workers.dev/?demo=1"><strong>Live demo</strong></a> ·
  <a href="#deploy-your-own"><strong>Deploy your own</strong></a> ·
  <a href="docs/guides/end-user.md"><strong>Docs</strong></a> ·
  <a href="CONTRIBUTING.md"><strong>Contributing</strong></a>
</p>

<p align="center">
  <a href="LICENSE"><img alt="License: Apache-2.0" src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" /></a>
  <a href="https://developers.cloudflare.com/durable-objects/"><img alt="Built on Durable Objects" src="https://img.shields.io/badge/built%20on-Workers%20%2B%20Durable%20Objects-f38020.svg" /></a>
</p>

A self-hosted, sharded SQL layer built entirely on Cloudflare Workers and Durable
Objects (SQLite-backed). No external database, no KV/D1/R2. One logical SQL
endpoint, tenant-scoped writes, cross-shard 2PC transactions, secondary
indexes, and online resharding (split/drain with a zero-downtime,
checksum-verified cutover), plus fail-closed fleet point-in-time restore.

Sharding is the primitive. Everything else (routing, transactions, secondary
indexes, resharding) sits on top of the same vBucket map.

## See it live

**[Try the live demo](https://cloudflare-shard-shardscope.ajill.workers.dev/?demo=1).**
It's a real dashboard (Shardscope) that visualizes topology, drives live load,
splits/drains/migrates shards under fire, and runs chaos attacks while a
correctness meter proves nothing was lost. Sample-data mode simulates the
topology/load walkthrough client-side: free, safe, no login needed. Driving
a real reshard or chaos attack needs your own live cluster. New here? Start
with the [end-user guide](docs/guides/end-user.md).

## Who this is for

- **Building an app on it?** → [Infrastructure builder guide](docs/guides/infrastructure-builder.md)
- **Running a cluster in production?** → [Operator guide](docs/guides/operator.md)
- **Evaluating the technology/architecture bet?** → [Investor guide](docs/guides/investor.md)
- **Just exploring?** → [End-user guide](docs/guides/end-user.md)

## Run it locally

```powershell
git clone https://github.com/ajillepalli/CloudflareShard.git
cd CloudflareShard
npm install
npm run dev
```

Needs Node.js 20+ and a Cloudflare account with Wrangler authenticated.
`npm run dev` starts both required Workers from their two Wrangler configs: the
route-less `cloudflare-shard-control-plane` Worker that owns
`JournalManifestDO` and `FleetManifestCatalogDO`, and the public
`cloudflare-shard-mvp` Worker that owns `CatalogDO`, `ShardDO`,
`CoordinatorDO`, and the non-restored `RestoreCoordinatorDO`. The public
Worker must not be run without that local `CONTROL_PLANE` service-binding
target.

## Deploy your own

Cloudflare's Deploy to Cloudflare flow does not deploy multiple Worker
applications from one repository together, so it cannot create this complete
topology. Clone the repository and use the ordered deployment command below;
do not use a one-Worker deploy button for this release.

The deployment has two Workers and six SQLite Durable Object classes: a
required route-less control-plane Worker owning `JOURNAL_MANIFEST` and
`FLEET_MANIFEST_CATALOG`, followed by the public Worker owning `CATALOG`,
`SHARD`, `COORDINATOR`, and `RESTORE_COORDINATOR`. No KV/D1/R2 resources are
required; the cluster is self-contained.

One deployment is one physical restore domain. Set `DEPLOYMENT_FLEET_ID` in
`wrangler.toml` (default `default`) before the first transaction and keep that
value stable for the lifetime of the Durable Object namespaces. A restore
preview whose `fleet_id` differs from it is rejected; logical fleets sharing
these namespaces cannot be rewound independently.

From a clone, use the ordered aggregate command:

```bash
npm run deploy
```

It deploys `cloudflare-shard-control-plane` first, then deploys
`cloudflare-shard-mvp` with its `CONTROL_PLANE` service binding. The equivalent
manual sequence is `npm run deploy:control-plane` followed by
`npm run deploy:root`; reversing or skipping the first step leaves the root
Worker without its mandatory commit-manifest dependency.

**Cost:** SQLite-backed Durable Objects are available on both Workers Free and
Paid. The Free plan is a bounded evaluation path: Cloudflare currently limits
it to 100,000 Worker requests/day, 5 million Durable Object rows read/day, and
100,000 rows written/day; operations fail after a daily limit is exhausted.
Check the current official [Durable Objects pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/)
and [Workers limits](https://developers.cloudflare.com/workers/platform/limits/)
before relying on those numbers. This is a real database in your account, not
a sandbox. Tear the two-Worker deployment down with `npm run delete` when done.
That command deliberately deletes the public root Worker first and the
route-less control-plane Worker second, so no surviving gateway can start a
transaction against a missing manifest service. Do not delete the control
plane first.

After setting the `ADMIN_TOKEN` secret, use the tested CLI preflight and
disposable-target verifier:

```bash
cd client && npm install && npm run build && cd ..
export CLOUDFLARESHARD_URL=https://<your-worker>.workers.dev
export CLOUDFLARESHARD_ADMIN_TOKEN=<your-ADMIN_TOKEN>
node client/dist/cli.js doctor
node client/dist/cli.js verify --disposable-target
```

`verify` never force-resets an initialized topology. It creates and retains an
isolated table and tenant, so the explicit flag is only appropriate for a
deployment you intend to tear down. It proves a transaction across two
distinct physical placements, idempotent replay, and tenant-scoped readback.
Receipts are written under `.cloudflareshard/receipts/`. A durable decision
whose manifest registration or participant acknowledgement remains outstanding
after idempotent replay is reported as `PENDING_RECONCILIATION`, never
`VERIFIED`. Verification defers strict readback until replay reports that the
transaction has converged to committed.

Fleet point-in-time restore is an explicit preview/confirm workflow. Preview
produces an immutable plan and SHA-256 plan hash; execute requires that exact
hash, fences all ordinary traffic, rewinds every physical shard, replays
manifest-committed transactions through the cutoff, and verifies shard
invariants. Only then does it durably discard post-cutoff transaction
coordinators and report hash-bound evidence for intentionally discarded writes.
While the original fence remains active and before that irreversible discard,
the same plan retains time-bounded shard undo bookmarks for an exact-hash
rollback of a partial restore.
Use the [fleet PITR runbook](docs/runbooks/fleet-pitr.md);
do not execute from a quickstart snippet. Production readiness requires 3/3
successful live provider rehearsals.

Full deploy/init/teardown walkthrough:
[docs/REFERENCE.md § Deploy your own cluster](docs/REFERENCE.md#deploy-your-own-cluster).

## Use it from your app

`client/` is a typed TypeScript SDK + CLI wrapping the whole HTTP API.
Recommended over hand-writing `fetch()`/`curl` calls. Full reference:
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

## Checks

```bash
npm run typecheck   # tsc --noEmit
npm test            # vitest run: backend suite (workerd)
npm run test:spa    # vitest run --config vitest.spa.config.ts: Shardscope SPA suite (jsdom)
npm run verify      # aggregate root, SPA, client, contracts, control-plane, and public benchmark gate
```

## Learn more

- **[docs/REFERENCE.md](docs/REFERENCE.md)**: deploy-your-own-cluster details, the
  full API walkthrough, catalog sharding, tenant authorization, RPC access,
  known limitations, and observability.
- **[docs/SPEC.md](docs/SPEC.md)**: the canonical protocol/architecture reference: schemas,
  every HTTP route's request/response shape, the routing algorithm, write-idempotency contract,
  transaction semantics, rebalancing/split/drain, and the security/multi-tenancy model.
- **[docs/guides/](docs/guides/)**: perspective guides: [end-user](docs/guides/end-user.md),
  [infrastructure builder](docs/guides/infrastructure-builder.md),
  [operator](docs/guides/operator.md), [investor](docs/guides/investor.md).
- **[docs/runbooks/fleet-pitr.md](docs/runbooks/fleet-pitr.md)**: destructive
  fleet restore preview, exact-hash execution, monitoring, fenced reconciliation,
  loss reporting, and the 3/3 live rehearsal gate.
- **[client/README.md](client/README.md)**: the typed TypeScript SDK + CLI reference.
- **[examples/rpc-consumer/README.md](examples/rpc-consumer/README.md)**: calling this API over
  a Durable Object RPC / Worker service binding instead of HTTP.
- **[examples/shardscope/README.md](examples/shardscope/README.md)**: the live mission-control
  dashboard's own documentation.
- **[TODOS.md](TODOS.md)**: the open roadmap and resolved milestones.
- **[CHANGELOG.md](CHANGELOG.md)**: release history.
- **[SECURITY.md](SECURITY.md)**: how to report a vulnerability.
- **[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)**: community standards for this project.

## License

[Apache-2.0](LICENSE). CloudflareShard is an independent project, not
affiliated with or endorsed by Cloudflare, Inc.
