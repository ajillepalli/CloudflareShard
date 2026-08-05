# Deploy your own CloudflareShard cluster

> **The former one-Worker Deploy button is retired.** Cloudflare does not deploy
> multiple Worker applications from one repository together, while the current
> cluster requires both the route-less control-plane Worker and the public
> Worker. Use the repo-root [`README.md`](../../../../README.md) instructions and
> the ordered `npm run deploy` command. Live deploy→teardown qualification is
> still pending; see [`NOTES.md`](./NOTES.md).

Spin up your own CloudflareShard cluster — the same multi-tenant, sharded,
transactional database Shardscope demos — in your own Cloudflare account from
a repository clone.

## What this creates (and what it costs)

The ordered root deployment command deploys **to your own Cloudflare account**.
It provisions:

- **Two Workers** — the required route-less `cloudflare-shard-control-plane`
  service first, then the public `cloudflare-shard-mvp` gateway.
- **Four SQLite Durable Object classes** — `JOURNAL_MANIFEST` in the control
  plane, plus `CATALOG`, `SHARD`, and `COORDINATOR` in the public Worker.

There are **no** KV/D1/R2/Queue resources. The two-Worker cluster is
self-contained.

**Cost — read this.** SQLite-backed Durable Objects are available on Workers
Free and Paid, subject to the current plan limits, and everything created here
is **billed to your account**. This is a real database in your account, not a
sandbox. Check the current official pricing and limits before a run, and tear
the cluster down when you're done.

Deploy from the repository root:

```bash
npm install
npm run deploy
```

This deploys the control plane first and the public Worker second. Do not
reverse or skip those steps.

## After deploy: set your admin token

The cluster gates its whole `/admin/*` surface on a secret, `ADMIN_TOKEN`. Set
it to a **strong random value** (`openssl rand -hex 32`) — anyone with it can
init, reshard, or drop your cluster. If it's unset the Worker returns
`500 ADMIN_TOKEN is not configured`.

Set it on the public Worker with Wrangler, then initialize the cluster from your
machine against the new public Worker URL:

```bash
npx wrangler secret put ADMIN_TOKEN --config wrangler.toml
```

```bash
# 1) create the shard topology
curl -X POST https://<your-worker>.workers.dev/admin/init \
  -H "authorization: Bearer $ADMIN_TOKEN" \
  -H "content-type: application/json" \
  -d '{"numShards": 2, "totalVBuckets": 16}'
# 2) register a tenant + create a table, then use /v1/* to read/write.
#    See the starter app you can download from Shardscope's "Build on it" panel —
#    it service-binds to exactly this Worker.
```

## Point an app (or the Shardscope dashboard) at it

An app or the Shardscope dashboard is a **separate** Worker in the same account
whose `SHARD_API` service binding targets the public `cloudflare-shard-mvp`
Worker:

```toml
[[services]]
binding = "SHARD_API"
service = "cloudflare-shard-mvp"      # the public Worker deployed above
entrypoint = "CloudflareShardRpc"
```

The starter repo from Shardscope's **"Build on it"** panel already has this
binding block — just set `service` to your cluster's Worker name and
`wrangler deploy`.

## Tear it down

```bash
npm run delete
```

The aggregate command deletes the public Worker first and the control plane
second. Do not delete the control plane first: a surviving gateway must never
start transactions against a missing manifest service. Delete any separate
dashboard/app Worker yourself.
