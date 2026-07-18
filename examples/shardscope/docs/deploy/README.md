# Deploy your own CloudflareShard cluster

> **The live Deploy button is in the repo-root [`README.md`](../../../../README.md)**
> ("Deploy your own cluster"). This directory is the detailed reference: the
> confirm-gated teardown script, the `.env.example` secret note, and a copy of
> the button-compatible cluster `wrangler.toml`. The one thing still open is the
> first real deploy→teardown verification against a live account — see
> [`NOTES.md`](./NOTES.md).

Spin up your own CloudflareShard cluster — the same multi-tenant, sharded,
transactional database Shardscope demos — in your own Cloudflare account, in a
few clicks.

[![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/ajillepalli/CloudflareShard)

## What this creates (and what it costs)

The button clones this repo into **your** GitHub and deploys **to your own
Cloudflare account**. It provisions:

- **One Worker** (`cloudflare-shard`, renameable on the setup page) — the cluster.
- **Three SQLite Durable Object classes** — `CATALOG` (control plane / routing +
  the topology lock), `SHARD` (data plane / your tenant data), `COORDINATOR`
  (2PC). Created automatically from the `[[migrations]]` in `wrangler.toml`.

There are **no** KV/D1/R2/Queue resources — the cluster is a single
self-contained Worker.

**Cost — read this.** Durable Objects require the **Workers Paid** plan, and
everything created here is **billed to your account** (Worker requests + Durable
Object requests/duration/storage; Workers Logs too, if you leave observability
on). This is a real database in your account, not a sandbox. Idle cost is low,
but it is not zero, and load costs money. Tear it down when you're done (below).

## After deploy: set your admin token

The cluster gates its whole `/admin/*` surface on a secret, `ADMIN_TOKEN`. The
setup page prompts for it (from [`.env.example`](./.env.example)). Set
it to a **strong random value** (`openssl rand -hex 32`) — anyone with it can
init, reshard, or drop your cluster. If it's unset the Worker returns
`500 ADMIN_TOKEN is not configured`.

Then initialize the cluster (from your machine, against your new Worker's URL):

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

The Deploy button provisions **one** Worker and does **not** wire a service
binding to a second one. So an app or the Shardscope dashboard is a **separate**
Worker in the same account whose `SHARD_API` service binding targets the
`cloudflare-shard` Worker above:

```toml
[[services]]
binding = "SHARD_API"
service = "cloudflare-shard"          # the Worker name you deployed above
entrypoint = "CloudflareShardRpc"
```

The starter repo from Shardscope's **"Build on it"** panel already has this
binding block — just set `service` to your cluster's Worker name and
`wrangler deploy`.

## Tear it down

```bash
./teardown.sh            # reads the Worker name from wrangler.toml
# or: ./teardown.sh <the-name-you-chose>
```

Deleting the Worker deletes its Durable Objects and all cluster data, and billing
for it stops. Delete any separate dashboard/app Worker yourself.
