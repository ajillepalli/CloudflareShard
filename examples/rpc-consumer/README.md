# cloudflare-shard-rpc-consumer

Demo Worker showing CloudflareShard's Durable Object RPC / Worker service-binding surface (see the main repo's issue #14) called from another Cloudflare Worker in the same account — **no HTTP request, no `Authorization` header built anywhere in this Worker's own code.**

This is additive to CloudflareShard's existing HTTP API, not a replacement: it only exists to demonstrate the alternative path for a same-account Worker-to-Worker consumer. Anything outside a Cloudflare Worker (external services, browsers, mobile apps, `curl`) still uses the HTTP API described in the main repo's README.

## How the binding works

`wrangler.toml` here declares:

```toml
[[services]]
binding = "SHARD_API"
service = "cloudflare-shard-mvp"
entrypoint = "CloudflareShardRpc"
```

`entrypoint` names the specific exported class in the main Worker (`src/index.ts`'s `CloudflareShardRpc`, a `WorkerEntrypoint` subclass) to bind to — without it, a service binding targets the default `fetch` export instead. The security boundary is this binding existing in this Worker's own config: there's no per-request credential the way an HTTP call needs a bearer token. The tenant token is still required and still checked — it's passed as an explicit method argument (e.g. `env.SHARD_API.mutate(tenantToken, ...)`) and validated internally by `CloudflareShardRpc`, the exact same `CatalogDO.checkTenantAuth` path an HTTP call goes through. Holding the binding is not, on its own, sufficient authorization.

Admin/topology operations aren't exposed here — see the main repo's follow-up issue.

## Running it locally

You need **two** `wrangler dev` processes running at once — one for the main CloudflareShard Worker, one for this consumer:

```bash
# Terminal 1, from the repo root:
npm run dev  # main Worker, defaults to http://localhost:8787

# Terminal 2, from this directory:
npm install
npm run dev -- --port 8788  # this consumer Worker
```

Wrangler's local dev registry connects the two automatically — you'll see `env.SHARD_API (cloudflare-shard-mvp#CloudflareShardRpc) [connected]` in this Worker's dev server output once both are up. If it says `[not connected]`, the main Worker's dev server likely isn't running yet — start it first.

## Verifying the RPC path actually works

```bash
node scripts/integration-test.mjs http://localhost:8787 http://localhost:8788 <admin-token>
```

This is a genuine integration test, not an in-process mock: it drives real setup (`/admin/init`, `/admin/register-tenant`, `/admin/create-table`, `/admin/create-index`) against the main Worker's existing HTTP admin API, then calls this consumer's `/demo/write-and-scan` and `/demo/index-query` routes — which internally call `env.SHARD_API.mutate()`, `.tableScan()`, and `.indexQuery()` over the real service binding — and asserts on the results.

## Deployed (not just local dev)

Once both Workers are deployed to the same Cloudflare account under these names (`cloudflare-shard-mvp` and `cloudflare-shard-rpc-consumer`), the service binding resolves automatically — no extra configuration needed beyond what's already in `wrangler.toml`.
