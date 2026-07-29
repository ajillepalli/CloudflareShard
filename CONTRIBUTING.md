# Contributing

Thanks for looking. This is a small project and PRs are genuinely welcome.

## Getting it running

```bash
git clone https://github.com/ajillepalli/CloudflareShard.git
cd CloudflareShard
npm install
```

Wrangler reads local secrets from `.dev.vars` (gitignored — never commit it).
The gateway Worker needs an `ADMIN_TOKEN` to serve `/admin/*`:

```bash
echo 'ADMIN_TOKEN=dev-secret' > .dev.vars
npm run dev
```

That starts the gateway Worker on `:8787`. Bring up a topology before
anything else works:

```bash
curl -X POST http://127.0.0.1:8787/admin/init \
  -H "authorization: Bearer dev-secret" -H "content-type: application/json" \
  -d '{"numShards": 2, "totalVBuckets": 16}'
```

To also run Shardscope (the mission-control dashboard) locally:

```bash
cd examples/shardscope
npm install
npm run dev -- --port 8789
```

Wrangler's local dev registry connects Shardscope's `SHARD_API` service
binding to the gateway Worker automatically once both are up — watch for
`env.SHARD_API (cloudflare-shard-mvp#CloudflareShardRpc) [connected]` in its
dev output.

## Before you open a PR

```bash
npm run typecheck   # tsc --noEmit
npm test            # vitest run -- backend suite, real workerd via @cloudflare/vitest-pool-workers
npm run test:spa    # vitest run --config vitest.spa.config.ts -- Shardscope SPA suite (jsdom)
```

There's no CI configured yet, so these are on you to run locally before
opening a PR — nothing enforces them automatically. Shardscope's own worker
code has a separate tsconfig and needs its own typecheck:

```bash
cd examples/shardscope && npm run typecheck
```

## Repository shape

One npm project at the root plus a few independent sub-projects, each with
their own `package.json` and `wrangler.toml`:

| Path                        | What it is                                              |
| ---------------------------- | -------------------------------------------------------- |
| `src/`                       | Gateway Worker (`index.ts`), `CatalogDO`, `ShardDO`, `CoordinatorDO` |
| `client/`                    | Typed TypeScript SDK + CLI for the HTTP API               |
| `examples/rpc-consumer/`     | Demo Worker calling the tenant path over a service binding (no HTTP) |
| `examples/tpc-c-benchmark/`  | TPC-C-derived OLTP benchmark and demo project              |
| `examples/shardscope/`       | Mission-control dashboard — service-bound to the gateway Worker's RPC entrypoint |

`examples/shardscope/` is service-bound to the gateway, not a copy of it —
changing a route's request/response shape in `src/` usually means updating
whatever in Shardscope calls it too.

Read [`docs/SPEC.md`](docs/SPEC.md) before anything structural — it's the
canonical protocol/architecture reference (schemas, every route's exact
request/response shape, the routing algorithm, transaction semantics,
rebalancing). [`docs/REFERENCE.md`](docs/REFERENCE.md) has the practical
how-to detail that doesn't belong in SPEC.md.

## Things worth knowing

- **Tenant isolation is structural, not a guard on top.** `/v1/mutate` and
  `/v1/tx` force the partition-key predicate; `/v1/sql` (raw SQL) is
  admin-only because a per-tenant SQL guard against passthrough strings
  proved unwinnable — see [`SPEC.md` §14](docs/SPEC.md#14-security-and-multi-tenancy).
  If you're touching anything under `/v1/sql`'s allowlist or the row-owner
  join, read that section first.
- **Idempotency is a first-class contract**, not best-effort: every mutation
  carries a `requestId`; replaying the same id with a different SQL/params
  pair is rejected, not silently accepted. Tests for a new mutation path
  should cover the replay case, not just the happy path.
- **Migrations move real data.** `/admin/split-vbucket` and
  `/admin/drain-shard` do a dual-write backfill with a fenced,
  checksum-verified cutover — see [`SPEC.md` §11`](docs/SPEC.md#11-rebalancing-split-and-drain-milestone-3--shipped)
  before changing anything in that path.
- **Shardscope's SPA suite is deliberately isolated** from the backend
  suite (`vitest.spa.config.ts`, jsdom) so a frontend-only change can't
  accidentally break real `workerd` semantics, and vice versa. Keep new SPA
  tests in `examples/shardscope/test/spa/`.

## Commit messages

Explain why the change is needed, not just what it does. If you fixed a bug,
say what was broken and how it showed up.

## Reporting security issues

Do not open an issue. See [SECURITY.md](SECURITY.md).
