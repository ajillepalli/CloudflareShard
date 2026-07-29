# CloudflareShard: A Technology Thesis

> **This is not a pitch deck.** CloudflareShard is a solo, AI-assisted, open-source
> (Apache-2.0) technology demonstration. There is no company, no funding round, no cap
> table, no revenue, no customers, and no team behind it — one person built this, working
> with Claude Code across many sessions. Nothing below should be read as evidence of a
> business. It is an honest technical assessment of an architectural approach, written in
> the voice of a due-diligence memo, for anyone evaluating whether the *approach* is worth
> backing, licensing, or building a company around.

For the artifact itself, start with [`README.md`](../../README.md) (quickstart, API
shape, deploy button) and [`docs/SPEC.md`](../SPEC.md) (the canonical protocol and
architecture reference). Open roadmap items are tracked in [`TODOS.md`](../../TODOS.md).
This document doesn't repeat their content — it argues about what it means.

## 1. The market problem

Sharded relational data on serverless/edge infrastructure is a genuinely unsolved
combination, not because sharding is a new problem, but because the constraints that
make an edge platform attractive (per-request billing, no long-lived connections, an
execution model that dies when the request does) actively fight the assumptions most
sharding systems were built on.

- **Vitess** shards MySQL by wrapping a fleet of long-running `mysqld` instances with a
  routing/query-rewriting layer (`vtgate`/`vttablet`). It assumes persistent server
  processes, connection pooling, and an operator who runs and patches that fleet. None of
  that exists in a Workers-native application — there is no server to run.
- **PlanetScale** productizes Vitess as a hosted MySQL-compatible service. It solves the
  operational burden, but it is still an external database your Worker calls over the
  network — a connection, not a co-located primitive. Every request pays a cross-service
  hop, and the sharding logic lives outside the platform you're deploying on.
- **Turso/libSQL** embeds SQLite at the edge with embedded-replica replication, which is
  much closer to Workers' execution model, but its scaling story is replica-based
  (many read replicas of one logical database), not partition-based — it doesn't give
  you independent, horizontally-splittable write shards with cross-shard transactional
  writes.

None of the three gives a Workers application a *storage primitive it already has a
binding to* that also shards, transacts across shards, and rebalances online. The
[`TODOS.md`](../../TODOS.md) "Cross-tenant/cross-shard analytics aggregation" item names
this landscape explicitly and records that the same limitation (no built-in cross-tenant
analytics without an external OLAP store) is acknowledged industry-wide even by these
systems — this project doesn't claim to have solved a problem nobody else has noticed,
only to be approaching a subset of it from a different substrate.

The specific gap CloudflareShard targets: a Workers application that wants strict
per-tenant ACID writes, cross-shard atomic transactions, and the ability to grow past a
single database's practical size — using only primitives that already live inside the
Cloudflare account it's deployed to, with no external database service, connection
string, or additional vendor to operate.

## 2. The technical bet

The architectural bet is that **Durable Objects (SQLite-backed) are a sufficient storage
primitive to build a real sharded SQL layer on**, without KV, D1, or R2 anywhere in the
system. Concretely, from [`docs/SPEC.md`](../SPEC.md):

- **ShardDO as the unit of physical storage.** Each shard is one Durable Object owning
  one SQLite database, executing statements serially — strict local ACID for free, since
  a DO is single-threaded by construction (§3, §6).
- **Deterministic hash routing with a rebalanceable indirection layer.** A composite key
  (`tenantId:table:partitionKey`) hashes via FNV-1a into one of `totalVBuckets` virtual
  buckets (default 1024); a catalog-owned `vbucket_map` maps vbuckets to physical shards.
  Rebalancing changes only the map, never the hash function (§4, §8).
- **Catalog-shard-of-catalog-shards.** The control plane itself is sharded: a fixed,
  well-known set of catalog DOs (`catalog-0..N-1`, default 4), with a tenant's catalog
  shard chosen by hashing `tenantId` directly — no lookup step, which sidesteps the
  chicken-and-egg problem of a sharded metadata store needing its own metadata store to
  find itself (§4).
- **Two-phase commit via a dedicated CoordinatorDO.** Cross-shard writes are not
  eventually-consistent or saga-based — they're genuine 2PC. One `CoordinatorDO`
  instance per transaction (`idFromName(txId)`, unsharded by design) fans out
  `/prepare` to every participant `ShardDO`, aborts everyone on any failure, or fans out
  `/commit` on universal success; an unacknowledged commit is retried by the DO's own
  `alarm()` with backoff (§10). Bounded to 8 participant shards per transaction.
- **Online resharding with a fenced, checksum-verified cutover.** `/admin/split-vbucket`
  and `/admin/drain-shard` don't just repoint metadata — they perform a real data move: a
  dual-write backfill phase (writes land on the source and mirror to the target via a
  durable per-shard retry queue), then a formal five-step cutover (fence the source at the
  data layer, drain the mirror queue to zero, verify a sha256 content checksum on both
  shards, flip the map, unfence and delete the source copy). Migration is idempotent and
  abortable at every point before the flip (§11).
- **A live correctness-proving demo, not just a design doc.** Shardscope
  ([`examples/shardscope/README.md`](../../examples/shardscope/README.md)) drives real
  TPC-C-shaped load and chaos testing against a real deployed cluster while resharding it
  live, with a running correctness meter whose entire premise is proving zero data loss
  under a real split/drain — not a simulated one.

The distinctive claim, stated precisely: this is not "SQLite behind a load balancer." It
is a control plane (itself sharded) driving a fenced, checksum-verified online migration
protocol across stateful compute primitives that have no independent network identity
outside the platform's own object-addressing scheme — the entire system, data plane and
control plane, is expressed as Durable Objects and nothing else.

## 3. What's actually proven vs. what's still open

### Proven, with real evidence

- **A real, growing test suite.** As of this writing, `npm test` (vitest +
  `@cloudflare/vitest-pool-workers`, which runs tests inside an actual Workers runtime,
  not a mock) passes **886 tests across 31 test files** in the core project alone (more
  exist under `client/` and the example projects). [`CHANGELOG.md`](../../CHANGELOG.md)
  documents the suite at 114 tests at v1.0.0.0 and 485 at v2.2.0.0/v2.3.0.0 — it has
  grown roughly 7x since first ship, tracking new functionality rather than being padded.
  It covers routing, admin auth, idempotency, 2PC, index maintenance, migration/cutover
  races, and multi-tenant isolation edge cases — several changelog entries describe a
  regression test written specifically to fail before a fix and pass after (e.g. the
  `/v1/tx` WHERE-guard silent-noop fix, PR #48).
- **A real TPC-C-derived benchmark against a live deployment**, not a synthetic
  microbenchmark. Two documented runs against the actual deployed Cloudflare Worker
  (`cloudflare-shard-mvp`), recorded verbatim in [`TODOS.md`](../../TODOS.md):
  - Run 1 (8 warehouses, ~4,400 rows, 90s at 15 concurrency, 1,094 transactions):
    per-type p50 latency — Payment 554ms, Order-Status 137ms, Stock-Level 963ms,
    New-Order 1,829ms, Delivery 4,927ms.
  - Run 2 (24 warehouses, ~57,500 rows, 180s at 30 concurrency, 3,378 transaction
    attempts, 466 tpmC-equivalent): per-type p50/p95/p99 — Payment 766/966/1,135ms,
    Order-Status 164/290/393ms, Stock-Level 1,056/3,406/4,305ms, New-Order
    1,978/2,439/2,563ms, Delivery 8,533/11,145/11,434ms. At this higher concurrency,
    37 attempts (1.1%) genuinely failed — almost all `409 TX_ABORTED` on New-Order's
    cross-shard 2PC prepare, real write contention rather than a coordinator artifact.
  These are two short runs at modest scale, not a production traffic pattern — but they
  are real numbers from a real deployment, not projections.
- **A live, self-correctness-checking demo.** Shardscope's chaos/reshard room drives real
  load and real chaos against a real cluster while running an online split or drain, and
  tracks a live correctness meter through it — the mechanism (not a vanity number) is the
  point: the demo is built to be falsifiable in front of a viewer, not staged.
- **A deployable artifact today.** The "Deploy to Cloudflare" button provisions the
  actual Worker + three Durable Object classes to a real Cloudflare account, from
  `wrangler.toml`'s `[[migrations]]` — no separate database service to provision. Per
  the README, the end-to-end button flow hasn't yet been run start-to-finish against a
  fresh account, so this is "wired and should work," not "verified clean-room."
- **An unusually deep review discipline for a solo project.** The Milestone 3 changelog
  entry documents seventeen successive `codex review` passes against one PR, each
  surfacing and closing a real correctness bug (SQL-guard bypasses, migration-cutover
  races, the concurrent-topology-operation class that motivated the write fence and
  lock). This is a process signal, not a product one — it says something about how
  seriously correctness bugs are hunted, not that none remain.

### Explicitly not proven

- **No production traffic.** Every number above comes from a benchmark run, not a real
  application's real users. There is no multi-tenant SaaS running on this today.
- **No unique-index support.** Secondary indexes are non-unique only —
  [`TODOS.md`](../../TODOS.md) scopes this out explicitly ("no chunk in that plan
  allocated space to build it"), gated on a real adopter's schema needing it.
- **No automatic split heuristics.** The split/drain *mechanism* is shipped and tested;
  deciding *when* to trigger a split (size/QPS/latency thresholds) is open, and
  `TODOS.md` is explicit that the two data points collected so far (even row
  distribution across 16 shards at both benchmark scales) are "far below any scale a
  real split-heuristic threshold would plausibly trigger on" — i.e., the project itself
  says it doesn't yet know what a hot shard looks like in practice.
- **No cross-tenant/cross-shard analytics.** By design — `/v1/scatter` plus app-level
  aggregation is the only current path, and `TODOS.md` notes this may never be built
  directly into the transactional path (an external OLAP export might be the right
  answer instead).
- **CoordinatorDO's keying choice (one DO per transaction, unsharded) is reasoned from
  Cloudflare's billing model and two benchmark runs, not from sustained production
  volume** — `TODOS.md` keeps this TODO explicitly open pending more data, while noting
  the abort-rate signal from run 2 (1.1% at 30 concurrency) is "worth watching as
  concurrency and cross-shard write density grow further."
- **Known, documented limitations, not hidden ones**: `/v1/tx` capped at 8 participants;
  `/v1/scatter` and `/v1/table-scan` can return duplicate (never missing) rows during an
  active migration window; `/v1/table-scan` supports no arbitrary column filtering; a
  documented partition-key-collision edge case where two tenants share a partition key on
  the same shard (mitigated, not eliminated, by the `partition_key_unique` gate in §14).

## 4. Defensibility / moat questions

Asked plainly: is this defensible technology, or a clever integration of existing
primitives that anyone with Durable Objects access and enough review discipline could
rebuild in a few months?

Honestly, closer to the latter than the former. The individual mechanisms —
hash-partitioned routing, 2PC via a coordinator, dual-write migration with a checksummed
cutover, a sharded control plane — are each well-understood distributed-systems
techniques, not novel research. Cloudflare itself does not offer this as a product;
nothing here depends on private APIs or non-public platform behavior. A well-resourced
team with Durable Objects experience and the same review rigor could plausibly build
something comparable.

What is real, if not exactly a moat:

- **Integration correctness at this scope is nontrivial and unproven to be easy.** The
  seventeen-review-pass history on Milestone 3 alone suggests the failure modes here
  (races between concurrent topology operations, stale index rings mid-evacuation, cutover
  ordering) are the kind that are easy to get subtly wrong and expensive to find late.
  That's evidence the problem has real teeth, not evidence of a durable advantage — a
  well-funded competitor pays the same tax, just with more people paying it in parallel.
- **Being first and public matters less here than usual**, precisely because this is
  built on a public platform's public primitives. There's no data network effect, no
  proprietary dataset, no regulatory moat, and (today) no customer lock-in, because there
  are no customers.
- **The realistic value, if any, is in the codebase and the demonstrated correctness
  process as a starting point** — a team that adopted this rather than starting from
  zero would skip real, already-paid engineering cost (2PC, fenced migration, index
  topology, the test suite). That's a build-vs-buy argument, not a moat argument.

## 5. Risks

- **Solo-maintainer risk.** One person, working with an AI coding assistant, has built
  and reviewed all of this. There is no bus-factor mitigation, no second human reviewer
  independent of that process, and no institutional continuity if the maintainer stops.
- **Platform lock-in risk.** The entire system is Durable Objects, full stop — no
  abstraction layer, no portability story. Cloudflare's DO pricing, consistency
  guarantees, SQLite storage limits, and roadmap decisions are all inherited directly and
  unconditionally. A pricing change or a platform-level constraint shift (e.g. to DO
  storage limits, alarm behavior, or billing) hits this project with no insulation.
- **Unproven-at-scale risk.** The largest real load test to date is 24 warehouses,
  ~57,500 rows, 180 seconds, 30 concurrent callers. That is a smoke test by production
  standards, not a scale validation. Nothing here has been run against sustained
  multi-day, multi-tenant, adversarial-in-the-wild traffic.
- **No dedicated security audit.** The security/multi-tenancy model in
  [`docs/SPEC.md` §14](../SPEC.md#14-security-and-multi-tenancy) is thoughtfully reasoned
  and iterated on through internal and Codex-assisted review (the raw-`/v1/sql` removal
  history is a good example of real vulnerabilities found and closed), but "reviewed by
  the same process that builds it, plus an AI reviewer" is not a substitute for an
  independent third-party security audit, and none has happened.
- **Single-operator trust model, explicitly scoped.** §14 states plainly that this is a
  per-deployment authorization boundary, not a multi-customer-SaaS boundary — in the
  current self-hosted model, the deploying developer holds both the operator role and
  every tenant role. The code keeps these structurally separate so a future hosted layer
  with a genuinely separate operator could be added without a rewrite, but that layer
  doesn't exist yet, and building it is nontrivial new work, not a config flip.
- **No support commitment.** This is an open-source project with no SLA, no paid
  support tier, and no team to escalate to.

## 6. What a real investment thesis would require next

None of the following exist today. Listing them is the honest complement to section 3 —
this is the gap between "interesting technology demo" and "fundable business":

- **A real pilot customer** — a team building a real Workers application willing to run
  production (or near-production) traffic against this, generating actual usage data to
  replace the benchmark-only evidence in section 3, and a real answer to "does this hold
  up under sustained, adversarial, multi-tenant load."
- **A company wrapper** — legal entity, at minimum one more engineer, and a support/SLA
  story, if this were ever to be sold or operated on someone else's behalf rather than
  self-hosted by the adopter.
- **An independent third-party security audit**, specifically of the multi-tenancy
  boundary, the 2PC/migration correctness properties, and the admin-token trust model —
  not another internal or AI-assisted review pass.
- **Production-scale load testing** — sustained, multi-day, realistic concurrency and
  data-volume testing well beyond the two TPC-C runs cited above, ideally against the
  automatic-split-heuristics question `TODOS.md` currently has no answer for.
- **Resolution of the genuinely open architecture questions** — unique-index support,
  split-trigger heuristics, and a real decision on cross-tenant analytics — each gated,
  per `TODOS.md`, on real demand or real data that doesn't exist yet.
- **A hosted/managed offering, if the business model requires one** — the self-hosted
  model works today because the deploying developer holds every role; a true SaaS
  offering (separate operator, separate tenants who are someone else's customers) is an
  explicitly-deferred architectural seam, not a shipped capability.

None of this is a criticism of the project for what it is — a solo, transparent,
well-tested technology demonstration with real correctness evidence behind its claims.
It is simply the honest distance between what exists and what "fundable business" would
require.
