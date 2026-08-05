# Preliminary CloudflareShard OLTP baseline methodology

## PRELIMINARY caveat

This is a bespoke, closed-loop CloudflareShard workload for early
reproducibility checks. It does not establish saturation, production capacity,
or comparative performance. Public artifacts must keep this caveat before any
metric.

The workload and its operation names are independent of industry benchmark
specifications. It uses no external benchmark transaction definitions,
schemas, names, or metrics.

## Workload

Each run creates a run-specific table and tenant on an already initialized,
disposable CloudflareShard target. A seeded deterministic mix schedules:

- `single-write` (55%): insert one unique tenant row.
- `paired-write` (30%): atomically insert two unique tenant rows through the
  transaction endpoint.
- `bounded-read` (15%): request one bounded tenant-scoped table-scan page.

The mix is intentionally small. It exercises the ordinary mutation,
transaction, and read paths without claiming a model of a particular business
or an industry-standard workload. Paired writes are atomic, but this lane does
not claim that every generated pair resides on distinct physical shards.

## Bounds and accounting

The driver stops scheduling at the first of the requested duration or maximum
operation count. Every HTTP request has an `AbortController` deadline. In-flight
requests receive a separate overall grace bound; a request still unresolved at
that boundary is recorded as `RUN_GRACE_EXPIRED` and makes the run incomplete.

Every issued attempt has exactly one primary outcome:

```text
attempted = successes + failures + timeouts
```

`lateCompletions` is a labeled subset: any attempt finishing after the
scheduling window, whether successful, failed, or timed out. The denominator
never drops failures, timeouts, or late attempts. Actual elapsed time includes
the completion grace actually consumed.

The neutral metrics are successful operation count, successful operations per
actual elapsed second, and all-attempt/successful-attempt latency summaries.
They are not capacity estimates. The driver is closed-loop, so it neither
discovers saturation nor corrects for coordinated omission.

## Reproduce

Use a fresh disposable target because the deterministic run ID also makes the
table, tenant, request IDs, and row keys deterministic.

```bash
cd examples/cloudflareshard-oltp
export CLOUDFLARESHARD_ADMIN_TOKEN='<target admin token>'
npm test
npm run benchmark -- --base-url https://your-worker.workers.dev --duration-seconds 30 --concurrency 4 --max-operations 100 --request-timeout-ms 5000 --grace-ms 10000 --seed 20260805
```

## Artifact contract

One canonical versioned result object produces both artifacts:

- JSON contains the complete sanitized attempt records and machine-readable
  environment, configuration, accounting, metrics, limitations, revision, and
  integrity fields.
- Markdown presents the same information in this order: preliminary caveat,
  identity, environment, configuration, outcomes, neutral metrics,
  limitations, reproduction, revision/checksum.

The canonical result checksum is SHA-256 over the JSON-shaped result with the
`resultSha256` value blanked. This avoids a recursive checksum while allowing a
consumer to verify that the result has not changed. Generated artifacts exclude
tokens, response bodies, raw SQL inputs, and request payloads. Only bounded
typed failure codes are retained.

No baseline result is committed by this implementation change. A dated public
artifact requires a separately authorized run on a declared environment and a
human inspection of both generated files.
