# CloudflareShard OLTP baseline

This package runs a small, bespoke CloudflareShard workload made from three
neutral operations: a single-row write, an atomic paired write, and a bounded
tenant-scoped read. It is independent of industry benchmark specifications and
does not use their workload material, names, or metrics.

Every run is explicitly preliminary. The driver is closed-loop and intended to
check reproducibility and surface obvious behavior—not establish saturation,
production capacity, or a comparison with another system.

## Run against a disposable target

The target must already be initialized and must be safe to populate with a new
table and tenant. The deterministic run identity means the exact same command
must be reproduced against a fresh disposable target, not rerun against the
same state.

```bash
cd examples/cloudflareshard-oltp
export CLOUDFLARESHARD_ADMIN_TOKEN='<target admin token>'
npm run benchmark -- \
  --base-url https://your-worker.workers.dev \
  --duration-seconds 30 \
  --concurrency 4 \
  --max-operations 100 \
  --request-timeout-ms 5000 \
  --grace-ms 10000 \
  --seed 20260805
```

PowerShell:

```powershell
$env:CLOUDFLARESHARD_ADMIN_TOKEN = '<target admin token>'
npm run benchmark -- --base-url https://your-worker.workers.dev --duration-seconds 30 --concurrency 4 --max-operations 100 --request-timeout-ms 5000 --grace-ms 10000 --seed 20260805
```

The command writes one sanitized JSON result and one Markdown report under
`artifacts/`. The Markdown is derived from the same result object used for JSON.
The admin and tenant tokens, response bodies, SQL inputs, and request payloads
are never included in either artifact.

## Checks

```bash
npm test
```

See [the methodology](../../docs/benchmarks/cloudflareshard-oltp/README.md)
for accounting rules, limitations, and artifact interpretation.
