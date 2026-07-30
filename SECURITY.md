# Security policy

CloudflareShard is a multi-tenant data plane. A leak across tenants or an
authentication bypass is the worst-case outcome, so security reports get
priority over everything else in the queue.

## Reporting a vulnerability

**Do not open a public issue.** Use GitHub's private reporting instead:

- Go to the [Security tab](../../security/advisories/new) and open a draft advisory.

Please include what you can:

- what an attacker gains, and what access they need to start
- steps to reproduce, ideally against a local `npm run dev` stack
- affected version or commit

You will get an acknowledgement within 3 working days and an assessment
within 10. We will tell you when a fix ships and credit you in the advisory
unless you would rather stay anonymous.

## Scope

In scope:

- authentication and token handling: `ADMIN_TOKEN`'s universal-bypass
  behavior, tenant `tenant_auth` bearer tokens, and token rotation/revocation
  (`/admin/register-tenant`, `/admin/revoke-tenant`)
- cross-tenant isolation: one tenant reading or mutating another tenant's
  rows, including via `/v1/table-scan`'s `__cf_row_owners` join or a
  partition-key collision across tenants
- the `/v1/sql` and `/v1/scatter` admin-only boundary, and the SQL-safety
  allowlist behind the read-only console (internal-table access, CTE-header
  classifier bypasses, and similar parsing gaps)
- Shardscope's two-tier auth: `SHARDSCOPE_GATE_TOKEN` (who can operate the
  dashboard) versus `ADMIN_TOKEN` (what the dashboard is allowed to do to the
  cluster it's bound to)
- privilege escalation through any admin-only route becoming reachable
  without `ADMIN_TOKEN`

Out of scope:

- anything requiring a compromised Cloudflare account or `wrangler`
  credentials
- rate limits or cost exposure on a deployment you control. Tune or accept
  that yourself
- Shardscope's `?demo=1` sample mode, which is intended to be publicly
  reachable, never touches `/api/*`, and is documented as such
- the MVP's permissive SQL policy where it's already documented as a known
  limitation (see [`docs/REFERENCE.md`](docs/REFERENCE.md#known-limitations))
  rather than a silent gap

## Deploying safely

- **`ADMIN_TOKEN` must be a real secret**, set with `wrangler secret put
  ADMIN_TOKEN` (never `.dev.vars` or a committed file), and different per
  deployment. Generate it with `openssl rand -hex 32`. Without it, the
  gateway Worker fails closed with `500 ADMIN_TOKEN is not configured`
  rather than accepting requests unauthenticated.
- **Tenant tokens are separate from `ADMIN_TOKEN`** and scoped to one
  `tenantId`. Rotate a compromised tenant's token
  (`POST /admin/register-tenant {"rotate": true}`) rather than the
  cluster-wide admin secret when only that tenant is affected.
- **Shardscope's `SHARDSCOPE_GATE_TOKEN`** gates every `/api/*` route on the
  dashboard itself, separate from the `ADMIN_TOKEN` it holds to talk to the
  cluster. Rotating one does not rotate the other.

## Supported versions

CloudflareShard is pre-1.0. Fixes land on `main`, and self-hosted
deployments should track it.
