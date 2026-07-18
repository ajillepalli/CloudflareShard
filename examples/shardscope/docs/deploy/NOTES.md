# Deploy-button prep — status, compatibility, and what's still gated

**What this directory is:** the detailed reference for increment ② of the
dev-conversion track (one-click "Deploy your own CloudflareShard cluster").
Option 1(a) was chosen — the repo is public and the **live Deploy button is now
in the repo-root `README.md`** ("Deploy your own cluster"), pointing at
`github.com/ajillepalli/CloudflareShard` (the button deploys the ROOT core
Worker). This dir holds the confirm-gated teardown script, the `.env.example`
secret note, and a button-compatible copy of the cluster `wrangler.toml`. The one
thing still open: the first real deploy→teardown verification against a live
account (nobody has clicked it end-to-end yet). Full design doc:
`~/.gstack/projects/ajillepalli-CloudflareShard/2026-07-17-design-shardscope-deploy-button.md`.

## Compatibility assessment (verified against the real core)
The CloudflareShard core Worker (repo-root `wrangler.toml` + `src/index.ts`) is
**Deploy-to-Cloudflare-button compatible as-is**:
- **Self-contained.** One Worker + three SQLite Durable Object classes
  (`CatalogDO`/`ShardDO`/`CoordinatorDO`), provisioned via `[[migrations]]`. No
  external service binding, no KV/D1/R2/Queue to wire — the button handles this
  shape natively.
- **Secret model fits the button.** The only secret is `ADMIN_TOKEN`
  (`env.ADMIN_TOKEN`); declaring it in `.env.example` makes the button's
  setup page prompt for it. (Draft included here.)
- **No blockers found in the config.** `compatibility_date`, `main`, DO bindings,
  and migrations are all button-readable.

The `wrangler.toml` / `.env.example` / `teardown.sh` / `README.md` in this
directory are drafts of what the public template repo would contain.

## What is READY (done autonomously, no account/publish needed)
- Verified core config is button-compatible (this file).
- Draft template `wrangler.toml` (mirrors core), `.env.example` (ADMIN_TOKEN),
  `README.md` (button markdown + honest cost + init + teardown), `teardown.sh`
  (confirm-gated `wrangler delete`).
- Honest cost + teardown framing (Workers Paid, DOs billed to the user's account).

## Resolved
1. **~~Public repo~~ — DONE.** Option 1(a): `ajillepalli/CloudflareShard` is
   public; the button is wired into the repo-root README, pointing at the repo
   (deploys the ROOT core Worker — self-contained, no npm workspaces, `deploy`
   script present).
2. **~~Core-repo coordination~~ — handled via PR.** The button is an additive
   README section landed by normal PR to `main`, not a surprise change.

## Still GATED — needs your account (I did NOT do this)
3. **A real deploy test.** Clicking the button provisions real Durable Objects on
   a real account and costs real money (Workers Paid). The first end-to-end run
   (deploy → set ADMIN_TOKEN → `/admin/init` → write → `teardown.sh`) needs your
   account, or your explicit OK to use one. The root README carries an honest
   "not yet run end-to-end" note until this is done.

## Recommended next step
Run one real deploy→teardown to verify the button end-to-end, then drop the
"not yet run" caveat from the root README. The dashboard/app "second Worker
pointed at the cluster" step stays a documented follow-on (the button can't wire
a cross-Worker service binding).
