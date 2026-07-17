# Deploy-button prep — status, compatibility, and what's still gated

**What this directory is:** staged, reviewable artifacts for increment ② of the
dev-conversion track (one-click "Deploy your own CloudflareShard cluster"). It is
a **design + config draft**, NOT a live deploy path. Nothing here is published or
deployed. See the full design doc:
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

## What is GATED — needs your decision / action (I did NOT do these)
1. **A PUBLIC repo.** The button requires a public github.com/gitlab.com repo.
   Options: (a) make `ajillepalli/CloudflareShard` public and add the button to
   its README; (b) create a dedicated public repo (e.g. `cloudflare-shard-deploy`)
   containing the core Worker code + this config. The button needs the actual
   Worker CODE, not just config — these drafts don't include `src/` (that's core).
2. **Core-repo coordination.** The real template = the core Worker's code + a
   button. Another session is actively landing PRs on the core repo. Adding the
   button / a deploy config there (or copying core into a new repo) should be
   coordinated with that work, not dropped in as a surprise.
3. **A real deploy test.** Clicking the button provisions real DOs on a real
   account and costs real money. The first end-to-end test (deploy → init →
   write → teardown) needs your account (or your explicit OK to use one).

## Recommended next step
Pick 1(a) vs 1(b) above (make-core-public vs dedicated-repo). Then either:
- You (or an OK'd me) create the public repo from these drafts + the core code,
  wire `<PUBLIC_REPO_URL>` into the button, and run one real deploy→teardown test; or
- Keep it as this staged design until the core repo work settles.

The dashboard/app "second Worker pointed at the cluster" step stays a documented
follow-on (the button can't wire a cross-Worker service binding).
