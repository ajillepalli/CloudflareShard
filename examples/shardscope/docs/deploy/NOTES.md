# Deploy-button prep — historical status and current limitation

> **Superseded on 2026-08-05.** These notes record the former single-Worker
> deployment design. The current topology requires two Worker applications,
> which Cloudflare's Deploy to Cloudflare flow does not deploy together. The
> live button has been removed; use the repository root's ordered
> `npm run deploy` instructions. Fresh-account deploy→teardown qualification
> remains pending.

## Historical record

This directory originally prepared a public deploy button for the former
single-Worker topology: one Worker with `CatalogDO`, `ShardDO`, and
`CoordinatorDO`. Its copied `wrangler.toml`, `.env.example`, and teardown script
are retained only as historical design artifacts. They are not a supported
deployment path for the current cluster.

The design record for that retired approach is
`~/.gstack/projects/ajillepalli-CloudflareShard/2026-07-17-design-shardscope-deploy-button.md`.

## Current status

- The current cluster requires `cloudflare-shard-control-plane` with
  `JournalManifestDO`, followed by the public `cloudflare-shard-mvp` Worker.
- Cloudflare's deploy-button flow cannot deploy both applications together, so
  the former button and one-Worker template are unsupported.
- The repository root's `npm run deploy` and `npm run delete` scripts enforce
  the safe creation and deletion order.
- Fresh-account deploy→verify→delete evidence remains pending. Local tests and
  dry runs do not satisfy that live qualification gate.

## Recommended next step

Run the supported root workflow against a disposable fresh account, retain a
sanitized receipt, and record the named staging result. Do not restore the
button unless Cloudflare supports and the project verifies the full two-Worker
topology end to end.
