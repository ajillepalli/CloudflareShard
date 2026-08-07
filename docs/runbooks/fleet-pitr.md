# Fleet point-in-time restore runbook

This runbook is the operator procedure for restoring one deployed
CloudflareShard fleet to a single UTC cutoff. Fleet restore is destructive: all
physical shard databases are rewound independently, post-cutoff writes are
intentionally discarded, and committed cross-shard transactions at or before
the cutoff are reconciled from the immutable manifest. Transaction coordinators
are fenced but not provider-rewound. Coordinators without a durable commit
decision at or before the cutoff are durably discarded only after every shard
is restored and verified.

Use the typed CLI or SDK rather than hand-writing requests. The exact HTTP
contracts are in [the specification](../SPEC.md#fleet-point-in-time-restore-admin-token).

## Restore-domain invariant

One deployment is exactly one physical restore domain. The Worker variable is
`DEPLOYMENT_FLEET_ID` (`default` in the checked-in `wrangler.toml`). The
`fleet_id` supplied to preview must match it exactly. Logical fleets cannot
share the same `CATALOG`, `SHARD`, `COORDINATOR`, and manifest namespaces and
then be restored independently.

For operator scripts, use `RESTORE_FLEET_ID` as a shell variable only:

```bash
export RESTORE_FLEET_ID=default
```

`RESTORE_FLEET_ID` is not a Worker configuration variable. It is a runbook
convention whose value must equal the deployed `DEPLOYMENT_FLEET_ID`. A
mismatch fails with `RESTORE_CONFLICT`; do not work around it by renaming the
request fleet.

The separately bound `RESTORE_COORDINATOR` Durable Object must stay outside
the shard histories it controls. Preserve the root and route-less
control-plane namespaces during deploys and rollback. Do not clone only a
subset of these namespaces into a new deployment and call it the same fleet.

## Before preview

1. Confirm both Workers and the `CONTROL_PLANE` service binding are healthy.
2. Confirm the requested cutoff is canonical UTC with millisecond precision,
   is not in the future, and is inside the safe provider window. The current
   implementation uses a 30-day window with a 24-hour safety margin.
3. Stop planned topology and schema work. Preview fails closed while a vBucket
   migration, shard drain, index build, ring evacuation, or topology lock is
   active, or when audited catalog changes exist after the cutoff.
4. Verify the deployment fleet ID. Set `RESTORE_FLEET_ID` to that exact value.
5. Choose and retain one stable preview idempotency key. Reusing it with
   different parameters is a conflict.
6. Declare the RPO and RTO you expect this rehearsal or incident to meet.

Preview also fails closed when any of these proofs is missing or inconsistent:

- catalog audit coverage through the cutoff and a complete physical inventory;
- a completed exact fleet close bound to its close key, generation, snapshot
  hash, fleet root, and partition-config hash;
- bounded manifest enumeration with every required receipt/evidence page;
- coordinator registry coverage and redo-envelope hashes;
- a provider target bookmark and current preview bookmark for every physical
  shard.

`RESTORE_ENUMERATION_INCOMPLETE`, `RESTORE_BOOKMARK_MISSING`,
`RESTORE_PLAN_STALE`, or a manifest conflict means there is no executable
plan. Do not reinterpret a partial preview as operator approval.

## 1. Preview

Build the client once, then request the preview:

```bash
cd client && npm install && npm run build && cd ..
export CLOUDFLARESHARD_URL=https://<your-worker>.workers.dev
export CLOUDFLARESHARD_ADMIN_TOKEN=<your-ADMIN_TOKEN>

node client/dist/cli.js restore-preview \
  --fleet-id "$RESTORE_FLEET_ID" \
  --cutoff 2026-08-06T18:30:00.000Z \
  --idempotency-key incident-2026-08-06-a
```

The first response can be `status: "previewing"` with `restore_id` and
`retry_after_ms`. Preview close/enumeration is durable and bounded. Status can
confirm its phase:

```bash
node client/dist/cli.js restore-status --restore-id <restore_id>
```

`restore-status` does not return the plan body. After `retry_after_ms`, replay
the exact same `restore-preview` command with the same fleet, cutoff, and
idempotency key. Same-key/same-parameters resumes and eventually returns the
`previewed` plan; do not invent a new key for polling.

Do not execute until preview returns `status: "previewed"` and an immutable
plan. Review at least:

- `cutoff`, `previewed_at`, and `execute_before`;
- `topology.topology_epoch` and `topology.topology_hash`;
- every manifest pin and `manifest.record_count`;
- the sorted physical-shard participant list and both bookmarks per shard;
- `impact.participant_count`, `impact.transaction_count`, and the intentional
  loss interval;
- `rollback.undo_supported`, `undo_expires_at`, and every limitation;
- the exact 64-character `plan_hash`.

The current execution window is 15 minutes from preview creation. If it
expires, or any pinned topology/bookmark changes, request a new preview. Never
copy a plan hash from a different preview.

## 2. Execute with exact hash confirmation

Execution accepts only the server-stored plan identified by both `restore_id`
and its exact `plan_hash`:

```bash
node client/dist/cli.js restore-execute \
  --restore-id <restore_id> \
  --plan-hash <exact_plan_hash>
```

The hash is the destructive confirmation. It covers the immutable plan body,
including fleet, cutoff, expiry, topology, manifest pins, participants, impact,
and rollback limitations. A changed character or stale plan is rejected.

After acceptance, the root ingress gate blocks every non-restore HTTP route
with `503 FLEET_RESTORE_IN_PROGRESS`. Catalog topology changes and shard
mutation/alarm paths are independently fenced to close admitted-before-fence
races; coordinator mutation/recovery consults the external gate. Reads are
blocked too, because a partially rewound fleet is not a valid read snapshot.

The restore coordinator performs the destructive work in this order:

1. Re-prove the pinned catalog topology, install catalog and physical-shard
   generation fences, and revalidate all shard preview bookmarks. Coordinator
   mutation/recovery paths consult the external fleet gate and fail closed.
2. Materialize a bounded journal of direct writes in `(cutoff, fence time]`.
   This is the evidence set for intentional loss.
3. For each shard, stage the target provider bookmark. Staging returns
   and durably records an undo bookmark before activation.
4. Activate the staged bookmark by restarting the Durable Object session. The
   activation response may be severed by that restart; post-activation
   verification, not the RPC response, decides whether the shard moved.
5. Recreate/recover every manifest-committed cross-shard transaction at or
   before the cutoff from its hash-verified redo envelope.
6. Verify every restored shard and its invariants.
7. Only after all shards restore and verify, durably discard every coordinator
   selected by the loss evidence: both post-cutoff decisions and transactions
   that were still undecided at the cutoff. This is irreversible and closes
   the rollback window. A pre-rollout coordinator that wakes while the fence is
   active is added to this work set; one first discovered after completion is
   quarantined before it can resume mutation.
8. Release shard and catalog fences, then reopen the external root ingress gate
   last.

This is a two-phase shard PITR protocol: **stage and capture undo first;
activate and verify second**. The captured bookmarks support the public
fleet-level rollback workflow below. `rollback.undo_supported: true` is not
permission to manually rewind or unfence one shard; rollback uses the
same exact-plan, fleet-fenced, stage/activate/verify discipline.

## 3. Monitor status

```bash
node client/dist/cli.js restore-status --restore-id <restore_id>
```

The phase is one of `previewing`, `previewed`, `fencing`, `restoring`,
`reconciliation_pending`, `reconciling`, `verifying`, `rolling_back`,
`parked_lease_lost`, `complete`, `rolled_back`, `manual_repair_required`, or
`failed`. Monitor both progress pairs:

- `participants_restored / participants_total`
- `transactions_reconciled / transactions_total`

The participant counters refer to the planned physical shards; coordinator
loss/discard work is a separate durable stage that must finish before release.

`complete` is valid only when both pairs are complete, `blockers` is empty,
and `report.discarded_write_report_complete` is `true`. Preserve the final
report:

- `discarded_write_count`: direct writes intentionally removed by the cutoff;
- `discarded_write_report_hash`: canonical hash of the complete durable loss
  evidence set (the public status response does not expose raw write data);
- `measured_rpo_ms`: cutoff-to-fence interval measured by the coordinator;
- `measured_rto_ms`: execution-start-to-completion interval;
- `verified_at`: final invariant-verification time.

If status remains nonterminal, poll again. An accepted response means durable
work started; it is not completion.

## 4. Reconcile a fenced interruption

When a post-fence step fails, status becomes `manual_repair_required` and the
deployment stays fenced. This is deliberate. Ordinary traffic continues to
receive `503 FLEET_RESTORE_IN_PROGRESS`, catalog topology changes remain
blocked, shard restore generations remain installed, and coordinator
mutation/recovery remains blocked by the external gate. Do not use
`/admin/force-release-topology-lock`, delete restore rows, or deploy around the
gate. The topology escape hatch cannot safely release a restore fence.

Read `blockers[]`; each blocker carries a typed code/message and, when known,
the affected `participant_id` or `tx_id`. Automatic reconcile is intentionally
limited to retryable `RESTORE_INTERRUPTED` and `RESTORE_UNAVAILABLE` blockers.
Repair that external condition, then resume the same durable stage with the
same immutable plan hash:

```bash
node client/dist/cli.js restore-reconcile \
  --restore-id <restore_id> \
  --plan-hash <exact_plan_hash>
```

Reconcile is idempotent. It refuses a different hash and refuses to resume when
the matching fleet fence is not active. If the blocker remains, the operation
parks again with the fence intact. A contradiction/invariant/evidence blocker
is not automatically retryable; it requires a reviewed versioned repair path
or an eligible rollback. Escalate rather than clearing state by hand.

An error detected before any shard restore begins follows a different
path: the coordinator releases installed fences and records `failed`. Fix the
cause and generate a fresh preview; a failed or `manual_repair_required`
operation cannot be restarted with `restore-execute`.

## 5. Roll back an interrupted active restore

Rollback is an escape path for a partially applied restore, not a way to undo a
successfully completed restore after its fence has opened. First read status
and the plan's `rollback` block. Rollback is accepted only when the original
fleet fence and generation remain active, at least one shard has already
captured an undo bookmark, no post-cutoff coordinator has crossed the
irreversible discard boundary, `undo_supported` is true, and
`undo_expires_at` has not passed. Confirm the exact original plan hash again:

```bash
node client/dist/cli.js restore-rollback \
  --restore-id <restore_id> \
  --plan-hash <exact_plan_hash>
```

The rollback coordinator retains the original fleet fence, marks shards that
never armed restore as rollback-not-needed, stages each captured shard undo
bookmark, and durably records activation-requested before the provider apply RPC
that can sever its own response. The next session verifies `mode=undo`; the RPC
response itself is not authority. After every armed shard converges, it releases
shard/catalog fences and releases the external fleet gate last. Monitor
`restore-status`:
`rolling_back` is in progress and `rolled_back` is terminal. A partial or
interrupted rollback remains fenced and surfaces a typed blocker; repair and
resume the same durable operation rather than releasing shards by hand.

Rollback returns only armed shard databases to their captured
pre-restore heads. `restore-rollback` rejects a `complete` restore because
successful completion already released the original fleet fence. Rollback does
not make the plan reusable, erase its audit trail, or relax the
one-deployment/one-domain invariant.

## HTTP and SDK equivalents

All five routes require `Authorization: Bearer $ADMIN_TOKEN` and exact V1
request keys:

```text
POST /admin/restore-preview   {protocol_version:1, format_version:1, fleet_id, cutoff, idempotency_key}
POST /admin/restore-execute   {protocol_version:1, format_version:1, restore_id, plan_hash}
POST /admin/restore-status    {protocol_version:1, format_version:1, restore_id}
POST /admin/restore-reconcile {protocol_version:1, format_version:1, restore_id, plan_hash}
POST /admin/restore-rollback  {protocol_version:1, format_version:1, restore_id, plan_hash}
```

The typed SDK methods are `restorePreview`, `restoreExecute`, `restoreStatus`,
`restoreReconcile`, `restoreRollback`, and `waitForRestore`. `waitForRestore`
defaults to polling every 500 ms for at most 30 minutes and returns `complete`,
`rolled_back`, `manual_repair_required`, and `failed` as visible terminal
states.

## Release gate: three live rehearsals

Unit/workerd tests do not qualify provider PITR behavior. Before declaring a
build production-ready, complete **3/3 independent live rehearsals** on
disposable, correctly bound two-Worker deployments. Each rehearsal must:

1. declare a cutoff, RPO, and RTO before execution;
2. include pre- and post-cutoff single-shard writes plus cross-shard
   transactions that exercise manifest redo;
3. record the preview plan hash and exact evidence pins;
4. reach `complete` with full participant/transaction progress, no blockers,
   and `discarded_write_report_complete: true`;
5. independently verify pre-cutoff committed data is present, post-cutoff
   direct writes are absent and counted, and no acknowledged write at or before
   the cutoff is lost;
6. meet the declared RPO/RTO and retain the status/report artifact.

In addition to the 3/3 successful restore gate, run one live rollback rehearsal:
induce a controlled fenced interruption after at least one undo bookmark is
captured but before irreversible coordinator discard, call `restore-rollback`
before `undo_expires_at`, reach `rolled_back`, and independently verify the
captured pre-restore head.

Any unexplained outcome, manual fence clearing, incomplete loss report, missed
target, or rehearsal that uses mocks instead of live provider bookmarks fails
the gate. Fix the cause and restart the count; the requirement is three
successful rehearsals, not three attempts.
