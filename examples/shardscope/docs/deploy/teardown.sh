#!/usr/bin/env bash
# Teardown for a "Deploy your own CloudflareShard cluster" deployment.
#
# Deleting the Worker also deletes the Durable Object namespaces it owns
# (CATALOG/SHARD/COORDINATOR) and their SQLite storage — there is no separate
# KV/D1/R2/Queue to clean up, because the cluster is a single self-contained
# Worker. After this runs, the cluster and all its data are gone and billing for
# it stops.
#
# Usage:  ./teardown.sh [worker-name]
#   worker-name defaults to the `name` in wrangler.toml. Pass the name you chose
#   on the Deploy setup page if you renamed it.
set -euo pipefail

WORKER_NAME="${1:-}"
if [ -z "$WORKER_NAME" ]; then
  # Best-effort read of `name = "..."` from wrangler.toml in the current dir.
  if [ -f wrangler.toml ]; then
    WORKER_NAME="$(grep -E '^\s*name\s*=' wrangler.toml | head -1 | sed -E 's/.*=\s*"([^"]+)".*/\1/')"
  fi
fi
if [ -z "$WORKER_NAME" ]; then
  echo "Could not determine the Worker name. Pass it explicitly: ./teardown.sh <worker-name>" >&2
  exit 1
fi

echo "This will PERMANENTLY delete Worker '$WORKER_NAME' and its Durable Objects"
echo "(CATALOG/SHARD/COORDINATOR) — including ALL cluster data. This cannot be undone."
read -r -p "Type the worker name to confirm: " CONFIRM
if [ "$CONFIRM" != "$WORKER_NAME" ]; then
  echo "Confirmation did not match. Aborting — nothing deleted." >&2
  exit 1
fi

# `wrangler delete` removes the Worker and its owned Durable Object namespaces.
npx wrangler delete --name "$WORKER_NAME"

echo ""
echo "Done. Verify in the dashboard (Workers & Pages) that '$WORKER_NAME' is gone."
echo "If you also deployed the Shardscope dashboard or a starter app as a SECOND"
echo "Worker pointed at this cluster, delete that separately."
