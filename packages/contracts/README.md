# Transaction contracts

This package is the binding-free owner of CloudflareShard's shared transaction
protocol. It intentionally contains only durable/wire formats, transition and
epoch validation, canonical hashing, manifest routing, and typed transaction
errors. Storage, Durable Object bindings, RPC, retries, and user-facing rendering
remain in their runtime packages.

Protocol version 1 writes only version 1 and reads the current and N-1 versions.
Because version 1 has no predecessor, its readable set is currently `{1}`. A
future version bump must add compatibility fixtures before changing
`CURRENT_PROTOCOL_VERSION` or `MIN_READABLE_PROTOCOL_VERSION`.

The normative decisions and ownership tables are in
`docs/adr/0001-idempotency-and-transaction-state-machine.md` and
`docs/adr/0003-coordinator-redo-and-manifest-protocol.md`.
