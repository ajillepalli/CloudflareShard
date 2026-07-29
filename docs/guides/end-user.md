# Shardscope: a visitor's guide

**Live demo (free, no login, no risk):**
**[cloudflare-shard-shardscope.ananth-jillepalli.workers.dev/?demo=1](https://cloudflare-shard-shardscope.ananth-jillepalli.workers.dev/?demo=1)**

Shardscope is a dashboard that watches — and pokes at — a real database while
it's running. This page explains what you're looking at, what to click, and
why the thing it shows you is actually hard to build. No database background
required.

## The problem, in one paragraph

Big applications store more data than one computer can comfortably hold, so
they split their database across many machines — this is called
**sharding** (think of it like dividing one giant filing cabinet into a row
of smaller cabinets, where each folder still has exactly one home). That's
easy to do once, at the start. It's hard to do *live*: what happens when one
cabinet gets overloaded and you need to move some folders to a quieter one
— while people are still reading and writing those exact folders? If you get
this wrong, a write can vanish, land twice, or get lost mid-move. CloudflareShard
is a system that shards a SQL database across Cloudflare's infrastructure and
tries to make that live reshuffling safe. Shardscope is the dashboard that
lets you watch it happen and try to break it.

## What you're looking at

Shardscope has five "rooms," reachable from the icon rail on the left:
**Topology**, **Reshard**, **App**, **Edge**, and **Play**. Everything below
is written for the `?demo=1` link above, which uses realistic embedded sample
data instead of a live cluster — see the FAQ for what changes in live mode.
There's also a guided tour (look for a "tour" prompt on first load, or
deep-link into any room with a `?room=` URL parameter) and a **"Share this
view"** button in the top bar that copies a link back to exactly what you're
looking at, so you can send someone else the same screen.

### 1. Start here: Topology

This is the living map of the cluster: which machine ("shard") owns which
slice of data, drawn as it changes in real time. Along the top is an
**invariant scoreboard** — a strip of numbers that should never lie to you,
covering writes made, writes lost, and a checksum status (more on both
below).

Click **"Start the scenario."** This kicks off background write load against
one shard, so it visibly heats up while the scoreboard keeps counting — watch
the writes number climb and one node in the topology canvas glow hotter than
the rest. (In sample mode this is a local simulation running entirely in your
browser — see the FAQ below; in live mode it's a real `/api/load/start` call
driving real writes.) This is the "wow, it's actually doing something"
moment — nothing here is a canned animation, but a reshard doesn't happen on
its own: that's the next room, and it's a deliberate, human-triggered step,
not something the scenario auto-schedules.

### 2. The payoff: Reshard, and "Chaos — Break It"

**These two rooms are the one part of the site that only works against a
real, live cluster** — they're visible and clickable in sample mode too, but
clicking any of their buttons there returns an honest "demo mode — no live
cluster" message instead of doing anything, rather than being hidden or
faked. If you're on a live deployment (see the FAQ for the difference), this
is where the hot shard from step 1 actually gets resolved: buttons to
**split**, **migrate**, or **drain** a shard on demand, each one a real
operation against the running cluster.

The real point of this room is the **"Chaos — Break It"** panel folded into
it. On a live cluster, each button fires a genuine attack at the running
cluster while load is still going:

- **double-submit** — sends the exact same write twice at once, testing
  whether the system applies it once or twice.
- **mismatched-replay** — resends a write's ID with different contents,
  testing whether the system correctly refuses the mismatch instead of
  silently accepting it.
- **drain-hot-node**, **split-hot-vbucket**, **migrate-hot-vbucket**,
  **abort-migration**, **blip-shard-offline** — reshaping and disruption
  attacks: evacuating a shard, splitting or moving the busiest vbucket mid-load,
  cancelling a reshard partway through, and simulating a shard going
  briefly unreachable.

Every attack reports what it actually did, what a correct system is
*supposed* to do in response, and what was actually observed — a pass/fail
you can check, not a vibe. This is the demo's real payoff: real attacks,
real load, and the scoreboard's numbers are the referee.

### 3. The other rooms, briefly

- **App** — a small real application (a warehouse/inventory demo) built on
  top of CloudflareShard: pick a demo tenant, browse its data, and run one
  real multi-row transaction ("Restock"). This is aimed at "what would I
  actually build with this," including copy-pasteable code snippets.
- **Edge** — measures the round-trip time between your browser and the
  request currently being served, alongside a few reference points, so you
  can get a feel for Cloudflare's edge network.
- **Play** — a raw console for developers: fire the underlying API calls
  (`mutate`, `tx`, `sql`, and friends) directly and see the request/response,
  plus a routing inspector that shows you exactly which shard owns a given
  key and why.

## What "lost 0" and the checksum actually mean

The scoreboard's headline number is `lost`, and it should always read `0`.
Here's what that promise really covers, stated honestly rather than sold:

**What it does mean:** the dashboard keeps a running list of specific rows
it wrote and remembers what value each one should currently hold. On a
recurring cycle, it reads every one of those rows back and checks it matches.
As long as `lost` stays `0`, every row it has checked, every time it checked,
came back correct — including through live reshards and while chaos attacks
were firing. It's a continuous, repeating check, not a one-time sample, and
the counter can only ever go up, never quietly reset — if something is
ever actually lost, the meter turns red and stays red.

**What it does not mean:** it's not a mathematical guarantee that *nothing,
ever, anywhere, the instant it happens* could theoretically slip through.
A freshly-written row sits in a short queue before the next check cycle
covers it, and only one specific, representative table is tracked this way
(the others count toward the "writes" total but aren't individually
verified). The dashboard is upfront about this trade-off: it's an honest,
live signal you can watch happen in real time, not a formal proof. (The
project's actual formal proof — a deterministic test that writes a known
batch of data, drives a real reshard to completion, and checks every single
key was preserved with zero timing gaps — lives in the codebase's automated
test suite, not in this live UI.)

The **checksum** label next to `lost` is a related but separate signal. When
CloudflareShard finishes moving a vbucket to a new home, it computes a
checksum as part of that move and refuses to complete the move if it doesn't
match — that's a one-time, event-triggered check, not something continuously
re-verified in the background. The label reflects that: "verifying…" while a
move's checksum computation is in progress, "cutover verified" once one has
recently passed, "aborted" if one failed and the system correctly backed
out. It is never faked into a permanent "OK" the system isn't currently
re-checking.

## FAQ

**Is this real, or a recorded video?**
It's real, not a recording — but which parts are "real" depends on the mode.
In live mode, every click hits an actual Cloudflare-hosted cluster: real
writes, real reshards, real attacks. The `?demo=1` link above — clearly
labeled "SAMPLE DATA" in the UI — never touches real infrastructure: the
Topology room's "Start the scenario" runs a local simulation in your browser
(same visual story, zero network calls), while the Reshard and Chaos rooms
are live-only and will tell you so honestly if you click their buttons in
sample mode, rather than faking a result.

**What happens if I break something?**
Nothing, in sample mode — `?demo=1` never touches real infrastructure, so
there's nothing to break. The guided tour also never fires real operations,
even in live mode. If you do get access to live mode, the chaos attacks are
designed to be safe to fire repeatedly against the demo cluster; that's the
whole point of the exercise.

**Where's the code?**
This is all part of the CloudflareShard project. Start at the repository
root [README.md](../../README.md) for the full project, or
[examples/shardscope/README.md](../../examples/shardscope/README.md) for
Shardscope itself.

**I'm a developer — where do I start building?**
See [docs/guides/infrastructure-builder.md](infrastructure-builder.md) for a
guide aimed at actually building on top of CloudflareShard.
