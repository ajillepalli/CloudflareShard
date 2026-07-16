# Design System — Shardscope

> Scope: this design system applies ONLY to Shardscope, the demo/mission-control UI
> under `examples/shardscope/`. The CloudflareShard core is a headless database and
> has no UI. Created by /design-consultation on 2026-07-16. Rendered preview:
> `~/.gstack/projects/ajillepalli-CloudflareShard/designs/mockup-20260716/shardscope-hero.html`.

## Product Context
- **What this is:** A live "mission control" dashboard that makes a sharded edge
  database's invisible power visible — the hero moment is watching a cluster reshard
  under load (and under attack) while a scoreboard never leaves `lost: 0`.
- **Who it's for:** Infrastructure evaluators (is this trustworthy as my datastore?)
  and developers (could I build on this?).
- **Space/industry:** Distributed databases / observability tooling. Peers to read
  against: Vitess/PlanetScale (online resharding), Grafana/Datadog (observability
  dashboards) — Shardscope deliberately departs from both.
- **Project type:** Real-time operator dashboard (single-page app, four rooms).

## Memorable thing (the one impression everything serves)
**"It healed itself under fire and lost nothing."** Calm under chaos. Every visual
decision serves this: the reshard + the steady scoreboard are the heroes; chaos is
spatially contained; the interface never raises its voice.

## Aesthetic Direction
- **Direction:** Industrial / Utilitarian control room (mission-control gravity, not
  a generic SaaS dashboard).
- **Decoration level:** Minimal — the data, the topology, and the motion do the work.
- **Mood:** Serious distributed-systems instrumentation that stays composed while the
  cluster is on fire. Matte, cold, precise, quietly alive.
- **Cross-model note:** Claude + Codex design voices converged independently on this
  exact direction (dark control room, green proof-of-life, contained red pressure).

## Typography
- **Data / metrics / counters (primary carrier):** JetBrains Mono (fallback Berkeley
  Mono, ui-monospace) — all numbers, IDs, rates, log lines. ALWAYS
  `font-variant-numeric: tabular-nums` so live-updating counters never jitter.
- **UI / labels / headings:** a technical grotesk. Preferred: General Sans or Neue
  Haas Grotesk Text. NOTE: avoid Inter and Space Grotesk as primary (gstack flags
  both as the AI-convergence default); the mockup used a Space Grotesk fallback stack
  only as a placeholder — pick a licensed grotesk before build.
- **Micro-labels:** uppercase, 10-11px, `letter-spacing: .12em`, dim color — for
  section headers ("CHAOS — BREAK IT", "EVENT LOG").
- **Loading:** self-host woff2 (this is an offline-capable demo Worker; do not depend
  on a font CDN).
- **Scale (px):** micro 10.5 · label 11 · body 12-13 · data 13-14 · scoreboard 15-16 ·
  hot-shard rate emphasized. Mono-forward: the terminal character carries the feel.

## Color
- **Approach:** Restrained + strictly semantic. Color is rare and always means
  something. This is the load-bearing part of the system — get the semantics right.
- **Neutrals (cool near-black):**
  - `--bg #0A0E14` (app base) · `--surface #121826` (panels) · `--well #070A0F`
    (canvas, the deepest layer) · `--line #1E2733` (hairline) · `--line-soft #17202B`.
  - text: `--text #E6EAF2` · `--muted #8A94A6` · `--dim #5A6474`.
- **`--safe #35E3B0`** (cyan-green) — THE hero accent: `lost 0`, `checksum OK`, "live",
  active nav. Gets a slow breathing text-glow. It is the steadiest, calmest element on
  screen. Nothing else may use this color.
- **Heat ramp (sequential, the topology star):** `#1B4D5C` idle → `#3FA796` warm →
  `#E0B341` amber → `#E0603A` hot → `#F04A4A` critical. Encodes shard load only.
- **`--migration #37C7FF`** (calm cyan-blue) — in-flight vBuckets, migration paths,
  the current reshard op. Reads as *routine flow*, NOT alarm (this is why it's blue,
  not violet — resharding is normal, not scary).
- **`--danger #F04A4A`** (with `--danger-bright #FF7A6B`) — chaos/attack events ONLY
  (the "break it" panel, injected-fault pressure waves).
- **CRITICAL SEMANTIC RULE:** **Red never means "health."** System health is judged by
  invariants (lost/checksum/quorum/latency), not by red-equals-panic dashboard theater.
  Heat-critical red = "this shard is hot" (a state). Danger red = "an attack is landing"
  (an event). If the database is surviving, the interface stays composed and green.
- **Light mode:** none. This is a dark-only control room by design; a light theme would
  contradict the memorable thing.

## Spacing
- **Base unit:** 4px.
- **Density:** Comfortable-dense — data-rich but never cramped; panels breathe, the
  canvas is generous.
- **Scale:** 2xs(2) xs(4) sm(8) md(12) lg(16) xl(20) 2xl(28) 3xl(40).

## Layout
- **Approach:** Composition-first, NOT a metric-tile grid. One living topology canvas
  owns the first viewport; charts/tables are subordinate evidence.
- **Shell:** top status strip (56px: brand · cluster identity · invariant scoreboard)
  · left icon rail (64px: App / Topology / Reshard / Playground) · center hero canvas
  (`--well`) · right console (320px: current op + operator buttons + chaos panel +
  live event log). Full-viewport, no page scroll; inner panels scroll.
- **Invariant scoreboard (always visible, top strip):** `writes N · lost 0 ·
  checksum OK` (+ `reshards: live`, `p99 edge` when the edge map ships). This is the
  contract with the viewer: it stays pinned and steady no matter what the canvas does.
- **Border radius:** small and technical — sm 3px, md 5-6px, chips 3px. No bubble radius.
- **Max content width:** none (full-bleed control surface).

## Motion
- **Approach:** Intentional and physical. Motion always represents real traffic,
  migration, replication, or attack — never decoration.
- **vBucket migration:** buckets travel as cyan packets along a routed, marching-ants
  path with a short phosphor trail and a snap-in commit pulse when the cutover lands.
- **Heat:** hot shards breathe via opacity/edge-intensity only — never frantic pulsing.
- **Calm under chaos:** attack events land as contained red pressure-waves on specific
  shards/regions while the topology keeps flowing, the safe counter stays green, and
  `lost 0` stays pinned. The contrast IS the thesis.
- **Scoreboard:** a slow (~4s) breathing glow on the safe values — alive but serene.
- **Easing/Duration:** enter ease-out, exit ease-in, move ease-in-out; micro 80ms ·
  short 200ms · medium 320ms · long 600ms.

## Deliberate departures (where Shardscope gets its own face)
1. **One living canvas, not a metric wall.** The primary object is the system's shape
   under stress (topology, ownership, migration, invariants) — not a grid of KPI tiles.
2. **Red ≠ panic.** Health is invariants, not color. A rejection of observability
   dashboard theater. Composure is the product.

## Anti-slop (never do)
No purple/violet gradients · no glassmorphism / frosted panels · no card grids · no
oversized KPI tiles · no rainbow heat maps · no decorative network spaghetti (every
animated line must mean traffic/migration/replication/attack) · no `system-ui` as the
primary display face in the shipped build (mockup fallback only) · no Inter/Space
Grotesk as primary · no centered-everything · no gradient CTA buttons.

## Decisions Log
| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-07-16 | Initial design system created | /design-consultation; anchor = "healed itself, lost nothing" (calm under chaos) |
| 2026-07-16 | Migration color = cyan `#37C7FF`, not violet | Cross-model (Codex): resharding is routine, must read as calm flow not alarm |
| 2026-07-16 | Red reserved for heat-state + attack; never "health" | Cross-model (Codex): health = invariants; reject red-equals-panic dashboard theater |
| 2026-07-16 | Dark-only, no light theme | A light theme contradicts the control-room memorable thing |
