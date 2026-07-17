/** Shardscope — Topology room client.
 *
 * No-build-step ESM. Connects to GET /api/stream (see ../src/aggregator.ts,
 * TopologyAggregator) and renders each `event: snapshot` frame as the living
 * topology canvas described in ../DESIGN.md.
 *
 * ----------------------------------------------------------------------------
 * Field mapping (must match aggregator.ts's TopologySnapshot exactly — no
 * invented fields):
 *
 *   snapshot.ts                                    -> last-updated timestamp
 *   snapshot.cluster.initialized                   -> empty-state gate
 *   snapshot.cluster.catalogShardCount              -> topbar cluster identity
 *   snapshot.cluster.shards.{total,active,draining} -> topbar scoreboard
 *   snapshot.catalogs[].catalogShardId               -> (not rendered directly;
 *                                                        used to tag rows)
 *   snapshot.catalogs[].totalVBuckets                -> topbar cluster identity
 *                                                        (summed across catalogs)
 *   snapshot.catalogs[].vbuckets[].vbucket           -> vbucket id (ring tooltip)
 *   snapshot.catalogs[].vbuckets[].shardId           -> current owner -> node
 *   snapshot.catalogs[].vbuckets[].migrationStatus   -> "none" vs in-flight
 *   snapshot.catalogs[].vbuckets[].targetShardId     -> migration path target
 *   snapshot.catalogs[].vbuckets[].cutoverStartedAt  -> migration phase label
 *   snapshot.shards[].shardId                        -> node identity
 *   snapshot.shards[].stats                          -> heat load proxy (see
 *                                                        computeLoadScore below)
 *   snapshot.shards[].error                          -> "unavailable" node state
 *   snapshot.scoreboard.writesAcked                  -> topbar "writes N"
 *   snapshot.scoreboard.trackedKeyCount              -> topbar "N keys continuously verified"
 *                                                        (the HONEST scope qualifier — as of
 *                                                        correctness.ts's round 7 this is a
 *                                                        COMPLETE count over the TRACKED SET
 *                                                        (every distinct tpcc_stock key this run
 *                                                        has resolved+promoted a write for), not a
 *                                                        bounded sample; see aggregator.ts's
 *                                                        Scoreboard doc comment and
 *                                                        correctness.ts's "ROUND 8" HONEST SCOPE
 *                                                        section: `lost` alone would still
 *                                                        overclaim — this is a LIVE, ongoing check
 *                                                        over the tracked set as of the last
 *                                                        verify() pass, never "every write this
 *                                                        run made is confirmed safe." The
 *                                                        deterministic, zero-window, complete
 *                                                        end-to-end guarantee lives in
 *                                                        ../src/load/reshard.integration.test.ts,
 *                                                        not in this live meter)
 *   snapshot.scoreboard.lost                         -> topbar "· lost N" (RED if > 0)
 *   snapshot.scoreboard.meterState                   -> "green" | "red"
 *   snapshot.scoreboard.verified                     -> design round 3: false means the tracked
 *                                                        set has changed (or nothing was ever
 *                                                        checked) since the last verify() pass —
 *                                                        renders a DISTINCT amber "not verified"
 *                                                        state instead of a reassuring green
 *                                                        `lost 0`. Computed once inside
 *                                                        ./load/correctness.ts's
 *                                                        CorrectnessTracker.snapshot() and
 *                                                        forwarded verbatim through
 *                                                        aggregator.ts — see renderScoreboard
 *                                                        below for why a RED `lost > 0` must
 *                                                        still win regardless of this flag
 *   snapshot.scoreboard.loadRunning                  -> false -> writes/lost render as 0, not stale
 *   snapshot.scoreboard.checksum.{label,state}        -> topbar "checksum <label>"
 *                                                        (see ../src/aggregator.ts's
 *                                                        deriveChecksumStatus — a
 *                                                        derived, honest label, never
 *                                                        a fabricated permanent "OK")
 *
 * event: error frames -> { message } -> shown as a calm inline banner; last
 * good render is left on screen (this is the whole point of the aggregator's
 * per-shard non-fatal error design — see aggregator.ts's pollSnapshot doc
 * comment).
 * ----------------------------------------------------------------------------
 */

// ============================================================================
// DOM hooks
// ============================================================================

const hook = (name) => document.querySelector(`[data-hook="${name}"]`);

const el = {
  sampleBadge: hook("sample-badge"),
  clusterId: hook("cluster-id"),
  clusterInit: hook("cluster-init"),
  sbShards: hook("sb-shards"),
  sbClusterStatus: hook("sb-cluster-status"),
  sbWrites: hook("sb-writes"),
  sbLost: hook("sb-lost"),
  sbChecksum: hook("sb-checksum"),
  liveDot: hook("live-dot"),
  canvasStatus: hook("canvas-status"),
  canvasSub: hook("canvas-sub"),
  statusBanner: hook("status-banner"),
  emptyState: hook("empty-state"),
  arcLayer: hook("arc-layer"),
  nodesLayer: hook("nodes-layer"),
  liveChip: hook("live-chip"),
  liveChipLabel: hook("live-chip-label"),
  eventLog: hook("event-log"),
  loginPanel: hook("login-panel"),
  loginForm: hook("login-form"),
  loginTokenInput: hook("login-token-input"),
  loginSubmit: hook("login-submit"),
  loginError: hook("login-error"),
  logoutBtn: hook("logout-btn"),

  // ---- Reshard room (T8) ----
  railTopology: hook("rail-topology"),
  railReshard: hook("rail-reshard"),
  consoleTitle: hook("console-title"),
  reshardPanel: hook("reshard-panel"),

  // ---- Edge room (T11) ----
  railEdge: hook("rail-edge"),
  canvasWrap: hook("canvas-wrap"),
  edgeWrap: hook("edge-wrap"),
  edgeLiveDot: hook("edge-live-dot"),
  edgeStatus: hook("edge-status"),
  edgeHeroValue: hook("edge-hero-value"),
  edgeHeroServed: hook("edge-hero-served"),
  edgeHeroCaption: hook("edge-hero-caption"),
  edgeRemeasureBtn: hook("edge-remeasure-btn"),
  edgeMapSvg: hook("edge-map-svg"),

  // ---- "The old way" contrast beat (T10) ----
  oldwayToggle: hook("oldway-toggle"),
  oldwayBody: hook("oldway-body"),
  oldwayChevron: hook("oldway-chevron"),
  opCard: hook("op-card"),
  opCardName: hook("op-card-name"),
  opCardDetail: hook("op-card-detail"),
  opAbortBtn: hook("op-abort-btn"),
  lockState: hook("lock-state"),
  lockDetail: hook("lock-detail"),
  lockReleaseBtn: hook("lock-release-btn"),
  lockError: hook("lock-error"),
  opTabSplit: hook("op-tab-split"),
  opTabMigrate: hook("op-tab-migrate"),
  opTabDrain: hook("op-tab-drain"),
  opFormSplit: hook("op-form-split"),
  opFormMigrate: hook("op-form-migrate"),
  opFormDrain: hook("op-form-drain"),
  splitCatalogSelect: hook("split-catalog"),
  splitVbucketSelect: hook("split-vbucket"),
  splitNewShardInput: hook("split-new-shard"),
  migrateCatalogSelect: hook("migrate-catalog"),
  migrateVbucketSelect: hook("migrate-vbucket"),
  migrateTargetSelect: hook("migrate-target"),
  drainCatalogSelect: hook("drain-catalog"),
  drainShardSelect: hook("drain-shard-select"),
  reshardError: hook("reshard-error"),

  // ---- Chaos "Break It" panel (T9) ----
  chaosAttackStack: hook("chaos-attack-stack"),
  chaosResult: hook("chaos-result"),
  chaosResultVerdict: hook("chaos-result-verdict"),
  chaosResultAttack: hook("chaos-result-attack"),
  chaosResultDid: hook("chaos-result-did"),
  chaosResultExpected: hook("chaos-result-expected"),
  chaosResultObserved: hook("chaos-result-observed"),
  chaosResultNote: hook("chaos-result-note"),
  chaosError: hook("chaos-error"),
};

// ============================================================================
// Constants
// ============================================================================

// DESIGN.md heat ramp: idle -> warm -> amber -> hot -> critical.
const HEAT_STOPS = ["#1B4D5C", "#3FA796", "#E0B341", "#E0603A", "#F04A4A"];

// How long to wait for a first "hello"/"snapshot" frame before assuming the
// live stream is unreachable (static preview, worker not running, etc.) and
// falling back to the embedded sample so the page still shows something.
const FALLBACK_TIMEOUT_MS = 6000;

const MAX_LOG_LINES = 60;
// Cap how many migration paths get drawn per tick — a real cluster mid-full
// reshard could have hundreds of individually-migrating vbuckets, and this is
// a topology view, not a table; paths are deduped by (source, target) pair
// first, so this cap is about "N distinct migration routes", not "N rows".
const MAX_MIGRATION_PATHS = 40;
// Stylized per-shard vbucket ring: always drawn with this many dots,
// proportionally colored/filled — NOT a literal 1-dot-per-vbucket rendering
// (a shard can own hundreds of vbuckets). Exact counts live in the vring's
// title attribute and the shard's secondary text line.
const RING_DOT_COUNT = 8;

// ============================================================================
// Small embedded sample TopologySnapshot — SAMPLE DATA, clearly labeled
// wherever it's rendered (topbar badge + canvas status + inline banner).
// Shaped exactly like aggregator.ts's TopologySnapshot / ShardDO.handleStats
// response (src/shard.ts) — every field here is one a real snapshot would
// actually carry, nothing invented. Used for `?demo=1` and as the fallback
// when a live /api/stream can't be reached.
// ============================================================================

function buildSampleSnapshot() {
  const now = Date.now();
  const cutoverTs = new Date(now - 4200).toISOString();

  const statsFor = (tables, pendingIntentCount, indexPendingJobCount) => ({
    ok: true,
    tables,
    idempotencyTableSize: tables.reduce((a, t) => a + t.rowCount, 0) + 40,
    pendingIntentCount,
    indexPendingJobCount,
    indexEntryCount: tables.reduce((a, t) => a + t.rowCount, 0) * 2,
    rowOwnerCount: tables.reduce((a, t) => a + t.rowCount, 0),
  });

  return {
    ts: now,
    cluster: {
      initialized: true,
      catalogShardCount: 2,
      shards: { total: 6, active: 5, draining: 1 },
    },
    catalogs: [
      {
        catalogShardId: "catalog-0",
        totalVBuckets: 8,
        vbuckets: [
          { vbucket: 0, shardId: "shard-0", migrationStatus: "none", targetShardId: null, cutoverStartedAt: null },
          { vbucket: 1, shardId: "shard-1", migrationStatus: "none", targetShardId: null, cutoverStartedAt: null },
          { vbucket: 2, shardId: "shard-2", migrationStatus: "none", targetShardId: null, cutoverStartedAt: null },
          { vbucket: 3, shardId: "shard-3", migrationStatus: "none", targetShardId: null, cutoverStartedAt: null },
          { vbucket: 4, shardId: "shard-4", migrationStatus: "none", targetShardId: null, cutoverStartedAt: null },
          { vbucket: 5, shardId: "shard-5", migrationStatus: "migrating", targetShardId: "shard-0", cutoverStartedAt: null },
          { vbucket: 6, shardId: "shard-5", migrationStatus: "cutover", targetShardId: "shard-0", cutoverStartedAt: cutoverTs },
          { vbucket: 7, shardId: "shard-1", migrationStatus: "none", targetShardId: null, cutoverStartedAt: null },
        ],
      },
      {
        catalogShardId: "catalog-1",
        totalVBuckets: 8,
        vbuckets: [
          { vbucket: 0, shardId: "shard-0", migrationStatus: "none", targetShardId: null, cutoverStartedAt: null },
          { vbucket: 1, shardId: "shard-1", migrationStatus: "none", targetShardId: null, cutoverStartedAt: null },
          { vbucket: 2, shardId: "shard-2", migrationStatus: "none", targetShardId: null, cutoverStartedAt: null },
          { vbucket: 3, shardId: "shard-3", migrationStatus: "none", targetShardId: null, cutoverStartedAt: null },
          { vbucket: 4, shardId: "shard-3", migrationStatus: "none", targetShardId: null, cutoverStartedAt: null },
          { vbucket: 5, shardId: "shard-4", migrationStatus: "none", targetShardId: null, cutoverStartedAt: null },
          { vbucket: 6, shardId: "shard-5", migrationStatus: "migrating", targetShardId: "shard-0", cutoverStartedAt: null },
          { vbucket: 7, shardId: "shard-5", migrationStatus: "cutover", targetShardId: "shard-0", cutoverStartedAt: cutoverTs },
        ],
      },
    ],
    shards: [
      { shardId: "shard-0", stats: statsFor([{ table: "orders", rowCount: 812 }, { table: "order_line", rowCount: 3120 }], 1, 0) },
      { shardId: "shard-1", stats: statsFor([{ table: "orders", rowCount: 240 }, { table: "order_line", rowCount: 900 }], 0, 0) },
      { shardId: "shard-2", stats: null, error: "adminShardStats failed: shard-2 unreachable (simulated blip)" },
      { shardId: "shard-3", stats: statsFor([{ table: "orders", rowCount: 4310 }, { table: "order_line", rowCount: 16220 }], 9, 4) },
      { shardId: "shard-4", stats: statsFor([{ table: "orders", rowCount: 1510 }, { table: "order_line", rowCount: 5920 }], 3, 1) },
      { shardId: "shard-5", stats: statsFor([{ table: "orders", rowCount: 96 }, { table: "order_line", rowCount: 310 }], 0, 0) },
    ],
    // Shardscope T4 — shaped exactly like aggregator.ts's Scoreboard (see
    // that file's header comment). Illustrative only: "verifying…" matches
    // this sample's in-flight cutover row (vbucket 7, above) — always
    // rendered behind the "SAMPLE DATA" badge, never mistaken for a live
    // read.
    scoreboard: {
      writesAcked: 48213,
      writesRetriedIdempotent: 12,
      txAbortedExpected: 340,
      lost: 0,
      trackedKeyCount: 50,
      meterState: "green",
      verified: true,
      loadRunning: true,
      checksum: { label: "verifying…", state: "verifying" },
    },
  };
}

// ============================================================================
// Math / formatting helpers
// ============================================================================

function clamp(v, lo, hi) {
  return Math.min(hi, Math.max(lo, v));
}
function lerp(a, b, t) {
  return a + (b - a) * t;
}
function normalize(v, lo, hi) {
  if (hi - lo < 1e-9) return 0;
  return clamp((v - lo) / (hi - lo), 0, 1);
}
function fmtInt(n) {
  if (typeof n !== "number" || !Number.isFinite(n)) return "—";
  return n.toLocaleString("en-US");
}
function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}
/** Deterministic 32-bit string hash (FNV-1a) — used only for stable, repeatable
 * layout jitter and migration-arc bow direction, never for anything security
 * sensitive. */
function hashString(str) {
  let h = 2166136261;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}

function hexToRgb(hex) {
  const h = hex.replace("#", "");
  return { r: parseInt(h.slice(0, 2), 16), g: parseInt(h.slice(2, 4), 16), b: parseInt(h.slice(4, 6), 16) };
}
function rgbToHex({ r, g, b }) {
  const c = (n) => clamp(Math.round(n), 0, 255).toString(16).padStart(2, "0");
  return `#${c(r)}${c(g)}${c(b)}`;
}
function lerpHex(hexA, hexB, t) {
  const a = hexToRgb(hexA);
  const b = hexToRgb(hexB);
  return rgbToHex({ r: lerp(a.r, b.r, t), g: lerp(a.g, b.g, t), b: lerp(a.b, b.b, t) });
}
/** Maps a normalized load t in [0,1] onto the 5-stop DESIGN.md heat ramp. */
function heatColorForT(t) {
  const stops = HEAT_STOPS;
  const scaled = clamp(t, 0, 1) * (stops.length - 1);
  const i = Math.min(stops.length - 2, Math.floor(scaled));
  const f = scaled - i;
  return lerpHex(stops[i], stops[i + 1], f);
}

function quadPoint(p0, p1, p2, t) {
  const x = (1 - t) * (1 - t) * p0.x + 2 * (1 - t) * t * p1.x + t * t * p2.x;
  const y = (1 - t) * (1 - t) * p0.y + 2 * (1 - t) * t * p1.y + t * t * p2.y;
  return { x, y };
}
/** Bows a straight source->target line into a gentle arc (mirrors the
 * mockup's curved migration path) — direction is derived from a hash of the
 * pair key so it's stable across re-renders instead of flickering. */
function controlPoint(a, b, pairKey) {
  const mx = (a.x + b.x) / 2;
  const my = (a.y + b.y) / 2;
  const dx = b.x - a.x;
  const dy = b.y - a.y;
  const len = Math.hypot(dx, dy) || 1;
  const px = -dy / len;
  const py = dx / len;
  const sign = hashString(pairKey) % 2 === 0 ? 1 : -1;
  const bow = clamp(len * 0.22, 4, 18) * sign;
  return { x: mx + px * bow, y: my + py * bow };
}

/** Deterministic layout: evenly spaced around an ellipse centered in the
 * canvas, with small per-shard jitter (from a hash of the id) so it doesn't
 * read as a perfectly mechanical ring. Stable across renders/tick as long as
 * the shard id set doesn't change, so nodes don't jump around every 900ms. */
function layoutShardPositions(shardIds) {
  const positions = new Map();
  const n = shardIds.length;
  if (n === 0) return positions;
  if (n === 1) {
    positions.set(shardIds[0], { x: 50, y: 50 });
    return positions;
  }
  const cx = 50;
  const cy = 52;
  const baseRadius = n <= 6 ? 28 : n <= 12 ? 33 : 37;
  const sorted = [...shardIds].sort();
  sorted.forEach((id, i) => {
    const angle = (i / n) * Math.PI * 2 - Math.PI / 2;
    const h = hashString(id);
    const rJitter = ((h % 100) / 100 - 0.5) * 6;
    const aJitter = (((h >>> 8) % 100) / 100 - 0.5) * 0.12;
    const r = baseRadius + rJitter;
    const a = angle + aJitter;
    const x = cx + r * Math.cos(a) * 1.55;
    const y = cy + r * Math.sin(a);
    positions.set(id, { x: clamp(x, 10, 90), y: clamp(y, 16, 84) });
  });
  return positions;
}

// ============================================================================
// UI micro-state helpers (banner / badges / log)
// ============================================================================

function showBanner(text, level) {
  el.statusBanner.textContent = text;
  el.statusBanner.className = "status-banner" + (level ? " " + level : "");
  el.statusBanner.hidden = false;
}
function clearBanner() {
  el.statusBanner.hidden = true;
}
function showSampleBadge(show) {
  el.sampleBadge.hidden = !show;
}
function setCanvasStatus(text) {
  el.canvasStatus.textContent = "Topology — " + text;
}

// ============================================================================
// Shardscope T4 — invariant scoreboard (writes / lost / checksum). Renders
// snapshot.scoreboard exactly as aggregator.ts's Scoreboard shapes it (see
// this file's field-mapping doc comment at the top) — replaces the earlier
// "not wired yet" placeholder. Per DESIGN.md: --safe breathing glow when
// lost is 0 and the checksum is idle/verified; a distinct, solid RED
// treatment when lost > 0 — the ONE legitimate health-red this dashboard
// ever shows, because a lost write IS a health failure (DESIGN.md: "red
// never means health" — this is the documented exception, not a violation
// of it).
// ============================================================================

/** Maps a checksum state (aggregator.ts's ChecksumState) onto the scoreboard
 * chip's visual treatment. "idle"/"verified" are calm, settled GREEN states
 * (nothing wrong, nothing in flight). "backfilling"/"verifying"/"aborting"
 * are ACTIVE, routine states — a reshard in progress is normal, not scary
 * (DESIGN.md: migration reads as calm flow, hence --migration cyan, not
 * red). "stalled" is the one checksum state worth flagging for operator
 * attention (amber, matching this scoreboard's existing "degraded" amber
 * convention) — NOT red, because a stalled cutover is an operational
 * concern, not by itself a proven data loss (only `lost` reports that).
 * "aborted" is neutral: an operator or an automatic guard cancelled a
 * migration, which is expected admin behavior, not a failure signal. */
function checksumClassFor(state) {
  if (state === "idle" || state === "verified") return "safe";
  if (state === "backfilling" || state === "verifying" || state === "aborting") return "migrate";
  if (state === "stalled") return "degraded";
  return ""; // "aborted" and any unrecognized future state — neutral, no color claim either way
}

function renderScoreboard(scoreboard) {
  if (!el.sbWrites || !el.sbLost || !el.sbChecksum) return; // defensive: hooks always exist in the shipped index.html, but never crash a render over a missing DOM node
  if (!scoreboard) {
    el.sbWrites.textContent = "writes —";
    el.sbLost.className = "sb-item";
    el.sbLost.textContent = "— keys continuously verified · lost —";
    el.sbChecksum.className = "sb-item";
    el.sbChecksum.textContent = "checksum —";
    return;
  }

  // scoreboard.loadRunning === false already means writesAcked/lost are 0 at
  // the source (aggregator.ts's mergeScoreboard forces this — see that
  // file's Scoreboard doc comment on why a previous run's stale totals are
  // never shown as if they were live). Rendered here exactly as received,
  // no separate client-side zeroing — one source of truth for "honest, not
  // fake-green".
  el.sbWrites.textContent = `writes ${fmtInt(scoreboard.writesAcked)}`;

  // HONEST FRAMING (see aggregator.ts's Scoreboard doc comment and
  // correctness.ts's "ROUND 8" HONEST SCOPE header section — this is the
  // round-8 fix for Codex round 7's finding: "the UI's 'complete over every
  // write' claim overclaims what a LIVE meter can guarantee"): as of
  // correctness.ts's round 7 ("ROUND 7 — EVICTION REMOVED"), `lost` is a
  // COMPLETE count over the TRACKED SET — every distinct tpcc_stock key this
  // run has resolved+promoted a write for — not a bounded, biased sample of
  // it (that was the pre-round-7 design). It is NOT "every write this run
  // made is confirmed safe": only tpcc_stock rows are individually
  // verifiable at all (only this run); and even for a tracked key, there is
  // an irreducible ack->resolve->promote->verify pipeline, so this is a
  // LIVE, continuously-updating check over the tracked set as of the last
  // verify() pass, not an instantaneous zero-window guarantee. The
  // deterministic, complete, zero-window end-to-end proof instead lives in
  // ../src/load/reshard.integration.test.ts (writes a known batch, drives a
  // real reshard, then verifies every one of those keys once, after the
  // fact). A bare "lost 0" would still imply more than this live meter
  // actually checked — always pair it with trackedKeyCount and frame it as
  // an ongoing check, not a settled guarantee: "N keys continuously verified
  // · lost 0", never "every write is safe".
  //
  // Design round 3, point 3: RED must be checked FIRST, independent of
  // `verified` — `scoreboard.verified` (see aggregator.ts's Scoreboard doc
  // comment) is only ever a gate on the GREEN case ("is a calm --safe claim
  // genuinely current"), never a way to suppress a proven loss. `lost > 0`
  // means verify() (or a disproven idempotent-replay claim) genuinely found
  // a missing/mismatched row — that is proven, not stale, no matter what
  // has happened to the tracked set since.
  const isRed = scoreboard.meterState === "red" || scoreboard.lost > 0;
  if (isRed) {
    el.sbLost.className = "sb-item lost-red";
    el.sbLost.textContent = `${fmtInt(scoreboard.trackedKeyCount)} keys continuously verified · lost ${fmtInt(scoreboard.lost)}`;
  } else if (!scoreboard.verified) {
    // `scoreboard.verified === false` means the tracked SET has changed (a
    // new key promoted, a value refreshed, an eviction) since the last
    // verify() pass covered it, or no pass with anything to check has ever
    // run — see ./load/correctness.ts's CorrectnessTracker.snapshot() doc
    // comment. That state must never render as the same calm --safe green
    // as "verified, lost 0" — it gets its own, visually distinct amber
    // treatment (the same .degraded convention this scoreboard already
    // uses for a stalled-but-not-proven-lost checksum): nothing is PROVEN
    // lost (handled above), but nothing is PROVEN safe either.
    el.sbLost.className = "sb-item degraded";
    el.sbLost.textContent = `${fmtInt(scoreboard.trackedKeyCount)} keys continuously verified · not verified yet`;
  } else {
    el.sbLost.className = "sb-item safe";
    el.sbLost.textContent = `${fmtInt(scoreboard.trackedKeyCount)} keys continuously verified · lost ${fmtInt(scoreboard.lost)}`;
  }

  const checksum = scoreboard.checksum || { label: "—", state: "idle" };
  const checksumClass = checksumClassFor(checksum.state);
  el.sbChecksum.className = "sb-item" + (checksumClass ? " " + checksumClass : "");
  el.sbChecksum.textContent = `checksum ${checksum.label}`;
}
/** state: 'connecting' | 'live' | 'warn' | 'demo' */
function setLiveState(state, label) {
  const isLive = state === "live";
  const isWarnish = state === "warn" || state === "demo";
  el.liveChip.className = "live-chip" + (isLive ? " live" : isWarnish ? " warn" : "");
  el.liveChipLabel.textContent = label || state;
  el.liveDot.className = "dot-live" + (isLive ? " live" : isWarnish ? " stale" : "");
}
function logLine(text, cls) {
  const div = document.createElement("div");
  div.className = "log-line" + (cls ? " " + cls : "");
  const t = document.createElement("span");
  t.className = "t";
  t.textContent = new Date().toLocaleTimeString();
  div.appendChild(t);
  div.appendChild(document.createTextNode("  " + text));
  el.eventLog.appendChild(div);
  // .log-box is column-reverse, so the newest DOM child renders at the top;
  // trim the oldest (first) child once we're over the cap.
  while (el.eventLog.childElementCount > MAX_LOG_LINES) {
    el.eventLog.removeChild(el.eventLog.firstChild);
  }
}

// ============================================================================
// Heat / load proxy
//
// The current TopologySnapshot has no writes/s or any time-series rate (T4 —
// the correctness/metrics core — hasn't landed). The only per-shard signal
// available is a single point-in-time stats snapshot shaped like
// ShardDO.handleStats (src/shard.ts): { tables:[{table,rowCount}],
// idempotencyTableSize, pendingIntentCount, indexPendingJobCount,
// indexEntryCount, rowOwnerCount }.
//
// Load proxy chosen: pendingIntentCount + indexPendingJobCount, i.e. work
// this shard has IN FLIGHT right now (prepared-but-uncommitted coordinator
// transactions + async index catch-up backlog). This is the closest thing to
// "how hot is this shard at this instant" the stats payload offers — unlike
// total row count (a size metric that says nothing about current activity),
// pending counts reflect live contention/backlog. Normalized min-max across
// shards that returned stats this tick, then mapped onto the 5-stop heat
// ramp. If every shard is quiescent (all pending counts are 0), everything
// renders idle — a legitimate "cluster is quiet" state, not a bug.
// ============================================================================

function computeLoadScore(stats) {
  if (!stats || typeof stats !== "object") return null;
  const pending = typeof stats.pendingIntentCount === "number" ? stats.pendingIntentCount : 0;
  const indexBacklog = typeof stats.indexPendingJobCount === "number" ? stats.indexPendingJobCount : 0;
  return pending + indexBacklog;
}
function computeTotalRows(stats) {
  if (!stats || !Array.isArray(stats.tables)) return null;
  let sum = 0;
  for (const t of stats.tables) {
    if (t && typeof t.rowCount === "number") sum += t.rowCount;
  }
  return sum;
}

// ============================================================================
// Render
// ============================================================================

let lastRenderedSnapshot = null;

function render(snapshot) {
  try {
    renderInner(snapshot);
    lastRenderedSnapshot = snapshot;
    // Reshard room's target pickers read straight off lastRenderedSnapshot —
    // keep them in step with every tick (?demo=1 and the sample fallback
    // included; there is no second data source for the Reshard room, see
    // this file's "Reshard room" section below).
    refreshReshardPickers();
  } catch (err) {
    // Never let a render bug blank the canvas — keep whatever was already
    // painted and surface the failure quietly.
    console.error("shardscope: render failed", err);
    showBanner("render error — showing last known state (see console)", "warn");
  }
}

function renderInner(snapshot) {
  const cluster = snapshot.cluster || {};
  const catalogs = Array.isArray(snapshot.catalogs) ? snapshot.catalogs : [];
  const shardStatsList = Array.isArray(snapshot.shards) ? snapshot.shards : [];

  // ---- topbar: cluster identity ----
  const totalVBuckets = catalogs.reduce((acc, c) => acc + (c.totalVBuckets || 0), 0);
  el.clusterInit.innerHTML =
    `<b>${fmtInt(cluster.catalogShardCount)}</b> catalog shard(s)` +
    `<span class="sep">·</span><b>${fmtInt(cluster.shards && cluster.shards.total)}</b> shards` +
    `<span class="sep">·</span><b>${fmtInt(totalVBuckets)}</b> vBuckets`;

  // ---- scoreboard ----
  el.sbShards.innerHTML =
    `shards <b>${fmtInt(cluster.shards && cluster.shards.active)}</b>/<b>${fmtInt(cluster.shards && cluster.shards.total)}</b> active` +
    (cluster.shards && cluster.shards.draining ? ` · <b>${fmtInt(cluster.shards.draining)}</b> draining` : "");

  const anyShardError = shardStatsList.some((s) => s.stats == null);
  el.sbClusterStatus.className = "sb-item" + (!cluster.initialized ? "" : anyShardError ? " degraded" : "");
  el.sbClusterStatus.textContent = !cluster.initialized ? "cluster: not initialized" : anyShardError ? "cluster: degraded" : "cluster: healthy";

  renderScoreboard(snapshot.scoreboard);

  // ---- empty state: cluster not initialized ----
  if (!cluster.initialized) {
    el.emptyState.hidden = false;
    el.nodesLayer.innerHTML = "";
    el.arcLayer.querySelectorAll("path.migration-path").forEach((p) => p.remove());
    el.canvasSub.textContent = "";
    setCanvasStatus("not initialized");
    return;
  }
  el.emptyState.hidden = true;

  // ---- shard id union: vbucket map (owners + targets) UNION shards[] ----
  const shardIds = new Set();
  for (const cat of catalogs) {
    for (const row of cat.vbuckets || []) {
      if (row.shardId) shardIds.add(row.shardId);
      if (row.targetShardId) shardIds.add(row.targetShardId);
    }
  }
  for (const s of shardStatsList) {
    if (s.shardId) shardIds.add(s.shardId);
  }
  const shardIdList = [...shardIds];

  if (shardIdList.length === 0) {
    el.nodesLayer.innerHTML = "";
    el.arcLayer.querySelectorAll("path.migration-path").forEach((p) => p.remove());
    el.canvasSub.textContent = "no shards reported";
    setCanvasStatus(el.canvasStatus.dataset.mode || "live");
    return;
  }

  const statsByShard = new Map(shardStatsList.map((s) => [s.shardId, s]));

  // owned vbucket rows per shard, across all catalogs
  const ownedByShard = new Map(shardIdList.map((id) => [id, []]));
  for (const cat of catalogs) {
    for (const row of cat.vbuckets || []) {
      const bucket = ownedByShard.get(row.shardId);
      if (bucket) bucket.push(row);
    }
  }

  // migration pairs, deduped by (source -> target)
  const migrationPairs = new Map();
  let migratingRowCount = 0;
  for (const cat of catalogs) {
    for (const row of cat.vbuckets || []) {
      if (row.migrationStatus && row.migrationStatus !== "none" && row.targetShardId) {
        migratingRowCount++;
        const key = `${row.shardId}=>${row.targetShardId}`;
        if (!migrationPairs.has(key)) {
          migrationPairs.set(key, { source: row.shardId, target: row.targetShardId, count: 0, anyCutover: false });
        }
        const p = migrationPairs.get(key);
        p.count++;
        if (row.cutoverStartedAt) p.anyCutover = true;
      }
    }
  }
  const incomingByShard = new Map();
  for (const p of migrationPairs.values()) {
    if (!incomingByShard.has(p.target)) incomingByShard.set(p.target, []);
    incomingByShard.get(p.target).push(p);
  }

  // load scores (heat) — normalize across shards that returned real stats
  const loadScores = new Map();
  for (const id of shardIdList) {
    const entry = statsByShard.get(id);
    const score = entry ? computeLoadScore(entry.stats) : null;
    if (score != null) loadScores.set(id, score);
  }
  const loadValues = [...loadScores.values()];
  const minLoad = loadValues.length ? Math.min(...loadValues) : 0;
  const maxLoad = loadValues.length ? Math.max(...loadValues) : 0;

  // sizing by owned-bucket count
  const ownedCounts = shardIdList.map((id) => ownedByShard.get(id).length);
  const minOwned = Math.min(...ownedCounts);
  const maxOwned = Math.max(...ownedCounts);

  const positions = layoutShardPositions(shardIdList);

  // ---- build shard nodes ----
  const nodeHtml = [];
  const flightDotHtml = [];
  let unavailableCount = 0;
  let drainingCount = 0;

  for (const id of shardIdList) {
    const pos = positions.get(id);
    const owned = ownedByShard.get(id);
    const outgoing = owned.filter((r) => r.migrationStatus && r.migrationStatus !== "none" && r.targetShardId);
    const isFullyDraining = owned.length > 0 && outgoing.length === owned.length;
    const incoming = incomingByShard.get(id) || [];
    const statsEntry = statsByShard.get(id);
    const isUnavailable = !!statsEntry && statsEntry.stats == null;

    const t = loadScores.has(id) ? normalize(loadScores.get(id), minLoad, maxLoad) : 0;
    const sizePx = lerp(58, 118, normalize(owned.length, minOwned, maxOwned));

    let stateClass = "";
    let tagHtml = "";
    if (isUnavailable) {
      unavailableCount++;
      stateClass = "unavailable";
      const errMsg = statsEntry.error ? escapeHtml(statsEntry.error) : "stats unavailable this tick";
      tagHtml = `<div class="shard-tag" title="${errMsg}">unavailable</div>`;
    } else if (isFullyDraining) {
      drainingCount++;
      stateClass = "draining";
      tagHtml = `<div class="shard-tag">draining · ${outgoing.length} leaving</div>`;
    } else if (incoming.length > 0) {
      stateClass = "target";
      const incomingCount = incoming.reduce((a, p) => a + p.count, 0);
      tagHtml = `<div class="shard-tag">receiving · ${incomingCount} in</div>`;
    } else if (!isUnavailable && t >= 0.85) {
      stateClass = "hot";
    }

    const heatColor = isUnavailable ? "var(--text-dim)" : heatColorForT(t);

    // secondary line: prefer live activity signal, fall back to a size proxy
    let rateText;
    if (isUnavailable) {
      rateText = "unreachable";
    } else if (statsEntry && statsEntry.stats) {
      const pending = typeof statsEntry.stats.pendingIntentCount === "number" ? statsEntry.stats.pendingIntentCount : null;
      const rows = computeTotalRows(statsEntry.stats);
      if (pending && pending > 0) rateText = `${fmtInt(pending)} pending tx`;
      else if (rows != null) rateText = `${fmtInt(rows)} rows`;
      else rateText = "—";
    } else {
      rateText = "no data";
    }

    // stylized vbucket ring: proportional fill, not 1:1 with real counts
    const migratingFraction = owned.length > 0 ? outgoing.length / owned.length : 0;
    const migratingDotCount = Math.round(migratingFraction * RING_DOT_COUNT);
    let dots = "";
    for (let i = 0; i < RING_DOT_COUNT; i++) {
      const angle = (i / RING_DOT_COUNT) * 360;
      const isMigrating = i < migratingDotCount;
      dots += `<span class="vdot${isMigrating ? " migrating" : ""}" style="transform:rotate(${angle}deg) translateY(calc(-1 * var(--ring-r)))"></span>`;
    }
    const ringTitle = `${owned.length} vbucket(s) owned` + (outgoing.length ? `, ${outgoing.length} migrating out` : "");

    nodeHtml.push(`
      <div class="shard${stateClass ? " " + stateClass : ""}" style="left:${pos.x}%; top:${pos.y}%; --sz:${sizePx.toFixed(0)}px; --heat:${heatColor};">
        <div class="shard-core">
          <div class="vring" style="--ring-r:${(sizePx / 2 + 12).toFixed(0)}px;" title="${escapeHtml(ringTitle)}">${dots}</div>
          <div>
            <div class="shard-id">${escapeHtml(id)}</div>
            <div class="shard-rate">${escapeHtml(rateText)}</div>
          </div>
          ${tagHtml}
        </div>
      </div>
    `);
  }

  // ---- migration paths (SVG, in the 0..100 viewBox matching node % coords) ----
  const pairList = [...migrationPairs.values()];
  const drawnPairs = pairList.slice(0, MAX_MIGRATION_PATHS);
  const pathHtml = [];
  for (const p of drawnPairs) {
    const a = positions.get(p.source);
    const b = positions.get(p.target);
    if (!a || !b) continue;
    const key = `${p.source}=>${p.target}`;
    const cp = controlPoint(a, b, key);
    pathHtml.push(`<path class="migration-path" d="M ${a.x} ${a.y} Q ${cp.x} ${cp.y} ${b.x} ${b.y}" />`);
    // a couple of animated flight dots riding the arc
    [0.3, 0.62].forEach((t, i) => {
      const pt = quadPoint(a, { x: cp.x, y: cp.y }, b, t);
      flightDotHtml.push(`<div class="flight-dot" style="left:${pt.x}%; top:${pt.y}%; animation-delay:${(i * 0.6 + (hashString(key) % 10) / 10).toFixed(2)}s;"></div>`);
    });
  }

  el.nodesLayer.innerHTML = nodeHtml.join("") + flightDotHtml.join("");
  el.arcLayer.innerHTML =
    `<defs><marker id="drain-arrow" markerWidth="6" markerHeight="6" refX="3" refY="3" orient="auto"><path d="M0,0 L6,3 L0,6 Z" fill="#5A6474" /></marker></defs>` +
    pathHtml.join("");

  // ---- canvas sub-label ----
  const subParts = [];
  if (migratingRowCount > 0) {
    subParts.push(`<div><span class="mono">${fmtInt(migratingRowCount)}</span> vbucket(s) migrating · <span class="mono">${fmtInt(pairList.length)}</span> route(s)</div>`);
  }
  if (pairList.length > drawnPairs.length) {
    subParts.push(`<div>+${fmtInt(pairList.length - drawnPairs.length)} more route(s) not drawn</div>`);
  }
  if (unavailableCount > 0) {
    subParts.push(`<div>${fmtInt(unavailableCount)} shard(s) unavailable this tick</div>`);
  }
  el.canvasSub.innerHTML = subParts.join("");
}

// ============================================================================
// Reshard room (T8): manual operator controls — split / migrate / drain,
// their status/abort, and the topology-operation-lock status + force-release
// escape hatch. Calls go to /api/reshard/* (see ../src/reshard.ts + the
// routing block in ../src/index.ts) — same-origin, gated by the same
// SHARDSCOPE_GATE_TOKEN session cookie every other /api/* call here uses.
//
// Target pickers are populated ENTIRELY from `lastRenderedSnapshot` (set by
// render() above, every tick) — there is no second fetch for topology data.
// vBucket ids are catalog-local (see src/reshard.ts's header comment), so
// every picker here is scoped to a chosen catalogShardId first.
// ============================================================================

let activeRoom = "topology";
let activeOpTab = "split";
/** The op this browser tab most recently started (if any) — polled via
 * GET /api/reshard/migrate-status or /drain-status until it reaches a
 * terminal state, or aborted. Null when nothing is in flight. Note this is
 * purely LOCAL bookkeeping for "what should this tab poll the status of";
 * the topology-lock status (below) is the actual cluster-wide source of
 * truth for whether *some* op is running. */
let activeOp = null; // { kind: 'migrate' | 'drain', catalogShardId, vbucket?, shardId? }
let reshardPollTimer = null;
const RESHARD_POLL_INTERVAL_MS = 1500;
let lockReleaseConfirmTimer = null;
let lastLockOperationId = null;

function setReshardError(msg, level) {
  if (!el.reshardError) return;
  if (!msg) {
    el.reshardError.hidden = true;
    el.reshardError.textContent = "";
    return;
  }
  el.reshardError.hidden = false;
  el.reshardError.className = "reshard-error" + (level ? " " + level : "");
  el.reshardError.textContent = msg;
}

function setLockError(msg) {
  if (!el.lockError) return;
  if (!msg) {
    el.lockError.hidden = true;
    el.lockError.textContent = "";
    return;
  }
  el.lockError.hidden = false;
  el.lockError.textContent = msg;
}

/** fetch() wrapper for every /api/reshard/* call: same-origin credentials +
 * a single 401 handling path (the gate session expired mid-visit — fall back
 * to the login panel exactly like the SSE stream's own gate does). Resolves
 * with the parsed JSON body on 2xx; rejects with an Error carrying the
 * server's message on everything else. */
function reshardFetch(path, options) {
  return fetch(path, Object.assign({ credentials: "same-origin" }, options)).then((res) => {
    if (res.status === 401) {
      handleLogout();
      throw new Error("session expired — please log in again");
    }
    return res.json().catch(() => ({})).then((body) => {
      if (!res.ok) {
        const errField = body && body.error;
        const message = (errField && (errField.message || errField)) || `request failed (${res.status})`;
        throw new Error(typeof message === "string" ? message : JSON.stringify(message));
      }
      return body;
    });
  });
}

// ---- "the old way" contrast beat (T10) ---------------------------------------

/** Collapsed by default (see index.html's oldway-body[hidden]) — purely a
 * copy panel, no data source, so a plain in-memory flag is all this needs;
 * nothing else in the app reads or depends on this state. */
let oldwayExpanded = false;

function toggleOldwayPanel() {
  oldwayExpanded = !oldwayExpanded;
  el.oldwayBody.hidden = !oldwayExpanded;
  el.oldwayToggle.setAttribute("aria-expanded", String(oldwayExpanded));
  el.oldwayChevron.textContent = oldwayExpanded ? "▴" : "▾";
}

// ---- room switching ---------------------------------------------------------

function setActiveRoom(room) {
  if (room === activeRoom) return;
  // The gate's login panel (login-panel) lives inside canvas-wrap and only
  // ever overlays the Topology room. The Edge room (T11) hides canvas-wrap
  // entirely while active (see below) — refuse to leave Topology while the
  // login panel is up, so a rail click can't silently hide the login prompt
  // behind an un-gated-looking Edge/Reshard room. (Reshard never hides
  // canvas-wrap, so it was never exposed to this; Edge is.)
  if (el.loginPanel && !el.loginPanel.hidden && room !== "topology") return;

  activeRoom = room;
  el.railTopology.classList.toggle("active", room === "topology");
  el.railReshard.classList.toggle("active", room === "reshard");
  if (el.railEdge) el.railEdge.classList.toggle("active", room === "edge");
  el.reshardPanel.hidden = room !== "reshard";
  if (el.canvasWrap) el.canvasWrap.hidden = room === "edge";
  if (el.edgeWrap) el.edgeWrap.hidden = room !== "edge";
  el.consoleTitle.textContent = room === "reshard" ? "Reshard Console" : room === "edge" ? "Edge" : "Live Feed";

  if (room === "reshard") {
    refreshReshardPickers();
    pollLockStatus();
    startReshardPolling();
  } else {
    stopReshardPolling();
  }

  if (room === "edge") {
    startEdgeRoom();
  } else {
    stopEdgeRoom();
  }
}

/** Forces the room back to Topology, bypassing setActiveRoom's normal
 * early-return + login-panel guard above — used by handleLogout() so a
 * session expiring while the Reshard/Edge room is open doesn't strand the
 * login panel hidden behind a hidden canvas-wrap (this is the "session just
 * expired" half of the same problem setActiveRoom's guard prevents going
 * forward). */
function forceTopologyRoomForLogin() {
  activeRoom = "topology";
  el.railTopology.classList.add("active");
  el.railReshard.classList.remove("active");
  if (el.railEdge) el.railEdge.classList.remove("active");
  el.reshardPanel.hidden = true;
  if (el.canvasWrap) el.canvasWrap.hidden = false;
  if (el.edgeWrap) el.edgeWrap.hidden = true;
  el.consoleTitle.textContent = "Live Feed";
  stopReshardPolling();
  stopEdgeRoom();
}

function startReshardPolling() {
  stopReshardPolling();
  reshardPollTimer = setInterval(() => {
    pollLockStatus();
    if (activeOp) pollActiveOp();
  }, RESHARD_POLL_INTERVAL_MS);
}

function stopReshardPolling() {
  if (reshardPollTimer) {
    clearInterval(reshardPollTimer);
    reshardPollTimer = null;
  }
}

// ---- target pickers, sourced from lastRenderedSnapshot ----------------------

/** Rebuilds a <select>'s options from `items` ({value,label}[]), preserving
 * the previous selection when it's still present in the new list — so a
 * routine 900ms snapshot tick doesn't yank the operator's in-progress
 * selection out from under them. */
function populateSelect(select, items, placeholder) {
  if (!select) return;
  const prevValue = select.value;
  select.innerHTML = "";
  if (placeholder !== undefined) {
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = placeholder;
    select.appendChild(opt);
  }
  for (const item of items) {
    const opt = document.createElement("option");
    opt.value = item.value;
    opt.textContent = item.label;
    select.appendChild(opt);
  }
  const stillPresent = [...select.options].some((o) => o.value === prevValue);
  select.value = stillPresent ? prevValue : select.options.length ? select.options[0].value : "";
}

function catalogsFromSnapshot() {
  const snap = lastRenderedSnapshot;
  return Array.isArray(snap && snap.catalogs) ? snap.catalogs : [];
}

/** vbucket rows for one catalog, straight off the live snapshot — no second
 * data source (DESIGN.md: one living topology canvas, not a parallel view). */
function vbucketsForCatalog(catalogShardId) {
  const catalog = catalogsFromSnapshot().find((c) => c.catalogShardId === catalogShardId);
  if (!catalog || !Array.isArray(catalog.vbuckets)) return [];
  return [...catalog.vbuckets].sort((a, b) => a.vbucket - b.vbucket);
}

/** shardIds a catalog currently owns (as a current owner OR a migration
 * target) — the catalog-local shard universe, derived the same way this
 * file's own renderInner() computes the topology canvas's shard-id union. */
function shardIdsForCatalog(catalogShardId) {
  const ids = new Set();
  for (const row of vbucketsForCatalog(catalogShardId)) {
    if (row.shardId) ids.add(row.shardId);
    if (row.targetShardId) ids.add(row.targetShardId);
  }
  return [...ids].sort();
}

function refreshReshardPickers() {
  if (!el.reshardPanel) return; // guard: called from render() before init() wires DOM hooks is impossible, but stay defensive
  const catalogItems = catalogsFromSnapshot().map((c) => ({ value: c.catalogShardId, label: c.catalogShardId }));

  populateSelect(el.splitCatalogSelect, catalogItems);
  populateSelect(el.migrateCatalogSelect, catalogItems);
  populateSelect(el.drainCatalogSelect, catalogItems);

  refreshVbucketPicker(el.splitCatalogSelect, el.splitVbucketSelect);
  refreshVbucketPicker(el.migrateCatalogSelect, el.migrateVbucketSelect, el.migrateTargetSelect);
  refreshShardPicker(el.drainCatalogSelect, el.drainShardSelect);
}

function refreshVbucketPicker(catalogSelect, vbucketSelect, targetSelect) {
  const catalogShardId = catalogSelect.value;
  const rows = catalogShardId ? vbucketsForCatalog(catalogShardId) : [];
  populateSelect(
    vbucketSelect,
    rows.map((r) => ({
      value: String(r.vbucket),
      label: `vbucket ${r.vbucket} (on ${r.shardId})${r.migrationStatus && r.migrationStatus !== "none" ? " · migrating" : ""}`,
    })),
  );
  if (targetSelect) {
    const shardIds = catalogShardId ? shardIdsForCatalog(catalogShardId) : [];
    const currentOwner = rows.find((r) => String(r.vbucket) === vbucketSelect.value);
    const items = shardIds.filter((id) => !currentOwner || id !== currentOwner.shardId).map((id) => ({ value: id, label: id }));
    populateSelect(targetSelect, items, "auto (new shard)");
  }
}

function refreshShardPicker(catalogSelect, shardSelect) {
  const catalogShardId = catalogSelect.value;
  const shardIds = catalogShardId ? shardIdsForCatalog(catalogShardId) : [];
  populateSelect(shardSelect, shardIds.map((id) => ({ value: id, label: id })));
}

// ---- op tabs (Split / Migrate / Drain) --------------------------------------

function setActiveOpTab(tab) {
  activeOpTab = tab;
  el.opTabSplit.classList.toggle("selected", tab === "split");
  el.opTabMigrate.classList.toggle("selected", tab === "migrate");
  el.opTabDrain.classList.toggle("selected", tab === "drain");
  el.opFormSplit.hidden = tab !== "split";
  el.opFormMigrate.hidden = tab !== "migrate";
  el.opFormDrain.hidden = tab !== "drain";
  setReshardError(null);
}

function setFormBusy(form, busy) {
  [...form.elements].forEach((elm) => {
    elm.disabled = busy;
  });
}

// ---- form submit handlers ----------------------------------------------------

function handleSplitSubmit(evt) {
  evt.preventDefault();
  const catalogShardId = el.splitCatalogSelect.value;
  const vbucket = Number(el.splitVbucketSelect.value);
  const newShardId = el.splitNewShardInput.value.trim() || undefined;
  if (!catalogShardId || !Number.isInteger(vbucket)) {
    setReshardError("select a catalog shard and vbucket first");
    return;
  }
  setReshardError(null);
  setFormBusy(el.opFormSplit, true);
  reshardFetch("/api/reshard/split", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ catalogShardId, vbucket, newShardId }),
  })
    .then((body) => {
      logLine(`split started · vbucket ${vbucket} (${catalogShardId}) → ${(body && body.toShard) || "(new shard)"}`, "mig");
      activeOp = { kind: "migrate", catalogShardId, vbucket };
      pollActiveOp();
    })
    .catch((err) => setReshardError((err && err.message) || "split failed"))
    .finally(() => setFormBusy(el.opFormSplit, false));
}

function handleMigrateSubmit(evt) {
  evt.preventDefault();
  const catalogShardId = el.migrateCatalogSelect.value;
  const vbucket = Number(el.migrateVbucketSelect.value);
  const targetShardId = el.migrateTargetSelect.value || undefined;
  if (!catalogShardId || !Number.isInteger(vbucket)) {
    setReshardError("select a catalog shard and vbucket first");
    return;
  }
  setReshardError(null);
  setFormBusy(el.opFormMigrate, true);
  reshardFetch("/api/reshard/migrate", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ catalogShardId, vbucket, targetShardId }),
  })
    .then((body) => {
      logLine(`migrate started · vbucket ${vbucket} (${catalogShardId}) → ${(body && body.toShard) || "(new shard)"}`, "mig");
      activeOp = { kind: "migrate", catalogShardId, vbucket };
      pollActiveOp();
    })
    .catch((err) => setReshardError((err && err.message) || "migrate failed"))
    .finally(() => setFormBusy(el.opFormMigrate, false));
}

function handleDrainSubmit(evt) {
  evt.preventDefault();
  const catalogShardId = el.drainCatalogSelect.value;
  const shardId = el.drainShardSelect.value;
  if (!catalogShardId || !shardId) {
    setReshardError("select a catalog shard and a shard to drain first");
    return;
  }
  setReshardError(null);
  setFormBusy(el.opFormDrain, true);
  reshardFetch("/api/reshard/drain", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ catalogShardId, shardId }),
  })
    .then(() => {
      logLine(`drain started · ${shardId} (${catalogShardId})`, "mig");
      activeOp = { kind: "drain", catalogShardId, shardId };
      pollActiveOp();
    })
    .catch((err) => setReshardError((err && err.message) || "drain failed"))
    .finally(() => setFormBusy(el.opFormDrain, false));
}

function handleAbortClick() {
  if (!activeOp || activeOp.kind !== "migrate") return;
  const op = activeOp;
  el.opAbortBtn.disabled = true;
  reshardFetch("/api/reshard/migrate-abort", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ catalogShardId: op.catalogShardId, vbucket: op.vbucket }),
  })
    .then(() => {
      logLine(`migrate aborted · vbucket ${op.vbucket} (${op.catalogShardId})`, "warn");
      if (activeOp === op) activeOp = null;
      renderOpCard(null);
    })
    .catch((err) => setReshardError((err && err.message) || "abort failed"))
    .finally(() => {
      el.opAbortBtn.disabled = false;
    });
}

// ---- current-op polling + render ---------------------------------------------

function pollActiveOp() {
  if (!activeOp) return;
  const op = activeOp;
  const req =
    op.kind === "migrate"
      ? reshardFetch(`/api/reshard/migrate-status?catalogShardId=${encodeURIComponent(op.catalogShardId)}&vbucket=${op.vbucket}`)
      : reshardFetch(`/api/reshard/drain-status?catalogShardId=${encodeURIComponent(op.catalogShardId)}&shardId=${encodeURIComponent(op.shardId)}`);

  req
    .then((status) => {
      if (activeOp !== op) return; // superseded by a newer op while this call was in flight
      renderOpCard(op, status);
      const terminal = op.kind === "migrate" ? status.status === "none" : status.status === "complete";
      if (terminal) {
        logLine(
          op.kind === "migrate"
            ? `migrate complete · vbucket ${op.vbucket} (${op.catalogShardId})`
            : `drain complete · ${op.shardId} (${op.catalogShardId})`,
          "safe",
        );
        activeOp = null;
      }
    })
    .catch((err) => {
      if (activeOp !== op) return;
      setReshardError(`status check failed: ${(err && err.message) || err}`, "warn");
    });
}

function renderOpCard(op, status) {
  if (!op) {
    el.opCard.hidden = true;
    return;
  }
  el.opCard.hidden = false;
  el.opAbortBtn.hidden = op.kind !== "migrate";
  if (op.kind === "migrate") {
    el.opCardName.textContent = `migrate-vbucket · ${(status && status.status) || "starting"}`;
    const parts = [`vbucket <b>${escapeHtml(String(op.vbucket))}</b>`, `<span class="arrow">&rarr;</span>`, `<b>${escapeHtml((status && status.toShard) || "?")}</b>`];
    if (status && typeof status.rowsCopied === "number") parts.push(`<span class="stat">· ${fmtInt(status.rowsCopied)} rows copied</span>`);
    if (status && status.mirrorQueueDepth) parts.push(`<span class="stat">· ${fmtInt(status.mirrorQueueDepth)} mirror queued</span>`);
    el.opCardDetail.innerHTML = parts.join(" ");
  } else {
    el.opCardName.textContent = `drain-shard · ${(status && status.status) || "starting"}`;
    const parts = [`<b>${escapeHtml(op.shardId)}</b>`];
    if (status && typeof status.vbucketsRemaining === "number") parts.push(`<span class="stat">· ${fmtInt(status.vbucketsRemaining)} vbucket(s) left</span>`);
    if (status && typeof status.ringsRemaining === "number") parts.push(`<span class="stat">· ${fmtInt(status.ringsRemaining)} ring(s) left</span>`);
    if (status && status.stallReason) parts.push(`<span class="stat">· stalled: ${escapeHtml(status.stallReason)}</span>`);
    el.opCardDetail.innerHTML = parts.join(" ");
  }
}

// ---- topology-operation lock status + force-release -------------------------

function pollLockStatus() {
  reshardFetch("/api/reshard/lock-status")
    .then((status) => {
      setLockError(null);
      lastLockOperationId = status.held ? status.operationId : null;
      el.lockState.textContent = !status.held ? "free" : status.expired ? "held (expired)" : "held";
      el.lockState.className = "lock-state mono " + (!status.held ? "free" : status.expired ? "expired" : "held");
      el.lockReleaseBtn.hidden = !status.held;
      if (status.held) {
        el.lockDetail.textContent = `${status.operationType} · op ${status.operationId} · acquired ${new Date(status.acquiredAt).toLocaleTimeString()}`;
      } else {
        el.lockDetail.textContent = "no topology operation is running cluster-wide";
      }
    })
    .catch((err) => setLockError(`lock status unavailable: ${(err && err.message) || err}`));
}

/** Guarded two-step confirm (a native confirm() dialog doesn't fit this
 * control room's aesthetic, but a force-release is a real operator escape
 * hatch that shouldn't fire on a single misclick) — first click arms it for
 * 4s, second click within that window actually calls the RPC. */
function handleForceReleaseClick() {
  if (!lastLockOperationId) return;
  if (!el.lockReleaseBtn.classList.contains("confirming")) {
    el.lockReleaseBtn.classList.add("confirming");
    el.lockReleaseBtn.textContent = "Confirm force-release?";
    clearTimeout(lockReleaseConfirmTimer);
    lockReleaseConfirmTimer = setTimeout(resetLockReleaseButton, 4000);
    return;
  }
  clearTimeout(lockReleaseConfirmTimer);
  const operationId = lastLockOperationId;
  el.lockReleaseBtn.disabled = true;
  reshardFetch("/api/reshard/force-release-lock", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ operationId }),
  })
    .then((body) => {
      logLine(`topology lock force-released · op ${operationId} (released: ${body && body.released})`, "warn");
      pollLockStatus();
    })
    .catch((err) => setLockError((err && err.message) || "force-release failed"))
    .finally(() => {
      el.lockReleaseBtn.disabled = false;
      resetLockReleaseButton();
    });
}

function resetLockReleaseButton() {
  el.lockReleaseBtn.classList.remove("confirming");
  el.lockReleaseBtn.textContent = "Force-release lock";
}

// ============================================================================
// Chaos "Break It" panel (T9): fires POST /api/chaos/<attack> for every
// attack button (see ../src/chaos.ts's header comment for the full thesis —
// every button does the REAL destructive thing against the live cluster;
// nothing here is simulated). Reuses reshardFetch (above) verbatim: same
// same-origin credentials, same 401 -> re-login handling, same error-message
// unwrapping every other /api/* call in this room already gets — chaos
// attacks are gated identically, not specially.
//
// "Blip shard offline" fires the core's real, admin-gated fault-injection
// primitive (env.SHARD_API.adminFaultInject via ../src/chaos.ts's
// runBlipShardOfflineAttack) — off by default (needs FAULT_INJECTION_ENABLED
// on the core Worker). A 403 from a disabled cluster is classified
// server-side into a calm ChaosPreconditionError (see chaos.ts's
// classifyBlipFaultInjectError), which surfaces here through the SAME
// `.catch()` path as any other "attack couldn't fire" precondition below
// (setChaosError + a "warn"-styled log line) — never a fabricated ✗ broke.
//
// The T4 invariant scoreboard (topbar, renderScoreboard above) is NOT
// duplicated here — it's already always visible in the top strip regardless
// of which room is open, which is the whole point (DESIGN.md: "fire an
// attack, watch lost stay 0"). This section only renders the ATTACK's own
// structured outcome (src/chaos.ts's ChaosOutcome: did/expected/observed/
// survived/note) — the receipt for what was just fired, not a second meter.
// ============================================================================

function setChaosError(msg) {
  if (!el.chaosError) return;
  if (!msg) {
    el.chaosError.hidden = true;
    el.chaosError.textContent = "";
    return;
  }
  el.chaosError.hidden = false;
  el.chaosError.textContent = msg;
}

/** Renders a src/chaos.ts ChaosOutcome verbatim — did/expected/observed/note
 * are shown exactly as the server judged them, never re-narrated client-side
 * (the whole credibility of this panel rests on the server's real judgment,
 * not a client-side retelling of it). */
function renderChaosOutcome(outcome) {
  if (!el.chaosResult || !outcome) return;
  el.chaosResult.hidden = false;
  const survived = !!outcome.survived;
  el.chaosResultVerdict.textContent = survived ? "✓ survived" : "✗ broke";
  el.chaosResultVerdict.className = "chaos-result-verdict mono " + (survived ? "survived" : "broke");
  el.chaosResultAttack.textContent = outcome.attack || "";
  el.chaosResultDid.textContent = outcome.did || "";
  el.chaosResultExpected.textContent = outcome.expected || "";
  el.chaosResultObserved.textContent = outcome.observed || "";
  el.chaosResultNote.textContent = outcome.note || "";
}

function handleChaosAttackClick(evt) {
  const btn = evt.currentTarget;
  const attack = btn.dataset.attack;
  if (!attack || btn.disabled) return;

  setChaosError(null);
  const stackButtons = el.chaosAttackStack ? [...el.chaosAttackStack.querySelectorAll("button[data-attack]")] : [btn];
  stackButtons.forEach((b) => {
    b.disabled = true;
  });
  logLine(`firing chaos attack: ${attack}…`, "danger");

  reshardFetch(`/api/chaos/${attack}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: "{}",
  })
    .then((outcome) => {
      renderChaosOutcome(outcome);
      const verdict = outcome && outcome.survived ? "survived ✓" : "BROKE ✗";
      logLine(`${attack}: ${verdict} — ${(outcome && outcome.note) || ""}`, outcome && outcome.survived ? "safe" : "danger");
    })
    .catch((err) => {
      // Covers both "the attack couldn't even fire" (a ChaosPreconditionError
      // 400 — e.g. no skew load running to derive a hot shard from, OR a
      // concurrent write moving the row mid-attack for double-submit /
      // mismatched-replay — see ../src/chaos.ts's classifyDoubleSubmit /
      // classifyMismatchedReplay "pre-PR review Fix 3" concurrent-move guard)
      // and a topology-lock-busy 409 from a reused ../src/reshard.ts wrapper
      // (drain/split/migrate/abort attacks all route through it) — both
      // already unwrap to a plain, calm message via reshardFetch above, same
      // as every Reshard console form's own error handling. This is the ONLY
      // rendering path an inconclusive (concurrent-move) result takes — it
      // never reaches renderChaosOutcome/the ✓/✗ verdict badge below, so it
      // can never paint as a fabricated "✗ broke".
      //
      // Also hide any STALE result from a previous, different fire: a
      // precondition/inconclusive message must never sit next to a leftover
      // ✓/✗ verdict badge from an earlier attack, which would read as a
      // "broke" verdict for THIS fire even though this fire produced no
      // outcome at all.
      if (el.chaosResult) el.chaosResult.hidden = true;
      setChaosError((err && err.message) || `${attack} failed`);
      logLine(`${attack}: could not fire — ${(err && err.message) || err}`, "warn");
    })
    .finally(() => {
      stackButtons.forEach((b) => {
        b.disabled = false;
      });
    });
}

// ============================================================================
// Edge room (Phase 3, T11): the viewer's REAL measured round-trip time to
// the nearest Cloudflare edge, via GET /api/edge (see ../src/edge.ts).
//
// HONESTY CONTRACT (read before touching this section): the ONLY "live"
// figure in this room is the hero value rendered by renderEdgeResult() below
// — a real browser round trip, averaged over a few real fetches of
// /api/edge, together with the REAL Cloudflare colo/city/country/region
// request.cf reports for this request (see ../src/edge.ts's buildEdgeInfo).
// Shardscope has no live multi-region probe network today, so
// ILLUSTRATIVE_REGIONS below are reference points ONLY — hand-picked
// lat/long used purely to place dots on the map, tagged "illustrative"
// everywhere they render, and NEVER given a latency number (a number there
// would have to be fabricated: nothing in this demo actually measures from
// those cities). When this Worker isn't running behind a real Cloudflare
// edge at all (request.cf undefined — local dev/miniflare), or ?demo=1 mode
// is active, or the measurement can't complete, this section renders an
// explicit honest state instead — never a fabricated number.
//
// TODO(shardscope): once this demo has a genuine global deployment with real
// multi-region probes, wire real measured numbers into (a subset of)
// ILLUSTRATIVE_REGIONS and drop the "illustrative" tagging for whichever
// regions become real measurements.
// ============================================================================

// Total /api/edge fetches per measurement; the first is discarded as a
// connection/TLS-setup warmup so it doesn't skew the average toward a worse
// number than steady-state round trips actually are.
const EDGE_SAMPLE_ROUNDS = 6;
const EDGE_MAP_WIDTH = 600;
const EDGE_MAP_HEIGHT = 300;

// Hand-picked reference cities spanning the globe, purely so the map reads
// as a world map rather than a single dot — see the honesty contract above.
// Approximate city-center lat/long only, used for dot placement and nothing
// else; NOT measured, NOT a latency claim.
const ILLUSTRATIVE_REGIONS = [
  { label: "Ashburn, US", lat: 39.04, lon: -77.49 },
  { label: "São Paulo, BR", lat: -23.55, lon: -46.63 },
  { label: "Frankfurt, DE", lat: 50.11, lon: 8.68 },
  { label: "Tokyo, JP", lat: 35.68, lon: 139.65 },
  { label: "Sydney, AU", lat: -33.87, lon: 151.21 },
  { label: "Johannesburg, ZA", lat: -26.2, lon: 28.05 },
];

let edgeMeasuring = false;

function projectLatLon(lat, lon) {
  const x = ((lon + 180) / 360) * EDGE_MAP_WIDTH;
  const y = ((90 - lat) / 180) * EDGE_MAP_HEIGHT;
  return { x, y };
}

function setEdgeStatus(text) {
  if (el.edgeStatus) el.edgeStatus.textContent = text;
}

/** Mirrors the Topology canvas's own dot-live state classes (see
 * setLiveState) — "live" (green pulse), "warn" (amber, no pulse), or
 * neither (idle/off). */
function setEdgeLive(state) {
  if (!el.edgeLiveDot) return;
  el.edgeLiveDot.className = "dot-live" + (state === "live" ? " live" : state === "warn" ? " stale" : "");
}

/** Renders the map: illustrative reference dots always; a real "you" dot
 * ONLY when we have real lat/long from request.cf for this measurement
 * (never estimated, never a default/guessed position). */
function renderEdgeMap(youPoint) {
  if (!el.edgeMapSvg) return;
  const parts = [];
  for (const region of ILLUSTRATIVE_REGIONS) {
    const { x, y } = projectLatLon(region.lat, region.lon);
    parts.push(
      `<circle class="edge-dot-illustrative" cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="4"><title>${escapeHtml(region.label)} — illustrative reference point, not measured</title></circle>`,
    );
  }
  if (youPoint) {
    const { x, y } = projectLatLon(youPoint.lat, youPoint.lon);
    parts.push(
      `<circle class="edge-dot-you-ring" cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="9"></circle>` +
        `<circle class="edge-dot-you" cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="5"><title>You — real, measured</title></circle>` +
        `<text class="edge-map-label you-label" x="${x.toFixed(1)}" y="${(y - 12).toFixed(1)}" text-anchor="middle">YOU</text>`,
    );
  }
  el.edgeMapSvg.innerHTML = parts.join("");
}

/** ?demo=1 and the honest "worker unreachable"/local-dev states all funnel
 * through here: illustrative reference map only, no fabricated "you" number
 * — Shardscope never invents a viewer latency it didn't actually measure. */
function renderEdgeUnmeasured(reason, caption) {
  setEdgeStatus("Edge — " + reason);
  setEdgeLive("warn");
  if (el.edgeHeroValue) {
    el.edgeHeroValue.textContent = "—";
    el.edgeHeroValue.classList.add("local");
  }
  if (el.edgeHeroServed) el.edgeHeroServed.textContent = reason;
  if (el.edgeHeroCaption) el.edgeHeroCaption.textContent = caption;
  renderEdgeMap(null);
}

function renderEdgeMeasuring() {
  setEdgeStatus("Edge — measuring");
  setEdgeLive("warn");
  if (el.edgeHeroValue) {
    el.edgeHeroValue.textContent = "measuring…";
    el.edgeHeroValue.classList.add("local");
  }
  if (el.edgeHeroServed) el.edgeHeroServed.textContent = `sampling ${EDGE_SAMPLE_ROUNDS - 1} real round trips…`;
  if (el.edgeHeroCaption) el.edgeHeroCaption.textContent = "";
  if (el.edgeRemeasureBtn) el.edgeRemeasureBtn.hidden = true;
  renderEdgeMap(null);
}

function renderEdgeLocal() {
  renderEdgeUnmeasured(
    "running locally — no Cloudflare edge data",
    "This Worker isn't running behind a real Cloudflare edge right now (no request.cf), so there's no real colo or latency to report. The reference dots below are illustrative only.",
  );
  if (el.edgeRemeasureBtn) el.edgeRemeasureBtn.hidden = false;
}

function renderEdgeError(message) {
  renderEdgeUnmeasured(
    "unreachable",
    "The reference dots below are illustrative only — nothing here is fabricated in place of a failed measurement.",
  );
  if (el.edgeHeroServed) el.edgeHeroServed.textContent = `couldn't measure — ${message}`;
  if (el.edgeRemeasureBtn) el.edgeRemeasureBtn.hidden = false;
}

function renderEdgeResult(avgMs, sampleCount, edgeInfo) {
  setEdgeStatus("Edge — live");
  setEdgeLive("live");
  if (el.edgeHeroValue) {
    el.edgeHeroValue.textContent = `${Math.round(avgMs)} ms`;
    el.edgeHeroValue.classList.remove("local");
  }
  const locBits = [edgeInfo.city, edgeInfo.country].filter(Boolean).join(", ");
  if (el.edgeHeroServed) el.edgeHeroServed.textContent = `served from ${edgeInfo.colo}${locBits ? " · " + locBits : ""}`;
  if (el.edgeHeroCaption) {
    el.edgeHeroCaption.textContent =
      `Real, measured: your browser's average round trip to this Worker over ${sampleCount} samples (a first warmup request is discarded). ` +
      `The colo (${edgeInfo.colo}) and geo above come straight from Cloudflare's own request.cf for this request — nothing estimated. ` +
      `The reference dots on the map are illustrative only — there's no live probe network behind this demo yet.`;
  }
  if (el.edgeRemeasureBtn) el.edgeRemeasureBtn.hidden = false;
  const lat = edgeInfo.latitude != null ? parseFloat(edgeInfo.latitude) : NaN;
  const lon = edgeInfo.longitude != null ? parseFloat(edgeInfo.longitude) : NaN;
  renderEdgeMap(Number.isFinite(lat) && Number.isFinite(lon) ? { lat, lon } : null);
}

/** fetch()-and-time a single GET /api/edge round trip. Rejects (never
 * fabricates a result) on a non-2xx status; a 401 is tagged `.unauthorized`
 * so the caller can route it into the same gate-expired flow every other
 * /api/* caller in this file uses. */
async function fetchEdgeOnce() {
  const t0 = performance.now();
  const res = await fetch("/api/edge", { credentials: "same-origin", cache: "no-store" });
  const t1 = performance.now();
  if (res.status === 401) {
    const err = new Error("unauthorized");
    err.unauthorized = true;
    throw err;
  }
  if (!res.ok) throw new Error(`request failed (${res.status})`);
  const body = await res.json();
  return { rtt: t1 - t0, body };
}

/** Runs one full measurement: EDGE_SAMPLE_ROUNDS fetches, first discarded as
 * warmup, remaining averaged. Every branch below renders an honest state —
 * live result, explicit "local" (no edge), or explicit error — never a
 * fabricated number. */
async function runEdgeMeasurement() {
  if (edgeMeasuring) return;
  edgeMeasuring = true;
  if (el.edgeRemeasureBtn) el.edgeRemeasureBtn.disabled = true;
  renderEdgeMeasuring();
  try {
    const samples = [];
    let lastBody = null;
    for (let i = 0; i < EDGE_SAMPLE_ROUNDS; i++) {
      const { rtt, body } = await fetchEdgeOnce();
      lastBody = body;
      if (i > 0) samples.push(rtt); // discard sample 0 (connection warmup)
    }
    if (!lastBody) throw new Error("no response");
    if (lastBody.local || !lastBody.edge) {
      renderEdgeLocal();
      logLine("edge probe: running locally — no Cloudflare edge data", "warn");
    } else {
      const avg = samples.reduce((a, b) => a + b, 0) / samples.length;
      renderEdgeResult(avg, samples.length, lastBody.edge);
      logLine(`edge probe: ${Math.round(avg)} ms avg round trip to ${lastBody.edge.colo} (${samples.length} samples)`, "safe");
    }
  } catch (err) {
    if (err && err.unauthorized) {
      logLine("edge probe: session expired — please log in again", "warn");
      handleLogout();
      return;
    }
    const message = err instanceof Error ? err.message : String(err);
    renderEdgeError(message);
    logLine(`edge probe failed: ${message}`, "warn");
  } finally {
    edgeMeasuring = false;
    if (el.edgeRemeasureBtn) el.edgeRemeasureBtn.disabled = false;
  }
}

/** Called by setActiveRoom() on entering the Edge room. ?demo=1 never opens
 * a live connection of any kind (see this room's honesty contract above) —
 * it renders the illustrative-only map immediately with no /api/* call. */
function startEdgeRoom() {
  if (mode === "demo") {
    renderEdgeUnmeasured(
      "demo mode — no live measurement",
      "No live measurement is taken in demo mode. The dots below are illustrative reference points only — Shardscope has no live multi-region probe network today, so nothing outside your own browser is ever measured.",
    );
    logLine("edge room: demo mode — rendering illustrative reference map only, no live probe", "mig");
    return;
  }
  runEdgeMeasurement();
}

/** Called by setActiveRoom() on leaving the Edge room. There is currently no
 * interval/timer to tear down here — runEdgeMeasurement is a short, self-
 * terminating fetch loop, left to finish naturally if a room switch happens
 * mid-measurement. Kept as its own function (rather than inlined at the
 * setActiveRoom call site) so a future periodic-refresh timer has a single,
 * obvious place to be added and cleared. */
function stopEdgeRoom() {}

// ============================================================================
// Auth gate (src/gate.ts): /api/* requires SHARDSCOPE_GATE_TOKEN, presented
// as a `shardscope_gate` HttpOnly cookie set by POST /login. EventSource
// can't read response status codes or set an Authorization header, so we
// preflight with a plain fetch (which CAN read the status) before ever
// opening the stream, and drive a small login panel off the result. ?demo=1
// and the no-live-cluster sample fallback never call /api/*, so neither
// touches this gate at all.
// ============================================================================

/** Resolves to "authorized" (200), "unauthorized" (401), or "error" (any
 * other status / the request itself failed — worker unreachable, static
 * preview with no backend, network drop, etc.). Never rejects. */
function authPreflight() {
  return fetch("/api/load/status", { credentials: "same-origin" })
    .then((res) => {
      if (res.status === 200) return "authorized";
      if (res.status === 401) return "unauthorized";
      return "error";
    })
    .catch(() => "error");
}

function setLoginError(msg) {
  if (!msg) {
    el.loginError.hidden = true;
    el.loginError.textContent = "";
    return;
  }
  el.loginError.hidden = false;
  el.loginError.textContent = msg;
}

function setLoginSubmitting(submitting) {
  el.loginSubmit.disabled = submitting;
  el.loginSubmit.textContent = submitting ? "Connecting…" : "Connect";
}

function showLoginPanel() {
  el.loginPanel.hidden = false;
  el.logoutBtn.hidden = true;
  setCanvasStatus("login required");
  setLiveState("warn", "login required");
  if (el.loginTokenInput) el.loginTokenInput.focus();
}

function hideLoginPanel() {
  el.loginPanel.hidden = true;
  setLoginError(null);
}

function handleLoginSubmit(evt) {
  evt.preventDefault();
  const token = el.loginTokenInput.value.trim();
  if (!token) {
    setLoginError("enter the gate token");
    return;
  }
  setLoginSubmitting(true);
  fetch("/login", {
    method: "POST",
    headers: { "content-type": "application/json" },
    credentials: "same-origin",
    body: JSON.stringify({ token }),
  })
    .then((res) => {
      if (res.status === 200) {
        el.loginTokenInput.value = "";
        hideLoginPanel();
        connectLive();
        return;
      }
      if (res.status === 401) {
        setLoginError("invalid gate token");
        return;
      }
      setLoginError(`login failed (${res.status}) — try again`);
    })
    .catch(() => {
      setLoginError("network error — check your connection and try again");
    })
    .finally(() => setLoginSubmitting(false));
}

function handleLogout() {
  if (es) {
    es.close();
    es = null;
  }
  clearTimeout(fallbackTimer);
  // Best-effort — the panel goes back up regardless of whether this
  // round-trip succeeds; there's no server-side session to leak if it fails.
  fetch("/logout", { method: "POST", credentials: "same-origin" }).catch(() => {});
  mode = "connecting";
  showSampleBadge(false);
  clearBanner();
  el.nodesLayer.innerHTML = "";
  el.arcLayer.querySelectorAll("path.migration-path").forEach((p) => p.remove());
  el.canvasSub.textContent = "";
  // The session that gated every /api/reshard/* call is gone — stop polling
  // it and drop any in-flight op's local bookkeeping (the op itself keeps
  // running server-side; this only clears what THIS tab was watching).
  stopReshardPolling();
  activeOp = null;
  if (el.opCard) el.opCard.hidden = true;
  // Bring canvas-wrap (and the login panel it hosts) back into view
  // regardless of which room was open when the session expired — see
  // forceTopologyRoomForLogin's own comment for why this matters for Edge.
  forceTopologyRoomForLogin();
  logLine("logged out", "warn");
  showLoginPanel();
}

/** Entry point for the live (non-demo) path: preflight the gate, then either
 * open the stream (already authorized) or show the login panel (401). Any
 * other outcome (worker unreachable, network error) falls back to the
 * embedded sample exactly like the old no-auth build did. */
function startLiveFlow() {
  setCanvasStatus("checking session");
  setLiveState("connecting", "checking session");
  authPreflight().then((result) => {
    if (result === "authorized") {
      hideLoginPanel();
      connectLive();
    } else if (result === "unauthorized") {
      showLoginPanel();
    } else {
      fallbackToSample("couldn't reach the server");
    }
  });
}

// ============================================================================
// Connection lifecycle: live SSE, ?demo=1, and the no-live-cluster fallback
// ============================================================================

let mode = "connecting"; // connecting | live | sample-fallback | demo
let fallbackTimer = null;
let es = null;

function fallbackToSample(reason) {
  if (mode === "live") return; // real data already arrived; ignore a racing timer
  mode = "sample-fallback";
  showSampleBadge(true);
  showBanner(`sample data — ${reason}; still trying to reach /api/stream`, "info");
  el.canvasStatus.dataset.mode = "sample";
  setCanvasStatus("sample");
  setLiveState("warn", "no live cluster");
  render(buildSampleSnapshot());
  logLine(`fallback to embedded sample data (${reason})`, "warn");
}

function connectLive() {
  // connectLive() is only ever reached after the gate preflight or a
  // successful POST /login, so we're authorized from here on — surface the
  // logout affordance.
  el.logoutBtn.hidden = false;

  if (typeof EventSource === "undefined") {
    fallbackToSample("EventSource unsupported in this browser");
    return;
  }

  setCanvasStatus("connecting");
  setLiveState("connecting", "connecting");
  logLine("connecting to /api/stream…");

  fallbackTimer = setTimeout(() => {
    if (mode === "connecting") fallbackToSample("no live cluster detected within 6s");
  }, FALLBACK_TIMEOUT_MS);

  es = new EventSource("/api/stream");

  es.addEventListener("hello", () => {
    logLine("connected to aggregator, waiting for first tick…", "mig");
  });

  es.addEventListener("snapshot", (ev) => {
    clearTimeout(fallbackTimer);
    let data;
    try {
      data = JSON.parse(ev.data);
    } catch (err) {
      console.error("shardscope: bad snapshot payload", err);
      return;
    }
    if (mode !== "live") {
      mode = "live";
      showSampleBadge(false);
      clearBanner();
    }
    el.canvasStatus.dataset.mode = "live";
    setCanvasStatus("live");
    setLiveState("live", "live");
    render(data);
    logLine(`snapshot @ ${new Date(data.ts || Date.now()).toLocaleTimeString()}`, "safe");
  });

  // NOTE: the server sends a NAMED SSE event literally called "error" (see
  // aggregator.ts's runTick -> sseEvent("error", ...)), and EventSource also
  // dispatches its own native connection-level Event of type "error" through
  // this exact same listener. They collide on the event name by construction
  // of the SSE spec + EventSource API. Native errors are plain Events (no
  // `.data`); server-sent named "error" events are MessageEvents (`.data` is
  // the JSON string). Branch on that to tell them apart.
  es.addEventListener("error", (ev) => {
    if (ev && typeof ev.data === "string") {
      let msg = "unknown error";
      try {
        msg = JSON.parse(ev.data).message || msg;
      } catch {
        /* leave default msg */
      }
      showBanner(`poll error: ${msg} — showing last known state`, "warn");
      logLine(`poll error: ${msg}`, "warn");
      return;
    }
    // native EventSource connection error (network drop, DO restart, cold
    // start, etc.) — EventSource auto-reconnects on its own; just reflect it.
    if (mode === "live" || mode === "connecting") {
      setLiveState("warn", "reconnecting");
      showBanner("connection lost — reconnecting…", "warn");
      logLine("connection lost — reconnecting…", "warn");
    }
  });
}

/** Click + Enter/Space (role="button" rail items aren't native <button>s). */
function onActivate(elm, fn) {
  if (!elm) return;
  elm.addEventListener("click", fn);
  elm.addEventListener("keydown", (evt) => {
    if (evt.key === "Enter" || evt.key === " ") {
      evt.preventDefault();
      fn();
    }
  });
}

function init() {
  el.loginForm.addEventListener("submit", handleLoginSubmit);
  el.logoutBtn.addEventListener("click", handleLogout);

  // ---- Reshard room (T8) wiring ----
  onActivate(el.railTopology, () => setActiveRoom("topology"));
  onActivate(el.railReshard, () => setActiveRoom("reshard"));
  onActivate(el.railEdge, () => setActiveRoom("edge"));
  if (el.edgeRemeasureBtn) el.edgeRemeasureBtn.addEventListener("click", runEdgeMeasurement);
  el.oldwayToggle.addEventListener("click", toggleOldwayPanel);
  el.opTabSplit.addEventListener("click", () => setActiveOpTab("split"));
  el.opTabMigrate.addEventListener("click", () => setActiveOpTab("migrate"));
  el.opTabDrain.addEventListener("click", () => setActiveOpTab("drain"));
  el.opFormSplit.addEventListener("submit", handleSplitSubmit);
  el.opFormMigrate.addEventListener("submit", handleMigrateSubmit);
  el.opFormDrain.addEventListener("submit", handleDrainSubmit);
  el.opAbortBtn.addEventListener("click", handleAbortClick);
  el.lockReleaseBtn.addEventListener("click", handleForceReleaseClick);
  el.splitCatalogSelect.addEventListener("change", () => refreshVbucketPicker(el.splitCatalogSelect, el.splitVbucketSelect));
  el.migrateCatalogSelect.addEventListener("change", () =>
    refreshVbucketPicker(el.migrateCatalogSelect, el.migrateVbucketSelect, el.migrateTargetSelect),
  );
  el.migrateVbucketSelect.addEventListener("change", () =>
    refreshVbucketPicker(el.migrateCatalogSelect, el.migrateVbucketSelect, el.migrateTargetSelect),
  );
  el.drainCatalogSelect.addEventListener("change", () => refreshShardPicker(el.drainCatalogSelect, el.drainShardSelect));

  // ---- Chaos "Break It" panel (T9) wiring ----
  // Every attack, including "Blip shard offline", is a real
  // <button data-attack> now — one generic handler for all of them.
  if (el.chaosAttackStack) {
    el.chaosAttackStack.querySelectorAll("button[data-attack]").forEach((btn) => btn.addEventListener("click", handleChaosAttackClick));
  }

  const params = new URLSearchParams(location.search);
  if (params.get("demo") === "1") {
    // Sample mode never touches /api/*, so it never touches the gate either
    // — skip the login flow entirely.
    mode = "demo";
    showSampleBadge(true);
    el.canvasStatus.dataset.mode = "demo";
    setCanvasStatus("demo");
    setLiveState("demo", "demo");
    el.logoutBtn.hidden = true;
    render(buildSampleSnapshot());
    logLine("demo mode (?demo=1) — rendering embedded sample snapshot, no live connection opened", "mig");
    return;
  }
  startLiveFlow();
}

init();
