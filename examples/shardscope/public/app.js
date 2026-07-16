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

function init() {
  el.loginForm.addEventListener("submit", handleLoginSubmit);
  el.logoutBtn.addEventListener("click", handleLogout);

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
