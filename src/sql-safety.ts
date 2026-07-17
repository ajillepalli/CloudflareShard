/** Strips leading whitespace, line comments (-- ...), and block comments
 * (/* ... *​/) so classification can't be fooled by a comment-obfuscated
 * leading keyword (e.g. "-- x\nDELETE FROM t" reads as a DELETE, not a SELECT). */
function stripLeadingComments(sql: string): string {
  let s = sql;
  let changed = true;
  while (changed) {
    changed = false;
    const trimmed = s.replace(/^\s+/, "");
    if (trimmed !== s) {
      s = trimmed;
      changed = true;
    }
    if (s.startsWith("--")) {
      const newlineIdx = s.indexOf("\n");
      s = newlineIdx === -1 ? "" : s.slice(newlineIdx + 1);
      changed = true;
    } else if (s.startsWith("/*")) {
      const endIdx = s.indexOf("*/");
      s = endIdx === -1 ? "" : s.slice(endIdx + 2);
      changed = true;
    }
  }
  return s;
}

/** Scans forward from an opening '(' at `start` to the index just past its
 * matching close paren, honoring quoted strings so a paren inside a string
 * literal doesn't unbalance the count. Returns sql.length if unterminated. */
function skipBalancedParens(sql: string, start: number): number {
  let depth = 0;
  let inSingle = false;
  let inDouble = false;
  for (let i = start; i < sql.length; i += 1) {
    const c = sql[i];
    if (inSingle) {
      if (c === "'") inSingle = false;
      continue;
    }
    if (inDouble) {
      if (c === '"') inDouble = false;
      continue;
    }
    if (c === "'") inSingle = true;
    else if (c === '"') inDouble = true;
    else if (c === "(") depth += 1;
    else if (c === ")") {
      depth -= 1;
      if (depth === 0) return i + 1;
    }
  }
  return sql.length;
}

/** Skips past a leading "WITH [RECURSIVE] name [(cols)] AS [[NOT] MATERIALIZED]
 * (body) [, ...]" common-table-expression clause to reach the terminal
 * statement (SELECT, INSERT, UPDATE, or DELETE — SQLite allows WITH before any
 * of them). Without this, "WITH x AS (SELECT 1) DELETE FROM t" reads as a
 * harmless SELECT because isMutation() only ever inspected the first keyword.
 * The optional MATERIALIZED / NOT MATERIALIZED hint (SQLite 3.35+) between AS
 * and the opening paren must be consumed too — "WITH x AS MATERIALIZED
 * (SELECT 1) DELETE FROM t" is the same bypass shape with the hint inserted,
 * and previously made this function bail (see beforeBody check below) leaving
 * the mutation unclassified. Bails out (returns the input unchanged) on
 * anything that doesn't match the expected CTE grammar — malformed input
 * isn't valid SQL SQLite would execute as a mutation anyway, so it's safe to
 * leave unclassified here. */
function skipLeadingCte(sql: string): string {
  if (!/^\s*with\b/i.test(sql)) return sql;
  let s = sql.replace(/^\s*with\s+(recursive\s+)?/i, "");
  for (;;) {
    const idMatch = /^\s*("([^"]+)"|`([^`]+)`|\[([^\]]+)\]|[A-Za-z_][A-Za-z0-9_]*)/.exec(s);
    if (!idMatch) return sql;
    s = s.slice(idMatch[0].length);

    const afterId = s.replace(/^\s+/, "");
    if (afterId.startsWith("(")) {
      s = s.slice(s.length - afterId.length);
      s = s.slice(skipBalancedParens(s, 0));
    }

    const asMatch = /^\s*as\s*/i.exec(s);
    if (!asMatch) return sql;
    s = s.slice(asMatch[0].length);

    // Optional MATERIALIZED / NOT MATERIALIZED hint between AS and the body's
    // opening paren (SQLite 3.35+). Must be consumed here, before the "(" check
    // below, or a hinted CTE bails out and returns the un-skipped original —
    // silently misclassifying the mutation that follows it.
    const materializedMatch = /^\s*(not\s+)?materialized\s*/i.exec(s);
    if (materializedMatch) {
      s = s.slice(materializedMatch[0].length);
    }

    const beforeBody = s.replace(/^\s+/, "");
    if (!beforeBody.startsWith("(")) return sql;
    s = s.slice(s.length - beforeBody.length);
    s = s.slice(skipBalancedParens(s, 0));

    const commaMatch = /^\s*,\s*/.exec(s);
    if (commaMatch) {
      s = s.slice(commaMatch[0].length);
      continue;
    }
    return s;
  }
}

/** Removes SQL line comments (dash-dash to newline) and block comments from
 * ANYWHERE in the SQL (each replaced with a space), while genuinely preserving
 * quoted spans — single-quoted string literals and double-quote / backtick /
 * bracket quoted identifiers (honoring the doubled-quote escape for the first
 * three). This is the quote-aware counterpart the leading-keyword classifiers
 * need: a comment can hide at any inter-token seam of a CTE header (between AS
 * and the body paren, around the MATERIALIZED hint, etc.), and skipLeadingCte
 * parses those seams with comment-blind regexes, so it must see comment-free
 * text.
 *
 * Why not reuse stripComments(): that helper is NOT actually quote-aware (its
 * doc comment overstates), so a block-comment opener inside a string literal
 * fools it — e.g. a CTE body of SELECT 'BLOCK-OPEN' (an unterminated block
 * opener held inside the string) makes stripComments eat everything through
 * end-of-input, deleting the trailing DELETE and misclassifying a real mutation
 * as a read. skipBalancedParens (used by skipLeadingCte) IS quote-aware and
 * classifies that case correctly today, so this pre-strip must not regress it. */
function stripCommentsPreservingStrings(sql: string): string {
  let out = "";
  let i = 0;
  while (i < sql.length) {
    const c = sql[i];
    // Quoted string / identifier span: copy verbatim through its matching
    // close, so a `--` or `/*` inside it is never treated as a comment.
    if (c === "'" || c === '"' || c === "`") {
      out += c;
      i += 1;
      while (i < sql.length) {
        out += sql[i];
        if (sql[i] === c) {
          if (sql[i + 1] === c) {
            out += sql[i + 1]; // doubled quote = escaped, stays inside the span
            i += 2;
            continue;
          }
          i += 1;
          break;
        }
        i += 1;
      }
      continue;
    }
    if (c === "[") {
      out += c;
      i += 1;
      while (i < sql.length) {
        out += sql[i];
        if (sql[i] === "]") {
          i += 1;
          break;
        }
        i += 1;
      }
      continue;
    }
    if (c === "-" && sql[i + 1] === "-") {
      const nl = sql.indexOf("\n", i);
      i = nl === -1 ? sql.length : nl;
      out += " ";
      continue;
    }
    if (c === "/" && sql[i + 1] === "*") {
      const end = sql.indexOf("*/", i + 2);
      i = end === -1 ? sql.length : end + 2;
      out += " ";
      continue;
    }
    out += c;
    i += 1;
  }
  return out;
}

export function isMutation(sql: string): boolean {
  // Strip comments GLOBALLY (quote-aware) first so a comment lodged inside the
  // CTE header ("WITH x AS /*c*/ (...) DELETE ...") can't make skipLeadingCte
  // bail and leave the real leading keyword ("with") in place — which read as a
  // harmless SELECT and defeated the read-only / internal-table guards.
  const afterCte = stripLeadingComments(skipLeadingCte(stripCommentsPreservingStrings(sql)));
  return /^(insert|update|delete|replace|create|drop|alter)/i.test(afterCte);
}

/** Milestone 3, Chunk 0: does this statement's leading keyword classify it as
 * a DELETE? Used by ShardDO to decide whether a successful write should
 * remove (delete) or upsert (insert/update/replace) the row's
 * `__cf_row_owners` provenance entry — derived from the SQL text itself, the
 * same "ShardDO classifies its own writes" philosophy isMutation() already
 * uses, rather than trusting a caller-supplied hint. */
export function isDeleteStatement(sql: string): boolean {
  // Same quote-aware global comment strip as isMutation (see its comment): a
  // comment in the CTE header must not hide the terminal DELETE keyword.
  const afterCte = stripLeadingComments(skipLeadingCte(stripCommentsPreservingStrings(sql)));
  return /^delete/i.test(afterCte);
}

/** The internal bookkeeping tables ShardDO owns — tenant SQL must never write
 * to any of these (lifting a cutover fence, forging/deleting row provenance,
 * or purging the mirror queue from the data plane would all be catastrophic).
 * Kept here (not only in shard.ts) so the tenant-facing gate can reject a
 * write against them without importing DO code; shard.ts imports this same
 * set for its INTERNAL_TABLES so the two can't drift. */
export const INTERNAL_TABLE_NAMES = [
  "applied_requests",
  "sqlite_sequence",
  "pending_intents",
  "row_locks",
  "__cf_indexes",
  "index_pending_jobs",
  "__cf_row_owners",
  "__cf_mirror_pending",
  "__cf_fenced_vbuckets",
  "__cf_fenced_index_rings",
] as const;

const INTERNAL_TABLE_SET = new Set<string>(INTERNAL_TABLE_NAMES);

// NOTE: the former quote-BLIND `stripComments` helper was removed. It claimed
// (falsely) to leave quoted spans intact but had no quote tracking, so a
// comment delimiter inside a string literal made it eat unbounded text — a
// security footgun for the write-target/CTE classifiers. All callers now use
// the quote-aware `stripCommentsPreservingStrings` above.

/** Unquotes a single identifier token: `"x"`/`` `x` ``/`[x]` (honoring the
 * doubled-quote escape for the first two). Bare tokens pass through. */
function unquoteIdentifier(tok: string): string {
  if (tok.length >= 2) {
    const first = tok[0];
    const last = tok[tok.length - 1];
    if (first === '"' && last === '"') return tok.slice(1, -1).replace(/""/g, '"');
    if (first === "`" && last === "`") return tok.slice(1, -1).replace(/``/g, "`");
    if (first === "[" && last === "]") return tok.slice(1, -1);
  }
  return tok;
}

/** Normalizes a table identifier for comparison: unquote, then lowercase
 * (SQLite table names are case-insensitive). */
export function normalizeTableName(name: string): string {
  return unquoteIdentifier(name.trim()).toLowerCase();
}

/** True if a normalized (unquoted, lowercased) table name is one of ShardDO's
 * internal tables or under a reserved `__cf_`/`sqlite_` prefix. */
export function isInternalTableName(name: string): boolean {
  return INTERNAL_TABLE_SET.has(name) || name.startsWith("__cf_") || name.startsWith("sqlite_");
}

/** Reads one identifier token starting at `pos` after skipping leading
 * whitespace: a quoted `"..."`/`` `...` ``/`[...]` span (honoring the doubled-
 * quote escape for the first two) or a bare identifier. Returns the raw
 * (still-quoted) token text and the index just past it, or null if none is
 * present or a quote is unterminated (ambiguous → fail closed). */
function readIdentifierToken(s: string, pos: number): { raw: string; next: number } | null {
  while (pos < s.length && /\s/.test(s[pos])) pos += 1;
  if (pos >= s.length) return null;
  const c = s[pos];
  if (c === '"' || c === "`" || c === "[") {
    const close = c === "[" ? "]" : c;
    let j = pos + 1;
    while (j < s.length) {
      if (s[j] === close) {
        if ((close === '"' || close === "`") && s[j + 1] === close) {
          j += 2;
          continue;
        }
        j += 1;
        return { raw: s.slice(pos, j), next: j };
      }
      j += 1;
    }
    return null; // unterminated quote
  }
  const m = /^[A-Za-z_][A-Za-z0-9_]*/.exec(s.slice(pos));
  if (!m) return null;
  return { raw: m[0], next: pos + m[0].length };
}

/** Extracts the single write-target table of an INSERT/REPLACE/UPDATE/DELETE
 * statement, normalized (unquoted, lowercased, `schema.` qualifier dropped —
 * last dotted component wins). Robust to leading comments/CTE, arbitrary
 * whitespace around a `schema . table` qualifier, and quoting on either
 * component. Returns null if the statement isn't a recognizable single-table
 * DML write, the target can't be parsed, or a third dotted component appears
 * (not valid SQLite table syntax). Used by mutationTargetIsInternal (the
 * admin-only /v1/sql write guardrail against internal bookkeeping tables). */
export function mutationWriteTarget(sql: string): string | null {
  // Quote-aware comment strip: a comment delimiter inside a CTE-body string
  // literal (e.g. WITH x AS (SELECT '<block-open>') DELETE FROM __cf_...) must
  // not corrupt target extraction, or the internal-table write guard is
  // bypassed. See stripCommentsPreservingStrings' comment for the quote-blind
  // failure mode this avoids.
  const s = stripLeadingComments(skipLeadingCte(stripCommentsPreservingStrings(sql)));
  const kw = /^\s*(?:insert(?:\s+or\s+\w+)?\s+into|replace\s+into|update(?:\s+or\s+\w+)?|delete\s+from)(?=\s|["`[])/i.exec(s);
  if (!kw) return null;
  const first = readIdentifierToken(s, kw[0].length);
  if (!first) return null;
  let k = first.next;
  while (k < s.length && /\s/.test(s[k])) k += 1;
  if (s[k] === ".") {
    const second = readIdentifierToken(s, k + 1);
    if (!second) return null;
    let k2 = second.next;
    while (k2 < s.length && /\s/.test(s[k2])) k2 += 1;
    if (s[k2] === ".") return null; // db.schema.table — ambiguous, fail closed
    return normalizeTableName(second.raw);
  }
  return normalizeTableName(first.raw);
}

/** True if `sql` is a MUTATION whose write TARGET is an internal bookkeeping
 * table (however quoted, `schema.`-qualified, or comment-obfuscated). This is
 * the light guardrail on the now-admin-only /v1/sql: it blocks a fat-fingered
 * operator write from corrupting fence/provenance/mirror state, while ALLOWING
 * internal-table READS (an operator may need to inspect them for debugging) and
 * cross-table access (admin is trusted). Target-based, NOT a reference block —
 * a mutation to a normal table that merely READS an internal table in a
 * subquery is allowed. Returns false for a non-mutation or an unparseable
 * target (admin is trusted; the shard executes or errors on genuinely malformed
 * SQL). */
export function mutationTargetIsInternal(sql: string): boolean {
  if (!isMutation(sql)) return false;
  const target = mutationWriteTarget(sql);
  return target !== null && isInternalTableName(target);
}

function hasMultiStatementOrKeyword(sql: string, bannedKeywords: RegExp): boolean {
  const s = sql.trim().toLowerCase();
  const noTrailingSemicolon = s.replace(/;\s*$/, "");

  // Disallow multi-statement payloads (e.g. "select 1; drop table ...").
  if (noTrailingSemicolon.includes(";")) return true;

  return bannedKeywords.test(noTrailingSemicolon);
}

/** Deny-list: block statements tenants must never be able to run. */
export function isDangerous(sql: string): boolean {
  return hasMultiStatementOrKeyword(sql, /\b(drop|truncate|attach|detach|pragma|vacuum|reindex|alter|create)\b/);
}

/** Same deny-list as isDangerous(), minus "create" — /admin/create-table's schema
 * field is required to start with CREATE TABLE, so it can't ban that keyword, but
 * still must reject multi-statement payloads and other destructive keywords. */
export function isDangerousSchema(sql: string): boolean {
  return hasMultiStatementOrKeyword(sql, /\b(drop|truncate|attach|detach|pragma|vacuum|reindex|alter)\b/);
}

/** Extracts the table name from a "CREATE TABLE [IF NOT EXISTS] <name> (...)"
 * statement, unquoting "name"/[name]/`name` if quoted. Returns null if the
 * statement doesn't match the expected shape, so callers can reject it rather
 * than silently create a table under a different name than the caller declared. */
export function extractCreateTableName(sql: string): string | null {
  const match = /^\s*create\s+table\s+(?:if\s+not\s+exists\s+)?("([^"]+)"|`([^`]+)`|\[([^\]]+)\]|([A-Za-z_][A-Za-z0-9_]*))/i.exec(
    sql,
  );
  if (!match) return null;
  return match[2] ?? match[3] ?? match[4] ?? match[5] ?? null;
}

/** Returns `sql` with `IF NOT EXISTS` injected after `CREATE TABLE` when it's a
 * CREATE TABLE statement that lacks it — so re-executing a captured schema
 * against a target that ALREADY has the table is a harmless no-op instead of a
 * 400 "table already exists". Case- and whitespace-insensitive; the name
 * (quoted or `schema.`-qualified) and the body are left untouched, and a
 * statement that already has IF NOT EXISTS (or isn't a leading CREATE TABLE) is
 * returned unchanged.
 *
 * Used ONLY at migration-provision time to make schema provisioning idempotent
 * regardless of applied_requests dedup/TTL state — the stored schema_sql and
 * /admin/create-table's own validation are deliberately NOT changed. */
export function ensureCreateTableIfNotExists(sql: string): string {
  const m = /^(\s*create\s+table\s+)(if\s+not\s+exists\s+)?/i.exec(sql);
  if (!m) return sql; // not a leading CREATE TABLE — leave as-is
  if (m[2]) return sql; // already idempotent
  return sql.slice(0, m[1].length) + "IF NOT EXISTS " + sql.slice(m[1].length);
}
