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

/** Skips past a leading "WITH [RECURSIVE] name [(cols)] AS (body) [, ...]"
 * common-table-expression clause to reach the terminal statement (SELECT,
 * INSERT, UPDATE, or DELETE — SQLite allows WITH before any of them). Without
 * this, "WITH x AS (SELECT 1) DELETE FROM t" reads as a harmless SELECT
 * because isMutation() only ever inspected the first keyword. Bails out
 * (returns the input unchanged) on anything that doesn't match the expected
 * CTE grammar — malformed input isn't valid SQL SQLite would execute as a
 * mutation anyway, so it's safe to leave unclassified here. */
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

export function isMutation(sql: string): boolean {
  const afterComments = stripLeadingComments(sql);
  const afterCte = stripLeadingComments(skipLeadingCte(afterComments));
  return /^(insert|update|delete|replace|create|drop|alter)/i.test(afterCte);
}

/** Milestone 3, Chunk 0: does this statement's leading keyword classify it as
 * a DELETE? Used by ShardDO to decide whether a successful write should
 * remove (delete) or upsert (insert/update/replace) the row's
 * `__cf_row_owners` provenance entry — derived from the SQL text itself, the
 * same "ShardDO classifies its own writes" philosophy isMutation() already
 * uses, rather than trusting a caller-supplied hint. */
export function isDeleteStatement(sql: string): boolean {
  const afterComments = stripLeadingComments(sql);
  const afterCte = stripLeadingComments(skipLeadingCte(afterComments));
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
] as const;

const INTERNAL_TABLE_SET = new Set<string>(INTERNAL_TABLE_NAMES);

/** Removes `-- ...` line and `/* ... *​/` block comments from anywhere in the
 * SQL (replacing each with a space), leaving all quoted spans intact. Used
 * before write-target extraction so an inter-token comment (`DELETE/**​/FROM
 * x`) can't hide the keyword/target structure. */
function stripComments(sql: string): string {
  let out = "";
  let i = 0;
  while (i < sql.length) {
    const c = sql[i];
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

/** Like stripComments, but ALSO strips single-quoted (`'x'`, `''` escape) AND
 * double-quoted (`"x"`, `""` escape) string spans, replacing each with a
 * space. Double-quoted spans are stripped because SQLite treats a
 * double-quoted token that doesn't resolve as an identifier as a STRING
 * LITERAL — so a legit `VALUES ('n1', "applied_requests")` would otherwise
 * false-positive against the internal-table reference check. The write TARGET
 * (a double-quoted table name after FROM/INTO/UPDATE) is handled separately by
 * mutationTargetIsInternal, so stripping double quotes here doesn't weaken the
 * guard. */
function stripStringsAndComments(sql: string): string {
  let out = "";
  let i = 0;
  while (i < sql.length) {
    const c = sql[i];
    if (c === "'" || c === '"') {
      const quote = c;
      i += 1;
      while (i < sql.length) {
        if (sql[i] === quote) {
          if (sql[i + 1] === quote) {
            i += 2;
            continue;
          }
          i += 1;
          break;
        }
        i += 1;
      }
      out += " ";
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

/** Case-insensitive word-boundary match for ANY internal table name. Derived
 * from INTERNAL_TABLE_NAMES so it can't drift: the four fixed names plus the
 * `__cf_`/`sqlite_` prefixes (which cover the prefixed entries AND any table
 * added later under those reserved namespaces). Word boundaries make it
 * agnostic to a `schema.` qualifier (`main.__cf_row_owners`), backtick/bracket
 * quoting (`` `row_locks` ``, `[pending_intents]`), and case. */
const INTERNAL_TABLE_REFERENCE_RE = new RegExp(
  `\\b(${[
    ...INTERNAL_TABLE_NAMES.filter((n) => !n.startsWith("__cf_") && !n.startsWith("sqlite_")),
    "__cf_\\w+",
    "sqlite_\\w+",
  ].join("|")})\\b`,
  "i",
);

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

/** True if the statement's write TARGET (the table right after INSERT INTO /
 * REPLACE INTO / UPDATE / DELETE FROM) is an internal table — handling an
 * inter-token comment (stripped first), a `schema.` qualifier (last dotted
 * component wins), quoting, and case. This is the layer that must still block a
 * double-quoted target like `DELETE FROM "applied_requests"`, which the
 * reference check below can't see (it strips double-quoted spans). */
function mutationTargetIsInternal(sql: string): boolean {
  const s = stripLeadingComments(skipLeadingCte(stripComments(sql)));
  const m = /^\s*(?:insert(?:\s+or\s+\w+)?\s+into|replace\s+into|update(?:\s+or\s+\w+)?|delete\s+from)\s+([^\s(]+)/i.exec(s);
  if (!m) return false;
  const parts = m[1].split("."); // drop any schema. qualifier
  const last = unquoteIdentifier(parts[parts.length - 1]).toLowerCase();
  return INTERNAL_TABLE_SET.has(last) || last.startsWith("__cf_") || last.startsWith("sqlite_");
}

/** True if `sql` is a mutation that touches any of ShardDO's internal
 * bookkeeping tables. The tenant-facing `/v1/sql` gate rejects these 403.
 *
 * Two layers:
 *  1. mutationTargetIsInternal — the write target itself, however quoted or
 *     schema-qualified (catches `DELETE FROM "applied_requests"`).
 *  2. a blanket REFERENCE block (word-boundary regex) over text with comments
 *     and single- AND double-quoted string literals stripped — catches a bare
 *     or backtick/bracket-quoted internal name anywhere else (subquery, join),
 *     while NOT false-positiving on a double-quoted STRING VALUE such as
 *     `VALUES ('n1', "applied_requests")` (SQLite treats a non-identifier
 *     double-quoted token as a string literal).
 *
 * Together they close the target-extraction bypasses a re-review confirmed
 * (mixed case, inter-token comments, `schema.` qualifiers) without the
 * double-quoted-string-value false positive a later pass found.
 *
 * Scoped to mutations (isMutation) so a legitimate read that happens to name a
 * column identically isn't blocked — the guard's job is to stop internal-table
 * WRITES from the data plane. Defense-in-depth at ShardDO /execute is NOT
 * added: /execute is a DO-internal route whose own callers (index maintenance
 * writing __cf_indexes, mirror delivery, provenance upserts) legitimately
 * write these tables, so a blanket block there would break them; tenants only
 * reach /execute THROUGH this Worker gate, which is therefore the correct
 * single chokepoint. */
export function isInternalTableWrite(sql: string): boolean {
  if (!isMutation(sql)) return false;
  if (mutationTargetIsInternal(sql)) return true;
  return INTERNAL_TABLE_REFERENCE_RE.test(stripStringsAndComments(sql));
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
