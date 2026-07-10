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

/** Extracts the write-target table name of an INSERT/REPLACE/UPDATE/DELETE
 * statement (comments + a leading CTE stripped first, mirroring isMutation),
 * unquoting `"x"`/`` `x` ``/`[x]`. Returns null if the statement isn't a
 * recognizable single-table DML write. Only the write target matters for the
 * internal-table guard: a subquery that merely *reads* an internal table
 * doesn't mutate it. */
function mutationTargetTable(sql: string): string | null {
  const afterComments = stripLeadingComments(sql);
  const s = stripLeadingComments(skipLeadingCte(afterComments));
  const ident = `("([^"]+)"|\`([^\`]+)\`|\\[([^\\]]+)\\]|([A-Za-z_][A-Za-z0-9_]*))`;
  const patterns = [
    new RegExp(`^\\s*insert(?:\\s+or\\s+\\w+)?\\s+into\\s+${ident}`, "i"),
    new RegExp(`^\\s*replace\\s+into\\s+${ident}`, "i"),
    new RegExp(`^\\s*update(?:\\s+or\\s+\\w+)?\\s+${ident}`, "i"),
    new RegExp(`^\\s*delete\\s+from\\s+${ident}`, "i"),
  ];
  for (const re of patterns) {
    const m = re.exec(s);
    if (m) return m[2] ?? m[3] ?? m[4] ?? m[5] ?? null;
  }
  return null;
}

/** True if `sql` is a mutation whose write target is one of ShardDO's internal
 * tables (or any `__cf_`/`sqlite_`-prefixed name, for forward-safety against a
 * table added later). The tenant-facing `/v1/sql` gate rejects these 403 —
 * the raw-execute path is otherwise reachable with an arbitrary SQL string,
 * and `body.table` (used only for routing) says nothing about what table the
 * SQL text actually touches. */
export function isInternalTableWrite(sql: string): boolean {
  const target = mutationTargetTable(sql);
  if (target === null) return false;
  const lower = target.toLowerCase();
  return INTERNAL_TABLE_SET.has(target) || lower.startsWith("__cf_") || lower.startsWith("sqlite_");
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
