/** Sentinel stored in table_rules.partition_key_column for tables registered
 * before this column existed (including anything live from the v1.0.0.0
 * deploy). Tables carrying this sentinel are rejected from the structured
 * paths until an operator explicitly upgrades them via
 * /admin/set-partition-key-column. */
export const UNSET_PARTITION_KEY_COLUMN = "__unset__";

export const IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;

export type StructuredMutation = {
  op: "insert" | "update" | "delete" | "upsert";
  table: string;
  tenantId: string;
  partitionKey: string;
  where?: Record<string, unknown>;
  values?: Record<string, unknown>;
  conflictColumns?: string[];
};

export type StructuredOperation = {
  mutations: StructuredMutation[];
  requestId?: string;
};

export type ValidationResult =
  | { ok: true }
  | { ok: false; status: 400 | 409; code: string; error: string };

/** Distinct-row identity key for (tenantId, table, partitionKey). JSON-encoded
 * rather than `${tenantId}:${table}:${partitionKey}` string interpolation — a
 * `:` inside any field would otherwise let two distinct logical rows collide
 * onto the same key. Shared by participantKey() here and ShardDO's row_locks
 * key derivation (shard.ts) — both must compute the identical key for the
 * same logical row, or Chunk 3's coordinator locks silently fail to line up
 * with what ShardDO actually locked. */
export function rowKey(tenantId: string, table: string, partitionKey: string): string {
  return JSON.stringify([tenantId, table, partitionKey]);
}

/** Distinct-row grouping key for a mutation. See rowKey(). */
export function participantKey(m: StructuredMutation): string {
  return rowKey(m.tenantId, m.table, m.partitionKey);
}

/** All validation for a StructuredMutation lives here, in one shared place —
 * every caller (/v1/mutate now, /v1/tx later) goes through this, so none of
 * them can accidentally skip a check another one enforces. */
export function validateMutation(m: StructuredMutation, partitionKeyColumn: string): ValidationResult {
  if (partitionKeyColumn === UNSET_PARTITION_KEY_COLUMN) {
    return {
      ok: false,
      status: 409,
      code: "PARTITION_KEY_COLUMN_UNSET",
      error: `Table ${m.table} has not been upgraded with a partition key column. Call /admin/set-partition-key-column first.`,
    };
  }
  if (!m.table) return { ok: false, status: 400, code: "MISSING_TABLE", error: "Missing table." };
  if (!m.tenantId) return { ok: false, status: 400, code: "MISSING_TENANT_ID", error: "Missing tenantId." };
  if (!m.partitionKey) return { ok: false, status: 400, code: "MISSING_PARTITION_KEY", error: "Missing partitionKey." };
  if (!["insert", "update", "delete", "upsert"].includes(m.op)) {
    return { ok: false, status: 400, code: "UNKNOWN_OP", error: `Unknown op: ${m.op}` };
  }

  for (const key of Object.keys(m.where ?? {})) {
    if (!IDENTIFIER_RE.test(key)) {
      return { ok: false, status: 400, code: "UNSAFE_IDENTIFIER", error: `Unsafe identifier in where: ${key}` };
    }
  }
  for (const key of Object.keys(m.values ?? {})) {
    if (!IDENTIFIER_RE.test(key)) {
      return { ok: false, status: 400, code: "UNSAFE_IDENTIFIER", error: `Unsafe identifier in values: ${key}` };
    }
  }
  for (const col of m.conflictColumns ?? []) {
    if (!IDENTIFIER_RE.test(col)) {
      return { ok: false, status: 400, code: "UNSAFE_IDENTIFIER", error: `Unsafe identifier in conflictColumns: ${col}` };
    }
  }

  const whereConflict = m.where?.[partitionKeyColumn];
  if (whereConflict !== undefined && whereConflict !== m.partitionKey) {
    return {
      ok: false,
      status: 400,
      code: "PARTITION_KEY_CONFLICT",
      error: `where.${partitionKeyColumn} conflicts with the declared partitionKey.`,
    };
  }
  const valuesConflict = m.values?.[partitionKeyColumn];
  if (valuesConflict !== undefined && valuesConflict !== m.partitionKey) {
    return {
      ok: false,
      status: 400,
      code: "PARTITION_KEY_CONFLICT",
      error: `values.${partitionKeyColumn} conflicts with the declared partitionKey.`,
    };
  }

  if ((m.op === "insert" || m.op === "upsert") && (!m.values || Object.keys(m.values).length === 0)) {
    return { ok: false, status: 400, code: "MISSING_VALUES", error: `${m.op} requires at least one value.` };
  }
  if (m.op === "update") {
    const settable = Object.keys(m.values ?? {}).filter((c) => c !== partitionKeyColumn);
    if (settable.length === 0) {
      return {
        ok: false,
        status: 400,
        code: "MISSING_VALUES",
        error: "update requires at least one value to set (besides the partition key).",
      };
    }
  }

  return { ok: true };
}

/** Shared WHERE-clause builder for update/delete: the partition-key predicate
 * unconditionally ANDed first, then any caller-supplied `where` predicates.
 * compileMutation's update/delete cases build their SQL from this, and index
 * maintenance's pre-read (index.ts) must filter by the SAME predicate before
 * computing a beforeRow-based index delta — otherwise a `where` that doesn't
 * match (0 rows actually affected) would still see a beforeRow snapshot and
 * wrongly delete/rewrite that row's index entry for a mutation that never
 * actually touched it. One function, used by both call sites, so they can't
 * drift apart. */
export function mutationWhereClause(m: StructuredMutation, partitionKeyColumn: string): { sql: string; params: unknown[] } {
  const pkCol = `"${partitionKeyColumn}"`;
  const whereConditions = [`${pkCol} = ?`];
  const whereParams: unknown[] = [m.partitionKey];
  for (const [key, value] of Object.entries(m.where ?? {})) {
    if (key === partitionKeyColumn) continue;
    whereConditions.push(`"${key}" = ?`);
    whereParams.push(value);
  }
  return { sql: whereConditions.join(" AND "), params: whereParams };
}

/** Compiles a validated StructuredMutation to parameterized SQL — never
 * string-concatenates caller-supplied values, only column identifiers already
 * checked against IDENTIFIER_RE by validateMutation. Row-ownership is
 * structural, not conditional: the partition-key column is always forced into
 * the affected row(s), regardless of what the caller did or didn't supply. */
export function compileMutation(m: StructuredMutation, partitionKeyColumn: string): { sql: string; params: unknown[] } {
  if (!IDENTIFIER_RE.test(m.table)) {
    throw new Error(`Unsafe table identifier: ${m.table}`);
  }
  const table = `"${m.table}"`;

  switch (m.op) {
    case "insert": {
      // Force-set, not merely checked-if-present: the caller's value for the
      // partition-key column (if any) is always overridden with partitionKey.
      const values = { ...m.values, [partitionKeyColumn]: m.partitionKey };
      const columns = Object.keys(values);
      const params = columns.map((c) => values[c]);
      return {
        sql: `INSERT INTO ${table} (${columns.map((c) => `"${c}"`).join(", ")}) VALUES (${columns.map(() => "?").join(", ")})`,
        params,
      };
    }
    case "upsert": {
      const values = { ...m.values, [partitionKeyColumn]: m.partitionKey };
      const columns = Object.keys(values);
      const params = columns.map((c) => values[c]);
      const conflictCols = m.conflictColumns && m.conflictColumns.length > 0 ? m.conflictColumns : [partitionKeyColumn];
      const updateCols = columns.filter((c) => !conflictCols.includes(c));
      const conflictClause =
        updateCols.length > 0
          ? `ON CONFLICT (${conflictCols.map((c) => `"${c}"`).join(", ")}) DO UPDATE SET ${updateCols.map((c) => `"${c}" = excluded."${c}"`).join(", ")}`
          : `ON CONFLICT (${conflictCols.map((c) => `"${c}"`).join(", ")}) DO NOTHING`;
      return {
        sql: `INSERT INTO ${table} (${columns.map((c) => `"${c}"`).join(", ")}) VALUES (${columns.map(() => "?").join(", ")}) ${conflictClause}`,
        params,
      };
    }
    case "update": {
      const values = m.values ?? {};
      const setColumns = Object.keys(values).filter((c) => c !== partitionKeyColumn);
      const setClause = setColumns.map((c) => `"${c}" = ?`).join(", ");
      const setParams = setColumns.map((c) => values[c]);
      const where = mutationWhereClause(m, partitionKeyColumn);
      return {
        sql: `UPDATE ${table} SET ${setClause} WHERE ${where.sql}`,
        params: [...setParams, ...where.params],
      };
    }
    case "delete": {
      const where = mutationWhereClause(m, partitionKeyColumn);
      return {
        sql: `DELETE FROM ${table} WHERE ${where.sql}`,
        params: where.params,
      };
    }
  }
}
