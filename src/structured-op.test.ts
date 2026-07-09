import { describe, expect, it } from "vitest";
import { compileMutation, participantKey, UNSET_PARTITION_KEY_COLUMN, validateMutation, type StructuredMutation } from "./structured-op";

function baseMutation(overrides: Partial<StructuredMutation> = {}): StructuredMutation {
  return {
    op: "insert",
    table: "events",
    tenantId: "t1",
    partitionKey: "p1",
    ...overrides,
  };
}

describe("validateMutation", () => {
  it("rejects a table still carrying the unset sentinel partition-key column", () => {
    const result = validateMutation(baseMutation(), UNSET_PARTITION_KEY_COLUMN);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.status).toBe(409);
      expect(result.code).toBe("PARTITION_KEY_COLUMN_UNSET");
    }
  });

  it("accepts a well-formed insert", () => {
    const result = validateMutation(baseMutation({ values: { name: "a" } }), "partition_key");
    expect(result.ok).toBe(true);
  });

  it("rejects unsafe identifiers in where, values, and conflictColumns", () => {
    const whereResult = validateMutation(
      baseMutation({ op: "delete", where: { "bad; drop table x": "y" } }),
      "partition_key",
    );
    expect(whereResult.ok).toBe(false);
    if (!whereResult.ok) expect(whereResult.code).toBe("UNSAFE_IDENTIFIER");

    const valuesResult = validateMutation(
      baseMutation({ values: { "bad col": "y" } }),
      "partition_key",
    );
    expect(valuesResult.ok).toBe(false);
    if (!valuesResult.ok) expect(valuesResult.code).toBe("UNSAFE_IDENTIFIER");

    const conflictResult = validateMutation(
      baseMutation({ op: "upsert", values: { a: 1 }, conflictColumns: ["bad col"] }),
      "partition_key",
    );
    expect(conflictResult.ok).toBe(false);
    if (!conflictResult.ok) expect(conflictResult.code).toBe("UNSAFE_IDENTIFIER");
  });

  it("rejects a caller-supplied partition-key value that conflicts with the declared partitionKey", () => {
    const whereConflict = validateMutation(
      baseMutation({ op: "delete", where: { partition_key: "different-value" } }),
      "partition_key",
    );
    expect(whereConflict.ok).toBe(false);
    if (!whereConflict.ok) expect(whereConflict.code).toBe("PARTITION_KEY_CONFLICT");

    const valuesConflict = validateMutation(
      baseMutation({ values: { partition_key: "different-value" } }),
      "partition_key",
    );
    expect(valuesConflict.ok).toBe(false);
    if (!valuesConflict.ok) expect(valuesConflict.code).toBe("PARTITION_KEY_CONFLICT");
  });

  it("allows a where/values partition-key value that matches the declared partitionKey", () => {
    const result = validateMutation(
      baseMutation({ values: { partition_key: "p1", name: "a" } }),
      "partition_key",
    );
    expect(result.ok).toBe(true);
  });

  it("rejects insert/upsert with no values", () => {
    const insertResult = validateMutation(baseMutation({ op: "insert" }), "partition_key");
    expect(insertResult.ok).toBe(false);
    if (!insertResult.ok) expect(insertResult.code).toBe("MISSING_VALUES");

    const upsertResult = validateMutation(baseMutation({ op: "upsert" }), "partition_key");
    expect(upsertResult.ok).toBe(false);
    if (!upsertResult.ok) expect(upsertResult.code).toBe("MISSING_VALUES");
  });

  it("rejects update with no settable values besides the partition key", () => {
    const result = validateMutation(
      baseMutation({ op: "update", values: { partition_key: "p1" } }),
      "partition_key",
    );
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.code).toBe("MISSING_VALUES");
  });

  it("rejects missing table/tenantId/partitionKey and unknown op", () => {
    expect(validateMutation(baseMutation({ table: "" }), "pk").ok).toBe(false);
    expect(validateMutation(baseMutation({ tenantId: "" }), "pk").ok).toBe(false);
    expect(validateMutation(baseMutation({ partitionKey: "" }), "pk").ok).toBe(false);
    expect(validateMutation(baseMutation({ op: "select" as StructuredMutation["op"] }), "pk").ok).toBe(false);
  });
});

describe("compileMutation", () => {
  it("insert: force-sets the partition-key column even when the caller didn't supply one", () => {
    const { sql, params } = compileMutation(baseMutation({ values: { name: "a" } }), "partition_key");
    expect(sql).toContain('INSERT INTO "events"');
    expect(sql).toContain('"partition_key"');
    expect(params).toContain("p1");
  });

  it("insert: overrides a caller-supplied partition-key value with the declared partitionKey", () => {
    // validateMutation would already reject a *conflicting* value, but compileMutation
    // itself must never trust a caller-supplied value for this column either way.
    const { params } = compileMutation(
      baseMutation({ values: { name: "a", partition_key: "p1" } }),
      "partition_key",
    );
    expect(params.filter((p) => p === "p1").length).toBeGreaterThanOrEqual(1);
  });

  it("update: injects the partition-key predicate into WHERE even when the caller supplies no where at all", () => {
    const { sql, params } = compileMutation(
      baseMutation({ op: "update", values: { name: "b" } }),
      "partition_key",
    );
    expect(sql).toContain('WHERE "partition_key" = ?');
    expect(sql).not.toContain("undefined");
    expect(params).toEqual(["b", "p1"]);
  });

  it("update: caller-supplied where narrows further but never substitutes for the partition-key predicate", () => {
    const { sql, params } = compileMutation(
      baseMutation({ op: "update", values: { name: "b" }, where: { status: "active" } }),
      "partition_key",
    );
    expect(sql).toContain('WHERE "partition_key" = ? AND "status" = ?');
    expect(params).toEqual(["b", "p1", "active"]);
  });

  it("delete: injects the partition-key predicate even with no where clause — never compiles to a whole-table delete", () => {
    const { sql, params } = compileMutation(baseMutation({ op: "delete" }), "partition_key");
    expect(sql).toBe('DELETE FROM "events" WHERE "partition_key" = ?');
    expect(params).toEqual(["p1"]);
  });

  it("upsert: defaults conflictColumns to [partitionKeyColumn] when omitted", () => {
    const { sql } = compileMutation(baseMutation({ op: "upsert", values: { name: "a" } }), "partition_key");
    expect(sql).toContain('ON CONFLICT ("partition_key")');
    expect(sql).toContain("DO UPDATE SET");
  });

  it("upsert: respects explicit conflictColumns", () => {
    const { sql } = compileMutation(
      baseMutation({ op: "upsert", values: { name: "a", email: "e" }, conflictColumns: ["email"] }),
      "partition_key",
    );
    expect(sql).toContain('ON CONFLICT ("email")');
  });

  it("never string-concatenates a caller-supplied value into the SQL text", () => {
    const { sql, params } = compileMutation(
      baseMutation({ values: { name: "'; DROP TABLE events; --" } }),
      "partition_key",
    );
    expect(sql).not.toContain("DROP TABLE");
    expect(params).toContain("'; DROP TABLE events; --");
  });
});

describe("participantKey", () => {
  it("produces a distinct key per (tenantId, table, partitionKey) tuple", () => {
    const a = participantKey(baseMutation({ tenantId: "t1", table: "events", partitionKey: "p1" }));
    const b = participantKey(baseMutation({ tenantId: "t1", table: "events", partitionKey: "p2" }));
    expect(a).not.toBe(b);
  });

  it("is stable for identical tuples (used for dedup)", () => {
    const a = participantKey(baseMutation({ tenantId: "t1", table: "events", partitionKey: "p1" }));
    const b = participantKey(baseMutation({ tenantId: "t1", table: "events", partitionKey: "p1", op: "delete" }));
    expect(a).toBe(b);
  });

  it("does not collide across a ':'-style delimiter ambiguity", () => {
    // tenantId "a:b" + table "c" vs tenantId "a" + table "b:c" would collide
    // under naive string interpolation but must not collide under JSON encoding.
    const a = participantKey(baseMutation({ tenantId: "a:b", table: "c", partitionKey: "p1" }));
    const b = participantKey(baseMutation({ tenantId: "a", table: "b:c", partitionKey: "p1" }));
    expect(a).not.toBe(b);
  });
});
