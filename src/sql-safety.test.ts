import { describe, expect, it } from "vitest";
import { extractCreateTableName, isDangerous, isDangerousSchema, isMutation } from "./sql-safety";

describe("isMutation", () => {
  it("classifies plain mutation statements", () => {
    expect(isMutation("INSERT INTO t (id) VALUES (1)")).toBe(true);
    expect(isMutation("update t set x = 1")).toBe(true);
    expect(isMutation("DELETE FROM t")).toBe(true);
    expect(isMutation("  \n  REPLACE INTO t VALUES (1)")).toBe(true);
  });

  it("classifies SELECT as non-mutation", () => {
    expect(isMutation("SELECT * FROM t")).toBe(false);
  });

  it("is not fooled by a leading line comment (CVE-class bypass)", () => {
    expect(isMutation("-- harmless looking comment\nDELETE FROM events")).toBe(true);
    expect(isMutation("--\nUPDATE events SET x = 1")).toBe(true);
  });

  it("is not fooled by a leading block comment (CVE-class bypass)", () => {
    expect(isMutation("/* comment */ DELETE FROM events")).toBe(true);
    expect(isMutation("/*x*/UPDATE events SET x = 1")).toBe(true);
  });

  it("is not fooled by stacked/nested leading comments", () => {
    expect(isMutation("-- a\n/* b */ -- c\nDELETE FROM events")).toBe(true);
  });

  it("still classifies a comment-prefixed SELECT as non-mutation", () => {
    expect(isMutation("-- just a select\nSELECT * FROM events")).toBe(false);
  });

  it("is not fooled by a leading WITH/CTE clause (CVE-class bypass)", () => {
    expect(isMutation("WITH x AS (SELECT 1) DELETE FROM events")).toBe(true);
    expect(isMutation("with x as (select 1) update events set y = 1")).toBe(true);
    expect(isMutation("WITH RECURSIVE x AS (SELECT 1) INSERT INTO events VALUES (1)")).toBe(true);
  });

  it("classifies a WITH/CTE-prefixed SELECT as non-mutation", () => {
    expect(isMutation("WITH x AS (SELECT 1) SELECT * FROM x")).toBe(false);
  });

  it("handles multiple CTEs before the terminal mutation", () => {
    expect(isMutation("WITH a AS (SELECT 1), b AS (SELECT 2) DELETE FROM events")).toBe(true);
  });

  it("is not fooled by a paren or comma inside a CTE's string literal", () => {
    expect(isMutation("WITH x AS (SELECT ')' AS c, '(' AS d) DELETE FROM events")).toBe(true);
  });

  it("is not fooled by a comment-then-WITH-then-comment stack", () => {
    expect(isMutation("-- a\nWITH x AS (SELECT 1) -- b\nDELETE FROM events")).toBe(true);
  });
});

describe("isDangerous", () => {
  it("rejects multi-statement payloads", () => {
    expect(isDangerous("SELECT 1; DROP TABLE t")).toBe(true);
  });

  it("rejects banned keywords anywhere in the statement", () => {
    expect(isDangerous("SELECT * FROM t; PRAGMA table_info(t)")).toBe(true);
    expect(isDangerous("DROP TABLE t")).toBe(true);
  });

  it("allows a plain SELECT", () => {
    expect(isDangerous("SELECT * FROM t WHERE id = ?")).toBe(false);
  });
});

describe("isDangerousSchema", () => {
  it("allows CREATE (schema statements are expected to contain it)", () => {
    expect(isDangerousSchema("CREATE TABLE t (id TEXT PRIMARY KEY)")).toBe(false);
  });

  it("rejects a semicolon-chained second statement", () => {
    expect(isDangerousSchema("CREATE TABLE t (id TEXT PRIMARY KEY); DROP TABLE t")).toBe(true);
  });

  it("rejects other destructive keywords", () => {
    expect(isDangerousSchema("CREATE TABLE t (id TEXT PRIMARY KEY) attach database 'x' as y")).toBe(true);
  });
});

describe("extractCreateTableName", () => {
  it("extracts a plain unquoted table name", () => {
    expect(extractCreateTableName("CREATE TABLE events (id TEXT PRIMARY KEY)")).toBe("events");
  });

  it("extracts the name past IF NOT EXISTS", () => {
    expect(extractCreateTableName("CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY)")).toBe("events");
  });

  it("extracts a double-quoted table name", () => {
    expect(extractCreateTableName('CREATE TABLE "events" (id TEXT PRIMARY KEY)')).toBe("events");
  });

  it("extracts a backtick-quoted table name", () => {
    expect(extractCreateTableName("CREATE TABLE `events` (id TEXT PRIMARY KEY)")).toBe("events");
  });

  it("extracts a bracket-quoted table name", () => {
    expect(extractCreateTableName("CREATE TABLE [events] (id TEXT PRIMARY KEY)")).toBe("events");
  });

  it("returns null for a malformed statement", () => {
    expect(extractCreateTableName("CREATE TABLE (id TEXT PRIMARY KEY)")).toBe(null);
  });
});
