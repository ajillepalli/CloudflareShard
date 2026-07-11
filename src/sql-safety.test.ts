import { describe, expect, it } from "vitest";
import {
  extractCreateTableName,
  isDangerous,
  isDangerousSchema,
  isInternalTableName,
  isInternalTableWrite,
  isMutation,
  mutationWriteTarget,
  normalizeTableName,
} from "./sql-safety";

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

// Codex review P1: the write-target extractor backs the /v1/sql ALLOWLIST
// gate (target must equal the caller's declared table). Exhaustive parsing
// coverage lives here as a pure unit test — zero worker/gateway round trips,
// so it doesn't add to index.test.ts's cumulative-latency budget.
describe("mutationWriteTarget", () => {
  it("extracts a bare target, normalized (lowercased)", () => {
    expect(mutationWriteTarget("DELETE FROM events")).toBe("events");
    expect(mutationWriteTarget("delete from Events where id = 1")).toBe("events");
    expect(mutationWriteTarget("UPDATE events SET v = 1")).toBe("events");
    expect(mutationWriteTarget("INSERT INTO events (id) VALUES (1)")).toBe("events");
    expect(mutationWriteTarget("INSERT INTO events(id) VALUES (1)")).toBe("events"); // no space before (
    expect(mutationWriteTarget("REPLACE INTO events VALUES (1)")).toBe("events");
    expect(mutationWriteTarget("INSERT OR IGNORE INTO events VALUES (1)")).toBe("events");
    expect(mutationWriteTarget("UPDATE OR REPLACE events SET v = 1")).toBe("events");
  });

  it("unquotes a quoted target (double, backtick, bracket) and lowercases it", () => {
    expect(mutationWriteTarget('DELETE FROM "Events"')).toBe("events");
    expect(mutationWriteTarget("DELETE FROM `events`")).toBe("events");
    expect(mutationWriteTarget("DELETE FROM [events]")).toBe("events");
    expect(mutationWriteTarget('INSERT INTO"events" VALUES (1)')).toBe("events"); // no space after INTO
  });

  it("drops a schema qualifier (last dotted component wins), tolerating whitespace around the dot", () => {
    expect(mutationWriteTarget("DELETE FROM main.events")).toBe("events");
    expect(mutationWriteTarget("DELETE FROM main . events")).toBe("events");
    expect(mutationWriteTarget("DELETE FROM main   .   events")).toBe("events");
    expect(mutationWriteTarget('DELETE FROM "main" . "events"')).toBe("events");
    // The confirmed 4th bypass shape: spaced qualifier + quoted internal table.
    expect(mutationWriteTarget('DELETE FROM main . "__cf_fenced_vbuckets"')).toBe("__cf_fenced_vbuckets");
  });

  it("sees through inter-token comments and a leading CTE", () => {
    expect(mutationWriteTarget("DELETE/**/FROM __cf_row_owners")).toBe("__cf_row_owners");
    expect(mutationWriteTarget("DELETE /* wipe */ FROM events")).toBe("events");
    expect(mutationWriteTarget("WITH x AS (SELECT 1) DELETE FROM events")).toBe("events");
  });

  it("fail-closed (null) on non-DML or an ambiguous/three-part target", () => {
    expect(mutationWriteTarget("SELECT * FROM events")).toBeNull();
    expect(mutationWriteTarget("CREATE TABLE events (id TEXT)")).toBeNull();
    expect(mutationWriteTarget("DELETE FROM db.main.events")).toBeNull(); // 3 parts — not valid table syntax
    expect(mutationWriteTarget('DELETE FROM "unterminated')).toBeNull();
  });
});

describe("isInternalTableName / normalizeTableName", () => {
  it("recognizes the internal tables and reserved prefixes, case-insensitively", () => {
    expect(isInternalTableName("applied_requests")).toBe(true);
    expect(isInternalTableName("__cf_fenced_vbuckets")).toBe(true);
    expect(isInternalTableName("__cf_anything_new")).toBe(true); // reserved prefix
    expect(isInternalTableName("sqlite_sequence")).toBe(true);
    expect(isInternalTableName("events")).toBe(false);
  });

  it("normalizes (unquote + lowercase)", () => {
    expect(normalizeTableName('"Events"')).toBe("events");
    expect(normalizeTableName("`Row_Locks`")).toBe("row_locks");
    expect(normalizeTableName("  Events  ")).toBe("events");
  });
});

describe("isInternalTableWrite (defense-in-depth denylist)", () => {
  it("blocks a mutation targeting an internal table, however spelled", () => {
    expect(isInternalTableWrite("DELETE FROM applied_requests")).toBe(true);
    expect(isInternalTableWrite('DELETE FROM main . "__cf_fenced_vbuckets"')).toBe(true);
    expect(isInternalTableWrite("INSERT INTO events (id) SELECT partition_key FROM __cf_row_owners")).toBe(true);
  });

  it("does not flag a legit own-table write, incl. a double-quoted string VALUE equal to an internal name", () => {
    expect(isInternalTableWrite("INSERT INTO events (id, v) VALUES ('n1', 'row_locks note')")).toBe(false);
    expect(isInternalTableWrite('INSERT INTO events (id, v) VALUES (\'n1\', "applied_requests")')).toBe(false);
    expect(isInternalTableWrite("SELECT * FROM __cf_row_owners")).toBe(false); // read, not a mutation
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
