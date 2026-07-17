import { describe, expect, it } from "vitest";
import {
  ensureCreateTableIfNotExists,
  extractCreateTableName,
  isDangerous,
  isDangerousSchema,
  isInternalTableName,
  isMutation,
  mutationTargetIsInternal,
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

  it("is not fooled by a MATERIALIZED/NOT MATERIALIZED hint on a leading CTE (CVE-class bypass)", () => {
    expect(isMutation("WITH x AS MATERIALIZED (SELECT 1) DELETE FROM events")).toBe(true);
    expect(isMutation("WITH x AS NOT MATERIALIZED (SELECT 1) UPDATE events SET y = 1")).toBe(true);
    // Multiple CTEs where only the LAST one carries the hint.
    expect(
      isMutation("WITH a AS (SELECT 1), b AS MATERIALIZED (SELECT 2) DELETE FROM events"),
    ).toBe(true);
    // Mixed case and arbitrary whitespace/newlines around the hint.
    expect(isMutation("WITH x AS\n  mAtErIaLiZeD\n  (SELECT 1)\nDELETE FROM events")).toBe(true);
    // Leading comments/whitespace before WITH, stacked with the hint.
    expect(
      isMutation("  -- op note\n  WITH x AS MATERIALIZED (SELECT 1) DELETE FROM events"),
    ).toBe(true);
  });

  it("classifies a MATERIALIZED-hinted, WITH/CTE-prefixed SELECT as non-mutation (no over-correction)", () => {
    expect(isMutation("WITH x AS MATERIALIZED (SELECT 1) SELECT * FROM x")).toBe(false);
    expect(isMutation("WITH x AS NOT MATERIALIZED (SELECT 1) SELECT * FROM x")).toBe(false);
    expect(isMutation("WITH x AS (SELECT 1) SELECT * FROM x")).toBe(false);
    expect(isMutation("SELECT * FROM events")).toBe(false);
  });

  it("is not fooled by a comment lodged inside the CTE header (CVE-class bypass)", () => {
    // Comment between AS and the body paren.
    expect(isMutation("WITH x AS /*c*/ (SELECT 1) DELETE FROM events")).toBe(true);
    // Comment between AS and the MATERIALIZED hint.
    expect(isMutation("WITH x AS/*c*/MATERIALIZED (SELECT 1) DELETE FROM events")).toBe(true);
    // Comment between the MATERIALIZED hint and the body paren.
    expect(isMutation("WITH x AS  MATERIALIZED/*c*/ (SELECT 1) DELETE FROM events")).toBe(true);
    // Comment between NOT and MATERIALIZED.
    expect(isMutation("WITH x AS NOT/*c*/MATERIALIZED (SELECT 1) UPDATE events SET y = 1")).toBe(true);
    // Line comment inside the header.
    expect(isMutation("WITH x AS -- hint\n (SELECT 1) DELETE FROM events")).toBe(true);
  });

  it("does NOT let a comment delimiter inside a CTE-body string literal hide the mutation (no regression from quote-blind stripping)", () => {
    // The `/*` lives inside a string literal — a quote-BLIND global comment
    // strip would eat through end-of-input and delete the trailing DELETE,
    // misclassifying this real mutation as a read. The quote-aware strip keeps
    // it a mutation.
    expect(isMutation("WITH x AS (SELECT '/*') DELETE FROM events")).toBe(true);
    expect(isMutation("WITH x AS (SELECT '--') DELETE FROM events")).toBe(true);
    // And the reverse: a genuine read whose CTE body merely mentions a comment
    // delimiter in a string is still non-mutating.
    expect(isMutation("WITH x AS (SELECT '/*') SELECT * FROM x")).toBe(false);
  });

  it("is not fooled by a paren inside a backtick/bracket-quoted identifier in the CTE body (CVE-class bypass)", () => {
    // A `(` / `)` inside a backtick- or bracket-quoted identifier must NOT
    // unbalance the CTE body-paren skip. skipBalancedParens previously tracked
    // only ' and ", so these desynced the skip and the DELETE was missed.
    expect(isMutation("WITH x AS (SELECT 1 AS `a(`) DELETE FROM events")).toBe(true);
    expect(isMutation("WITH x AS (SELECT 1 AS `a)`) DELETE FROM events")).toBe(true);
    expect(isMutation("WITH x AS (SELECT 1 AS [a(]) DELETE FROM events")).toBe(true);
    expect(isMutation("WITH x AS (SELECT 1 AS [a)]) UPDATE events SET y = 1")).toBe(true);
    expect(isMutation("WITH x AS (SELECT 1 AS `a(`) INSERT INTO events VALUES (1)")).toBe(true);
  });

  it("is not fooled by a doubled-quote-escaped CTE name (CVE-class bypass)", () => {
    // `"a""b"` is one identifier (a"b); the old regex matched only `"a"` and
    // desynced the parse, leaving 'with' as the leading keyword. INSERT/UPDATE
    // variants too.
    expect(isMutation('WITH "a""b" AS (SELECT 1) DELETE FROM events')).toBe(true);
    expect(isMutation("WITH `a``b` AS (SELECT 1) DELETE FROM events")).toBe(true);
    expect(isMutation('WITH "a""b" AS (SELECT 1) INSERT INTO events VALUES (1)')).toBe(true);
    expect(isMutation("WITH `a``b` AS (SELECT 1) UPDATE events SET y = 1")).toBe(true);
    // A doubled-quote-named CTE before a genuine SELECT stays non-mutating.
    expect(isMutation('WITH "a""b" AS (SELECT 1) SELECT * FROM "a""b"')).toBe(false);
  });
});

// The write-target extractor backs mutationTargetIsInternal, the admin-only
// /v1/sql write guardrail. Exhaustive parsing coverage lives here as a pure
// unit test — zero worker/gateway round trips, so it doesn't add to
// index.test.ts's cumulative-latency budget.
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

  it("extracts the target through a comment delimiter held inside a CTE-body string literal (quote-aware)", () => {
    // A quote-BLIND comment strip would eat from the `/*` inside the string to
    // end-of-input, corrupting extraction to null; the quote-aware strip keeps
    // the string intact and resolves the real DELETE target.
    expect(mutationWriteTarget("WITH x AS (SELECT '/*') DELETE FROM __cf_fenced_vbuckets")).toBe(
      "__cf_fenced_vbuckets",
    );
    expect(mutationWriteTarget("WITH x AS (SELECT '--') DELETE FROM events")).toBe("events");
  });

  it("extracts the target through a paren in a backtick/bracket identifier, and a doubled-quote CTE name (unified quote walker)", () => {
    expect(mutationWriteTarget("WITH x AS (SELECT 1 AS `a(`) DELETE FROM __cf_row_owners")).toBe("__cf_row_owners");
    expect(mutationWriteTarget("WITH x AS (SELECT 1 AS [a)]) DELETE FROM events")).toBe("events");
    expect(mutationWriteTarget('WITH "a""b" AS (SELECT 1) DELETE FROM __cf_row_owners')).toBe("__cf_row_owners");
    expect(mutationWriteTarget("WITH `a``b` AS (SELECT 1) DELETE FROM events")).toBe("events");
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

// Architecture change: /v1/sql is admin-only; the remaining guardrail blocks a
// MUTATION whose write TARGET is an internal table (however spelled), while
// ALLOWING internal-table READS and mutations that only READ an internal table
// in a subquery (target is a normal table). Target-based, not a reference block.
describe("mutationTargetIsInternal (admin /v1/sql write guardrail)", () => {
  it("blocks a mutation whose TARGET is an internal table, however quoted/qualified/obfuscated", () => {
    expect(mutationTargetIsInternal("DELETE FROM applied_requests")).toBe(true);
    expect(mutationTargetIsInternal('DELETE FROM main . "__cf_fenced_vbuckets"')).toBe(true);
    expect(mutationTargetIsInternal("DELETE/**/FROM __cf_row_owners")).toBe(true);
    expect(mutationTargetIsInternal("UPDATE `row_locks` SET x = 1")).toBe(true);
    expect(mutationTargetIsInternal("INSERT INTO [pending_intents] (x) VALUES (1)")).toBe(true);
  });

  it("blocks a MATERIALIZED-hinted CTE bypass targeting an internal table (CVE-class bypass)", () => {
    expect(mutationTargetIsInternal("WITH x AS MATERIALIZED (SELECT 1) DELETE FROM __cf_fenced_vbuckets")).toBe(
      true,
    );
  });

  it("blocks a comment-in-CTE-header bypass targeting an internal table (CVE-class bypass)", () => {
    expect(mutationTargetIsInternal("WITH x AS /*c*/ (SELECT 1) DELETE FROM __cf_fenced_vbuckets")).toBe(true);
    expect(
      mutationTargetIsInternal("WITH x AS/*c*/MATERIALIZED (SELECT 1) DELETE FROM __cf_fenced_vbuckets"),
    ).toBe(true);
  });

  it("blocks a string-literal-comment CTE bypass targeting an internal table (quote-aware extraction)", () => {
    // The comment delimiter lives inside a CTE-body string literal. A quote-BLIND
    // strip would corrupt mutationWriteTarget (null) and let this internal-table
    // write through; the quote-aware strip extracts the target correctly.
    expect(mutationTargetIsInternal("WITH x AS (SELECT '/*') DELETE FROM __cf_fenced_vbuckets")).toBe(true);
    expect(mutationTargetIsInternal("WITH x AS (SELECT '--') DELETE FROM __cf_fenced_vbuckets")).toBe(true);
  });

  it("blocks a backtick/bracket-quoted-paren CTE bypass targeting an internal table (CVE-class bypass)", () => {
    // Paren inside a backtick/bracket identifier in the CTE body — must not
    // desync the body-paren skip and hide the internal-table DELETE.
    expect(mutationTargetIsInternal("WITH x AS (SELECT 1 AS `a(`) DELETE FROM __cf_row_owners")).toBe(true);
    expect(mutationTargetIsInternal("WITH x AS (SELECT 1 AS [a(]) DELETE FROM __cf_row_owners")).toBe(true);
    expect(mutationTargetIsInternal("WITH x AS (SELECT 1 AS `a)`) DELETE FROM __cf_row_owners")).toBe(true);
    expect(mutationTargetIsInternal("WITH x AS (SELECT 1 AS `a(`) INSERT INTO __cf_row_owners VALUES (1)")).toBe(
      true,
    );
  });

  it("blocks a doubled-quote-CTE-name bypass targeting an internal table (CVE-class bypass)", () => {
    expect(mutationTargetIsInternal('WITH "a""b" AS (SELECT 1) DELETE FROM __cf_row_owners')).toBe(true);
    expect(mutationTargetIsInternal("WITH `a``b` AS (SELECT 1) DELETE FROM __cf_row_owners")).toBe(true);
  });

  it("ALLOWS a read-only CTE whose body string mentions a comment delimiter (no over-correction)", () => {
    expect(mutationTargetIsInternal("WITH x AS (SELECT '/*') SELECT * FROM __cf_row_owners")).toBe(false);
  });

  it("ALLOWS a mutation to a normal table, even one that reads an internal table in a subquery", () => {
    expect(mutationTargetIsInternal("INSERT INTO events (id) VALUES ('x')")).toBe(false);
    expect(mutationTargetIsInternal("INSERT INTO events (id) SELECT partition_key FROM __cf_row_owners")).toBe(false);
    expect(mutationTargetIsInternal("UPDATE events SET v = (SELECT count(*) FROM applied_requests)")).toBe(false);
  });

  it("ALLOWS reads (not a mutation), including reads of internal tables", () => {
    expect(mutationTargetIsInternal("SELECT * FROM __cf_row_owners")).toBe(false);
    expect(mutationTargetIsInternal('SELECT * FROM "applied_requests"')).toBe(false);
    expect(mutationTargetIsInternal("SELECT v FROM events WHERE id = 'x'")).toBe(false);
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

  it("reads a doubled-quote-escaped name WHOLE (not truncated) so the caller's name-match check can't be desynced", () => {
    // Old naive `"([^"]+)"` regex returned `a` for `"a""b"`; the escape-aware
    // reader returns the real identifier a"b (and ``a`b`` for backticks).
    expect(extractCreateTableName('CREATE TABLE "a""b" (id TEXT)')).toBe('a"b');
    expect(extractCreateTableName("CREATE TABLE `a``b` (id TEXT)")).toBe("a`b");
  });
});

// Codex review P2: migration-time schema provisioning must be idempotent —
// re-executing a captured CREATE TABLE against a target that already has the
// table (once applied_requests' dedup row is pruned by TTL) must no-op, not
// 400 and throw the migration into a retry loop.
describe("ensureCreateTableIfNotExists", () => {
  it("injects IF NOT EXISTS into a bare CREATE TABLE, leaving name and body untouched", () => {
    expect(ensureCreateTableIfNotExists("CREATE TABLE events (id TEXT PRIMARY KEY, v TEXT)")).toBe(
      "CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, v TEXT)",
    );
  });

  it("leaves a statement that already has IF NOT EXISTS unchanged", () => {
    const sql = "CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY)";
    expect(ensureCreateTableIfNotExists(sql)).toBe(sql);
  });

  it("is case- and whitespace-insensitive", () => {
    expect(ensureCreateTableIfNotExists("create   table events (id TEXT)")).toBe("create   table IF NOT EXISTS events (id TEXT)");
    expect(ensureCreateTableIfNotExists("Create Table Events (id TEXT)")).toBe("Create Table IF NOT EXISTS Events (id TEXT)");
    expect(ensureCreateTableIfNotExists("  CREATE TABLE events (id TEXT)")).toBe("  CREATE TABLE IF NOT EXISTS events (id TEXT)");
    // already-present, mixed case + spacing → unchanged
    const inx = "create table if  not  exists events (id TEXT)";
    expect(ensureCreateTableIfNotExists(inx)).toBe(inx);
  });

  it("handles quoted and schema-qualified names (name untouched)", () => {
    expect(ensureCreateTableIfNotExists('CREATE TABLE "events" (id TEXT)')).toBe('CREATE TABLE IF NOT EXISTS "events" (id TEXT)');
    expect(ensureCreateTableIfNotExists("CREATE TABLE main.events (id TEXT)")).toBe("CREATE TABLE IF NOT EXISTS main.events (id TEXT)");
    expect(ensureCreateTableIfNotExists("CREATE TABLE [events] (id TEXT)")).toBe("CREATE TABLE IF NOT EXISTS [events] (id TEXT)");
  });

  it("returns a non-CREATE-TABLE statement unchanged", () => {
    expect(ensureCreateTableIfNotExists("INSERT INTO events VALUES (1)")).toBe("INSERT INTO events VALUES (1)");
    expect(ensureCreateTableIfNotExists("CREATE INDEX idx ON events (v)")).toBe("CREATE INDEX idx ON events (v)");
  });
});
