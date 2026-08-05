import assert from "node:assert/strict";
import test from "node:test";
import { SourceProvenanceError, sourceRevision } from "../src/cli.mjs";

const REVISION = "0123456789abcdef0123456789abcdef01234567";

function gitFixture({ revision = REVISION, status = "" } = {}) {
  const calls = [];
  const execFile = (file, args) => {
    calls.push([file, args]);
    if (args[0] === "rev-parse") return `${revision}\n`;
    if (args[0] === "status") return status;
    throw new Error(`Unexpected command: ${file} ${args.join(" ")}`);
  };
  return { calls, execFile };
}

test("source provenance accepts a full revision only when tracked and untracked state is clean", () => {
  const { calls, execFile } = gitFixture();

  assert.equal(sourceRevision(execFile), REVISION);
  assert.deepEqual(calls.map(([, args]) => args), [
    ["rev-parse", "HEAD"],
    ["status", "--porcelain=v1", "--untracked-files=normal"],
  ]);
});

test("source provenance fails closed when the worktree is dirty", () => {
  const { execFile } = gitFixture({ status: " M src/workload.mjs\n?? local-change.mjs\n" });

  assert.throws(
    () => sourceRevision(execFile),
    (error) => error instanceof SourceProvenanceError && /clean Git worktree/.test(error.message),
  );
});

test("source provenance fails closed when Git is unavailable or the revision is not full length", () => {
  assert.throws(
    () => sourceRevision(() => { throw new Error("git unavailable"); }),
    (error) => error instanceof SourceProvenanceError && /readable Git revision/.test(error.message),
  );
  assert.throws(
    () => sourceRevision(gitFixture({ revision: "deadbeef" }).execFile),
    (error) => error instanceof SourceProvenanceError && /invalid source revision/.test(error.message),
  );
});
