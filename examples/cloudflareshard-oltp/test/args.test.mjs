import assert from "node:assert/strict";
import test from "node:test";
import { InvocationError, parseArgs } from "../src/args.mjs";

test("numeric options require finite positive values and integer options stay integral", () => {
  for (const [flag, value] of [
    ["duration-seconds", "0"],
    ["duration-seconds", "Infinity"],
    ["duration-seconds", "NaN"],
    ["concurrency", "1.5"],
    ["max-operations", "-1"],
    ["request-timeout-ms", "0"],
    ["grace-ms", "-10"],
    ["seed", "0"],
  ]) {
    assert.throws(() => parseArgs(["--base-url", "https://example.com", `--${flag}`, value]), InvocationError);
  }
});

test("base URL rejects secrets and non-local cleartext targets", () => {
  const credentialTarget = new URL("https://example.com");
  credentialTarget.username = "user";
  credentialTarget.password = "secret";
  assert.throws(() => parseArgs(["--base-url", credentialTarget.href]), /credentials/);
  assert.throws(() => parseArgs(["--base-url", "http://example.com"]), /HTTPS/);
  assert.equal(parseArgs(["--base-url", "http://localhost:8787/"]).baseUrl, "http://localhost:8787");
});

test("unknown, duplicate, and valueless flags fail closed", () => {
  assert.throws(() => parseArgs(["--base-url", "https://example.com", "--mystery", "1"]), /Unknown option/);
  assert.throws(() => parseArgs(["--base-url", "https://example.com", "--seed", "1", "--seed", "2"]), /only once/);
  assert.throws(() => parseArgs(["--base-url"]), /requires a value/);
});
