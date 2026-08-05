import path from "node:path";

export const DEFAULTS = Object.freeze({
  durationSeconds: 30,
  concurrency: 4,
  maxOperations: 100,
  requestTimeoutMs: 5_000,
  graceMs: 10_000,
  seed: 20_260_805,
  outputDir: "artifacts",
});

const VALUE_FLAGS = new Set([
  "base-url",
  "duration-seconds",
  "concurrency",
  "max-operations",
  "request-timeout-ms",
  "grace-ms",
  "seed",
  "output-dir",
]);

export class InvocationError extends Error {
  constructor(message) {
    super(message);
    this.name = "InvocationError";
  }
}

function positiveNumber(raw, flag, { integer = false, maximum = Number.MAX_SAFE_INTEGER } = {}) {
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0 || (integer && !Number.isInteger(value)) || value > maximum) {
    const kind = integer ? "a positive finite integer" : "a positive finite number";
    throw new InvocationError(`--${flag} must be ${kind} no greater than ${maximum}; received ${String(raw)}.`);
  }
  return value;
}

export function normalizeBaseUrl(raw) {
  let parsed;
  try {
    parsed = new URL(raw);
  } catch {
    throw new InvocationError("--base-url must be a valid absolute URL.");
  }
  const localHttp = parsed.protocol === "http:" && ["127.0.0.1", "localhost", "::1", "[::1]"].includes(parsed.hostname);
  if (parsed.protocol !== "https:" && !localHttp) {
    throw new InvocationError("--base-url must use HTTPS, except for an HTTP localhost target.");
  }
  if (parsed.username || parsed.password || parsed.search || parsed.hash || (parsed.pathname !== "/" && parsed.pathname !== "")) {
    throw new InvocationError("--base-url must be an origin only, with no credentials, path, query, or fragment.");
  }
  return parsed.origin;
}

export function parseArgs(argv) {
  if (argv.includes("--help") || argv.includes("-h")) return { help: true };

  const values = new Map();
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith("--")) throw new InvocationError(`Unexpected positional argument: ${token}`);
    const flag = token.slice(2);
    if (!VALUE_FLAGS.has(flag)) throw new InvocationError(`Unknown option: --${flag}`);
    if (values.has(flag)) throw new InvocationError(`Option --${flag} may be specified only once.`);
    const value = argv[index + 1];
    if (value === undefined || value.startsWith("--")) throw new InvocationError(`Option --${flag} requires a value.`);
    values.set(flag, value);
    index += 1;
  }

  const baseUrlRaw = values.get("base-url");
  if (!baseUrlRaw) throw new InvocationError("Missing required option: --base-url.");
  const outputDirRaw = values.get("output-dir") ?? DEFAULTS.outputDir;
  if (!outputDirRaw.trim()) throw new InvocationError("--output-dir must not be empty.");

  return {
    help: false,
    baseUrl: normalizeBaseUrl(baseUrlRaw),
    durationMs: positiveNumber(values.get("duration-seconds") ?? DEFAULTS.durationSeconds, "duration-seconds", { maximum: 3_600 }) * 1_000,
    concurrency: positiveNumber(values.get("concurrency") ?? DEFAULTS.concurrency, "concurrency", { integer: true, maximum: 256 }),
    maxOperations: positiveNumber(values.get("max-operations") ?? DEFAULTS.maxOperations, "max-operations", { integer: true, maximum: 1_000_000 }),
    requestTimeoutMs: positiveNumber(values.get("request-timeout-ms") ?? DEFAULTS.requestTimeoutMs, "request-timeout-ms", { integer: true, maximum: 300_000 }),
    graceMs: positiveNumber(values.get("grace-ms") ?? DEFAULTS.graceMs, "grace-ms", { integer: true, maximum: 300_000 }),
    seed: positiveNumber(values.get("seed") ?? DEFAULTS.seed, "seed", { integer: true, maximum: 0xffff_ffff }),
    outputDir: path.resolve(outputDirRaw),
  };
}

export function usage() {
  return `CloudflareShard bespoke OLTP baseline

Usage:
  npm run benchmark -- --base-url URL [options]

Required environment:
  CLOUDFLARESHARD_ADMIN_TOKEN   Admin token for a disposable initialized target

Options:
  --base-url URL                HTTPS target origin (HTTP allowed for localhost)
  --duration-seconds N          Scheduling window; default ${DEFAULTS.durationSeconds}
  --concurrency N               Closed-loop workers; default ${DEFAULTS.concurrency}
  --max-operations N            Hard attempt cap; default ${DEFAULTS.maxOperations}
  --request-timeout-ms N        Per-request deadline; default ${DEFAULTS.requestTimeoutMs}
  --grace-ms N                  Post-window completion grace; default ${DEFAULTS.graceMs}
  --seed N                      Positive 32-bit deterministic seed; default ${DEFAULTS.seed}
  --output-dir PATH             JSON and Markdown directory; default ${DEFAULTS.outputDir}
  --help                        Show this help
`;
}
