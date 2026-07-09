import type { Env as WorkerEnv } from "./index";

declare global {
  namespace Cloudflare {
    interface Env extends WorkerEnv {}
  }
}
