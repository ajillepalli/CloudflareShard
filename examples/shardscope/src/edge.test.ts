import { describe, expect, it } from "vitest";
import { buildEdgeInfo } from "./edge";

describe("edge.ts — buildEdgeInfo", () => {
  it("reports local:true, edge:null when request.cf is undefined (local dev / miniflare)", () => {
    const result = buildEdgeInfo(undefined, 1_700_000_000_000);
    expect(result).toEqual({ local: true, edge: null, serverReceivedAt: 1_700_000_000_000 });
  });

  it("reports the real edge fields when request.cf is present with full geo", () => {
    const cf = {
      colo: "AMS",
      country: "NL",
      city: "Amsterdam",
      region: "North Holland",
      latitude: "52.37403",
      longitude: "4.88969",
    } as unknown as IncomingRequestCfProperties;

    const result = buildEdgeInfo(cf, 1_700_000_000_000);

    expect(result).toEqual({
      local: false,
      edge: {
        colo: "AMS",
        country: "NL",
        city: "Amsterdam",
        region: "North Holland",
        latitude: "52.37403",
        longitude: "4.88969",
      },
      serverReceivedAt: 1_700_000_000_000,
    });
  });

  it("nulls out missing optional geo fields but keeps the real colo", () => {
    const cf = { colo: "NRT" } as unknown as IncomingRequestCfProperties;

    const result = buildEdgeInfo(cf, 42);

    expect(result).toEqual({
      local: false,
      edge: { colo: "NRT", country: null, city: null, region: null, latitude: null, longitude: null },
      serverReceivedAt: 42,
    });
  });

  it("treats a cf object with an empty-string colo as local (never reports a blank colo as real)", () => {
    const cf = { colo: "" } as unknown as IncomingRequestCfProperties;

    const result = buildEdgeInfo(cf, 1);

    expect(result).toEqual({ local: true, edge: null, serverReceivedAt: 1 });
  });

  it("defaults `now` to the current time when omitted", () => {
    const before = Date.now();
    const result = buildEdgeInfo(undefined);
    const after = Date.now();
    expect(result.serverReceivedAt).toBeGreaterThanOrEqual(before);
    expect(result.serverReceivedAt).toBeLessThanOrEqual(after);
  });
});
