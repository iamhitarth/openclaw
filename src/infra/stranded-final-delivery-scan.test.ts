import fsPromises from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { clearSessionStoreCacheForTest, loadSessionStore } from "../config/sessions/store.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { scanStrandedFinalDeliveries } from "./stranded-final-delivery-scan.js";

function makeLogger() {
  return {
    info: vi.fn(),
    warn: vi.fn(),
  };
}

async function makeTmpStore(
  initial: Record<string, unknown>,
): Promise<{ dir: string; storePath: string }> {
  const dir = await fsPromises.mkdtemp(path.join(os.tmpdir(), "openclaw-stranded-final-delivery-"));
  const storePath = path.join(dir, "sessions.json");
  await fsPromises.writeFile(storePath, JSON.stringify(initial, null, 2), "utf-8");
  return { dir, storePath };
}

function makeCfg(storePath: string): OpenClawConfig {
  return { session: { store: storePath } } as unknown as OpenClawConfig;
}

const alwaysReady = async () => true;
const noSleep = async (_ms: number) => undefined;

describe("scanStrandedFinalDeliveries", () => {
  afterEach(() => {
    clearSessionStoreCacheForTest();
  });

  it("emits a warn line and re-dispatches each stranded marker", async () => {
    const now = 1_700_000_000_000;
    const stale = now - 120_000;
    const fresh = now - 1_000;
    const { storePath, dir } = await makeTmpStore({
      "agent:main:stale": {
        sessionId: "s-stale",
        updatedAt: stale,
        pendingFinalDelivery: true,
        pendingFinalDeliveryCreatedAt: stale,
        pendingFinalDeliveryText: "hello — this reply never made it out the door",
        pendingFinalDeliveryAttemptCount: 2,
        lastChannel: "discord",
        lastTo: "1234567890",
      },
      "agent:main:fresh": {
        sessionId: "s-fresh",
        updatedAt: fresh,
        pendingFinalDelivery: true,
        pendingFinalDeliveryCreatedAt: fresh,
        pendingFinalDeliveryText: "still pending, give it a chance",
      },
      "agent:main:clean": {
        sessionId: "s-clean",
        updatedAt: now - 5_000,
      },
    });
    const log = makeLogger();
    const deliverOutboundPayloads = vi.fn().mockResolvedValue([]);

    try {
      const result = await scanStrandedFinalDeliveries({
        cfg: makeCfg(storePath),
        log,
        nowMs: now,
        deps: {
          deliverOutboundPayloads: deliverOutboundPayloads as never,
          checkChannelReady: alwaysReady,
          sleep: noSleep,
        },
      });

      expect(result.scanned).toBe(3);
      expect(result.stranded).toBe(1);
      expect(result.redispatched).toBe(1);
      expect(result.failed).toBe(0);
      expect(result.exhausted).toBe(0);
      expect(deliverOutboundPayloads).toHaveBeenCalledTimes(1);
      const sendArgs = deliverOutboundPayloads.mock.calls[0]?.[0] as {
        channel: string;
        to: string;
        payloads: { text?: string }[];
      };
      expect(sendArgs.channel).toBe("discord");
      expect(sendArgs.to).toBe("1234567890");
      expect(sendArgs.payloads[0]?.text).toContain("never made it out the door");

      const warnLines = log.warn.mock.calls.map(([msg]) => msg);
      expect(warnLines).toEqual(
        expect.arrayContaining([
          expect.stringContaining("sessionKey=agent:main:stale"),
          expect.stringContaining("found 1 stranded"),
        ]),
      );
      expect(warnLines.some((line: string) => line.includes("agent:main:fresh"))).toBe(false);

      // Success path emits an info-level "re-dispatched" line.
      const infoLines = log.info.mock.calls.map(([msg]) => msg);
      expect(
        infoLines.some(
          (line: string) =>
            line.includes("re-dispatched sessionKey=agent:main:stale") &&
            line.includes("via discord"),
        ),
      ).toBe(true);

      // Marker is cleared after successful re-dispatch.
      const reloaded = loadSessionStore(storePath, { skipCache: true });
      expect(reloaded["agent:main:stale"]?.pendingFinalDelivery).toBeUndefined();
      expect(reloaded["agent:main:stale"]?.pendingFinalDeliveryText).toBeUndefined();
      expect(reloaded["agent:main:stale"]?.pendingFinalDeliveryClaimedBy).toBeUndefined();
    } finally {
      await fsPromises.rm(dir, { recursive: true, force: true });
    }
  });

  it("logs the all-clean summary when no markers are stranded", async () => {
    const now = 1_700_000_000_000;
    const { storePath, dir } = await makeTmpStore({
      "agent:main:clean": {
        sessionId: "s-clean",
        updatedAt: now - 5_000,
      },
    });
    const log = makeLogger();

    try {
      const result = await scanStrandedFinalDeliveries({
        cfg: makeCfg(storePath),
        log,
        nowMs: now,
        deps: {
          deliverOutboundPayloads: vi.fn() as never,
          checkChannelReady: alwaysReady,
          sleep: noSleep,
        },
      });

      expect(result.scanned).toBe(1);
      expect(result.stranded).toBe(0);
      expect(result.redispatched).toBe(0);
      expect(log.warn).not.toHaveBeenCalled();
      expect(log.info).toHaveBeenCalledTimes(1);
      expect(log.info.mock.calls[0][0]).toContain("scan clean");
    } finally {
      await fsPromises.rm(dir, { recursive: true, force: true });
    }
  });

  it("does not throw when the store path cannot be read", async () => {
    const log = makeLogger();
    const cfg = makeCfg("/nonexistent/openclaw-stranded-scan/sessions.json");

    const result = await scanStrandedFinalDeliveries({
      cfg,
      log,
      nowMs: 1_700_000_000_000,
      deps: {
        deliverOutboundPayloads: vi.fn() as never,
        checkChannelReady: alwaysReady,
        sleep: noSleep,
      },
    });

    expect(result.stranded).toBe(0);
    expect(result.redispatched).toBe(0);
  });

  it("gives up and clears the marker when retry count exceeds the limit", async () => {
    const now = 1_700_000_000_000;
    const stale = now - 120_000;
    const { storePath, dir } = await makeTmpStore({
      "agent:main:dead": {
        sessionId: "s-dead",
        updatedAt: stale,
        pendingFinalDelivery: true,
        pendingFinalDeliveryCreatedAt: stale,
        pendingFinalDeliveryText: "kept failing for three startups in a row",
        pendingFinalDeliveryRetryCount: 3,
        lastChannel: "discord",
        lastTo: "9999",
      },
    });
    const log = makeLogger();
    const deliverOutboundPayloads = vi.fn().mockResolvedValue([]);

    try {
      const result = await scanStrandedFinalDeliveries({
        cfg: makeCfg(storePath),
        log,
        nowMs: now,
        deps: {
          deliverOutboundPayloads: deliverOutboundPayloads as never,
          checkChannelReady: alwaysReady,
          sleep: noSleep,
        },
      });

      expect(result.stranded).toBe(1);
      expect(result.exhausted).toBe(1);
      expect(result.redispatched).toBe(0);
      expect(deliverOutboundPayloads).not.toHaveBeenCalled();

      const warnLines = log.warn.mock.calls.map(([msg]) => msg);
      expect(warnLines.some((line: string) => line.includes("giving up"))).toBe(true);

      // Marker is cleared so future scans do not warn about it.
      const reloaded = loadSessionStore(storePath, { skipCache: true });
      expect(reloaded["agent:main:dead"]?.pendingFinalDelivery).toBeUndefined();
      expect(reloaded["agent:main:dead"]?.pendingFinalDeliveryText).toBeUndefined();
      expect(reloaded["agent:main:dead"]?.pendingFinalDeliveryRetryCount).toBeUndefined();
    } finally {
      await fsPromises.rm(dir, { recursive: true, force: true });
    }
  });

  it("skips entries already claimed by a concurrent heartbeat replay", async () => {
    const now = 1_700_000_000_000;
    const stale = now - 120_000;
    const claimedAt = now - 5_000;
    const { storePath, dir } = await makeTmpStore({
      "agent:main:claimed": {
        sessionId: "s-claimed",
        updatedAt: stale,
        pendingFinalDelivery: true,
        pendingFinalDeliveryCreatedAt: stale,
        pendingFinalDeliveryText: "heartbeat picked this up first",
        pendingFinalDeliveryClaimedBy: `heartbeat-${claimedAt}`,
        lastChannel: "discord",
        lastTo: "1234",
      },
    });
    const log = makeLogger();
    const deliverOutboundPayloads = vi.fn().mockResolvedValue([]);

    try {
      const result = await scanStrandedFinalDeliveries({
        cfg: makeCfg(storePath),
        log,
        nowMs: now,
        deps: {
          deliverOutboundPayloads: deliverOutboundPayloads as never,
          checkChannelReady: alwaysReady,
          sleep: noSleep,
        },
      });

      expect(result.stranded).toBe(1);
      expect(result.skipped).toBe(1);
      expect(result.redispatched).toBe(0);
      expect(deliverOutboundPayloads).not.toHaveBeenCalled();

      // The claim from the heartbeat path is preserved untouched.
      const reloaded = loadSessionStore(storePath, { skipCache: true });
      expect(reloaded["agent:main:claimed"]?.pendingFinalDelivery).toBe(true);
      expect(reloaded["agent:main:claimed"]?.pendingFinalDeliveryClaimedBy).toBe(
        `heartbeat-${claimedAt}`,
      );
    } finally {
      await fsPromises.rm(dir, { recursive: true, force: true });
    }
  });

  it("increments retry count and releases the claim on dispatch failure", async () => {
    const now = 1_700_000_000_000;
    const stale = now - 120_000;
    const { storePath, dir } = await makeTmpStore({
      "agent:main:fail": {
        sessionId: "s-fail",
        updatedAt: stale,
        pendingFinalDelivery: true,
        pendingFinalDeliveryCreatedAt: stale,
        pendingFinalDeliveryText: "discord rejected this with 403",
        lastChannel: "discord",
        lastTo: "1234",
      },
    });
    const log = makeLogger();
    const deliverOutboundPayloads = vi
      .fn()
      .mockRejectedValueOnce(new Error("403 Forbidden — channel removed"));

    try {
      const result = await scanStrandedFinalDeliveries({
        cfg: makeCfg(storePath),
        log,
        nowMs: now,
        deps: {
          deliverOutboundPayloads: deliverOutboundPayloads as never,
          checkChannelReady: alwaysReady,
          sleep: noSleep,
        },
      });

      expect(result.stranded).toBe(1);
      expect(result.failed).toBe(1);
      expect(result.redispatched).toBe(0);
      expect(deliverOutboundPayloads).toHaveBeenCalledTimes(1);

      const warnLines = log.warn.mock.calls.map(([msg]) => msg);
      expect(warnLines.some((line: string) => line.includes("re-dispatch FAILED"))).toBe(true);

      const reloaded = loadSessionStore(storePath, { skipCache: true });
      // Marker is still present — must not be double-cleared.
      expect(reloaded["agent:main:fail"]?.pendingFinalDelivery).toBe(true);
      expect(reloaded["agent:main:fail"]?.pendingFinalDeliveryText).toBe(
        "discord rejected this with 403",
      );
      // Retry count incremented exactly once.
      expect(reloaded["agent:main:fail"]?.pendingFinalDeliveryRetryCount).toBe(1);
      // Claim released so heartbeat can pick this up.
      expect(reloaded["agent:main:fail"]?.pendingFinalDeliveryClaimedBy).toBeUndefined();
      // Last error captured.
      expect(reloaded["agent:main:fail"]?.pendingFinalDeliveryLastError).toContain("403");
    } finally {
      await fsPromises.rm(dir, { recursive: true, force: true });
    }
  });

  it("defers re-dispatch when the channel is not ready", async () => {
    const now = 1_700_000_000_000;
    const stale = now - 120_000;
    const { storePath, dir } = await makeTmpStore({
      "agent:main:cold": {
        sessionId: "s-cold",
        updatedAt: stale,
        pendingFinalDelivery: true,
        pendingFinalDeliveryCreatedAt: stale,
        pendingFinalDeliveryText: "channel still connecting",
        lastChannel: "discord",
        lastTo: "1234",
      },
    });
    const log = makeLogger();
    const deliverOutboundPayloads = vi.fn().mockResolvedValue([]);
    const checkChannelReady = vi.fn().mockResolvedValue(false);

    try {
      const result = await scanStrandedFinalDeliveries({
        cfg: makeCfg(storePath),
        log,
        nowMs: now,
        deps: {
          deliverOutboundPayloads: deliverOutboundPayloads as never,
          checkChannelReady,
          sleep: noSleep,
        },
      });

      expect(result.stranded).toBe(1);
      expect(result.skipped).toBe(1);
      expect(result.redispatched).toBe(0);
      expect(deliverOutboundPayloads).not.toHaveBeenCalled();
      expect(checkChannelReady).toHaveBeenCalledTimes(2);

      const reloaded = loadSessionStore(storePath, { skipCache: true });
      // Marker remains intact (retry count untouched) so heartbeat retries.
      expect(reloaded["agent:main:cold"]?.pendingFinalDelivery).toBe(true);
      expect(reloaded["agent:main:cold"]?.pendingFinalDeliveryRetryCount).toBeUndefined();
      expect(reloaded["agent:main:cold"]?.pendingFinalDeliveryClaimedBy).toBeUndefined();
    } finally {
      await fsPromises.rm(dir, { recursive: true, force: true });
    }
  });
});
