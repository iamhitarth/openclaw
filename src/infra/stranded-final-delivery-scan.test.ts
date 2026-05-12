import fsPromises from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { clearSessionStoreCacheForTest } from "../config/sessions/store.js";
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

describe("scanStrandedFinalDeliveries", () => {
  afterEach(() => {
    clearSessionStoreCacheForTest();
  });

  it("emits a warn line for each marker older than the threshold", async () => {
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

    try {
      const result = await scanStrandedFinalDeliveries({
        cfg: makeCfg(storePath),
        log,
        nowMs: now,
      });

      expect(result.scanned).toBe(3);
      expect(result.stranded).toBe(1);

      const warnLines = log.warn.mock.calls.map(([msg]) => msg);
      expect(warnLines).toEqual(
        expect.arrayContaining([
          expect.stringContaining("sessionKey=agent:main:stale"),
          expect.stringContaining("found 1 stranded"),
        ]),
      );
      expect(warnLines.some((line: string) => line.includes("agent:main:fresh"))).toBe(false);
      expect(log.info).not.toHaveBeenCalled();
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
      });

      expect(result.scanned).toBe(1);
      expect(result.stranded).toBe(0);
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
    });

    // An empty/missing store is treated as scanned=0, stranded=0, and we
    // still emit the all-clean summary so operators see the scan ran.
    expect(result.stranded).toBe(0);
  });
});
