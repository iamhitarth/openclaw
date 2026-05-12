// Stranded final-delivery scan + re-dispatch.
//
// On gateway startup, look for session entries whose `pendingFinalDelivery`
// marker was written but never cleared. This catches the failure mode where
// the agent runner produced a final assistant reply, stored the pending text
// (see `agent-runner.ts` near "buildPendingFinalDeliveryText"), but the
// outbound dispatch never confirmed (`dispatch-from-config.ts` would have
// otherwise called `clearPendingFinalDeliveryAfterSuccess`).
//
// Until this patch the only recovery hook for these markers lived inside the
// heartbeat path (`get-reply.ts` checks `isHeartbeat`). Gateways without an
// aggressive heartbeat schedule could therefore sit on a stranded reply
// indefinitely. The scan now (a) logs each stranded marker, then (b) attempts
// to re-deliver the stored payload via `deliverOutboundPayloads`, mirroring
// what the heartbeat replay path eventually does.
//
// Coordination with the heartbeat path is cooperative via the
// `pendingFinalDeliveryClaimedBy` session-entry field. Either path may take
// the claim; the other treats a fresh claim (<30s) as in-flight and skips.
//
// See coding-agent-logs/2026-04-19_richa-silent-replies-rca.md for the RCA
// that motivated this scan.

import { resolveDefaultAgentId } from "../agents/agent-scope.js";
import { getChannelPlugin } from "../channels/plugins/index.js";
import { resolveStorePath } from "../config/sessions/paths.js";
import { loadSessionStore } from "../config/sessions/store-load.js";
import { updateSessionStoreEntry } from "../config/sessions/store.js";
import type { SessionEntry } from "../config/sessions/types.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { getActivePluginChannelRegistry } from "../plugins/runtime.js";
import { deliveryContextFromSession } from "../utils/delivery-context.shared.js";
import { deliverOutboundPayloads } from "./outbound/deliver.js";
import type { OutboundChannel } from "./outbound/targets.js";

const DEFAULT_STRANDED_MIN_AGE_MS = 60_000;
const DEFAULT_MAX_RETRIES = 3;
const CLAIM_STALE_MS = 30_000;
const CLAIM_OWNER = "startup-scan";
const CHANNEL_READY_RETRY_DELAY_MS = 1_500;
const CHANNEL_READY_RETRY_ATTEMPTS = 2;

export type StrandedFinalDeliveryScanLogger = {
  info: (msg: string) => void;
  warn: (msg: string) => void;
};

export type StrandedFinalDeliveryScanResult = {
  scanned: number;
  stranded: number;
  redispatched: number;
  failed: number;
  exhausted: number;
  skipped: number;
};

type StrandedFinalDeliveryDeps = {
  /**
   * Inject the channel re-dispatch for tests. Production callers should leave
   * this undefined and let the scan use `deliverOutboundPayloads`.
   */
  deliverOutboundPayloads?: typeof deliverOutboundPayloads;
  /**
   * Inject a channel readiness probe for tests. Returns true if the channel
   * is ready to send right now; the scan retries once after a short delay
   * before giving up.
   */
  checkChannelReady?: (params: {
    cfg: OpenClawConfig;
    channel: Exclude<OutboundChannel, "none">;
    accountId?: string;
  }) => Promise<boolean>;
  /** Sleep override for tests. */
  sleep?: (ms: number) => Promise<void>;
};

const defaultSleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

async function defaultCheckChannelReady(params: {
  cfg: OpenClawConfig;
  channel: Exclude<OutboundChannel, "none">;
  accountId?: string;
}): Promise<boolean> {
  // Mirrors heartbeat's channel-readiness probe (see `heartbeat-runner.ts`).
  // If the plugin does not expose a heartbeat.checkReady hook we optimistically
  // assume ready — the worst case is the underlying send fails and we burn one
  // retry slot, which is the documented behaviour.
  const activePlugin = getActivePluginChannelRegistry()?.channels.find(
    (entry) => entry.plugin.id === params.channel,
  )?.plugin;
  const plugin = activePlugin ?? getChannelPlugin(params.channel);
  if (!plugin?.heartbeat?.checkReady) {
    return true;
  }
  try {
    const readiness = await plugin.heartbeat.checkReady({
      cfg: params.cfg,
      accountId: params.accountId ?? null,
    });
    return readiness.ok;
  } catch {
    // A throw from checkReady is best-effort treated as "not ready"; the
    // retry loop will give the channel another second to come online.
    return false;
  }
}

function safeSlice(value: unknown, max: number): string {
  if (typeof value !== "string") {
    return "";
  }
  const trimmed = value.trim();
  return trimmed.length <= max ? trimmed : `${trimmed.slice(0, max)}…`;
}

function describeChannel(entry: {
  lastChannel?: string;
  channel?: string;
  deliveryContext?: { channel?: string };
}): string {
  return entry.deliveryContext?.channel ?? entry.lastChannel ?? entry.channel ?? "unknown";
}

function isClaimFresh(claim: string | null | undefined, nowMs: number): boolean {
  if (typeof claim !== "string" || claim.length === 0) {
    return false;
  }
  const match = claim.match(/-(\d+)$/);
  if (!match) {
    // Unknown format — treat as fresh so we don't race with whoever wrote it.
    return true;
  }
  const claimedAt = Number(match[1]);
  if (!Number.isFinite(claimedAt)) {
    return true;
  }
  return nowMs - claimedAt < CLAIM_STALE_MS;
}

/**
 * Atomically take the re-dispatch claim. Returns the claim string on success
 * (and the caller must release/clear it before returning), or `null` if the
 * marker no longer applies or another claimant already owns it.
 */
async function tryClaimMarker(params: {
  storePath: string;
  sessionKey: string;
  nowMs: number;
  expectedText: string;
}): Promise<string | null> {
  const claim = `${CLAIM_OWNER}-${params.nowMs}`;
  const updated = await updateSessionStoreEntry({
    storePath: params.storePath,
    sessionKey: params.sessionKey,
    update: async (entry) => {
      if (
        entry.pendingFinalDelivery !== true ||
        typeof entry.pendingFinalDeliveryText !== "string" ||
        entry.pendingFinalDeliveryText.length === 0
      ) {
        return null;
      }
      if (entry.pendingFinalDeliveryText !== params.expectedText) {
        // Text changed under us — another path already re-dispatched and a new
        // marker was written. Skip rather than send stale content.
        return null;
      }
      if (isClaimFresh(entry.pendingFinalDeliveryClaimedBy, params.nowMs)) {
        return null;
      }
      return {
        pendingFinalDeliveryClaimedBy: claim,
        updatedAt: params.nowMs,
      };
    },
  });
  if (!updated || updated.pendingFinalDeliveryClaimedBy !== claim) {
    return null;
  }
  return claim;
}

async function releaseClaim(params: {
  storePath: string;
  sessionKey: string;
  claim: string;
  patch?: Partial<SessionEntry>;
}): Promise<void> {
  await updateSessionStoreEntry({
    storePath: params.storePath,
    sessionKey: params.sessionKey,
    update: async (entry) => {
      if (entry.pendingFinalDeliveryClaimedBy !== params.claim) {
        // Someone else now owns it (heartbeat completed and rewrote, or marker
        // cleared by clearPendingFinalDeliveryAfterSuccess). Do not stomp.
        return null;
      }
      return {
        pendingFinalDeliveryClaimedBy: undefined,
        updatedAt: Date.now(),
        ...(params.patch ?? {}),
      };
    },
  }).catch(() => {
    // Best-effort cleanup; the claim will go stale after 30s either way.
  });
}

async function clearMarker(params: {
  storePath: string;
  sessionKey: string;
  claim: string;
}): Promise<void> {
  await updateSessionStoreEntry({
    storePath: params.storePath,
    sessionKey: params.sessionKey,
    update: async (entry) => {
      // Only the owner of the current claim is allowed to clear so a heartbeat
      // that succeeded under us (and therefore already cleared + rewrote the
      // marker for a new turn) is not flattened.
      if (entry.pendingFinalDeliveryClaimedBy !== params.claim) {
        return null;
      }
      return {
        pendingFinalDelivery: undefined,
        pendingFinalDeliveryText: undefined,
        pendingFinalDeliveryCreatedAt: undefined,
        pendingFinalDeliveryLastAttemptAt: undefined,
        pendingFinalDeliveryAttemptCount: undefined,
        pendingFinalDeliveryLastError: undefined,
        pendingFinalDeliveryContext: undefined,
        pendingFinalDeliveryClaimedBy: undefined,
        pendingFinalDeliveryRetryCount: undefined,
        updatedAt: Date.now(),
      };
    },
  }).catch(() => {
    // Best-effort; if the write fails, the next scan still has the claim guard
    // to avoid double-sending (claim stays fresh for 30s).
  });
}

function resolveTarget(entry: SessionEntry):
  | {
      channel: Exclude<OutboundChannel, "none">;
      to: string;
      accountId?: string;
      threadId?: string | number;
    }
  | undefined {
  const pendingCtx = entry.pendingFinalDeliveryContext;
  const sessionCtx = deliveryContextFromSession(entry);
  const channelRaw =
    pendingCtx?.channel ?? sessionCtx?.channel ?? entry.lastChannel ?? entry.channel ?? null;
  const to = pendingCtx?.to ?? sessionCtx?.to ?? entry.lastTo;
  if (!channelRaw || channelRaw === "none" || !to) {
    return undefined;
  }
  return {
    channel: channelRaw as Exclude<OutboundChannel, "none">,
    to,
    accountId: pendingCtx?.accountId ?? sessionCtx?.accountId ?? entry.lastAccountId,
    threadId: pendingCtx?.threadId ?? sessionCtx?.threadId ?? entry.lastThreadId,
  };
}

async function redispatchOne(params: {
  cfg: OpenClawConfig;
  sessionKey: string;
  entry: SessionEntry;
  storePath: string;
  log: StrandedFinalDeliveryScanLogger;
  ageMs: number;
  nowMs: number;
  maxRetries: number;
  deps?: StrandedFinalDeliveryDeps;
}): Promise<"redispatched" | "failed" | "exhausted" | "skipped"> {
  const text = params.entry.pendingFinalDeliveryText;
  if (typeof text !== "string" || text.length === 0) {
    return "skipped";
  }
  const retryCount = params.entry.pendingFinalDeliveryRetryCount ?? 0;
  if (retryCount >= params.maxRetries) {
    // Hard give up: clear the marker so it stops polluting the scan. We still
    // take a claim so we don't race a fresh heartbeat replay attempt.
    const claim = await tryClaimMarker({
      storePath: params.storePath,
      sessionKey: params.sessionKey,
      nowMs: params.nowMs,
      expectedText: text,
    });
    if (!claim) {
      return "skipped";
    }
    params.log.warn(
      `[stranded-final-delivery] giving up sessionKey=${params.sessionKey} ` +
        `retryCount=${retryCount} maxRetries=${params.maxRetries} — clearing marker`,
    );
    await clearMarker({
      storePath: params.storePath,
      sessionKey: params.sessionKey,
      claim,
    });
    return "exhausted";
  }

  const target = resolveTarget(params.entry);
  if (!target) {
    params.log.warn(
      `[stranded-final-delivery] cannot re-dispatch sessionKey=${params.sessionKey}: ` +
        "no usable channel/to in session entry; leaving marker for heartbeat",
    );
    return "skipped";
  }

  // Channel readiness probe with a brief retry. Channels may still be
  // connecting when the scan fires (sidecars are up but Discord/WA gateway
  // sockets are mid-handshake), so probe once, wait briefly, and probe again
  // before giving up. If we still are not ready we leave the marker for the
  // heartbeat path to retry — counts as a skip, not a retry burn.
  const checkReady = params.deps?.checkChannelReady ?? defaultCheckChannelReady;
  const sleep = params.deps?.sleep ?? defaultSleep;
  let ready = false;
  for (let attempt = 0; attempt < CHANNEL_READY_RETRY_ATTEMPTS; attempt += 1) {
    ready = await checkReady({
      cfg: params.cfg,
      channel: target.channel,
      accountId: target.accountId,
    });
    if (ready) {
      break;
    }
    if (attempt < CHANNEL_READY_RETRY_ATTEMPTS - 1) {
      params.log.warn(
        `[stranded-final-delivery] channel ${target.channel} not ready for ` +
          `sessionKey=${params.sessionKey}; waiting ${CHANNEL_READY_RETRY_DELAY_MS}ms before retry`,
      );
      await sleep(CHANNEL_READY_RETRY_DELAY_MS);
    }
  }
  if (!ready) {
    params.log.warn(
      `[stranded-final-delivery] channel ${target.channel} still not ready for ` +
        `sessionKey=${params.sessionKey}; deferring re-dispatch to heartbeat`,
    );
    return "skipped";
  }

  const claim = await tryClaimMarker({
    storePath: params.storePath,
    sessionKey: params.sessionKey,
    nowMs: params.nowMs,
    expectedText: text,
  });
  if (!claim) {
    // Either the heartbeat is already replaying this marker (fresh claim) or
    // the marker was cleared between scan-load and claim. Either way, leave
    // it alone.
    return "skipped";
  }

  const send = params.deps?.deliverOutboundPayloads ?? deliverOutboundPayloads;
  try {
    await send({
      cfg: params.cfg,
      channel: target.channel,
      to: target.to,
      accountId: target.accountId,
      threadId: target.threadId,
      payloads: [{ text }],
      session: { key: params.sessionKey },
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const nextRetry = retryCount + 1;
    params.log.warn(
      `[stranded-final-delivery] re-dispatch FAILED sessionKey=${params.sessionKey} ` +
        `channel=${target.channel} retry=${nextRetry}/${params.maxRetries} error=${message}`,
    );
    await releaseClaim({
      storePath: params.storePath,
      sessionKey: params.sessionKey,
      claim,
      patch: {
        pendingFinalDeliveryRetryCount: nextRetry,
        pendingFinalDeliveryLastError: message,
        pendingFinalDeliveryLastAttemptAt: Date.now(),
      },
    });
    return "failed";
  }

  // Success path: clear the marker (guarded by claim so we don't stomp a
  // freshly-written marker for a new turn).
  await clearMarker({
    storePath: params.storePath,
    sessionKey: params.sessionKey,
    claim,
  });
  params.log.info(
    `[stranded-final-delivery] re-dispatched sessionKey=${params.sessionKey} ` +
      `via ${target.channel} after ${Math.round(params.ageMs)}ms`,
  );
  return "redispatched";
}

/**
 * Scan the session store for stranded `pendingFinalDelivery` markers older
 * than `minAgeMs` (default 60s). For each match: log a warn line, then attempt
 * a single re-dispatch via the same outbound path a real reply uses. Markers
 * that have already been retried `maxRetries` times are dropped with a hard
 * warn.
 *
 * Safe to call on gateway startup. Failures during the scan are caught and
 * logged at warn — they must not block startup.
 */
export async function scanStrandedFinalDeliveries(params: {
  cfg: OpenClawConfig;
  log: StrandedFinalDeliveryScanLogger;
  agentId?: string;
  nowMs?: number;
  minAgeMs?: number;
  maxRetries?: number;
  deps?: StrandedFinalDeliveryDeps;
}): Promise<StrandedFinalDeliveryScanResult> {
  const minAgeMs = params.minAgeMs ?? DEFAULT_STRANDED_MIN_AGE_MS;
  const maxRetries = params.maxRetries ?? DEFAULT_MAX_RETRIES;
  const now = params.nowMs ?? Date.now();
  const result: StrandedFinalDeliveryScanResult = {
    scanned: 0,
    stranded: 0,
    redispatched: 0,
    failed: 0,
    exhausted: 0,
    skipped: 0,
  };

  try {
    const agentId = params.agentId ?? resolveDefaultAgentId(params.cfg);
    const storePath = resolveStorePath(params.cfg.session?.store, { agentId });
    const store = loadSessionStore(storePath, { skipCache: true });
    const entries = Object.entries(store);
    result.scanned = entries.length;

    const stranded: { sessionKey: string; entry: SessionEntry; ageMs: number }[] = [];
    for (const [sessionKey, entry] of entries) {
      if (entry?.pendingFinalDelivery !== true) {
        continue;
      }
      const createdAt = entry.pendingFinalDeliveryCreatedAt ?? entry.updatedAt;
      const ageMs =
        typeof createdAt === "number" && Number.isFinite(createdAt)
          ? Math.max(0, now - createdAt)
          : Number.POSITIVE_INFINITY;
      if (ageMs < minAgeMs) {
        continue;
      }
      result.stranded += 1;
      const attemptCount = entry.pendingFinalDeliveryAttemptCount ?? 0;
      const retryCount = entry.pendingFinalDeliveryRetryCount ?? 0;
      const channel = describeChannel(entry);
      const textPreview = safeSlice(entry.pendingFinalDeliveryText, 80);
      const textLen =
        typeof entry.pendingFinalDeliveryText === "string"
          ? entry.pendingFinalDeliveryText.length
          : 0;
      params.log.warn(
        `[stranded-final-delivery] sessionKey=${sessionKey} channel=${channel} ` +
          `ageMs=${Number.isFinite(ageMs) ? Math.round(ageMs) : "unknown"} ` +
          `attempts=${attemptCount} retries=${retryCount} textLen=${textLen} ` +
          (textPreview ? `preview="${textPreview}"` : "preview=<empty>"),
      );
      stranded.push({ sessionKey, entry, ageMs });
    }

    if (result.stranded > 0) {
      params.log.warn(
        `[stranded-final-delivery] found ${result.stranded} stranded reply marker(s) ` +
          `on startup (of ${result.scanned} sessions scanned); attempting re-dispatch.`,
      );
      for (const item of stranded) {
        const outcome = await redispatchOne({
          cfg: params.cfg,
          sessionKey: item.sessionKey,
          entry: item.entry,
          storePath,
          log: params.log,
          ageMs: item.ageMs,
          nowMs: now,
          maxRetries,
          deps: params.deps,
        });
        result[outcome] += 1;
      }
    } else {
      params.log.info(
        `[stranded-final-delivery] scan clean (${result.scanned} session(s) checked)`,
      );
    }
  } catch (err) {
    params.log.warn(
      `[stranded-final-delivery] scan failed (non-fatal): ${
        err instanceof Error ? err.message : String(err)
      }`,
    );
  }

  return result;
}
