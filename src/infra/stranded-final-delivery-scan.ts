// Stranded final-delivery scan.
//
// On gateway startup, look for session entries whose `pendingFinalDelivery`
// marker was written but never cleared. This catches the failure mode where
// the agent runner produced a final assistant reply, stored the pending text
// (see `agent-runner.ts` near "buildPendingFinalDeliveryText"), but the
// outbound dispatch never confirmed (`dispatch-from-config.ts` would have
// otherwise called `clearPendingFinalDeliveryAfterSuccess`).
//
// Today the only recovery hook for these markers lives inside the heartbeat
// path (`get-reply.ts` checks `isHeartbeat`). Gateways without an aggressive
// heartbeat schedule can therefore sit on a stranded reply indefinitely. The
// scan logs a clearly-labeled warning so the operator can surface the failure
// (and a future patch can extend this to re-dispatch the queued text).
//
// Scope: log-only, read-only on the session store, no re-dispatch. Re-dispatch
// requires channel-plugin runtime that may not yet be live when this scan
// runs, and getting it wrong is much worse than logging a missed reply.

import { resolveDefaultAgentId } from "../agents/agent-scope.js";
import { resolveStorePath } from "../config/sessions/paths.js";
import { loadSessionStore } from "../config/sessions/store-load.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";

const DEFAULT_STRANDED_MIN_AGE_MS = 60_000;

export type StrandedFinalDeliveryScanLogger = {
  info: (msg: string) => void;
  warn: (msg: string) => void;
};

export type StrandedFinalDeliveryScanResult = {
  scanned: number;
  stranded: number;
};

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

/**
 * Scan the session store for stranded `pendingFinalDelivery` markers older
 * than `minAgeMs` (default 60s). Each match is logged at warn level. The scan
 * itself does not mutate the store or attempt re-dispatch; it is observability
 * only. Returns the count of entries scanned and the count flagged stranded.
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
}): Promise<StrandedFinalDeliveryScanResult> {
  const minAgeMs = params.minAgeMs ?? DEFAULT_STRANDED_MIN_AGE_MS;
  const now = params.nowMs ?? Date.now();
  const result: StrandedFinalDeliveryScanResult = { scanned: 0, stranded: 0 };

  try {
    const agentId = params.agentId ?? resolveDefaultAgentId(params.cfg);
    const storePath = resolveStorePath(params.cfg.session?.store, { agentId });
    const store = loadSessionStore(storePath, { skipCache: true });
    const entries = Object.entries(store);
    result.scanned = entries.length;

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
      const channel = describeChannel(entry);
      const textPreview = safeSlice(entry.pendingFinalDeliveryText, 80);
      const textLen =
        typeof entry.pendingFinalDeliveryText === "string"
          ? entry.pendingFinalDeliveryText.length
          : 0;
      params.log.warn(
        `[stranded-final-delivery] sessionKey=${sessionKey} channel=${channel} ` +
          `ageMs=${Number.isFinite(ageMs) ? Math.round(ageMs) : "unknown"} ` +
          `attempts=${attemptCount} textLen=${textLen} ` +
          (textPreview ? `preview="${textPreview}"` : "preview=<empty>"),
      );
    }

    if (result.stranded > 0) {
      params.log.warn(
        `[stranded-final-delivery] found ${result.stranded} stranded reply marker(s) ` +
          `on startup (of ${result.scanned} sessions scanned). Heartbeats or the next ` +
          `inbound on the affected session(s) will attempt recovery.`,
      );
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
