import { resolveAccountEntry } from "openclaw/plugin-sdk/account-core";
import { resolveInboundDebounceMs } from "openclaw/plugin-sdk/channel-inbound-debounce";
import { formatCliCommand } from "openclaw/plugin-sdk/cli-runtime";
import { hasControlCommand } from "openclaw/plugin-sdk/command-detection";
import { drainPendingDeliveries } from "openclaw/plugin-sdk/delivery-queue-runtime";
import { DEFAULT_GROUP_HISTORY_LIMIT } from "openclaw/plugin-sdk/reply-history";
import { buildGroupHistoryKey, resolveAgentRoute } from "openclaw/plugin-sdk/routing";
import { logVerbose } from "openclaw/plugin-sdk/runtime-env";
import { registerUnhandledRejectionHandler } from "openclaw/plugin-sdk/runtime-env";
import { getChildLogger } from "openclaw/plugin-sdk/runtime-env";
import {
  defaultRuntime,
  formatDurationPrecise,
  type RuntimeEnv,
} from "openclaw/plugin-sdk/runtime-env";
import { enqueueSystemEvent } from "openclaw/plugin-sdk/system-event-runtime";
import { resolveWhatsAppAccount, resolveWhatsAppMediaMaxBytes } from "../accounts.js";
import { WHATSAPP_AUTH_UNSTABLE_CODE, WhatsAppAuthUnstableError } from "../auth-store.js";
import {
  WhatsAppConnectionController,
  type ManagedWhatsAppListener,
} from "../connection-controller.js";
import { attachWebInboxToSocket, type WhatsAppGroupMetadataCache } from "../inbound/monitor.js";
import {
  newConnectionId,
  resolveHeartbeatSeconds,
  resolveReconnectPolicy,
  sleepWithAbort,
} from "../reconnect.js";
import { formatError, getWebAuthAgeMs, logoutWeb, readWebSelfId } from "../session.js";
import { resolveWhatsAppSocketTiming } from "../socket-timing.js";
import { getRuntimeConfig, getRuntimeConfigSourceSnapshot } from "./config.runtime.js";
import { whatsappHeartbeatLog, whatsappLog } from "./loggers.js";
import { buildMentionConfig } from "./mentions.js";
import { createWebChannelStatusController } from "./monitor-state.js";
import { createEchoTracker } from "./monitor/echo.js";
import { createWebOnMessageHandler } from "./monitor/on-message.js";
import type { WebInboundMsg, WebMonitorTuning } from "./types.js";
import { isLikelyWhatsAppCryptoError } from "./util.js";

function isNonRetryableWebCloseStatus(statusCode: unknown): boolean {
  // WhatsApp 440 = session conflict ("Unknown Stream Errored (conflict)").
  // This is persistent until the operator resolves the conflicting session.
  return statusCode === 440;
}

type ReplyResolver = typeof import("./reply-resolver.runtime.js").getReplyFromConfig;

let replyResolverRuntimePromise: Promise<typeof import("./reply-resolver.runtime.js")> | null =
  null;

function loadReplyResolverRuntime() {
  replyResolverRuntimePromise ??= import("./reply-resolver.runtime.js");
  return replyResolverRuntimePromise;
}

function normalizeReconnectAccountId(accountId?: string | null): string {
  return (accountId ?? "").trim() || "default";
}

type SeededEntry = {
  sender: string;
  body: string;
  timestamp?: number;
  id?: string;
  senderJid?: string;
};

// Patch P: seed groupHistories from wacli's local store on listener startup,
// so restarts don't wipe the agent's group context. Best-effort and non-blocking;
// soft-fails if wacli is missing or its session differs from OC's.
async function seedGroupHistoriesFromWacli(params: {
  groupHistories: Map<string, SeededEntry[]>;
  groupHistoryLimit: number;
  accountId: string | undefined;
}): Promise<void> {
  const { execFile } = await import("node:child_process");
  const { promisify } = await import("node:util");
  const execFileP = promisify(execFile);

  const run = async (args: string[], timeoutMs: number): Promise<string | null> => {
    try {
      const { stdout } = await execFileP("wacli", args, {
        timeout: timeoutMs,
        maxBuffer: 16 * 1024 * 1024,
      });
      return stdout;
    } catch {
      return null;
    }
  };

  const chatsJson = await run(["chats", "list", "--json"], 10_000);
  if (!chatsJson) {
    logVerbose("wacli warmup: wacli not available or chats list failed; skipping");
    return;
  }
  let chatsParsed: { success?: boolean; data?: Array<{ JID?: string; Kind?: string }> };
  try {
    chatsParsed = JSON.parse(chatsJson);
  } catch {
    return;
  }
  if (!chatsParsed?.success || !Array.isArray(chatsParsed.data)) {
    return;
  }
  const groups = chatsParsed.data.filter(
    (c): c is { JID: string; Kind: string } =>
      c?.Kind === "group" && typeof c?.JID === "string" && c.JID.length > 0,
  );

  const nameCache = new Map<string, string>();
  const resolveName = async (jid: string): Promise<string> => {
    const cached = nameCache.get(jid);
    if (cached !== undefined) return cached;
    const out = await run(["contacts", "show", "--jid", jid, "--json"], 3_000);
    let name = "";
    if (out) {
      try {
        const parsed = JSON.parse(out) as { data?: { Name?: string; Alias?: string } };
        name = parsed?.data?.Alias || parsed?.data?.Name || "";
      } catch {
        /* ignore */
      }
    }
    if (!name) {
      const local = jid.split("@")[0] ?? "";
      name = local.split(":")[0] || jid;
    }
    nameCache.set(jid, name);
    return name;
  };

  let seededMessages = 0;
  let seededGroups = 0;
  for (const group of groups) {
    const msgsOut = await run(
      [
        "messages",
        "list",
        "--chat",
        group.JID,
        "--limit",
        String(params.groupHistoryLimit),
        "--json",
      ],
      10_000,
    );
    if (!msgsOut) continue;
    let msgsParsed: {
      data?: {
        messages?: Array<{
          MsgID?: string;
          SenderJID?: string;
          Timestamp?: string;
          Text?: string;
          DisplayText?: string;
        }>;
      };
    };
    try {
      msgsParsed = JSON.parse(msgsOut);
    } catch {
      continue;
    }
    const msgs = msgsParsed?.data?.messages;
    if (!Array.isArray(msgs) || msgs.length === 0) continue;

    const key = buildGroupHistoryKey({
      channel: "whatsapp",
      accountId: params.accountId ?? null,
      peerKind: "group",
      peerId: group.JID,
    });
    const existing = params.groupHistories.get(key) ?? [];
    const existingIds = new Set(
      existing.map((e) => e.id).filter((id): id is string => Boolean(id)),
    );

    // wacli returns newest-first; reverse to oldest-first for chronological history.
    const seeded: SeededEntry[] = [];
    for (let i = msgs.length - 1; i >= 0; i--) {
      const m = msgs[i];
      const body = String(m.DisplayText || m.Text || "").trim();
      if (!body) continue;
      if (m.MsgID && existingIds.has(m.MsgID)) continue;
      const senderJid = typeof m.SenderJID === "string" ? m.SenderJID : "";
      const sender = senderJid ? await resolveName(senderJid) : "Unknown";
      const tsParsed = m.Timestamp ? Date.parse(m.Timestamp) : Number.NaN;
      seeded.push({
        sender,
        body,
        timestamp: Number.isFinite(tsParsed) ? tsParsed : undefined,
        id: m.MsgID || undefined,
        senderJid: senderJid || undefined,
      });
    }

    if (seeded.length === 0) continue;
    // Preserve any live entries that may have arrived concurrently; seed goes first
    // so live entries keep their position at the end (most recent).
    const merged = [...seeded, ...existing].slice(-params.groupHistoryLimit);
    params.groupHistories.set(key, merged);
    seededMessages += seeded.length;
    seededGroups += 1;
  }

  logVerbose(
    `wacli warmup: seeded ${seededMessages} messages across ${seededGroups}/${groups.length} groups`,
  );
}

function isNoListenerReconnectError(lastError?: string): boolean {
  return typeof lastError === "string" && /No active WhatsApp Web listener/i.test(lastError);
}

function resolveExplicitWhatsAppDebounceOverride(params: {
  cfg: ReturnType<typeof getRuntimeConfig>;
  sourceCfg?: ReturnType<typeof getRuntimeConfig> | null;
  accountId: string;
}): number | undefined {
  const channel = params.sourceCfg?.channels?.whatsapp;
  if (!channel) {
    return undefined;
  }

  const accountId = normalizeReconnectAccountId(params.accountId);
  const accountDebounce = resolveAccountEntry(channel.accounts, accountId)?.debounceMs;
  if (accountDebounce !== undefined) {
    return accountDebounce;
  }
  if (accountId !== "default") {
    const defaultAccountDebounce = resolveAccountEntry(channel.accounts, "default")?.debounceMs;
    if (defaultAccountDebounce !== undefined) {
      return defaultAccountDebounce;
    }
  }

  return channel.debounceMs;
}

function isRetryableAuthUnstableError(error: unknown): error is WhatsAppAuthUnstableError {
  return (
    error instanceof WhatsAppAuthUnstableError ||
    (typeof error === "object" &&
      error !== null &&
      "code" in error &&
      (error as { code?: unknown }).code === WHATSAPP_AUTH_UNSTABLE_CODE)
  );
}

async function clearTerminalWebAuthState(params: {
  account: ReturnType<typeof resolveWhatsAppAccount>;
  runtime: RuntimeEnv;
  statusLabel: number | "unknown";
  healthState: "logged-out" | "conflict";
  log: ReturnType<typeof getChildLogger>;
}) {
  try {
    const cleared = await logoutWeb({
      authDir: params.account.authDir,
      isLegacyAuthDir: params.account.isLegacyAuthDir,
      runtime: params.runtime,
    });
    params.log.warn(
      {
        accountId: params.account.accountId,
        cleared,
        healthState: params.healthState,
        status: params.statusLabel,
      },
      "web reconnect: cleared cached auth after terminal close",
    );
  } catch (error) {
    params.log.warn(
      {
        accountId: params.account.accountId,
        error: formatError(error),
        healthState: params.healthState,
        status: params.statusLabel,
      },
      "web reconnect: failed clearing cached auth after terminal close",
    );
    params.runtime.error(
      `WhatsApp Web cleanup failed after terminal close (status ${params.statusLabel}). Run \`${formatCliCommand("openclaw channels logout --channel whatsapp")}\`, then relink with \`${formatCliCommand("openclaw channels login --channel whatsapp")}\`.`,
    );
  }
}
const DEFAULT_TRANSPORT_TIMEOUT_MS = 5 * 60 * 1000;

export async function monitorWebChannel(
  verbose: boolean,
  listenerFactory: typeof attachWebInboxToSocket | undefined = attachWebInboxToSocket,
  keepAlive = true,
  replyResolver?: ReplyResolver,
  runtime: RuntimeEnv = defaultRuntime,
  abortSignal?: AbortSignal,
  tuning: WebMonitorTuning = {},
) {
  const activeReplyResolver =
    replyResolver ?? (await loadReplyResolverRuntime()).getReplyFromConfig;
  const runId = newConnectionId();
  const replyLogger = getChildLogger({ module: "web-auto-reply", runId });
  const heartbeatLogger = getChildLogger({ module: "web-heartbeat", runId });
  const reconnectLogger = getChildLogger({ module: "web-reconnect", runId });
  const statusController = createWebChannelStatusController(tuning.statusSink);
  statusController.emit();

  const baseCfg = getRuntimeConfig();
  const sourceCfg = getRuntimeConfigSourceSnapshot();
  const account = resolveWhatsAppAccount({
    cfg: baseCfg,
    accountId: tuning.accountId,
  });
  const cfg = {
    ...baseCfg,
    channels: {
      ...baseCfg.channels,
      whatsapp: {
        ...baseCfg.channels?.whatsapp,
        ackReaction: account.ackReaction,
        messagePrefix: account.messagePrefix,
        allowFrom: account.allowFrom,
        groupAllowFrom: account.groupAllowFrom,
        groupPolicy: account.groupPolicy,
        textChunkLimit: account.textChunkLimit,
        chunkMode: account.chunkMode,
        mediaMaxMb: account.mediaMaxMb,
        blockStreaming: account.blockStreaming,
        groups: account.groups,
      },
    },
  } satisfies ReturnType<typeof getRuntimeConfig>;

  const maxMediaBytes = resolveWhatsAppMediaMaxBytes(account);
  const heartbeatSeconds = resolveHeartbeatSeconds(cfg, tuning.heartbeatSeconds);
  const reconnectPolicy = resolveReconnectPolicy(cfg, tuning.reconnect);
  const socketTiming = resolveWhatsAppSocketTiming(cfg, tuning.socketTiming);
  const baseMentionConfig = buildMentionConfig(cfg);
  const groupHistoryLimit =
    account.historyLimit ??
    cfg.channels?.whatsapp?.historyLimit ??
    cfg.messages?.groupChat?.historyLimit ??
    DEFAULT_GROUP_HISTORY_LIMIT;
  const groupHistories = new Map<
    string,
    Array<{
      sender: string;
      body: string;
      timestamp?: number;
      id?: string;
      senderJid?: string;
    }>
  >();
  const groupMemberNames = new Map<string, Map<string, string>>();
  const groupMetadataCache: WhatsAppGroupMetadataCache = new Map();
  const echoTracker = createEchoTracker({ maxItems: 100, logVerbose });

  // Patch P: warm in-memory groupHistories from wacli's local store so the agent
  // doesn't lose group context across restarts. Fire-and-forget; soft-fails.
  void seedGroupHistoriesFromWacli({
    groupHistories,
    groupHistoryLimit,
    accountId: account.accountId,
  }).catch((err) => {
    logVerbose(`wacli warmup failed: ${String(err)}`);
  });

  const sleep =
    tuning.sleep ??
    ((ms: number, signal?: AbortSignal) => sleepWithAbort(ms, signal ?? abortSignal));
  const stopRequested = () => abortSignal?.aborted === true;

  // Avoid noisy MaxListenersExceeded warnings in test environments where
  // multiple gateway instances may be constructed.
  const currentMaxListeners = process.getMaxListeners?.() ?? 10;
  if (process.setMaxListeners && currentMaxListeners < 50) {
    process.setMaxListeners(50);
  }

  let sigintStop = false;
  const handleSigint = () => {
    sigintStop = true;
  };
  process.once("SIGINT", handleSigint);

  const transportTimeoutMs = tuning.transportTimeoutMs ?? DEFAULT_TRANSPORT_TIMEOUT_MS;
  const messageTimeoutMs = tuning.messageTimeoutMs ?? 3 * 60 * 60 * 1000;
  const watchdogCheckMs = tuning.watchdogCheckMs ?? 60 * 1000;
  const controller = new WhatsAppConnectionController({
    accountId: account.accountId,
    authDir: account.authDir,
    verbose,
    keepAlive,
    heartbeatSeconds,
    transportTimeoutMs,
    messageTimeoutMs,
    watchdogCheckMs,
    reconnectPolicy,
    socketTiming,
    abortSignal,
    sleep,
    isNonRetryableStatus: isNonRetryableWebCloseStatus,
  });

  try {
    while (true) {
      if (stopRequested()) {
        break;
      }

      const connectionId = newConnectionId();
      const inboundDebounceMs = resolveInboundDebounceMs({
        cfg,
        channel: "whatsapp",
        overrideMs: resolveExplicitWhatsAppDebounceOverride({
          cfg,
          sourceCfg,
          accountId: account.accountId,
        }),
      });
      const shouldDebounce = (msg: WebInboundMsg) => {
        if (msg.mediaPath || msg.mediaType) {
          return false;
        }
        if (msg.location) {
          return false;
        }
        if (msg.replyToId || msg.replyToBody) {
          return false;
        }
        return !hasControlCommand(msg.body, cfg);
      };

      let connection;
      try {
        connection = await controller.openConnection({
          connectionId,
          createListener: async ({ sock, connection }) => {
            const onMessage = createWebOnMessageHandler({
              cfg,
              verbose,
              connectionId,
              maxMediaBytes,
              groupHistoryLimit,
              groupHistories,
              groupMemberNames,
              echoTracker,
              backgroundTasks: connection.backgroundTasks,
              replyResolver: activeReplyResolver,
              replyLogger,
              baseMentionConfig,
              account,
            });

            return (await (listenerFactory ?? attachWebInboxToSocket)({
              cfg,
              verbose,
              accountId: account.accountId,
              authDir: account.authDir,
              mediaMaxMb: account.mediaMaxMb,
              selfChatMode: account.selfChatMode,
              sendReadReceipts: account.sendReadReceipts,
              debounceMs: inboundDebounceMs,
              shouldDebounce,
              socketRef: controller.socketRef,
              shouldRetryDisconnect: () => !sigintStop && controller.shouldRetryDisconnect(),
              disconnectRetryPolicy: reconnectPolicy,
              disconnectRetryAbortSignal: controller.getDisconnectRetryAbortSignal(),
              groupMetadataCache,
              onMessage: async (msg: WebInboundMsg) => {
                const inboundAt = Date.now();
                controller.noteInbound(inboundAt);
                statusController.noteInbound(inboundAt);
                await onMessage(msg);
              },
              sock,
            })) as ManagedWhatsAppListener;
          },
          onHeartbeat: (snapshot) => {
            const authAgeMs = getWebAuthAgeMs(account.authDir);
            const minutesSinceLastMessage = snapshot.lastInboundAt
              ? Math.floor((Date.now() - snapshot.lastInboundAt) / 60000)
              : null;

            const logData = {
              connectionId: snapshot.connectionId,
              reconnectAttempts: snapshot.reconnectAttempts,
              messagesHandled: snapshot.handledMessages,
              lastInboundAt: snapshot.lastInboundAt,
              lastTransportActivityAt: snapshot.lastTransportActivityAt,
              authAgeMs,
              uptimeMs: snapshot.uptimeMs,
              ...(minutesSinceLastMessage !== null && minutesSinceLastMessage > 30
                ? { minutesSinceLastMessage }
                : {}),
            };
            statusController.noteTransportActivity(snapshot.lastTransportActivityAt);

            if (minutesSinceLastMessage && minutesSinceLastMessage > 30) {
              heartbeatLogger.warn(
                logData,
                "⚠️ web gateway heartbeat - no messages in 30+ minutes",
              );
            } else {
              heartbeatLogger.info(logData, "web gateway heartbeat");
            }
          },
          onWatchdogTimeout: (snapshot) => {
            const now = Date.now();
            const transportSilentMs = now - snapshot.lastTransportActivityAt;
            const appBaselineAt = snapshot.lastInboundAt ?? snapshot.startedAt;
            const minutesSinceTransportActivity = Math.floor(transportSilentMs / 60000);
            const minutesSinceAppActivity = Math.floor((now - appBaselineAt) / 60000);
            const watchdogReason =
              transportSilentMs > transportTimeoutMs ? "transport-inactive" : "app-silent";
            statusController.noteWatchdogStale();
            heartbeatLogger.warn(
              {
                connectionId: snapshot.connectionId,
                watchdogReason,
                minutesSinceTransportActivity,
                minutesSinceAppActivity,
                lastInboundAt: snapshot.lastInboundAt ? new Date(snapshot.lastInboundAt) : null,
                lastTransportActivityAt: new Date(snapshot.lastTransportActivityAt),
                messagesHandled: snapshot.handledMessages,
              },
              "WhatsApp watchdog timeout detected - forcing reconnect",
            );
            whatsappHeartbeatLog.warn(
              `WhatsApp watchdog timeout (${watchdogReason}) - restarting connection`,
            );
          },
        });
      } catch (error) {
        if (!isRetryableAuthUnstableError(error)) {
          throw error;
        }
        const retryDecision = controller.consumeReconnectAttempt();
        statusController.noteReconnectAttempts(retryDecision.reconnectAttempts);
        statusController.noteClose({
          error: error.message,
          reconnectAttempts: retryDecision.reconnectAttempts,
          healthState: retryDecision.healthState,
        });
        if (retryDecision.action === "stop") {
          reconnectLogger.warn(
            {
              connectionId,
              reconnectAttempts: retryDecision.reconnectAttempts,
              maxAttempts: reconnectPolicy.maxAttempts,
            },
            "web reconnect: auth state stayed unstable; max attempts reached",
          );
          runtime.error(
            `WhatsApp auth state is still stabilizing after ${retryDecision.reconnectAttempts}/${reconnectPolicy.maxAttempts} attempts. Stopping web monitoring.`,
          );
          await controller.shutdown();
          break;
        }
        reconnectLogger.info(
          {
            connectionId,
            reconnectAttempts: retryDecision.reconnectAttempts,
            delayMs: retryDecision.delayMs,
          },
          "web reconnect: auth state still stabilizing during inbox attach; retrying",
        );
        runtime.error(
          `WhatsApp auth state is still stabilizing. Retry ${retryDecision.reconnectAttempts}/${reconnectPolicy.maxAttempts || "∞"} for inbox attach in ${formatDurationPrecise(retryDecision.delayMs ?? 0)}.`,
        );
        try {
          await controller.waitBeforeRetry(retryDecision.delayMs ?? 0);
        } catch {
          break;
        }
        continue;
      }

      statusController.noteConnected();
      controller.setUnhandledRejectionCleanup(
        registerUnhandledRejectionHandler((reason) => {
          if (!isLikelyWhatsAppCryptoError(reason)) {
            return false;
          }
          const errorStr = formatError(reason);
          reconnectLogger.warn(
            { connectionId: connection.connectionId, error: errorStr },
            "web reconnect: unhandled rejection from WhatsApp socket; forcing reconnect",
          );
          controller.forceClose({
            status: 499,
            isLoggedOut: false,
            error: reason,
          });
          return true;
        }),
      );

      const { e164: selfE164 } = readWebSelfId(account.authDir);
      const connectRoute = resolveAgentRoute({
        cfg,
        channel: "whatsapp",
        accountId: account.accountId,
      });
      enqueueSystemEvent(`WhatsApp gateway connected${selfE164 ? ` as ${selfE164}` : ""}.`, {
        sessionKey: connectRoute.sessionKey,
        trusted: true,
      });

      const normalizedAccountId = normalizeReconnectAccountId(account.accountId);
      void drainPendingDeliveries({
        drainKey: `whatsapp:${normalizedAccountId}`,
        logLabel: "WhatsApp reconnect drain",
        cfg,
        log: reconnectLogger,
        selectEntry: (entry) => ({
          match:
            entry.channel === "whatsapp" &&
            normalizeReconnectAccountId(entry.accountId) === normalizedAccountId,
          bypassBackoff: isNoListenerReconnectError(entry.lastError),
        }),
      }).catch((err) => {
        reconnectLogger.warn(
          { connectionId: connection.connectionId, error: String(err) },
          "reconnect drain failed",
        );
      });

      whatsappLog.info("Listening for personal WhatsApp inbound messages.");
      if (process.stdout.isTTY || process.stderr.isTTY) {
        whatsappLog.raw("Ctrl+C to stop.");
      }

      if (!keepAlive) {
        await controller.shutdown();
        return;
      }

      const reason = await controller.waitForClose();
      if (stopRequested() || sigintStop || reason === "aborted") {
        await controller.shutdown();
        break;
      }

      const decision = controller.resolveCloseDecision(reason);
      if (decision === "aborted") {
        await controller.shutdown();
        break;
      }
      statusController.noteReconnectAttempts(controller.getReconnectAttempts());

      reconnectLogger.info(
        {
          connectionId: connection.connectionId,
          status: decision.normalized.statusLabel,
          loggedOut: decision.normalized.isLoggedOut,
          reconnectAttempts: decision.reconnectAttempts,
          error: decision.normalized.errorText,
        },
        "web reconnect: connection closed",
      );

      enqueueSystemEvent(
        `WhatsApp gateway disconnected (status ${decision.normalized.statusLabel})`,
        {
          sessionKey: connectRoute.sessionKey,
          trusted: true,
        },
      );

      if (decision.action === "stop") {
        await controller.closeCurrentConnection();
        statusController.noteClose({
          statusCode: decision.normalized.statusCode,
          loggedOut: decision.normalized.isLoggedOut,
          error: decision.normalized.errorText,
          reconnectAttempts: decision.reconnectAttempts,
          healthState: decision.healthState,
        });

        if (decision.healthState === "logged-out") {
          await clearTerminalWebAuthState({
            account,
            runtime,
            statusLabel: decision.normalized.statusLabel,
            healthState: decision.healthState,
            log: reconnectLogger,
          });
          runtime.error(
            `WhatsApp session logged out. Run \`${formatCliCommand("openclaw channels login --channel whatsapp")}\` to relink.`,
          );
        } else if (decision.healthState === "conflict") {
          await clearTerminalWebAuthState({
            account,
            runtime,
            statusLabel: decision.normalized.statusLabel,
            healthState: decision.healthState,
            log: reconnectLogger,
          });
          reconnectLogger.warn(
            {
              connectionId: connection.connectionId,
              status: decision.normalized.statusLabel,
              error: decision.normalized.errorText,
            },
            "web reconnect: non-retryable close status; stopping monitor",
          );
          runtime.error(
            `WhatsApp Web connection closed (status ${decision.normalized.statusLabel}: session conflict). Resolve conflicting WhatsApp Web sessions, then relink with \`${formatCliCommand("openclaw channels login --channel whatsapp")}\`. Stopping web monitoring.`,
          );
        } else {
          reconnectLogger.warn(
            {
              connectionId: connection.connectionId,
              status: decision.normalized.statusLabel,
              reconnectAttempts: decision.reconnectAttempts,
              maxAttempts: reconnectPolicy.maxAttempts,
            },
            "web reconnect: max attempts reached; continuing in degraded mode",
          );
          runtime.error(
            `WhatsApp Web reconnect: max attempts reached (${decision.reconnectAttempts}/${reconnectPolicy.maxAttempts}). Stopping web monitoring.`,
          );
        }

        await controller.shutdown();
        break;
      }

      statusController.noteClose({
        statusCode: decision.normalized.statusCode,
        error: decision.normalized.errorText,
        reconnectAttempts: decision.reconnectAttempts,
        healthState: decision.healthState,
      });
      reconnectLogger.info(
        {
          connectionId: connection.connectionId,
          status: decision.normalized.statusLabel,
          reconnectAttempts: decision.reconnectAttempts,
          maxAttempts: reconnectPolicy.maxAttempts || "unlimited",
          delayMs: decision.delayMs,
        },
        "web reconnect: scheduling retry",
      );
      runtime.error(
        `WhatsApp Web connection closed (status ${decision.normalized.statusLabel}). Retry ${decision.reconnectAttempts}/${reconnectPolicy.maxAttempts || "∞"} in ${formatDurationPrecise(decision.delayMs ?? 0)}… (${decision.normalized.errorText})`,
      );
      await controller.closeCurrentConnection();
      try {
        await controller.waitBeforeRetry(decision.delayMs ?? 0);
      } catch {
        break;
      }
    }
  } finally {
    statusController.markStopped();
    process.removeListener("SIGINT", handleSigint);
    await controller.shutdown();
  }
}
