import { isSilentReplyPayloadText } from "../../auto-reply/tokens.js";
import { extractLeadingHttpStatus } from "../../shared/assistant-error-format.js";
import { isGpt5ModelId } from "../gpt5-prompt-overlay.js";
import type { ModelFallbackResultClassification } from "../model-fallback.js";
import { classifyFailoverSignal } from "../pi-embedded-helpers/errors.js";
import type { FailoverReason } from "../pi-embedded-helpers/types.js";
import { hasOutboundDeliveryEvidence, hasVisibleAgentPayload } from "./delivery-evidence.js";
import type { EmbeddedPiRunResult } from "./types.js";

const EMPTY_TERMINAL_REPLY_RE = /Agent couldn't generate a response/i;
const PLAN_ONLY_TERMINAL_REPLY_RE = /Agent stopped after repeated plan-only turns/i;
// Local patch (#7): a 4xx/5xx response returned by a fallback candidate must
// be treated as a candidate failure, not a success. Without this guard, the
// embedded run finishes with an isError:true payload (e.g. "Google Generative
// AI API error (400): ...") but the classifier returns null because the text
// does not match EMPTY_TERMINAL_REPLY_RE — so the model-fallback layer logs
// `candidate_succeeded` and pins the session to the broken candidate. See
// the 2026-05-22 fallback-classifier RCA and the May 12 failover-chain RCA
// in coding-agent-logs/ for the original symptom + repro.
const PROVIDER_ERROR_HINT_RE =
  /\b(api\s*error|http\s*\d{3}|\d{3}\s*(?:bad request|unauthorized|forbidden|not\s+found|internal\s+server\s+error|service\s+unavailable|gateway|too\s+many\s+requests)|provider\s+rejected|provider\s+error|llm\s+request\s+(?:failed|rejected)|model[_\s-]+not[_\s-]+found|insufficient[_\s-]+quota|quota\s+exceeded|out\s+of\s+(?:extra\s+)?usage|billing|rate[\s-]*limit|overloaded|generative\s+ai\s+(?:api\s+)?error)\b/i;

function isEmbeddedPiRunResult(value: unknown): value is EmbeddedPiRunResult {
  return Boolean(
    value &&
    typeof value === "object" &&
    "meta" in value &&
    (value as { meta?: unknown }).meta &&
    typeof (value as { meta?: unknown }).meta === "object",
  );
}

function hasDeliberateSilentTerminalReply(result: EmbeddedPiRunResult): boolean {
  return [result.meta.finalAssistantRawText, result.meta.finalAssistantVisibleText].some(
    (text) => typeof text === "string" && isSilentReplyPayloadText(text),
  );
}

type ErrorOnlyPayloadClassification = {
  message: string;
  reason: FailoverReason;
  status?: number;
  code: string;
  rawError?: string;
};

// Local patch (#7): inspect a run that produced only isError payloads (no
// visible content, no outbound delivery) and decide whether it is a fallback
// candidate failure. Returns the failover signal we want to surface, or null
// when the error payloads do not look like a provider rejection (in which
// case existing classifier branches keep their behaviour).
function classifyErrorOnlyPayloads(params: {
  provider: string;
  model: string;
  result: EmbeddedPiRunResult;
}): ErrorOnlyPayloadClassification | null {
  const payloads = params.result.payloads ?? [];
  if (payloads.length === 0) {
    return null;
  }
  const errorPayloadTexts: string[] = [];
  for (const payload of payloads) {
    if (!payload || typeof payload !== "object") {
      continue;
    }
    if (payload.isError !== true) {
      // A non-error payload is in the mix; let the existing branches decide.
      // `hasVisibleAgentPayload` (above) already short-circuits when the
      // non-error payload carries deliverable content, so reaching this point
      // means the non-error payload was reasoning or otherwise non-deliverable.
      return null;
    }
    if (typeof payload.text === "string" && payload.text.trim().length > 0) {
      errorPayloadTexts.push(payload.text);
    }
  }
  const combinedText = errorPayloadTexts.join("\n");
  // "Agent couldn't generate a response" handled by the existing
  // EMPTY_TERMINAL_REPLY_RE branch — keep its `incomplete_result` code.
  if (EMPTY_TERMINAL_REPLY_RE.test(combinedText)) {
    return null;
  }
  if (!combinedText.trim()) {
    // Error payload without text — fall through to the existing
    // empty-result/incomplete-result branches for the safety net.
    return null;
  }
  // Heuristic: only flag as a provider error when the text clearly looks like
  // one (HTTP status leader, "API error", "rate limit", etc.). Plain warning
  // strings without provider-error markers fall through to the existing
  // branches so we do not over-trigger fallback for legitimate one-line
  // warnings that the runner happened to emit as isError:true.
  const httpStatus = extractLeadingHttpStatus(combinedText)?.code;
  if (typeof httpStatus !== "number" && !PROVIDER_ERROR_HINT_RE.test(combinedText)) {
    return null;
  }
  const classification = classifyFailoverSignal({
    provider: params.provider,
    message: combinedText,
    status: httpStatus,
  });
  const reason: FailoverReason =
    classification?.kind === "reason" ? classification.reason : "format";
  const previewMessage = summarizeProviderErrorText(combinedText);
  const code =
    typeof httpStatus === "number" ? `provider_http_${httpStatus}` : "provider_http_error";
  return {
    message: `${params.provider}/${params.model} returned an error response: ${previewMessage}`,
    reason,
    status: httpStatus,
    code,
    rawError: combinedText.length > 600 ? `${combinedText.slice(0, 600)}…` : combinedText,
  };
}

function summarizeProviderErrorText(raw: string): string {
  const trimmed = raw.trim();
  if (!trimmed) {
    return "(empty)";
  }
  const firstLine = trimmed.split(/\r?\n/, 1)[0]?.trim() ?? "";
  const snippet = firstLine || trimmed;
  return snippet.length > 200 ? `${snippet.slice(0, 200)}…` : snippet;
}

function classifyHarnessResult(params: {
  provider: string;
  model: string;
  result: EmbeddedPiRunResult;
}): ModelFallbackResultClassification {
  switch (params.result.meta.agentHarnessResultClassification) {
    case "empty":
      return {
        message: `${params.provider}/${params.model} ended without a visible assistant reply`,
        reason: "format",
        code: "empty_result",
      };
    case "reasoning-only":
      return {
        message: `${params.provider}/${params.model} ended with reasoning only`,
        reason: "format",
        code: "reasoning_only_result",
      };
    case "planning-only":
      return {
        message: `${params.provider}/${params.model} exhausted plan-only retries without taking action`,
        reason: "format",
        code: "planning_only_result",
      };
    default:
      return null;
  }
}

export function classifyEmbeddedPiRunResultForModelFallback(params: {
  provider: string;
  model: string;
  result: unknown;
  hasDirectlySentBlockReply?: boolean;
  hasBlockReplyPipelineOutput?: boolean;
}): ModelFallbackResultClassification {
  if (!isEmbeddedPiRunResult(params.result)) {
    return null;
  }
  if (
    params.result.meta.aborted ||
    params.hasDirectlySentBlockReply === true ||
    params.hasBlockReplyPipelineOutput === true ||
    hasVisibleAgentPayload(params.result, {
      includeErrorPayloads: false,
      includeReasoningPayloads: false,
    })
  ) {
    return null;
  }
  if (hasOutboundDeliveryEvidence(params.result)) {
    return null;
  }

  const harnessClassification = classifyHarnessResult({
    provider: params.provider,
    model: params.model,
    result: params.result,
  });
  if (harnessClassification) {
    return harnessClassification;
  }

  const payloads = params.result.payloads ?? [];
  const errorText = payloads
    .filter((payload) => payload?.isError === true)
    .map((payload) => (typeof payload.text === "string" ? payload.text : ""))
    .join("\n");
  if (EMPTY_TERMINAL_REPLY_RE.test(errorText)) {
    return {
      message: `${params.provider}/${params.model} ended with an incomplete terminal response`,
      reason: "format",
      code: "incomplete_result",
    };
  }

  // Local patch (#7): when the embedded run ends with only error payloads —
  // i.e. the provider returned a 4xx/5xx that propagated as an isError:true
  // terminal payload — classify the candidate as failed so the fallback
  // chain continues to the next candidate AND the session-store rollback
  // fires (see persistFallbackCandidateSelection in
  // src/auto-reply/reply/agent-runner-execution.ts). Without this branch,
  // a Gemini 400 looks like `candidate_succeeded` and pins the session to
  // the broken candidate. Runs as a generic check before the gpt-5-only
  // branches so all providers benefit.
  const errorOnlyClassification = classifyErrorOnlyPayloads({
    provider: params.provider,
    model: params.model,
    result: params.result,
  });
  if (errorOnlyClassification) {
    return errorOnlyClassification;
  }

  if (!isGpt5ModelId(params.model)) {
    return null;
  }

  if (payloads.length === 0 && hasDeliberateSilentTerminalReply(params.result)) {
    return null;
  }
  if (payloads.length === 0) {
    return {
      message: `${params.provider}/${params.model} ended without a visible assistant reply`,
      reason: "format",
      code: "empty_result",
    };
  }
  if (payloads.every((payload) => payload.isReasoning === true)) {
    return {
      message: `${params.provider}/${params.model} ended with reasoning only`,
      reason: "format",
      code: "reasoning_only_result",
    };
  }

  if (PLAN_ONLY_TERMINAL_REPLY_RE.test(errorText)) {
    return {
      message: `${params.provider}/${params.model} exhausted plan-only retries without taking action`,
      reason: "format",
      code: "planning_only_result",
    };
  }
  if (!EMPTY_TERMINAL_REPLY_RE.test(errorText)) {
    return null;
  }

  return {
    message: `${params.provider}/${params.model} ended with an incomplete terminal response`,
    reason: "format",
    code: "incomplete_result",
  };
}
