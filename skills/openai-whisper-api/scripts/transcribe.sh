#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage:
  transcribe.sh <audio-file> [--model whisper-large-v3-turbo] [--out /path/to/out.txt] [--language en] [--prompt "hint"] [--json] [--provider groq|openai]
EOF
  exit 2
}

if [[ "${1:-}" == "" || "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
fi

in="${1:-}"
shift || true

provider=""
model=""
out=""
language=""
prompt=""
response_format="text"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --provider)
      provider="${2:-}"
      shift 2
      ;;
    --model)
      model="${2:-}"
      shift 2
      ;;
    --out)
      out="${2:-}"
      shift 2
      ;;
    --language)
      language="${2:-}"
      shift 2
      ;;
    --prompt)
      prompt="${2:-}"
      shift 2
      ;;
    --json)
      response_format="json"
      shift 1
      ;;
    *)
      echo "Unknown arg: $1" >&2
      usage
      ;;
  esac
done

if [[ ! -f "$in" ]]; then
  echo "File not found: $in" >&2
  exit 1
fi

# Auto-detect provider: prefer Groq (faster & cheaper), fall back to OpenAI
if [[ -z "$provider" ]]; then
  if [[ -n "${GROQ_API_KEY:-}" ]]; then
    provider="groq"
  elif [[ -n "${OPENAI_API_KEY:-}" ]]; then
    provider="openai"
  else
    echo "Missing GROQ_API_KEY or OPENAI_API_KEY" >&2
    exit 1
  fi
fi

case "$provider" in
  groq)
    api_key="${GROQ_API_KEY:-}"
    base_url="https://api.groq.com/openai/v1"
    [[ -z "$model" ]] && model="whisper-large-v3-turbo"
    if [[ -z "$api_key" ]]; then
      echo "Missing GROQ_API_KEY" >&2
      exit 1
    fi
    ;;
  openai)
    api_key="${OPENAI_API_KEY:-}"
    base_url="https://api.openai.com/v1"
    [[ -z "$model" ]] && model="gpt-4o-mini-transcribe"
    if [[ -z "$api_key" ]]; then
      echo "Missing OPENAI_API_KEY" >&2
      exit 1
    fi
    ;;
  *)
    echo "Unknown provider: $provider (use groq or openai)" >&2
    exit 1
    ;;
esac

if [[ "$out" == "" ]]; then
  base="${in%.*}"
  if [[ "$response_format" == "json" ]]; then
    out="${base}.json"
  else
    out="${base}.txt"
  fi
fi

mkdir -p "$(dirname "$out")"

curl -sS "${base_url}/audio/transcriptions" \
  -H "Authorization: Bearer $api_key" \
  -H "Accept: application/json" \
  -F "file=@${in}" \
  -F "model=${model}" \
  -F "response_format=${response_format}" \
  ${language:+-F "language=${language}"} \
  ${prompt:+-F "prompt=${prompt}"} \
  >"$out"

echo "$out"
