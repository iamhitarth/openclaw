---
name: openai-whisper-api
description: Transcribe audio via Groq or OpenAI Audio Transcriptions API.
homepage: https://platform.openai.com/docs/guides/speech-to-text
metadata:
  {
    "openclaw":
      {
        "emoji": "☁️",
        "requires": { "bins": ["curl"] },
      },
  }
---

# Audio Transcription (Groq / OpenAI)

Transcribe an audio file via Groq or OpenAI's `/v1/audio/transcriptions` endpoint.

**Provider priority:** Groq (faster & cheaper) if `GROQ_API_KEY` is set, otherwise OpenAI.

## Quick start

```bash
{baseDir}/scripts/transcribe.sh /path/to/audio.m4a
```

Defaults:

- Provider: Groq (if `GROQ_API_KEY` set), else OpenAI
- Model: `whisper-large-v3-turbo` (Groq) / `gpt-4o-mini-transcribe` (OpenAI)
- Output: `<input>.txt`

## Useful flags

```bash
{baseDir}/scripts/transcribe.sh /path/to/audio.ogg --provider openai    # force OpenAI
{baseDir}/scripts/transcribe.sh /path/to/audio.ogg --model whisper-1    # specific model
{baseDir}/scripts/transcribe.sh /path/to/audio.m4a --out /tmp/transcript.txt
{baseDir}/scripts/transcribe.sh /path/to/audio.m4a --language en
{baseDir}/scripts/transcribe.sh /path/to/audio.m4a --prompt "Speaker names: Peter, Daniel"
{baseDir}/scripts/transcribe.sh /path/to/audio.m4a --json --out /tmp/transcript.json
```

## API keys

Set `GROQ_API_KEY` (preferred) or `OPENAI_API_KEY` in `~/.openclaw/.env`.
