# Agent Engineering Patterns

Reference patterns from Stanford Meta-Harness, Ramp self-maintaining codebase, and Agent Orchestrator research. Consult when spawning coding agents or designing agentic workflows.

## 1. Environment Bootstrap (saves 2-5 turns)

Before giving the agent its task, inject a snapshot:

```bash
echo "=== ENV CONTEXT ==="
echo "CWD: $(pwd)"
echo "FILES: $(ls -la)"
echo "NODE: $(node --version 2>/dev/null || echo 'N/A')"
echo "PYTHON: $(python3 --version 2>/dev/null || echo 'N/A')"
echo "BUN: $(bun --version 2>/dev/null || echo 'N/A')"
echo "GIT: $(git log --oneline -3 2>/dev/null || echo 'not a git repo')"
echo "PACKAGE: $(cat package.json 2>/dev/null | head -5 || echo 'none')"
```

Inject output into the agent's initial prompt. Don't let agents waste turns on discovery.

## 2. Double-Confirm Completion

Never trust an agent's first "done." Before accepting:

- Does the solution meet original requirements?
- Does it handle edge cases (different values, sizes, configs)?
- Verified from: test engineer, QA, and user perspectives?

Pattern: require agents to call `task_complete` twice — first triggers self-review, second confirms.

## 3. Forced Reasoning Before Execution

When designing agent tools, require:

- **Analysis:** What's the current state?
- **Plan:** What will you do and why?
- **Commands:** The actual actions

Models that explain their plan before acting produce better results.

## 4. Triage Before Notification (Ramp pattern)

When agents detect issues:

1. Evaluate: Is this real or noise?
2. If real → fix + notify
3. If noise → tune the detection
4. If duplicate → check for existing fix, stand down

## 5. Three-Tier Escalation

| Tier | Handler                  | Trigger                                  |
| ---- | ------------------------ | ---------------------------------------- |
| 1    | Agent self-healing       | CI fails → inject error → retry up to 5x |
| 2    | Orchestrator             | Agent can't fix after retries            |
| 3    | Human (Discord/Telegram) | Orchestrator can't resolve               |

Same message format at every boundary.

## 6. Session Identity = Task Identity

When sessions die and respawn, tie identity to the TASK, not a sequential ID. Prevents confusion about which session is doing what.

---

_Sources: Stanford Meta-Harness (tbench2), Ramp Labs self-maintaining codebase, Composio Agent Orchestrator_
