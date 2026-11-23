# 07 — Human gate (when a person takes over)

**A human approves every action.** The system detects, groups, labels, and drafts; only a person changes an incident's state. Built across Milestones 4 (console) and 5 (agent test). Drawn from `PROJECT.md` → What is being built (item 9) and the former domain-model section 5.

## Console (Streamlit, `src/console/`)
- Humans review incidents: evidence charts, signature checks behind each label, the classifier's labels and confidence, and the agent's draft (shown **as a draft**).
- Actions: **approve**, **correct** (labels and/or root node), **close**, **escalate**. Each records `human_decision_by` and an `audit_trail` event (`04-correlation.md`).
- **Corrections become labeled data** (`human_correction`) for the next reviewed retrain (`05-classification.md`).
- Agent output never overwrites labels or root cause directly (guardrail A5).

## Incident lifecycle

```
open ──► approved ──► closed
  │         ▲
  ├──► corrected ──┘
  └──► escalated
```

Every transition requires `actor = human`. The agent has no transition rights (`08-guardrails.md` A1). Allowed transitions are listed in `lifecycle.yaml`; the console refuses anything else.

## Enforcement (in code, not prompts)
- Action endpoints require an authenticated human user; there is no service or agent path to them (Rule 4, guardrail A1).
- The console's DB user can change incident status only; the agent's DB user is read-only (guardrail S4).
- **Human-gate test:** every close/escalate/re-route attempt from the agent is blocked — 100%, in `tests/` (Definition of done).
