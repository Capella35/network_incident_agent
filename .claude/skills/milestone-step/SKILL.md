---
name: milestone-step
description: Build one step of the current milestone end to end — read specs/HANDOFF.md, plan against the step's "Done when", build with tests, run make check, check guardrails, then update specs/HANDOFF.md / BUILD_LOG.md / docs and propose a commit. Use when the user says "next step", "do step N", "continue the milestone", or starts a build session.
---

# Milestone step

One step of `specs/milestones/milestone-N.md`, from handoff to commit. The step is done only when its **Done when** line is met and verified, not when the code is written.

## 1. Orient (read before touching anything)
1. `specs/HANDOFF.md`: current state, **Next step**, **Blocked on**. If it is blocked, stop and tell the user.
2. The current milestone file: the step's bullets, its **Done when**, the **Guardrails due this milestone**, and **Out of scope**.
3. Only the docs the step names (e.g., `specs/02-telemetry.md`, `specs/01-domain-model.md`, `specs/09-evals.md`). Don't read the whole repo.
4. `git status`: if there is uncommitted work from an earlier step, say so and ask whether to finish or commit it first. Never mix two steps in one commit.

## 2. Plan (short, in chat)
- Restate the step in 3–6 bullets: files to add/change, tests to add, how **Done when** will be verified.
- Flag anything that needs the user: a manual download, `.env` edits, a choice not settled in `DECISIONS.md`. If the plan changes a fixed quality bar or a decided item, stop and ask (Rule 5).

## 3. Build
- Follow `CLAUDE.md` conventions. In particular:
  - Names (entities, KPIs, teams, faults, states) come from `common.domain.load_domain()`; IDs via `domain.entity(<type>).make_id(...)`.
  - Anything that fits or tunes on data goes through `common.splits` (`splits.require(day, Split.TRAIN|VALIDATION)`). Never touch test weeks (Rule 7).
  - Ground-truth fault labels only inside `src/evaluation/` (Rule 6). The fault injector never imports fault signatures (Rule 15).
  - Synthetic or modeled outputs are labeled as such in docstrings and docs (Rule 9).
  - Compose/infra: ports on `127.0.0.1`, pinned image tags, memory limits, no default passwords (Rule 12, S1/S2/S5, R1).
- A new package under `src/`? Use the `new-package` skill.
- Every new behavior gets a test in `tests/<package>/` with small hand-written fixtures (never Milan files). Docker-dependent tests are `@pytest.mark.integration`.
- Data files are produced only by the data scripts (`make data`, `make topology`, `make calibrate`). Dependencies only via `uv add`.
- **Review the code last, before any verification** (the "Code quality" rules in `CLAUDE.md`), so the evidence in §4 describes the final code:
  - Run the `simplify` skill on the changed code: reuse, duplication, dead code, needless layers. Apply what it finds.
  - Run the `code-review` skill at `medium` on the diff for correctness bugs. Fix every confirmed finding, or say in the report why one is left.
  - Grep the new code for anything that duplicates an existing helper (`common/`, `tests/factories.py`, `tests/fakes.py`, `tests/integration/conftest.py`); move shared code instead of copying it.
  - Note what the review found and fixed, for the step summary.
  - If anything in §4 later forces a code change, re-run this review on that change before re-verifying.

## 4. Verify
1. `make check` must pass. Paste the summary line (tests passed, coverage).
2. Verify the step's **Done when** for real: run the command it names and quote the result. If it needs Docker, the user's LAN, or another device, give the user the exact command and wait for their result. Don't claim it.
3. Walk the guardrails due this milestone that the step touched; note pass/fail for each.
4. Re-read the diff (`git diff`) once for: hard-coded domain names, string-formatted IDs, `0.0.0.0`, `latest` tags, secrets or file paths that could reach an LLM prompt, leftover debug output.

## 5. Record
Update, in this order:
1. The milestone file: mark the step `— *done YYYY-MM-DD*`; update its **Status** paragraph.
2. `BUILD_LOG.md`: new entry at the top using the template (what was built; tests run and measured results with real numbers; decisions; open items).
3. `DECISIONS.md` (and `PROJECT.md`'s decision log if it is project-level): any decision made, with date and reason.
4. `CLAUDE.md`: commands table if a `make` target now works (move it out of "Planned"); file table if a new top-level file appeared.
5. `specs/HANDOFF.md` last: **Where things stand**, **Last session**, **Next step** (concrete, numbered), **Blocked on**.

Use absolute dates (today's date), never "yesterday" or "last week".

## 6. Commit (only when the user says so)
- Propose one commit per step: `Milestone N step K: <short title>`. Show the file list first.
- Never `--no-verify`; if gitleaks or ruff fails, fix the cause.
- Don't push unless asked; `main` is branch-protected.

## Report back
End with: what was built, the `make check` line, the **Done when** evidence (or what the user must run), guardrail results, and the next step from `specs/HANDOFF.md`.
