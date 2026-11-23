# prompts/

Reusable procedures for building this project now live as Claude Code skills in `.claude/skills/` (one folder per skill, `SKILL.md` inside), so they load only when used:

- `milestone-step`: one milestone step from `specs/HANDOFF.md` to commit.
- `new-package`: add a `src/<component>/` package wired into the build, mypy, and tests.

Keep this folder for prompts that are not procedures (e.g., one-off review prompts).
