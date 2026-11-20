# Build Log

One entry per working session, newest at the top. Copy the template below.

---

<!--
## YYYY-MM-DD — Milestone N, step N: <short title>

**What was built:**
-

**Tests run and results:**
-

**Decisions made:** (also add to DECISIONS.md)
-

**Open items:**
-
-->

## 2026-11-19 — Milestone 1, steps 0–2: resource check, tooling, domain model

**What was built:**
- Step 0 (partial): laptop resource check — 8 GB RAM total (~47% free at check), Intel i5-10500, 12 threads, 54 GB disk free; Docker 24.0.2 installed (daemon not running at check). Milan files present in `data/raw/` (checked later the same session: grid MD5 `e64ed3858e97347c14cd049efde8c7d3` matches the published value; 2013-11-01 MD5 `658c49f8e3c7910bd3bb2bafbf62cdce`, 322,874,887 bytes). An earlier note that `data/raw/` was empty came from a truncated directory listing and was wrong.
- Step 1: `pyproject.toml` (Python 3.11, uv, pinned via `uv.lock`), `Makefile` (`setup`, `lint`, `format`, `typecheck`, `test`, `check`, `clean`), `.pre-commit-config.yaml` (gitleaks v8.30.1, ruff, large-file/private-key checks), `.github/workflows/ci.yml` (gitleaks + `make check`), `.claude/settings.json` (deny `.env` reads/edits and edits under `data/`).
- Step 2: `src/common/domain/entities.yaml`, `kpis.yaml`, `loader.py` (pydantic, frozen, cached `load_domain()`).

**Tests run and results:**
- `make check`: ruff clean, mypy strict clean, 17 tests passed, coverage 93%.
- `pre-commit run --all-files`: all hooks passed.
- gitleaks flags a fake Groq-style key (tested in a scratch dir, not the repo).

**Decisions made:** (also in DECISIONS.md)
- Milestone 1 started; plan defaults confirmed. Repo stays public. Subset ~150 squares and Spark behind a Compose profile (8 GB RAM). Tooling choices.

**Open items:**
- Set Docker Desktop memory to ~5 GB.
- Branch protection on `main` (GitHub settings).
- CI not yet seen running on GitHub (needs a push).
