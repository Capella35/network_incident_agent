---
name: new-package
description: Add a new Python package under src/ (e.g., streaming, correlation, models, agent, console, evaluation) wired into the wheel build, mypy strict, and tests. Use when a step first puts code into a src/<component>/ folder that has only a README, or when the user asks to create a new component package.
---

# New package

Packages are one per component under `src/` (`DECISIONS.md` 2026-11-14). Component folders already exist with a `README.md` and no code; a package becomes "real" when it gets its first module. Every step below is required: missing one shows up later as an import or type-check failure in CI.

Below, `<pkg>` is the folder name, e.g. `correlation`.

## Steps
1. **Package files**
   - `src/<pkg>/__init__.py`: a one-line module docstring stating what the component does; mark it **synthetic/modeled** if it produces such data (Rule 9). No imports needed.
   - Keep the existing `src/<pkg>/README.md`. Replace its "No code yet." line with a short list of modules and what each does.
2. **`pyproject.toml`** (edit both lists, keep them in the same order):
   - `[tool.hatch.build.targets.wheel] packages` → add `"src/<pkg>"`.
   - `[tool.mypy] packages` → add `"<pkg>"`.
3. **Dependencies**: only if the package needs a new library, `uv add <lib>` (runtime) or `uv add --dev <lib>` (tools). Never edit `uv.lock` by hand. If the library ships no types, add its `types-*` stub with `--dev`, or a scoped `[[tool.mypy.overrides]]` with `ignore_missing_imports = true` for that module only. Record why in `DECISIONS.md` if it is a stack choice.
4. **Tests**: `tests/<pkg>/__init__.py` (empty) and `tests/<pkg>/test_<module>.py` with at least one real test of the first behavior. Small hand-written fixtures; shared ones go in `tests/<pkg>/conftest.py`.
5. **Domain rules** (check the first module against them):
   - Names from `common.domain.load_domain()`, IDs from `domain.entity(<type>).make_id(...)`. No string literals for entities, KPIs, teams, faults, or states.
   - Config in a YAML next to the module (like `producer/topology_config.yaml`), loaded and validated with pydantic.
   - A CLI entry point, if any, is `main(argv: list[str] | None = None) -> int` with `argparse`, run as `python -m <pkg>.<module>`, and exposed as a `make` target with a `## help` comment.
   - Special packages:
     - `evaluation`: the only place ground-truth fault labels may be read (Rule 6).
     - `agent`: tools read-only and parameterized; no SQL from the model; nothing that closes, escalates, or re-routes (Rules 4, 13). The human-gate test lives in `tests/agent/`.
     - Fault injector (in `producer`): must not import fault signatures (Rule 15).
6. **Verify**: `make check`. Confirm mypy lists the new package (no "not found" error) and the new tests ran.
7. **Docs**: if the package adds a `make` target, add it to `CLAUDE.md`'s commands table, or to "Planned" if it doesn't work yet.

## Done when
`make check` passes with the new package type-checked and at least one test in `tests/<pkg>/`, and `uv run python -c "import <pkg>"` succeeds.
