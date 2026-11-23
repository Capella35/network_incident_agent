.PHONY: help setup lint format typecheck test check data topology calibrate clean

help:  ## List targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

setup:  ## Create .venv from uv.lock and install the git hooks
	uv sync --frozen
	uv run pre-commit install

lint:  ## Ruff lint + format check
	uv run ruff check .
	uv run ruff format --check .

format:  ## Apply ruff formatting and safe fixes
	uv run ruff format .
	uv run ruff check --fix .

typecheck:  ## mypy (strict) on src/ and the Claude Code guard hook
	uv run mypy
	uv run mypy --strict .claude/hooks/guard.py

test:  ## Unit tests with coverage (integration tests excluded)
	uv run pytest -m "not integration" --cov --cov-report=term-missing

check: lint typecheck test  ## Everything CI runs (except the secret scan)

data:  ## Verify + process Milan files in data/raw, then build the topology (ARGS="--date ... --delete-raw")
	uv run python -m producer.data_prep $(ARGS)
	uv run python -m producer.topology

topology:  ## Rebuild only the SYNTHETIC topology from data/processed/subset.json
	uv run python -m producer.topology

calibrate:  ## Fit MODELED cell capacity on processed training days -> kpi_calibration.json
	uv run python -m producer.kpi_model

clean:  ## Remove local caches (data/ is never touched)
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov
	find . -path ./.venv -prune -o -name __pycache__ -type d -exec rm -rf {} +
