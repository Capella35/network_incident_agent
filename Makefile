.PHONY: help setup lint format typecheck test check data clean

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

typecheck:  ## mypy (strict) on src/
	uv run mypy

test:  ## Unit tests with coverage (integration tests excluded)
	uv run pytest -m "not integration" --cov --cov-report=term-missing

check: lint typecheck test  ## Everything CI runs (except the secret scan)

data:  ## Verify + process the Milan files in data/raw (ARGS="--date 2013-11-01 --delete-raw")
	uv run python -m producer.data_prep $(ARGS)

clean:  ## Remove local caches (data/ is never touched)
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov
	find . -path ./.venv -prune -o -name __pycache__ -type d -exec rm -rf {} +
