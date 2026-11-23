.PHONY: help setup lint format typecheck test check data topology calibrate clean \
	env-check up down ps logs spark-smoke db-topology test-integration

# Local stack. .env is read by Compose and sourced for Python; its values are never printed.
COMPOSE := docker compose --env-file .env -f infra/docker-compose.yml
LOAD_ENV := set -a; . ./.env; set +a;

help:  ## List targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-17s %s\n", $$1, $$2}'

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

env-check:
	@test -f .env || { echo "No .env: cp .env.example .env, then set every password (12+ chars)"; exit 1; }

up: env-check  ## Start the local stack (Kafka, Postgres, Grafana, MLflow) and wait until healthy
	$(COMPOSE) up -d --wait

down:  ## Stop the stack (volumes kept)
	$(COMPOSE) down

ps:  ## Service status
	$(COMPOSE) ps -a

logs:  ## Last 50 log lines per service
	$(COMPOSE) logs --tail=50

spark-smoke: env-check  ## One-off Spark local-mode job (Spark profile; off by default)
	$(COMPOSE) --profile spark run --rm spark

db-topology: env-check  ## Load the SYNTHETIC topology CSVs into Postgres
	@$(LOAD_ENV) uv run python -m producer.topology_db

test-integration: env-check  ## Integration tests against the running stack (not in CI)
	@$(LOAD_ENV) uv run pytest -m integration -p no:cacheprovider

clean:  ## Remove local caches (data/ is never touched)
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov
	find . -path ./.venv -prune -o -name __pycache__ -type d -exec rm -rf {} +
