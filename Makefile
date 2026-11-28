.PHONY: help setup lint format typecheck test check data topology calibrate baseline features candidates evaluate train union diagnose separation promote rollback inject clean \
	env-check up down ps logs spark-smoke detect detect-stop detect-reset db-migrate db-topology replay sink replay-check dashboards \
	test-integration

# Local stack. .env is read by Compose and sourced for Python; its values are never printed.
COMPOSE := docker compose --env-file .env -f infra/docker-compose.yml
LOAD_ENV := set -a; . ./.env; set +a;
# Schema changes after 001_init.sql, in name order. Postgres runs infra/db/ only on an empty
# volume; `make db-migrate` applies these to an existing one. They must be idempotent
# (tests/test_db_migrations.py); each file sets ON_ERROR_STOP itself.
DB_MIGRATIONS := $(sort $(filter-out 001_init.sql,$(notdir $(wildcard infra/db/0*.sql))))
PSQL_ADMIN = $(COMPOSE) exec -T postgres sh -c 'psql -q -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -f /docker-entrypoint-initdb.d/$(1)'

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

baseline:  ## Fit the seasonal KPI baseline (cell, weekday, hour) on processed training days
	uv run python -m streaming.baseline $(ARGS)

features:  ## Compute detection features for a split and report time + memory (ARGS="--split validation --inject")
	uv run python -m streaming.features_check $(ARGS)

candidates: env-check  ## Train each detector candidate (own process: memory budget), score a validation day, log to MLflow (ARGS="--score-day 2013-12-02")
	@cands=$$(uv run python -c 'from models.detectors import CANDIDATES; print(*CANDIDATES)') && test -n "$$cands" || { echo "could not list the detector candidates"; exit 1; }; \
	for c in $$cands; do \
		$(LOAD_ENV) MLFLOW_DISABLE_AGENT_HINT=1 uv run python -m models.candidates --candidate $$c $(ARGS) || exit 1; \
	done

train: env-check  ## Optuna study per detector candidate (own process: memory budget) on validation weeks, then the comparison table (MLflow)
	@cands=$$(uv run python -c 'from models.detectors import CANDIDATES; print(*CANDIDATES)') && test -n "$$cands" || { echo "could not list the detector candidates"; exit 1; }; \
	for c in $$cands; do \
		$(LOAD_ENV) MLFLOW_DISABLE_AGENT_HINT=1 uv run python -m models.tune --candidate $$c $(ARGS) || exit 1; \
	done; \
	$(LOAD_ENV) MLFLOW_DISABLE_AGENT_HINT=1 uv run python -m models.tune --compare

evaluate: env-check  ## Validation report: z-score baseline at a threshold vs the ground truth, logged to MLflow (ARGS="--threshold 4 [--start … --end …]")
	$(LOAD_ENV) MLFLOW_DISABLE_AGENT_HINT=1 uv run python -m evaluation.report $(ARGS)

union: env-check  ## Union of the latest tuned detectors (each at its own threshold) vs each alone, on validation weeks (MLflow)
	$(LOAD_ENV) MLFLOW_DISABLE_AGENT_HINT=1 uv run python -m models.ensemble $(ARGS)

diagnose: env-check  ## Why faults are missed: per-fault diagnosis of the latest tuned detectors on validation weeks (MLflow)
	$(LOAD_ENV) MLFLOW_DISABLE_AGENT_HINT=1 uv run python -m evaluation.misses $(ARGS)

promote: env-check  ## Promotion gate on validation weeks: register + alias the named candidate if it passes (ARGS="--candidate autoencoder")
	$(LOAD_ENV) MLFLOW_DISABLE_AGENT_HINT=1 uv run python -m models.promote $(ARGS)

rollback: env-check  ## Move the detector's champion alias back to the version it replaced
	$(LOAD_ENV) MLFLOW_DISABLE_AGENT_HINT=1 uv run python -m models.promote --rollback

separation:  ## Does a signal separate a fault type from normal busy rows? Validation weeks, stdout (ARGS="--fault-type congestion")
	MLFLOW_DISABLE_AGENT_HINT=1 uv run python -m evaluation.separation $(ARGS)

inject:  ## Write a split's SYNTHETIC fault schedule (ground truth, for evaluation) (ARGS="--split validation")
	uv run python -m producer.injector $(ARGS)

env-check:
	@test -f .env || { echo "No .env: cp .env.example .env, then set every password (12+ chars)"; exit 1; }

up: env-check  ## Start the local stack (Kafka, Postgres, Grafana, MLflow) and wait until healthy
	$(COMPOSE) up -d --wait

down:  ## Stop the stack, the detector job included (volumes kept)
	$(COMPOSE) --profile spark down

ps:  ## Service status
	$(COMPOSE) ps -a

logs:  ## Last 50 log lines per service
	$(COMPOSE) logs --tail=50

spark-smoke: env-check  ## One-off Spark local-mode job in the detector image (no stack needed)
	$(COMPOSE) --profile spark run --rm --no-deps --entrypoint python detector -c \
		"from pyspark.sql import SparkSession; print(SparkSession.builder.master('local[1]').getOrCreate().range(10).count())"

detect: env-check  ## Build the detector image and start the Spark job on the champion (Spark profile; make logs)
	$(COMPOSE) --profile spark up -d --build --wait detector

detect-stop: env-check  ## Stop the Spark detector job (its checkpoint is kept)
	$(COMPOSE) --profile spark stop detector

detect-reset: env-check  ## Remove the detector job and its checkpoint (needed after a promotion); then make detect
	$(COMPOSE) --profile spark rm --stop --force detector
	docker volume rm --force netops_spark-checkpoints

db-migrate: env-check  ## Apply the idempotent DB migrations (DB_MIGRATIONS) to the running stack
	$(foreach m,$(DB_MIGRATIONS),$(call PSQL_ADMIN,$(m)) &&) true

db-topology: env-check  ## Load the SYNTHETIC topology CSVs into Postgres
	@$(LOAD_ENV) uv run python -m producer.topology_db

replay: env-check  ## Replay processed days into Kafka telemetry.raw (ARGS="--start 2013-11-04 --speed 0")
	@$(LOAD_ENV) uv run python -m producer.replay $(ARGS)

sink: env-check  ## Kafka telemetry.raw -> Postgres telemetry_raw (ARGS="--idle-exit 10"; Ctrl-C stops)
	@$(LOAD_ENV) uv run python -m producer.sink $(ARGS)

replay-check: env-check  ## Milestone 1 Definition of done: replay twice, compare Kafka keys + DB rows (ARGS="--start 2013-11-04 --end 2013-11-10")
	@$(LOAD_ENV) uv run python -m producer.replay_check $(ARGS)

dashboards:  ## Regenerate Grafana dashboards from kpis.yaml -> infra/grafana/dashboards/
	uv run python -m common.grafana

test-integration: env-check  ## Integration tests against the running stack (not in CI)
	@$(LOAD_ENV) uv run pytest -m integration -p no:cacheprovider

clean:  ## Remove local caches (data/ is never touched)
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov
	find . -path ./.venv -prune -o -name __pycache__ -type d -exec rm -rf {} +
