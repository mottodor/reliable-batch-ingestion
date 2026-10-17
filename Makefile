SHELL := /bin/bash
UV ?= uv
COMPOSE ?= docker compose
COMPOSE_ENV_FILE ?= .env
COMPOSE_PROJECT_NAME ?= reliable-batch-ingestion
COMPOSE_WAIT_TIMEOUT_SECONDS ?= 60
DEMO_TIMEOUT_SECONDS ?= 210
SOURCE_SEED ?= 20260901
export SOURCE_SEED

.PHONY: help doctor policy setup lock format lint test fixtures fixtures-check init-local-env check-local-env ingest-api ingest-file ingest-postgres demo-happy-path demo-duplicate demo-api-retry demo-bad-file demo-recovery demo-inspect up down logs seed integration reset reset-destructive verify

help: ## Show available commands
	@grep -E '^[a-zA-Z0-9_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

doctor: ## Check the local development environment
	@./scripts/doctor.sh

policy: ## Check repository language and task-state consistency
	@./scripts/check-repository-policy.sh

setup: ## Install all locked dependencies
	$(UV) sync --locked --all-groups

lock: ## Resolve and update uv.lock after an intentional dependency change
	$(UV) lock

format: ## Apply Ruff formatting and safe lint fixes
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

lint: ## Check formatting, lint, and static types
	$(UV) run ruff check .
	$(UV) run ruff format --check .
	$(UV) run mypy src mock_api tests

test: ## Run tests that do not require external services
	$(UV) run pytest -m 'not integration'

fixtures: ## Regenerate the committed supplier corpus for SOURCE_SEED
	$(UV) run python scripts/generate_fixtures.py --output fixtures

fixtures-check: ## Compare two regenerations with the committed supplier corpus
	$(UV) run python scripts/check_fixtures.py --committed fixtures

init-local-env: ## Initialize placeholders and validate complete local configuration
	@$(UV) run python scripts/init_local_env.py "$(COMPOSE_ENV_FILE)"

check-local-env: ## Validate complete application and Compose-only local settings
	@$(UV) run python scripts/check_local_env.py $(COMPOSE_ENV_FILE) 1>&2

ingest-api: check-local-env ## Run one REST date or inclusive backfill with ARGS="..."
	@$(UV) run --env-file $(COMPOSE_ENV_FILE) -- python -m batch_ingestion.cli $(ARGS)

ingest-file: check-local-env ## Stage/run or resume one supplier file with ARGS="..."
	@$(UV) run --env-file $(COMPOSE_ENV_FILE) -- python -m batch_ingestion.files.cli $(ARGS)

ingest-postgres: check-local-env ## Run or resume one bounded PostgreSQL snapshot with ARGS="..."
	@$(UV) run --env-file $(COMPOSE_ENV_FILE) -- python -m batch_ingestion.postgres_snapshot.cli $(ARGS)

demo-happy-path: check-local-env ## Run the bounded all-source happy-path demo
	@$(UV) run python scripts/demo.py happy-path --timeout-seconds "$(DEMO_TIMEOUT_SECONDS)" --env-file "$(COMPOSE_ENV_FILE)" --project-name "$(COMPOSE_PROJECT_NAME)"

demo-duplicate: check-local-env ## Run the bounded idempotent-redelivery demo
	@$(UV) run python scripts/demo.py duplicate --timeout-seconds "$(DEMO_TIMEOUT_SECONDS)" --env-file "$(COMPOSE_ENV_FILE)" --project-name "$(COMPOSE_PROJECT_NAME)"

demo-api-retry: check-local-env ## Run the bounded API rate-limit demo
	@$(UV) run python scripts/demo.py api-retry --timeout-seconds "$(DEMO_TIMEOUT_SECONDS)" --env-file "$(COMPOSE_ENV_FILE)" --project-name "$(COMPOSE_PROJECT_NAME)"

demo-bad-file: check-local-env ## Run the bounded rejected-file demo
	@$(UV) run python scripts/demo.py bad-file --timeout-seconds "$(DEMO_TIMEOUT_SECONDS)" --env-file "$(COMPOSE_ENV_FILE)" --project-name "$(COMPOSE_PROJECT_NAME)"

demo-recovery: check-local-env ## Run one allow-listed recovery scenario with SCENARIO=...
	@$(UV) run python scripts/demo.py recovery --scenario "$(SCENARIO)" --timeout-seconds "$(DEMO_TIMEOUT_SECONDS)" --env-file "$(COMPOSE_ENV_FILE)" --project-name "$(COMPOSE_PROJECT_NAME)"

demo-inspect: check-local-env ## Read and verify recorded demo descriptors
	@$(UV) run python scripts/demo.py inspect --timeout-seconds "$(DEMO_TIMEOUT_SECONDS)" --env-file "$(COMPOSE_ENV_FILE)" --project-name "$(COMPOSE_PROJECT_NAME)"

up: check-local-env ## Start local services and wait up to the bounded readiness deadline
	$(COMPOSE) --project-name $(COMPOSE_PROJECT_NAME) --env-file $(COMPOSE_ENV_FILE) up --detach --wait --wait-timeout $(COMPOSE_WAIT_TIMEOUT_SECONDS)

down: check-local-env ## Stop local services without deleting persistent volumes
	$(COMPOSE) --project-name $(COMPOSE_PROJECT_NAME) --env-file $(COMPOSE_ENV_FILE) down --remove-orphans

logs: check-local-env ## Follow local service logs until interrupted
	$(COMPOSE) --project-name $(COMPOSE_PROJECT_NAME) --env-file $(COMPOSE_ENV_FILE) logs --follow

seed: ## Start infrastructure, then transactionally replace deterministic OmniRetail orders
	@$(UV) run python scripts/validate_source_seed.py
	$(MAKE) up
	$(COMPOSE) --project-name $(COMPOSE_PROJECT_NAME) --env-file $(COMPOSE_ENV_FILE) exec -T postgres sh -c 'exec psql -X --username "$$POSTGRES_USER" --dbname "$$POSTGRES_DB" --set=ON_ERROR_STOP=on' < postgres/init/001_omni_retail_schema.sql
	$(COMPOSE) --project-name $(COMPOSE_PROJECT_NAME) --env-file $(COMPOSE_ENV_FILE) exec -T -e SOURCE_SEED postgres sh -c 'exec psql -X --username "$$POSTGRES_USER" --dbname "$$POSTGRES_DB" --set=ON_ERROR_STOP=on "$$@"' seed-orders "--set=source_seed=$$SOURCE_SEED" < postgres/init/002_seed_orders.sql
	@printf 'Deterministic OmniRetail orders seeded with SOURCE_SEED=%s.\n' "$$SOURCE_SEED"

integration: seed ## Verify storage contracts, bootstrap, persistence, and recovery
	BI020_MARKER_MODE=create $(UV) run pytest tests/integration -m integration
	$(MAKE) seed
	BI020_MARKER_MODE=verify $(UV) run pytest tests/integration/test_infrastructure.py -m integration
	$(UV) run python scripts/check_infrastructure_recovery.py $(COMPOSE_ENV_FILE) --protected-project-name $(COMPOSE_PROJECT_NAME)

reset: ## Refuse implicit destructive reset and show the confirmed command
	@printf 'ERROR: reset is destructive and was not run. Use make reset-destructive CONFIRM=destroy-local-data.\n' >&2
	@false

reset-destructive: ## Delete selected-project containers and volumes after exact confirmation
	@$(UV) run python scripts/reset_local_infrastructure.py --confirm "$(CONFIRM)" --compose "$(COMPOSE)" --env-file $(COMPOSE_ENV_FILE) --project-name $(COMPOSE_PROJECT_NAME)

verify: ## Run the repository foundation quality gate
	@./scripts/verify.sh
