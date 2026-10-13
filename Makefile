SHELL := /bin/bash
UV ?= uv
COMPOSE ?= docker compose
COMPOSE_ENV_FILE ?= .env
COMPOSE_PROJECT_NAME ?= reliable-batch-ingestion
COMPOSE_WAIT_TIMEOUT_SECONDS ?= 60

.PHONY: help doctor policy setup lock format lint test check-local-env up down logs seed integration reset reset-destructive verify

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

check-local-env: ## Validate ignored local infrastructure configuration
	@$(UV) run python scripts/check_local_env.py $(COMPOSE_ENV_FILE)

up: check-local-env ## Start local services and wait up to the bounded readiness deadline
	$(COMPOSE) --project-name $(COMPOSE_PROJECT_NAME) --env-file $(COMPOSE_ENV_FILE) up --detach --wait --wait-timeout $(COMPOSE_WAIT_TIMEOUT_SECONDS)

down: check-local-env ## Stop local services without deleting persistent volumes
	$(COMPOSE) --project-name $(COMPOSE_PROJECT_NAME) --env-file $(COMPOSE_ENV_FILE) down --remove-orphans

logs: check-local-env ## Follow local service logs until interrupted
	$(COMPOSE) --project-name $(COMPOSE_PROJECT_NAME) --env-file $(COMPOSE_ENV_FILE) logs --follow

seed: up ## Idempotently ensure infrastructure bootstrap and readiness
	@printf 'Infrastructure bootstrap is ready; deterministic source data belongs to BI-040.\n'

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
