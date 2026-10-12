SHELL := /bin/bash
UV ?= uv

.PHONY: help doctor policy setup lock format lint test verify

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
	$(UV) run mypy src tests

test: ## Run tests that do not require external services
	$(UV) run pytest -m 'not integration'

verify: ## Run the repository foundation quality gate
	@./scripts/verify.sh
