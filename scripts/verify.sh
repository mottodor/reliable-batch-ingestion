#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

printf '==> doctor\n'
./scripts/doctor.sh

printf '\n==> repository policy\n'
./scripts/check-repository-policy.sh

printf '\n==> locked dependency sync\n'
uv sync --locked --all-groups

printf '\n==> ruff check\n'
uv run ruff check .

printf '\n==> ruff format check\n'
uv run ruff format --check .

printf '\n==> mypy\n'
uv run mypy src mock_api tests

printf '\n==> unit/default tests\n'
uv run pytest -m 'not integration'

if [[ -f compose.yaml || -f compose.yml ]]; then
  printf '\n==> compose config\n'
  if docker compose version >/dev/null 2>&1; then
    docker compose --env-file .env.example config --quiet
  else
    printf 'FAIL compose file exists, but Docker Compose is unavailable\n' >&2
    exit 1
  fi
fi

printf '\nVerification result: PASS\n'
