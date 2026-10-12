#!/usr/bin/env bash
set -uo pipefail

failures=0
warnings=0

ok() {
  printf 'OK   %s\n' "$1"
}

warn() {
  printf 'WARN %s\n' "$1" >&2
  warnings=$((warnings + 1))
}

fail() {
  printf 'FAIL %s\n' "$1" >&2
  failures=$((failures + 1))
}

require_command() {
  local command_name=$1
  if command -v "$command_name" >/dev/null 2>&1; then
    ok "$command_name: $(command -v "$command_name")"
  else
    fail "required command is missing: $command_name"
  fi
}

if [[ ! -f AGENTS.md || ! -f docs/batch-ingestion.md ]]; then
  fail "run this script from the repository root"
fi

require_command python3
require_command uv
require_command make

if command -v python3 >/dev/null 2>&1; then
  python_version=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:3])))')
  python_minor=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
  if [[ "$python_minor" == "3.12" ]]; then
    ok "Python $python_version satisfies the 3.12 baseline"
  else
    fail "Python 3.12 is required, found $python_version"
  fi
fi

if command -v uv >/dev/null 2>&1; then
  ok "$(uv --version)"
fi

if command -v git >/dev/null 2>&1; then
  if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    ok "Git work tree detected"
  else
    warn "directory is not a Git work tree"
  fi
else
  warn "git is not installed"
fi

if command -v docker >/dev/null 2>&1; then
  if docker info >/dev/null 2>&1; then
    ok "Docker daemon is reachable"
    if docker compose version >/dev/null 2>&1; then
      ok "Docker Compose plugin is available"
    else
      warn "Docker daemon works, but docker compose plugin is unavailable"
    fi
  else
    warn "Docker CLI is present, but the daemon is not reachable; integration tasks will not run"
  fi
else
  warn "Docker CLI is missing; foundation checks can run, integration tasks cannot"
fi

if ((failures > 0)); then
  printf '\nDoctor result: %d failure(s), %d warning(s)\n' "$failures" "$warnings" >&2
  exit 1
fi

printf '\nDoctor result: PASS with %d warning(s)\n' "$warnings"
