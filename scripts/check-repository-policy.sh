#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

failures=0

fail() {
  printf 'FAIL %s\n' "$1" >&2
  failures=$((failures + 1))
}

if [[ ! -f docs/START_HERE.md ]]; then
  fail "session navigation entry point is missing: docs/START_HERE.md"
elif [[ -e START_HERE.md ]]; then
  fail "legacy root session navigation entry point still exists: START_HERE.md"
elif ! grep -Fq '`docs/START_HERE.md`' AGENTS.md; then
  fail "AGENTS.md does not reference docs/START_HERE.md"
elif grep -Fq '`START_HERE.md`' AGENTS.md; then
  fail "AGENTS.md still references the legacy root START_HERE.md path"
else
  printf 'OK   session navigation entry point is docs/START_HERE.md\n'
fi

printf 'Checking repository-owned text for Cyrillic characters...\n'
set +e
python3 - <<'PY'
from pathlib import Path

excluded_parts = {
    ".git",
    ".venv",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "__pycache__",
    "artifacts",
}
excluded_names = {".env"}
matches: list[str] = []

for path in sorted(Path(".").rglob("*")):
    if not path.is_file() or path.name in excluded_names:
        continue
    if any(part in excluded_parts for part in path.parts):
        continue
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except UnicodeDecodeError:
        continue
    for line_number, line in enumerate(lines, start=1):
        if any("\u0400" <= character <= "\u052f" for character in line):
            matches.append(f"{path}:{line_number}:{line}")

if matches:
    print("\n".join(matches))
    raise SystemExit(1)
PY
language_status=$?
set -e

if [[ "$language_status" -eq 0 ]]; then
  printf 'OK   repository-owned text contains no Cyrillic characters\n'
else
  fail "Cyrillic text found or the language scan failed"
fi

state_active=$(awk '$1 == "active_task_id:" {print $2; exit}' docs/STATE.md)
backlog_active=$(awk '$1 == "active_task_id:" {print $2; exit}' tasks/backlog.yaml)

if [[ -z "$state_active" || -z "$backlog_active" ]]; then
  fail "active_task_id is missing from docs/STATE.md or tasks/backlog.yaml"
elif [[ "$state_active" != "$backlog_active" ]]; then
  fail "active_task_id differs: state=$state_active backlog=$backlog_active"
else
  printf 'OK   active_task_id is synchronized: %s\n' "$backlog_active"
fi

mapfile -t task_ids < <(awk '$1 == "-" && $2 == "id:" {print $3}' tasks/backlog.yaml)
if ((${#task_ids[@]} == 0)); then
  fail "no backlog tasks found"
else
  duplicate_ids=$(printf '%s\n' "${task_ids[@]}" | sort | uniq -d)
  if [[ -n "$duplicate_ids" ]]; then
    fail "duplicate backlog task IDs: ${duplicate_ids//$'\n'/, }"
  fi
fi

if [[ "$backlog_active" == "null" ]]; then
  non_done_tasks=$(awk '
    $1 == "-" && $2 == "id:" {task_id = $3}
    $1 == "status:" && $2 != "done" {print task_id ":" $2}
  ' tasks/backlog.yaml)
  if [[ -n "$non_done_tasks" ]]; then
    fail "active_task_id may be null only when every task is done: ${non_done_tasks//$'\n'/, }"
  else
    printf 'OK   terminal backlog state has no active task and all tasks are done\n'
  fi
else
  active_count=$(printf '%s\n' "${task_ids[@]}" | grep -Fxc "$backlog_active" || true)
  if [[ "$active_count" -ne 1 ]]; then
    fail "active task must exist exactly once in the backlog: $backlog_active"
  fi

  active_status=$(awk -v active="$backlog_active" '
    $1 == "-" && $2 == "id:" {selected = ($3 == active)}
    selected && $1 == "status:" {print $2; exit}
  ' tasks/backlog.yaml)
  if [[ "$active_status" != "todo" && "$active_status" != "in_progress" ]]; then
    fail "active task must be todo or in_progress: $backlog_active is ${active_status:-missing}"
  else
    printf 'OK   active task status is %s\n' "$active_status"
  fi
fi

state_last_completed=$(awk '$1 == "last_completed_task_id:" {print $2; exit}' docs/STATE.md)
last_completed_count=$(printf '%s\n' "${task_ids[@]}" | grep -Fxc "$state_last_completed" || true)
last_completed_status=$(awk -v completed="$state_last_completed" '
  $1 == "-" && $2 == "id:" {selected = ($3 == completed)}
  selected && $1 == "status:" {print $2; exit}
' tasks/backlog.yaml)
if [[ -z "$state_last_completed" || "$last_completed_count" -ne 1 ]]; then
  fail "last_completed_task_id must identify exactly one backlog task"
elif [[ "$last_completed_status" != "done" ]]; then
  fail "last_completed_task_id must reference a done task: $state_last_completed"
else
  printf 'OK   last_completed_task_id references a done task: %s\n' "$state_last_completed"
fi

mapfile -t dependency_ids < <(awk '
  $1 == "depends_on:" {
    line = $0
    gsub(/.*\[/, "", line)
    gsub(/\].*/, "", line)
    gsub(/,/, " ", line)
    count = split(line, values, /[[:space:]]+/)
    for (i = 1; i <= count; i++) {
      if (values[i] ~ /^BI-[0-9][0-9][0-9]$/) print values[i]
    }
  }
' tasks/backlog.yaml)
for dependency_id in "${dependency_ids[@]}"; do
  dependency_count=$(printf '%s\n' "${task_ids[@]}" | grep -Fxc "$dependency_id" || true)
  if [[ "$dependency_count" -ne 1 ]]; then
    fail "backlog dependency does not identify exactly one task: $dependency_id"
  fi
done

if ((failures > 0)); then
  printf '\nRepository policy result: %d failure(s)\n' "$failures" >&2
  exit 1
fi

printf '\nRepository policy result: PASS\n'
