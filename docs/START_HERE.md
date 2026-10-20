# Start here

This file is the short entry point for a new coding-agent session. It is navigation only; the binding operating contract is `AGENTS.md`.

## Session bootstrap

1. Read and follow `AGENTS.md`.
2. Use `docs/STATE.md` for the current handoff snapshot.
3. Use `tasks/backlog.yaml` for canonical task status, scope, acceptance criteria, and either the active task or the terminal `active_task_id: null` marker.
4. Read the product specification and relevant accepted ADRs before making changes.
5. Run `./scripts/doctor.sh` before editing and `./scripts/verify.sh` before completion.

## Repository map

| Concern | Canonical file |
| --- | --- |
| Agent operating rules | `AGENTS.md` |
| Product scope and required behavior | `docs/specification/batch-ingestion.md` |
| Architectural decisions | `docs/adr/README.md` and accepted ADRs |
| Active/terminal state and atomic backlog | `tasks/backlog.yaml` |
| Current handoff snapshot | `docs/STATE.md` |
| Task ordering rationale | `docs/IMPLEMENTATION_PLAN.md` |
| Historical session record | `docs/WORKLOG.md` |

The **Possible extensions** section in the product specification is not part of the current scope.
