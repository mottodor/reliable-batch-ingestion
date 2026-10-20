# Commit-message policy

Every durable commit after BI-100 must use this subject format:

```text
type(scope): outcome (BI-NNN)
```

Example:

```text
feat(api): preserve raw page before parsing (BI-102)
```

## Install the local hook

Run once in each clone:

```bash
make hooks-install
```

This explicitly sets the repository-local Git configuration:

```text
core.hooksPath=.githooks
```

The versioned `.githooks/commit-msg` hook invokes the same standard-library validator as the repository gate. Installation is idempotent. Setup, tests, imports, and verification do not silently modify Git configuration.

## Subject contract

| Part | Rule |
| --- | --- |
| `type` | One of `build`, `chore`, `ci`, `docs`, `feat`, `fix`, `perf`, `refactor`, `style`, or `test` |
| `scope` | Required lowercase ASCII kebab-case, for example `api`, `git`, or `postgres-snapshot` |
| separator | Exactly `: ` |
| `outcome` | Non-empty, starts with a lowercase ASCII letter or digit, has no leading/trailing or repeated spaces |
| task | Exactly `(BI-NNN)`, and that ID must exist in `tasks/backlog.yaml` |
| length | At most 100 characters for the complete subject |

Commit bodies are optional. Merge, revert, fixup, and squash subjects have no automatic exception: `main` is linear and every durable commit must identify its atomic backlog task.

Valid examples:

```text
docs(git): explain local hook installation (BI-101)
test(git-policy): reject unknown task references (BI-101)
chore(backlog): close commit-message enforcement (BI-101)
```

Invalid examples:

```text
feat: missing scope (BI-101)
Feat(git): uppercase type (BI-101)
feat(Git): uppercase scope (BI-101)
feat(git): Uppercase outcome (BI-101)
feat(git): unknown task (BI-999)
feat(git): missing task
```

## Run the durable check

Validate every commit after the BI-100 policy boundary:

```bash
make commit-message-check
```

The same range check runs through:

```bash
make policy
make verify
```

`git commit --no-verify` can bypass the local hook, as with any Git hook, but it cannot make an invalid committed subject pass `make policy` or `make verify`. Correct the subject before sharing the commit.

Earlier history is outside the enforcement range and is not rewritten by this policy.
