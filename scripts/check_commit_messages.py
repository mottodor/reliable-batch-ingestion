#!/usr/bin/env python3
"""Validate task-linked commit subjects for hooks and repository policy."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from collections.abc import Sequence, Set
from pathlib import Path

ALLOWED_TYPES = frozenset(
    {
        "build",
        "chore",
        "ci",
        "docs",
        "feat",
        "fix",
        "perf",
        "refactor",
        "style",
        "test",
    }
)
MAX_SUBJECT_LENGTH = 100
_FORMAT_EXAMPLE = "type(scope): outcome (BI-NNN)"
_SUBJECT_PATTERN = re.compile(
    rf"(?P<type>{'|'.join(sorted(ALLOWED_TYPES))})"
    r"\((?P<scope>[a-z0-9]+(?:-[a-z0-9]+)*)\): "
    r"(?P<outcome>[a-z0-9][^\r\n]*?) "
    r"\((?P<task_id>BI-[0-9]{3})\)"
)
_TASK_PATTERN = re.compile(r"^\s*-\s+id:\s+(BI-[0-9]{3})\s*$", re.MULTILINE)


class CommitMessagePolicyError(ValueError):
    """Raised when a message or history range violates the repository contract."""


def load_task_ids(backlog_path: Path) -> frozenset[str]:
    """Load exact task IDs without requiring a YAML runtime dependency."""
    try:
        content = backlog_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise CommitMessagePolicyError("cannot read tasks/backlog.yaml") from error
    task_ids = frozenset(_TASK_PATTERN.findall(content))
    if not task_ids:
        raise CommitMessagePolicyError("tasks/backlog.yaml contains no BI task IDs")
    return task_ids


def validate_subject(subject: str, *, known_task_ids: Set[str]) -> None:
    """Validate one complete commit subject or raise a bounded policy error."""
    if "\n" in subject or "\r" in subject:
        raise CommitMessagePolicyError("subject must be a single line")
    if len(subject) > MAX_SUBJECT_LENGTH:
        raise CommitMessagePolicyError(f"subject must be at most {MAX_SUBJECT_LENGTH} characters")
    match = _SUBJECT_PATTERN.fullmatch(subject)
    if match is None:
        raise CommitMessagePolicyError(f"subject format must match {_FORMAT_EXAMPLE}")
    outcome = match.group("outcome")
    if outcome != outcome.strip() or "  " in outcome:
        raise CommitMessagePolicyError(f"subject format must match {_FORMAT_EXAMPLE}")
    task_id = match.group("task_id")
    if task_id not in known_task_ids:
        raise CommitMessagePolicyError(f"subject references unknown task {task_id}")


def _git(repo_root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise CommitMessagePolicyError(f"git {args[0]} failed while checking history")
    return result.stdout.strip()


def check_commit_range(repo_root: Path, *, after_commit: str) -> tuple[str, ...]:
    """Validate every commit reachable from HEAD after one fixed boundary."""
    known_task_ids = load_task_ids(repo_root / "tasks" / "backlog.yaml")
    _git(repo_root, "cat-file", "-e", f"{after_commit}^{{commit}}")
    ancestor = subprocess.run(
        ["git", "merge-base", "--is-ancestor", after_commit, "HEAD"],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )
    if ancestor.returncode != 0:
        raise CommitMessagePolicyError("history boundary is not an ancestor of HEAD")
    revision_output = _git(repo_root, "rev-list", "--reverse", f"{after_commit}..HEAD")
    revisions = tuple(revision_output.splitlines()) if revision_output else ()
    for revision in revisions:
        subject = _git(repo_root, "show", "-s", "--format=%s", revision)
        try:
            validate_subject(subject, known_task_ids=known_task_ids)
        except CommitMessagePolicyError as error:
            raise CommitMessagePolicyError(
                f"commit {revision[:12]} {subject!r}: {error}"
            ) from error
    return revisions


def _subject_from_message_file(path: Path) -> str:
    try:
        content = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise CommitMessagePolicyError("cannot read commit message file") from error
    lines = content.splitlines()
    if not lines:
        raise CommitMessagePolicyError("commit message has no subject")
    return lines[0]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--subject")
    inputs.add_argument("--message-file", type=Path)
    inputs.add_argument("--commits-after")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    repo_root = Path(__file__).resolve().parents[1]
    try:
        if args.commits_after is not None:
            check_commit_range(repo_root, after_commit=args.commits_after)
        else:
            subject = (
                args.subject
                if args.subject is not None
                else _subject_from_message_file(args.message_file)
            )
            validate_subject(
                subject,
                known_task_ids=load_task_ids(repo_root / "tasks" / "backlog.yaml"),
            )
    except CommitMessagePolicyError as error:
        print("Commit message policy: FAIL", file=sys.stderr)
        print(f"- {error}", file=sys.stderr)
        return 1
    print("Commit message policy: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
