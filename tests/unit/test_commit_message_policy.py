"""Contract tests for local and repository commit-message enforcement."""

from __future__ import annotations

import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest
from scripts import check_commit_messages

ALLOWED_TYPES = check_commit_messages.ALLOWED_TYPES
MAX_SUBJECT_LENGTH = check_commit_messages.MAX_SUBJECT_LENGTH
CommitMessagePolicyError = check_commit_messages.CommitMessagePolicyError
check_commit_range = check_commit_messages.check_commit_range
validate_subject = check_commit_messages.validate_subject

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CHECK_SCRIPT = REPOSITORY_ROOT / "scripts" / "check_commit_messages.py"
HOOK_PATH = REPOSITORY_ROOT / ".githooks" / "commit-msg"
MAKEFILE_PATH = REPOSITORY_ROOT / "Makefile"
POLICY_SCRIPT_PATH = REPOSITORY_ROOT / "scripts" / "check-repository-policy.sh"
GUIDE_PATH = REPOSITORY_ROOT / "docs" / "guides" / "commit-messages.md"
README_PATH = REPOSITORY_ROOT / "README.md"
KNOWN_TASKS = frozenset({"BI-101"})


def _run(*args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
    )


def _git(repo: Path, *args: str) -> str:
    result = _run("git", *args, cwd=repo)
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def _commit(repo: Path, subject: str) -> str:
    tracked = repo / "tracked.txt"
    tracked.write_text(tracked.read_text(encoding="utf-8") + "x", encoding="utf-8")
    _git(repo, "add", "tracked.txt")
    _git(repo, "commit", "-m", subject)
    return _git(repo, "rev-parse", "HEAD")


@pytest.mark.parametrize("commit_type", sorted(ALLOWED_TYPES))
def test_subject_accepts_every_allowed_type(commit_type: str) -> None:
    validate_subject(
        f"{commit_type}(git-policy): enforce commit subjects (BI-101)",
        known_task_ids=KNOWN_TASKS,
    )


def test_subject_accepts_exact_maximum_length() -> None:
    prefix = "chore(git): "
    suffix = " (BI-101)"
    outcome = "a" * (MAX_SUBJECT_LENGTH - len(prefix) - len(suffix))
    subject = f"{prefix}{outcome}{suffix}"

    assert len(subject) == MAX_SUBJECT_LENGTH
    validate_subject(subject, known_task_ids=KNOWN_TASKS)


@pytest.mark.parametrize(
    ("subject", "expected_fragment"),
    [
        ("feat: add policy (BI-101)", "format"),
        ("Feat(git): add policy (BI-101)", "format"),
        ("feat(Git): add policy (BI-101)", "format"),
        ("feat(git_policy): add policy (BI-101)", "format"),
        ("feat(git):  add policy (BI-101)", "format"),
        ("feat(git): Add policy (BI-101)", "format"),
        ("feat(git): add policy", "format"),
        ("feat(git): add policy (BI-12)", "format"),
        ("feat(git): add policy (BI-999)", "unknown task"),
        ("feat(git): add policy (BI-101)\nbody", "single line"),
    ],
)
def test_subject_rejects_malformed_values(subject: str, expected_fragment: str) -> None:
    with pytest.raises(CommitMessagePolicyError, match=expected_fragment):
        validate_subject(subject, known_task_ids=KNOWN_TASKS)


def test_subject_rejects_more_than_maximum_length() -> None:
    prefix = "chore(git): "
    suffix = " (BI-101)"
    outcome = "a" * (MAX_SUBJECT_LENGTH + 1 - len(prefix) - len(suffix))

    with pytest.raises(CommitMessagePolicyError, match="100 characters"):
        validate_subject(f"{prefix}{outcome}{suffix}", known_task_ids=KNOWN_TASKS)


def test_cli_accepts_valid_subject_and_rejects_invalid_subject() -> None:
    valid = _run(
        sys.executable,
        str(CHECK_SCRIPT),
        "--subject",
        "test(git): cover message policy (BI-101)",
        cwd=REPOSITORY_ROOT,
    )
    invalid = _run(
        sys.executable,
        str(CHECK_SCRIPT),
        "--subject",
        "invalid subject",
        cwd=REPOSITORY_ROOT,
    )

    assert valid.returncode == 0
    assert valid.stdout == "Commit message policy: PASS\n"
    assert valid.stderr == ""
    assert invalid.returncode == 1
    assert invalid.stdout == ""
    assert invalid.stderr.startswith("Commit message policy: FAIL\n")
    assert "format" in invalid.stderr


def test_commit_range_checks_only_commits_after_boundary(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "--initial-branch=main")
    _git(repo, "config", "user.name", "Commit Policy Test")
    _git(repo, "config", "user.email", "commit-policy@example.invalid")
    (repo / "tasks").mkdir()
    (repo / "tasks" / "backlog.yaml").write_text(
        "tasks:\n  - id: BI-101\n",
        encoding="utf-8",
    )
    (repo / "tracked.txt").write_text("baseline", encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "legacy subject outside policy")
    boundary = _git(repo, "rev-parse", "HEAD")
    valid_commit = _commit(repo, "test(git): validate temporary range (BI-101)")

    assert check_commit_range(repo, after_commit=boundary) == (valid_commit,)

    invalid_commit = _commit(repo, "invalid bypassed subject")
    with pytest.raises(CommitMessagePolicyError) as error:
        check_commit_range(repo, after_commit=boundary)
    assert invalid_commit[:12] in str(error.value)
    assert "invalid bypassed subject" in str(error.value)


def test_versioned_hook_uses_canonical_validator(tmp_path: Path) -> None:
    mode = HOOK_PATH.stat().st_mode
    hook = HOOK_PATH.read_text(encoding="utf-8")
    valid_message = tmp_path / "valid-message"
    invalid_message = tmp_path / "invalid-message"
    valid_message.write_text("test(git): exercise commit hook (BI-101)\n", encoding="utf-8")
    invalid_message.write_text("invalid hook subject\n", encoding="utf-8")

    valid = _run(str(HOOK_PATH), str(valid_message), cwd=REPOSITORY_ROOT)
    invalid = _run(str(HOOK_PATH), str(invalid_message), cwd=REPOSITORY_ROOT)

    assert mode & stat.S_IXUSR
    assert "scripts/check_commit_messages.py" in hook
    assert "--message-file" in hook
    assert valid.returncode == 0
    assert invalid.returncode == 1
    assert "Commit message policy: FAIL" in invalid.stderr


def test_make_policy_and_documentation_expose_commit_workflow() -> None:
    makefile = MAKEFILE_PATH.read_text(encoding="utf-8")
    policy = POLICY_SCRIPT_PATH.read_text(encoding="utf-8")
    guide = GUIDE_PATH.read_text(encoding="utf-8")
    readme = README_PATH.read_text(encoding="utf-8")

    assert "commit-message-check:" in makefile
    assert "hooks-install:" in makefile
    assert "core.hooksPath .githooks" in makefile
    assert "check_commit_messages.py --commits-after" in policy
    assert "type(scope): outcome (BI-NNN)" in guide
    assert "make hooks-install" in guide
    assert "make commit-message-check" in guide
    assert "--no-verify" in guide
    assert "commit-messages.md" in readme


def test_installed_hook_path_is_exact_when_present() -> None:
    result = _run("git", "config", "--local", "--get", "core.hooksPath", cwd=REPOSITORY_ROOT)

    assert result.returncode == 0
    assert result.stdout.strip() == ".githooks"
    assert result.stderr == ""


def test_hook_has_portable_shell_shebang() -> None:
    first_line = HOOK_PATH.read_text(encoding="utf-8").splitlines()[0]

    assert first_line == "#!/bin/sh"
    assert os.access(HOOK_PATH, os.X_OK)
