"""Git as the storage layer: read a branch, and replace a branch with a single orphan commit.

Replacing instead of appending keeps the repository small: history never grows, and git only uploads the files that
actually changed. The remote URL may contain a token, so it is never printed.
"""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

AUTHOR_NAME = "lakewire-bot"
AUTHOR_EMAIL = "lakewire-bot@users.noreply.github.com"
SNAPSHOT_BRANCH = "lakewire-snapshot"
_BASE = ["-c", "core.autocrlf=false", "-c", "core.safecrlf=false"]


def _git(args: list[str], cwd: Path | None = None, hide: str | None = None, check: bool = True):
    proc = subprocess.run(
        ["git", *_BASE, *args], cwd=cwd, capture_output=True, text=True, encoding="utf-8", errors="replace"
    )
    if check and proc.returncode != 0:
        def scrub(text: str) -> str:
            return text.replace(hide, "<remote>") if hide else text

        detail = scrub(proc.stderr.strip())[-400:]
        raise RuntimeError(f"git {scrub(' '.join(args))} failed ({proc.returncode}): {detail}")
    return proc


def fetch_branch(remote: str, branch: str, dest: Path) -> bool:
    """Check the branch out into `dest` (latest commit only). False, with an empty `dest`, if it does not exist."""
    probe = _git(["ls-remote", "--exit-code", "--heads", remote, branch], hide=remote, check=False)
    if probe.returncode == 0:
        if dest.exists():
            shutil.rmtree(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        clone = ["clone", "--quiet", "--depth", "1", "--branch", branch, "--single-branch", remote, str(dest)]
        _git(clone, hide=remote)
        return True
    if probe.returncode == 2:  # --exit-code: 2 means the remote is fine but has no such branch
        if dest.exists():
            shutil.rmtree(dest)
        dest.mkdir(parents=True)
        return False
    raise RuntimeError(f"cannot read the remote: {probe.stderr.replace(remote, '<remote>').strip()[-300:]}")


def push_tree(src: Path, remote: str, branch: str, message: str) -> None:
    """Make the current contents of `src` the one and only commit of `branch`, replacing whatever was there."""
    if (src / ".git").exists():
        _git(["checkout", "--quiet", "--orphan", SNAPSHOT_BRANCH], cwd=src)
    else:
        _git(["init", "--quiet", "-b", SNAPSHOT_BRANCH], cwd=src)
    _git(["add", "-A"], cwd=src)
    _git(["-c", f"user.name={AUTHOR_NAME}", "-c", f"user.email={AUTHOR_EMAIL}", "commit", "--quiet", "--allow-empty",
          "-m", message], cwd=src)
    _git(["push", "--quiet", "--force", remote, f"{SNAPSHOT_BRANCH}:{branch}"], cwd=src, hide=remote)
