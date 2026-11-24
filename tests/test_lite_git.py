import subprocess
from pathlib import Path

import pytest

from lakewire.lite import gitstore


def make_remote(tmp_path):
    bare = tmp_path / "remote.git"
    subprocess.run(["git", "init", "--quiet", "--bare", str(bare)], check=True)
    return str(bare)


def commits(remote, branch):
    out = subprocess.run(["git", "--git-dir", remote, "rev-list", "--count", branch], capture_output=True, text=True)
    return int(out.stdout.strip())


def test_a_branch_that_does_not_exist_yields_an_empty_directory(tmp_path):
    remote = make_remote(tmp_path)
    dest = tmp_path / "lake"
    assert gitstore.fetch_branch(remote, "data", dest) is False
    assert dest.is_dir() and not any(dest.iterdir())


def test_pushing_replaces_the_branch_with_a_single_commit(tmp_path):
    remote = make_remote(tmp_path)
    work = tmp_path / "work"
    work.mkdir()
    (work / "a.txt").write_text("one")
    (work / "old.txt").write_text("remove me")
    gitstore.push_tree(work, remote, "data", "first")
    assert commits(remote, "data") == 1

    # Read it back, change it, push again: still one commit, removed files are gone.
    lake = tmp_path / "lake"
    assert gitstore.fetch_branch(remote, "data", lake) is True
    assert (lake / "a.txt").read_text() == "one"
    (lake / "old.txt").unlink()
    (lake / "b.txt").write_text("two")
    gitstore.push_tree(lake, remote, "data", "second")
    assert commits(remote, "data") == 1

    again = tmp_path / "again"
    gitstore.fetch_branch(remote, "data", again)
    assert sorted(p.name for p in again.iterdir() if p.name != ".git") == ["a.txt", "b.txt"]


def test_commits_are_authored_by_the_bot_not_by_a_person(tmp_path):
    remote = make_remote(tmp_path)
    work = tmp_path / "work"
    work.mkdir()
    (work / "a.txt").write_text("x")
    gitstore.push_tree(work, remote, "data", "msg")
    out = subprocess.run(["git", "--git-dir", remote, "log", "-1", "--format=%an <%ae>", "data"],
                         capture_output=True, text=True).stdout.strip()
    assert out == "lakewire-bot <lakewire-bot@users.noreply.github.com>"


def test_errors_never_reveal_the_remote_url(tmp_path):
    secret_remote = str(tmp_path / "SECRET-TOKEN-123" / "missing.git")
    work = tmp_path / "work"
    work.mkdir()
    (work / "a.txt").write_text("x")
    with pytest.raises(RuntimeError) as failure:
        gitstore.push_tree(work, secret_remote, "data", "msg")
    assert "SECRET-TOKEN-123" not in str(failure.value)
    with pytest.raises(RuntimeError) as failure:
        gitstore.fetch_branch(secret_remote, "data", Path(tmp_path / "x"))
    assert "SECRET-TOKEN-123" not in str(failure.value)
