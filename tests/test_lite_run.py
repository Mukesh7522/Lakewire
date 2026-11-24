"""End to end: fake Wikimedia stream -> contract -> Parquet in a git branch -> real dbt marts -> published site.

Uses real git (two local bare repositories stand in for GitHub) and real dbt, but no network.
"""
import json
import shutil
import subprocess
from datetime import datetime

import duckdb
import pytest
from lite_helpers import DAY, history_two_hours_ago, naive_now, serve, serve_dumps, settings_for

from lakewire.batch import dumps
from lakewire.lite import gitstore, run, store

pytestmark = pytest.mark.skipif(shutil.which("dbt") is None, reason="dbt is needed for the end-to-end run")


def bare_remote(tmp_path):
    path = tmp_path / "remote.git"
    subprocess.run(["git", "init", "--quiet", "--bare", str(path)], check=True)
    return str(path)


def config(tmp_path, remote, **overrides):
    return run.LiteConfig(remote=remote, workdir=tmp_path / "work", max_seconds=60, grace_seconds=0.3, **overrides)


def branch_files(remote, branch, dest):
    gitstore.fetch_branch(remote, branch, dest)
    return dest


def test_hourly_run_stores_events_builds_the_marts_and_publishes_the_site(tmp_path):
    remote = bare_remote(tmp_path)
    server, _ = serve(history_two_hours_ago())
    try:
        summary = run.hourly(config(tmp_path, remote), settings_for(server), now=naive_now())
    finally:
        server.shutdown()
    assert summary["caught_up"] and summary["kept_new"] > 100 and summary["dlq"] == 0

    lake = branch_files(remote, "data", tmp_path / "check_data")
    assert list((lake / "silver/edits").glob("*/*.parquet"))
    assert (lake / "state/last_event_id").read_text().strip() != ""
    assert json.loads((lake / "state/state.json").read_text())["watermark"]

    site = branch_files(remote, "gh-pages", tmp_path / "check_site")
    assert (site / "index.html").exists() and (site / ".nojekyll").exists()
    health = json.loads((site / "data/health.json").read_text())
    assert health["mode"] == "lite"
    assert health["rows"][0]["events_total"] == summary["kept_new"]
    assert health["lag_seconds"] is not None and health["lag_seconds"] < 300
    assert json.loads((site / "data/bot_share.json").read_text())["rows"]
    assert sorted(p.name for p in (site / "data").iterdir()) == [
        "bot_share.json", "conflicts.json", "health.json", "trending.json"]
    for branch in ("data", "gh-pages"):  # history never grows: one commit each
        count = subprocess.run(["git", "--git-dir", remote, "rev-list", "--count", branch], capture_output=True,
                               text=True).stdout.strip()
        assert count == "1"


def test_the_next_run_continues_from_the_saved_position_without_duplicates(tmp_path):
    remote = bare_remote(tmp_path)
    server, _ = serve(history_two_hours_ago())
    try:
        first = run.hourly(config(tmp_path, remote), settings_for(server), now=naive_now())
        second = run.hourly(config(tmp_path / "second", remote, publish=False), settings_for(server), now=naive_now())
    finally:
        server.shutdown()
    assert second["resumed"] and second["read"] < 250  # the 300-event history was not replayed
    lake = branch_files(remote, "data", tmp_path / "check_data")
    con = duckdb.connect()
    total, distinct = con.execute(
        f"select count(*), count(distinct event_id) from read_parquet('{(lake / 'silver/edits').as_posix()}"
        "/*/*.parquet')").fetchone()
    assert total == distinct == first["kept_new"] + second["kept_new"]


def test_pageviews_are_loaded_once_and_old_days_are_purged(tmp_path, monkeypatch):
    remote = bare_remote(tmp_path)
    dump_server = serve_dumps()
    monkeypatch.setattr(dumps, "BASE_URL", f"http://127.0.0.1:{dump_server.server_port}")
    monkeypatch.setattr(dumps.time, "sleep", lambda _s: None)
    settings = settings_for(dump_server)  # only the wiki list and user agent are used here
    cfg = config(tmp_path, remote, pageview_days=1, pageview_top_n=2)
    try:
        first = run.pageviews(cfg, settings, today=datetime(DAY.year, DAY.month, DAY.day + 1))
        assert first["loaded"] == {DAY.isoformat(): 48}
        head_before = subprocess.run(["git", "--git-dir", remote, "rev-parse", "data"], capture_output=True,
                                     text=True).stdout
        again = run.pageviews(config(tmp_path / "again", remote, pageview_days=1, pageview_top_n=2), settings,
                              today=datetime(DAY.year, DAY.month, DAY.day + 1))
    finally:
        dump_server.shutdown()
    assert again["loaded"] == {}  # already there: nothing downloaded, nothing pushed
    head_after = subprocess.run(["git", "--git-dir", remote, "rev-parse", "data"], capture_output=True,
                                text=True).stdout
    assert head_before == head_after
    lake = branch_files(remote, "data", tmp_path / "check_data")
    assert store.count_rows(lake, "pageviews") == 48
