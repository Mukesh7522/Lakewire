import json

import duckdb

from lakewire import export

TABLES = {
    "mart_pipeline_health": "built_at timestamp, latest_event_ts timestamp, events_last_hour bigint, "
                            "events_last_24h bigint, events_total bigint, wikis bigint",
    "mart_trending_articles": "page_title varchar, view_hour timestamp, views bigint, views_prev_day bigint, "
                              "growth_vs_prev_day double, edits_1h bigint, trend_score double",
    "mart_bot_vs_human": "wiki varchar, hour_ts timestamp, edits_total bigint, bot_edits bigint, "
                         "human_edits bigint, bot_share double",
    "mart_edit_conflict_signals": "wiki varchar, title varchar, event_date date, edits bigint, "
                                  "distinct_editors bigint, rapid_alternations bigint, bytes_swing bigint, "
                                  "conflict_score bigint, is_flagged boolean",
}


def _database(tmp_path):
    path = str(tmp_path / "marts.duckdb")
    con = duckdb.connect(path)
    for name, columns in TABLES.items():
        con.execute(f"create table {name} ({columns})")
    con.execute("insert into mart_pipeline_health values (now(), now() - interval 5 minute, 10, 100, 1000, 2)")
    con.close()
    return path


def test_health_snapshot_records_which_pipeline_produced_it(tmp_path, monkeypatch):
    db = _database(tmp_path)
    monkeypatch.delenv("PIPELINE_MODE", raising=False)
    export.export(db, str(tmp_path / "out_full"))
    assert json.loads((tmp_path / "out_full" / "health.json").read_text())["mode"] == "full"

    monkeypatch.setenv("PIPELINE_MODE", "lite")
    export.export(db, str(tmp_path / "out_lite"))
    health = json.loads((tmp_path / "out_lite" / "health.json").read_text())
    assert health["mode"] == "lite"
    assert 250 <= health["lag_seconds"] <= 400  # the newest event is about 5 minutes old
