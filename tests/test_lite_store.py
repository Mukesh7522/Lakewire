from datetime import date, datetime

import duckdb

from lakewire.lite import store


def ev(i, ts="2026-11-23T11:15:00.000Z", wiki="enwiki", **extra):
    base = {"event_id": f"e{i}", "event_ts": ts, "wiki": wiki, "event_type": "edit", "title": f"T{i}",
            "namespace": 0, "user": "U", "is_bot": False, "is_minor": False, "length_old": 10, "length_new": 12,
            "rev_old": 1, "rev_new": 2}
    return {**base, **extra}


def rows(root, table="edits"):
    con = duckdb.connect()
    pattern = (root / "silver" / table).as_posix() + "/*/*.parquet"
    return con.execute(f"select * from read_parquet('{pattern}') order by event_id").fetchall()


def test_timestamps_with_z_or_offset_become_naive_utc():
    assert store.parse_ts("2026-11-23T11:15:00.250Z") == datetime(2026, 11, 23, 11, 15, 0, 250000)
    assert store.parse_ts("2026-11-23T16:45:00+05:30") == datetime(2026, 11, 23, 11, 15)


def test_merge_writes_one_file_per_hour_of_event_time(tmp_path):
    stats = store.merge_edits(tmp_path, [ev(1), ev(2, ts="2026-11-23T11:59:59Z"), ev(3, ts="2026-11-23T12:00:01Z")])
    assert (stats.received, stats.new_rows, stats.hours_touched) == (3, 3, 2)
    assert (tmp_path / "silver/edits/2026-11-23/11.parquet").exists()
    assert (tmp_path / "silver/edits/2026-11-23/12.parquet").exists()
    assert not list(tmp_path.rglob("*.tmp"))  # no half-written files left behind


def test_merging_the_same_events_again_changes_nothing(tmp_path):
    events = [ev(i) for i in range(5)]
    store.merge_edits(tmp_path, events)
    first = rows(tmp_path)
    again = store.merge_edits(tmp_path, events)
    assert again.new_rows == 0
    assert rows(tmp_path) == first


def test_late_events_are_added_to_an_existing_hour_and_duplicates_dropped(tmp_path):
    store.merge_edits(tmp_path, [ev(1), ev(2)])
    stats = store.merge_edits(tmp_path, [ev(2), ev(3), ev(3)])  # one already stored, one repeated in the batch
    assert stats.new_rows == 1
    assert [r[0] for r in rows(tmp_path)] == ["e1", "e2", "e3"]


def test_stored_columns_match_what_dbt_reads(tmp_path):
    store.merge_edits(tmp_path, [ev(1)])
    con = duckdb.connect()
    described = con.execute(
        f"describe select * from read_parquet('{(tmp_path / 'silver/edits').as_posix()}/*/*.parquet')").fetchall()
    assert [(name, kind) for name, kind, *_ in described] == [(n, t) for n, t in store.EDIT_COLUMNS]
    assert store.count_rows(tmp_path, "edits") == 1
    assert store.count_rows(tmp_path, "pageviews") == 0


def test_purge_removes_only_old_day_directories(tmp_path):
    for day in ("2026-11-10", "2026-11-16", "2026-11-23"):
        (tmp_path / "silver/edits" / day).mkdir(parents=True)
        (tmp_path / "silver/edits" / day / "00.parquet").write_bytes(b"x")
    (tmp_path / "silver/edits/not-a-date").mkdir()
    removed = store.purge_older_than(tmp_path, "edits", days=7, today=date(2026, 11, 23))
    assert removed == ["2026-11-10"]
    assert sorted(p.name for p in (tmp_path / "silver/edits").iterdir()) == ["2026-11-16", "2026-11-23", "not-a-date"]
    assert store.purge_older_than(tmp_path, "pageviews", days=3) == []  # a table that does not exist yet


def test_dead_letter_log_keeps_only_the_most_recent_lines(tmp_path):
    for batch in range(5):
        store.append_dlq(tmp_path, [{"errors": [f"bad {batch}"], "payload": {"n": batch}}], keep=3)
    lines = (tmp_path / "dlq/recent.jsonl").read_text().splitlines()
    assert len(lines) == 3 and "bad 4" in lines[-1] and "bad 2" in lines[0]
    assert store.append_dlq(tmp_path, []) == 0


def test_state_round_trips(tmp_path):
    assert store.read_state(tmp_path) == {}
    store.write_state(tmp_path, {"watermark": "2026-11-23T11:00:00", "runs": 3})
    assert store.read_state(tmp_path) == {"runs": 3, "watermark": "2026-11-23T11:00:00"}
