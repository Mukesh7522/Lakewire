"""A tiny file-based lakehouse for the GitHub Actions pipeline: Parquet files in plain directories.

The whole tree is what gets stored in the `data` branch:

    silver/edits/YYYY-MM-DD/HH.parquet         one file per hour of event time, rewritten as late events arrive
    silver/pageviews/YYYY-MM-DD/part.parquet   the top pages per hour for one day
    state/last_event_id                        Wikimedia stream position (written by the producer's resume logic)
    state/state.json                           bookkeeping used to detect gaps between runs
    dlq/recent.jsonl                           the most recent contract violations

Everything is idempotent: merging the same events twice leaves the files unchanged.
"""
from __future__ import annotations

import json
import os
import shutil
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

EDIT_COLUMNS = (
    ("event_id", "VARCHAR"),
    ("event_ts", "TIMESTAMP"),
    ("event_date", "DATE"),
    ("wiki", "VARCHAR"),
    ("event_type", "VARCHAR"),
    ("title", "VARCHAR"),
    ("namespace", "BIGINT"),
    ("user", "VARCHAR"),
    ("is_bot", "BOOLEAN"),
    ("is_minor", "BOOLEAN"),
    ("length_old", "BIGINT"),
    ("length_new", "BIGINT"),
    ("rev_old", "BIGINT"),
    ("rev_new", "BIGINT"),
    ("ingested_at", "TIMESTAMP"),
)
_COLUMN_SQL = ", ".join(f'"{name}" {sql_type}' for name, sql_type in EDIT_COLUMNS)
_PLACEHOLDERS = ", ".join("?" for _ in EDIT_COLUMNS)


def parse_ts(value: str) -> datetime:
    """ISO 8601 text (with a Z or an offset) to a naive datetime in UTC, which is how timestamps are stored."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(UTC).replace(tzinfo=None) if parsed.tzinfo else parsed


def utc_now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _literal(path: Path) -> str:
    """A SQL string literal for a path we built ourselves (single quotes doubled for safety)."""
    return "'" + str(path).replace("\\", "/").replace("'", "''") + "'"


def edit_row(event: dict[str, Any], ingested_at: datetime) -> tuple:
    ts = parse_ts(event["event_ts"])
    return (
        event["event_id"], ts, ts.date(), event["wiki"], event["event_type"], event["title"],
        event.get("namespace"), event["user"], bool(event.get("is_bot")), bool(event.get("is_minor")),
        event.get("length_old"), event.get("length_new"), event.get("rev_old"), event.get("rev_new"), ingested_at,
    )


@dataclass
class MergeStats:
    received: int = 0
    new_rows: int = 0
    hours_touched: int = 0


def merge_edits(root: Path, events: Iterable[dict[str, Any]], ingested_at: datetime | None = None) -> MergeStats:
    """Add normalised events to the hourly files, dropping any event_id that is already stored."""
    ingested_at = ingested_at or utc_now()
    by_hour: dict[tuple[date, int], list[tuple]] = defaultdict(list)
    stats = MergeStats()
    for event in events:
        row = edit_row(event, ingested_at)
        by_hour[(row[2], row[1].hour)].append(row)
        stats.received += 1
    con = duckdb.connect()
    try:
        for (day, hour), rows in sorted(by_hour.items()):
            path = root / "silver" / "edits" / day.isoformat() / f"{hour:02d}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            con.execute("DROP TABLE IF EXISTS incoming")
            con.execute(f"CREATE TEMP TABLE incoming ({_COLUMN_SQL})")
            con.executemany(f"INSERT INTO incoming VALUES ({_PLACEHOLDERS})", rows)
            before = 0
            source = "SELECT * FROM incoming"
            if path.exists():
                before = con.execute(f"SELECT count(*) FROM read_parquet({_literal(path)})").fetchone()[0]
                source = f"SELECT * FROM read_parquet({_literal(path)}) UNION ALL BY NAME SELECT * FROM incoming"
            con.execute("DROP TABLE IF EXISTS merged")
            con.execute(
                f"CREATE TEMP TABLE merged AS SELECT * FROM ({source}) "
                "QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY ingested_at, event_ts) = 1"
            )
            after = con.execute("SELECT count(*) FROM merged").fetchone()[0]
            tmp = path.with_name(f".{path.stem}.tmp")  # not *.parquet, so readers never pick up half a file
            con.execute(
                f"COPY (SELECT * FROM merged ORDER BY event_ts, event_id) TO {_literal(tmp)} "
                "(FORMAT parquet, COMPRESSION zstd)"
            )
            os.replace(tmp, path)
            stats.new_rows += after - before
            stats.hours_touched += 1
    finally:
        con.close()
    return stats


def purge_older_than(root: Path, table: str, days: int, today: date | None = None) -> list[str]:
    """Delete whole day directories older than `days` days. Returns the names removed."""
    today = today or utc_now().date()
    cutoff = today - timedelta(days=days)
    base = root / "silver" / table
    removed: list[str] = []
    if not base.exists():
        return removed
    for child in sorted(base.iterdir()):
        try:
            day = date.fromisoformat(child.name)
        except ValueError:
            continue
        if child.is_dir() and day < cutoff:
            shutil.rmtree(child)
            removed.append(child.name)
    return removed


def count_rows(root: Path, table: str) -> int:
    files = list((root / "silver" / table).glob("*/*.parquet"))
    if not files:
        return 0
    con = duckdb.connect()
    try:
        pattern = (root / "silver" / table).as_posix() + "/*/*.parquet"
        return con.execute(f"SELECT count(*) FROM read_parquet('{pattern}', union_by_name=true)").fetchone()[0]
    finally:
        con.close()


def append_dlq(root: Path, records: list[dict[str, Any]], keep: int = 500, now: datetime | None = None) -> int:
    """Remember the most recent contract violations (newest last), trimmed to `keep` lines."""
    if not records:
        return 0
    path = root / "dlq" / "recent.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    stamp = (now or utc_now()).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    for record in records:
        payload = json.dumps(record.get("payload"), default=str)
        lines.append(json.dumps({"at": stamp, "errors": record.get("errors"), "payload": payload[:1000]}))
    path.write_text("\n".join(lines[-keep:]) + "\n", encoding="utf-8")
    return len(records)


def read_state(root: Path) -> dict[str, Any]:
    path = root / "state" / "state.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def write_state(root: Path, state: dict[str, Any]) -> None:
    path = root / "state" / "state.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=1, sort_keys=True), encoding="utf-8")
