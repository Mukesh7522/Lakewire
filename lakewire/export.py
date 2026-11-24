"""Export small JSON snapshots of the Gold marts for the public dashboard."""
from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import duckdb

QUERIES = {
    "health": "select * from mart_pipeline_health",
    "trending": """
        select page_title, view_hour, views, views_prev_day, growth_vs_prev_day, edits_1h, trend_score
        from mart_trending_articles
        where view_hour = (select max(view_hour) from mart_trending_articles)
        order by trend_score desc limit 25""",
    "bot_share": """
        select wiki, hour_ts, edits_total, bot_edits, human_edits, bot_share
        from mart_bot_vs_human
        where hour_ts >= (select max(hour_ts) from mart_bot_vs_human) - interval 72 hour
        order by hour_ts, wiki""",
    "conflicts": """
        select wiki, title, event_date, edits, distinct_editors, rapid_alternations, bytes_swing, conflict_score
        from mart_edit_conflict_signals
        where is_flagged
          and event_date >= (select max(event_date) from mart_edit_conflict_signals) - interval 2 day
        order by conflict_score desc limit 25""",
}


def _jsonable(value):
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat() if value.tzinfo else value.isoformat() + "Z"
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def run_query(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict]:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [{c: _jsonable(v) for c, v in zip(cols, row, strict=True)} for row in cur.fetchall()]


def export(db_path: str, out_dir: str) -> dict[str, int]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(db_path, read_only=True)
    now = datetime.now(UTC)
    counts: dict[str, int] = {}
    try:
        for name, sql in QUERIES.items():
            rows = run_query(con, sql)
            payload: dict = {"generated_at": now.isoformat(), "rows": rows}
            if name == "health" and rows:
                latest = rows[0].get("latest_event_ts")
                lag = None
                if latest:
                    ts = datetime.fromisoformat(str(latest).replace("Z", "+00:00"))
                    ts = ts if ts.tzinfo else ts.replace(tzinfo=UTC)
                    lag = int((now - ts).total_seconds())
                payload["lag_seconds"] = lag
                # Which pipeline produced this snapshot: "full" (Kafka, Spark, Airflow) or "lite" (GitHub Actions).
                # The dashboard uses it to describe itself accurately.
                payload["mode"] = os.getenv("PIPELINE_MODE", "full")
            tmp = out / f".{name}.json.tmp"
            tmp.write_text(json.dumps(payload, indent=1, default=str))
            tmp.replace(out / f"{name}.json")  # atomic swap so readers never see half a file
            counts[name] = len(rows)
    finally:
        con.close()
    return counts


if __name__ == "__main__":
    db = os.getenv("DUCKDB_PATH", "/data/warehouse/lakewire.duckdb")
    target = os.getenv("EXPORT_DIR", "/data/exports")
    print(json.dumps(export(db, target)))
    sys.exit(0)
