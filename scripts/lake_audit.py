"""Read-only audit of the lake. Used by the failure drills in docs/RUNBOOK.md to prove no data was lost or doubled.

Run it inside the Airflow container, which has DuckDB with the delta extension and the S3 settings:

    docker compose exec -T airflow /opt/dbt-venv/bin/python - < scripts/lake_audit.py

Checks:
  * Bronze: per Kafka partition, rows vs the offset range. gaps = (max - min + 1) - rows must be 0.
  * Bronze: duplicate (partition, offset) pairs must be 0.
  * Silver: duplicate event_id values must be 0.
  * Silver vs Bronze: distinct Bronze event_ids missing from Silver. Anything older than 5 minutes must be 0.
  * Pageviews: rows, total views and a content fingerprint per day (to prove a rebuilt day is identical).
"""
from __future__ import annotations

import json
import os

import duckdb

bucket = os.getenv("LAKE_BUCKET", "lake")
con = duckdb.connect()
con.execute("INSTALL httpfs; INSTALL delta; LOAD httpfs; LOAD delta;")
con.execute(
    "CREATE SECRET lake (TYPE s3, KEY_ID '{k}', SECRET '{s}', REGION 'us-east-1', ENDPOINT '{e}', "
    "URL_STYLE 'path', USE_SSL false)".format(
        k=os.getenv("S3_ACCESS_KEY", "lakewire"), s=os.environ["S3_SECRET_KEY"], e=os.getenv("S3_HOSTPORT", "s3:9000")
    )
)
bronze = f"delta_scan('s3://{bucket}/bronze/edits')"
silver = f"delta_scan('s3://{bucket}/silver/edits')"
pageviews = f"delta_scan('s3://{bucket}/silver/pageviews')"


def one(sql: str):
    return con.execute(sql).fetchone()[0]


report: dict = {}

partitions = con.execute(
    f"select kafka_partition, min(kafka_offset), max(kafka_offset), count(*) from {bronze} "
    "group by 1 order by 1"
).fetchall()
report["bronze_partitions"] = [
    {"partition": p, "min_offset": lo, "max_offset": hi, "rows": n, "gaps": (hi - lo + 1) - n}
    for p, lo, hi, n in partitions
]
report["bronze_rows"] = one(f"select count(*) from {bronze}")
report["bronze_duplicate_offsets"] = one(
    f"select count(*) from (select kafka_partition, kafka_offset from {bronze} "
    "group by 1, 2 having count(*) > 1)"
)
report["bronze_distinct_events"] = one(
    f"select count(distinct json_extract_string(raw_json, '$.event_id')) from {bronze}"
)
report["silver_rows"] = one(f"select count(*) from {silver}")
report["silver_distinct_events"] = one(f"select count(distinct event_id) from {silver}")
report["silver_duplicate_events"] = report["silver_rows"] - report["silver_distinct_events"]
missing = (
    f"select count(*) from (select distinct json_extract_string(raw_json, '$.event_id') as id, "
    f"max(ingested_at) as at from {bronze} group by 1) b where b.id not in (select event_id from {silver})"
)
report["bronze_events_missing_from_silver_total"] = one(missing)
# Events written to Bronze in the last 5 minutes may legitimately still be in flight to Silver.
report["bronze_events_missing_from_silver_settled"] = one(missing + " and b.at < now() - interval 5 minute")
try:
    report["pageviews_by_day"] = {
        str(d): {"rows": n, "total_views": v, "fingerprint": f}
        for d, n, v, f in con.execute(
            "select view_date, count(*), sum(views), "
            "bit_xor(hash(page_title || '|' || view_hour::varchar || '|' || views::varchar)) "
            f"from {pageviews} group by 1 order by 1"
        ).fetchall()
    }
except duckdb.Error as exc:  # table may not exist yet on a fresh deploy
    report["pageviews_by_day"] = f"unavailable: {str(exc)[:80]}"

report["ok"] = (
    all(p["gaps"] == 0 for p in report["bronze_partitions"])
    and report["bronze_duplicate_offsets"] == 0
    and report["silver_duplicate_events"] == 0
    and report["bronze_events_missing_from_silver_settled"] == 0
)
print(json.dumps(report, indent=2, default=str))
