# Drill 4: delete a day of pageviews and rebuild it

**Type:** planned failure drill. **Date:** 2026-11-23, all times UTC. **Result:** the rebuilt day is identical.

## What we tested

If a day of Silver pageviews is lost or damaged, rerunning the Airflow DAG for that date must restore it exactly.

## Method

1. Recorded the day before touching anything: `scripts/lake_audit.py` for 2026-11-22.
2. Deleted that day from the live Delta table with a Spark `DELTA delete` (`view_date = '2026-11-22'`). The table was then empty.
3. Cleared the DAG's tasks for that date: `airflow tasks clear lakewire_pageviews_daily --start-date 2026-11-22 --end-date 2026-11-22 -y`.
   Airflow reran the sensor and the ingest task, which re-downloaded the 24 hourly dump files from Wikimedia.
4. Ran the audit again and compared.

## Timeline

| Time | Event |
|---|---|
| 07:00:29 | The hourly marts run finished. The delete waited for it, so the published snapshot could not capture a half-rebuilt table. |
| 07:00:42 | Delete started. Table empty at 07:01:10. |
| 07:01:18 | Tasks cleared. DAG run started 07:01:20. |
| 07:16:25 | DAG run succeeded (15 minutes; an earlier clean run took about 9, this one overlapped the consumer catch-up). |

## Evidence

| 2026-11-22 | Before | After rebuild |
|---|---|---|
| Rows | 1,200,000 | 1,200,000 |
| Total views | 93,800,476 | 93,800,476 |
| Content fingerprint (`bit_xor` of a hash of every row) | 10124036022780857597 | 10124036022780857597 |

Row count, total views and a fingerprint over every row are all identical, so the rebuild reproduced the data, not just
its size. It is deterministic because the top 50,000 pages per hour are ranked by views and then page title.

## Why it works

The ingest overwrites exactly one date partition with `replaceWhere view_date = ...`, so rerunning a day is idempotent
and never touches other days.

## Notes

- Airflow runs on SQLite, so it uses the sequential executor: while the ingest ran, the scheduler could not start other
  DAG runs. That is documented in the runbook and is why the delete waited for the 07:00 marts run.
- The manual delete used a second Spark session on a table that Airflow normally owns. It was safe only because no DAG
  run was active. The delete is not needed in a real recovery: rerunning the DAG overwrites the day anyway.
