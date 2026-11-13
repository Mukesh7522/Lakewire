# Lakewire

A live streaming and batch lakehouse for Wikipedia activity. It runs 24/7 on a free VM, deploys itself from GitHub, and publishes a public dashboard.

```
Wikimedia EventStreams ─► producer ─► Kafka ─► Spark Structured Streaming ─► Delta (Bronze ─► Silver) ─┐
                         (contract + DLQ)                                      on SeaweedFS (S3-compatible) │
Wikimedia pageview dumps ─► Airflow ─► PySpark ─► Delta (Silver, per-day overwrite) ─────────────────────┤
                                                                                                        ▼
                                          dbt + DuckDB (hourly, Airflow) ─► Gold marts ─► JSON ─► GitHub Pages
Prometheus + Grafana  ·  Slack alerts  ·  GitHub Actions CI/CD
```

Everything runs from `docker-compose.yml`. All software is open source. Hosting can be $0 on a free-tier VM with a public GitHub repo, with caveats (see `docs/DEPLOY.md`).

## What runs where

| Piece | Where | Schedule |
|---|---|---|
| Producer, Kafka, Spark streaming, SeaweedFS (S3), Airflow, Prometheus, Grafana | Docker on one VM | always on, `restart: unless-stopped` |
| Marts refresh + JSON export | Airflow `lakewire_marts_hourly` | hourly |
| Pageview backfill | Airflow `lakewire_pageviews_daily` | daily, waits for the dump, retries |
| Delta compaction, retention, VACUUM | inside the streaming driver | every 6 h / daily |
| CI (lint, tests, dbt, end-to-end) | GitHub Actions `ci.yml` | every push and PR |
| CD (deploy to VM + health check) | GitHub Actions `cd.yml` | after CI passes on `main` |
| Dashboard publish + freshness alarm | GitHub Actions `publish.yml` | every 2 h |

## Design decisions worth knowing

- **One writer per Delta table.** Delta on S3-compatible storage has no atomic put-if-absent, so compaction and retention run inside the same JVM as the streaming queries instead of in a second process. Airflow only writes the pageviews table.
- **Effectively-once.** Kafka is at-least-once. A watermark plus `dropDuplicatesWithinWatermark` bounds state, and an insert-only `MERGE` on `event_id` makes redelivery harmless. CI replays the same events twice and asserts Silver does not change.
- **Data contract at the edge.** Events that break `lakewire/contracts/recentchange.schema.json` go to `wiki.edits.dlq`. Additive fields pass; type changes and missing required fields do not.
- **Disk budget.** Bronze keeps 14 days, Silver 120 days, Kafka 48 hours, and only a few wikis are ingested. Marts in DuckDB are small and kept. Measure your real growth in the first week (`docs/RUNBOOK.md`).
- **Honest freshness.** The dashboard shows a banner when the newest event is older than 30 minutes, and `publish.yml` fails (GitHub emails you) at 45.

## Test status

Run in the authoring sandbox (no Docker there):

- `ruff` clean, `pytest` 16 passed: data contract, SSE parser, live-stream reconnect with `Last-Event-ID` against a local server, Spark transforms, streaming dedupe with watermark, pageview parsing, pageview download retry after a dropped connection.
- `dbt build --target ci` on parquet fixtures: 24 of 24 models and tests pass, twice (full and incremental).

**Verified locally with Docker** (the `end-to-end` CI job's steps, run by hand against SeaweedFS as the S3 store):

- Producer and Spark images build and run as a non-root user; Kafka, SeaweedFS and Spark start together.
- Kafka to Bronze to Silver with Delta on SeaweedFS: 15 Bronze, 12 distinct Silver, 2 dead-letter messages.
- Replay idempotency: after replaying the same events, Silver is unchanged and the dead-letter count doubles.
- DuckDB `delta_scan` reading Delta from SeaweedFS: `dbt build --target prod` passes 24 of 24, and the export reports 12 events.

**Verified locally with the full stack running against the live Wikimedia stream** (`docker compose up`, 2 wikis):

- Producer to Kafka to Spark to Delta on SeaweedFS with real events; Prometheus scrapes all three targets and all 8 Grafana panel queries return data.
- Airflow image builds, both DAGs import, the hourly marts DAG runs `dbt build --target prod` (24 of 24) and the export, and the dashboard renders the exported JSON as LIVE.
- The pageview task ingested a full real day (1,200,000 rows) from the Wikimedia dumps. Running it for real found and fixed two bugs: a dropped download connection was not retried, and the hourly marts failed until the pageviews table existed.
- Measured memory: about 5.5 GB steady state, about 7.5 GB during the daily ingest.

**Not run yet** (needs your accounts and a VM): the Oracle VM, the three GitHub workflows (the `end-to-end` job itself has not run on GitHub), Slack alerts, and long-running behaviour (a week of disk growth, reconnects after Wikimedia outages, compaction and retention).

**Storage note:** MinIO withdrew its community Docker images in 2025, so the S3-compatible store is SeaweedFS (Apache-2.0), pinned in `docker-compose.yml`.

## Quick start

```bash
make install && make test && make dbt-ci     # local checks
cp .env.example .env && docker compose up -d --build   # needs ~8 GB RAM free
```

Production setup: `docs/DEPLOY.md`. Operating it: `docs/RUNBOOK.md`.
