# Lakewire

A live streaming and batch lakehouse for Wikipedia activity. It runs 24/7 on a free VM, deploys itself from GitHub, and publishes a public dashboard.

```
Wikimedia EventStreams ─► producer ─► Kafka ─► Spark Structured Streaming ─► Delta (Bronze ─► Silver) ─┐
                         (contract + DLQ)                                      on MinIO (S3-compatible) │
Wikimedia pageview dumps ─► Airflow ─► PySpark ─► Delta (Silver, per-day overwrite) ─────────────────────┤
                                                                                                        ▼
                                          dbt + DuckDB (hourly, Airflow) ─► Gold marts ─► JSON ─► GitHub Pages
Prometheus + Grafana  ·  Slack alerts  ·  GitHub Actions CI/CD
```

Everything runs from `docker-compose.yml`. Cost: $0 (see `docs/DEPLOY.md` for the free-tier caveats).

## What runs where

| Piece | Where | Schedule |
|---|---|---|
| Producer, Kafka, Spark streaming, MinIO, Airflow, Prometheus, Grafana | Docker on one VM | always on, `restart: unless-stopped` |
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

- `ruff` clean, `pytest` 15 passed: data contract, SSE parser, live-stream reconnect with `Last-Event-ID` against a local server, Spark transforms, streaming dedupe with watermark, pageview parsing.
- `dbt build --target ci` on parquet fixtures: 24 of 24 models and tests pass, twice (full and incremental).

**Not run yet** (needs Docker, network and your accounts). The `end-to-end` CI job exists to prove these on its first run:

- Docker images build; Kafka, MinIO and Spark start together.
- Kafka to Bronze to Silver with Delta on MinIO; replay idempotency.
- DuckDB `delta_scan` reading MinIO (most likely place for a first-run fix).
- The Oracle VM, Airflow DAG execution, Grafana, and the three workflows.

## Quick start

```bash
make install && make test && make dbt-ci     # local checks
cp .env.example .env && docker compose up -d --build   # needs ~16 GB RAM
```

Production setup: `docs/DEPLOY.md`. Operating it: `docs/RUNBOOK.md`.
