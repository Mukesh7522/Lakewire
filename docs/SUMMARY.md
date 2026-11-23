# Project summary

Lakewire is a streaming and batch lakehouse built on public Wikipedia data. Every edit on the ingested wikis flows from
Wikimedia's live event stream through a Kafka topic into Spark Structured Streaming, lands in Delta Lake tables
(Bronze, then deduplicated Silver) on S3-compatible storage, and feeds hourly dbt marts that power a public dashboard.
Daily pageview dumps are loaded by an Airflow DAG into a second Silver table and joined into a trending-articles mart.
The whole system runs from one `docker-compose.yml`.

## What was measured

| | |
|---|---|
| Live stream filtering | about 30 events per second arrive from Wikimedia; about 3 per second are kept for two wikis (`enwiki`, `dewiki`) |
| Pageview load | 1,200,000 rows per day (top 50,000 pages per hour, English desktop and mobile), about 1.2 GB downloaded, 9 to 15 minutes to ingest |
| Memory | about 5.5 GB steady, about 7.5 GB while the daily ingest runs, for the full stack including Airflow and Grafana |
| Tests | 26 unit tests, 24 dbt model and data tests, and an end-to-end CI job that runs real Kafka, SeaweedFS, Spark and Delta in GitHub's runners |
| Idempotency | CI replays the same events twice and asserts that Silver does not change |
| Consumer outage drill | consumer killed for 13 minutes; afterwards 0 gaps in Bronze offsets, 0 duplicate offsets, 0 duplicate events in Silver |
| Laptop sleep (unplanned) | host asleep for 58 minutes; the missed hour was backfilled in about 30 seconds, 0 gaps, 0 duplicates |
| Rebuild drill | a deleted pageview day was rebuilt with identical rows, total views and a content fingerprint over every row |

Figures from a longer run (events per day, disk growth, restarts) are not included yet: the system has only been running
continuously for a short time. See [`incidents/`](incidents/) for the exact timelines and evidence.

## Things it does on purpose

- A JSON-Schema data contract at the edge, with a dead-letter topic for events that break it.
- Effectively-once processing: a watermark deduplicates within a bounded window and an insert-only `MERGE` on `event_id`
  makes any redelivery harmless.
- One writer per Delta table, because object storage without atomic put-if-absent is unsafe with concurrent writers.
- Resumable ingestion: the producer saves its position in the Wikimedia stream and resumes from it after a restart.
- Honest freshness: the dashboard always states how old its snapshot is and shows a banner when data goes stale.

## Problems found by running it, and what changed

Running the whole system against the live stream, not just fixtures, found real defects. Each is written up.

| Problem | Evidence | Change |
|---|---|---|
| The Silver stream crashed when a compaction overlapped a merge | reproduced: 17 of 40 merges failed under constant compaction, 0 after the fix | commit-conflict retry for the merge and the compaction |
| Wikimedia's hourly canary test events were dead-lettered | found in the dead-letter queue during the drills | canary events are skipped, with a regression test |
| A fresh deployment failed every hourly marts run until the first pageview day existed | first hourly run on live data | a missing pageviews table is treated as empty; covered in CI |
| Spark declared its own executor dead after the host slept, then crashed | log line reproduced with `docker pause`; after the fix a 5-minute freeze left Spark intact | longer Spark network timeout, tested |
| A dropped download connection killed the whole pageview task | first full-day ingest | per-file retries; test that cuts the connection mid-body |

## Honest limits

- It runs on a single machine. Docker restarts crashed containers and the producer resumes after the machine sleeps, but nothing is ingested while it is off, and a very long outage can exceed Wikimedia's replay window.
- Airflow runs on SQLite with the sequential executor, so the daily pageview ingest pauses scheduling for its duration.
- Kafka keeps 48 hours of data; an outage longer than that loses the oldest events.
- It is a portfolio system on a public dataset, not a production service with an on-call rotation.
