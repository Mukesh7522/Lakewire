# Defect: Kafka's data was never stored in its persistent volume

**Type:** configuration defect found by sanity-checking a report, before it caused an outage.
**Date:** 2026-11-23, all times UTC. **Impact:** none yet; fixed and migrated without losing data.

## Detection

The new run report (`scripts/week_report.py`) printed `Kafka data on disk: 0.00 GiB` while the topic held about 60,000
messages. A topic that size cannot be empty, so the number was wrong or the data was somewhere unexpected.

## What was wrong

`docker-compose.yml` mounted a named volume, `kafka-data`, at `/var/lib/kafka/data`. But the Kafka image's default
`log.dirs` is `/tmp/kafka-logs`, so the topics (and the KRaft metadata) were written inside the container's own
filesystem. The volume was mounted and empty (4 KB) the whole time. Searching the container
(`find / -name 'wiki.edits.raw-*'`) found the real location.

## Why it mattered

Recreating the container (`docker compose down` then `up`, a changed image, `--force-recreate`) destroys that
filesystem, so every buffered message and the topic definitions would vanish. Spark's checkpoint, stored in the lake,
would still remember offsets from the old topic, which would be ahead of a brand-new empty one. I did not test what Spark
does in that situation, so the consequence is a risk, not a measured failure. Restarting the container did not trigger it,
which is why nothing had gone wrong yet.

## Fix

`KAFKA_LOG_DIRS=/var/lib/kafka/data` in the Kafka service, so the data lives in the volume.

The running data was in the old location, so it was migrated rather than recreated empty:

| Time | Step |
|---|---|
| 11:19:32 | Stopped the producer and Spark gracefully (the producer saves its Wikimedia position when it stops). |
| 11:19 | Recorded end offsets: raw topic 54,987 and 6,143, dead-letter topic 8. Stopped Kafka cleanly (`.kafka_cleanshutdown` present). |
| 11:19 | Copied `/tmp/kafka-logs` out of the stopped container as a 17.3 MB tar (also kept as a backup), then extracted it into the volume with the right owner. |
| 11:20 | Recreated Kafka with the new setting. Offsets identical to the recorded ones; topics present; files now in the volume. |
| 11:20:21 | Producer and Spark restarted. The producer resumed from its saved position and backfilled the few minutes it missed. |

Ingestion was paused for about a minute. The audit afterwards: Bronze 61,497 rows, 0 offset gaps, 0 duplicate offsets,
0 duplicate events in Silver.

## Proof that the fix works

At 11:22:02 the Kafka container was force-recreated while the rest of the stack kept running, which is exactly the
operation that used to wipe it. The new container had a different id, and the raw topic's end offsets went from
55,421 and 6,197 to 55,472 and 6,201: they continued from where they were instead of resetting to zero, and the
dead-letter topic was still at 8. Afterwards Spark and the producer had 0 restarts, and the audit was clean again
(Bronze 61,889 rows, 0 gaps, 0 duplicates). The producer logged one connection failure during Kafka's brief downtime
and recovered without a delivery error.

## Checked at the same time

Every other stateful service writes inside its mounted volume: the S3 store (`/data`), Prometheus, Grafana, Airflow's
SQLite database (`/opt/airflow`), the DuckDB and export files (`/data` in the Airflow container) and the producer's
saved position. Kafka was the only mismatch.

## Limits

- `docker compose down -v` still deletes the volumes, including Kafka's. That is intended, but after it the lake
  checkpoints must be removed too, or Spark would resume from offsets that no longer exist.
- The tar backup of the old data was left outside the repository and can be deleted.

## Prevention

Check where a stateful container really writes, not only where a volume is mounted. A size of zero in a report was the
clue; reports that print real sizes catch this kind of mistake.
