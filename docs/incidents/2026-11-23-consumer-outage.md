# Drill 1: the Spark consumer is down for 13 minutes

**Type:** planned failure drill. **Date:** 2026-11-23, all times UTC. **Result:** no gaps, no duplicates.

## What we tested

The streaming consumer (Kafka to Bronze to Silver) dies without warning while the producer keeps running. When it comes
back it must catch up on its own, and afterwards Bronze must have no missing offsets and Silver no duplicate events.

## Timeline

| Time | Event |
|---|---|
| 06:58:14 | `docker compose stop -t 0 spark-stream` (immediate kill, no graceful shutdown). Kafka offsets: partition 1 at 11,683, partition 2 at 902. |
| 06:58:30 | Prometheus starts failing to scrape the Spark metrics endpoint. |
| 06:58 to 07:11 | The producer keeps accepting events (last-event age never above 1 second). Kafka accumulates the backlog. |
| 07:11:14 | `docker compose start spark-stream`. Offsets: partition 1 at 13,621, partition 2 at 1,182. Backlog: about 2,200 events. |
| 07:13:00 | Spark metrics scrape succeeds again (JVM and Delta startup took under 2 minutes). |
| 07:13:54 | Bronze has ingested the whole backlog (partition 1 at 13,878, partition 2 at 1,200). |
| 07:24 | Full audit (below). Silver had drained as well. |

Outage as seen by Prometheus: `up{job="spark-stream"}` was 0 from 06:58:30 to 07:12:45.

## Evidence

`scripts/lake_audit.py` at the end of the drill:

| Check | Result |
|---|---|
| Bronze partition 1 | offsets 0 to 15,621, 15,622 rows, 0 gaps |
| Bronze partition 2 | offsets 0 to 1,373, 1,374 rows, 0 gaps |
| Duplicate (partition, offset) pairs in Bronze | 0 |
| Bronze rows vs distinct event ids | 16,996 and 16,996 |
| Silver rows vs distinct event ids | 16,903 and 16,903 (0 duplicates) |
| Bronze events older than 5 minutes missing from Silver | 0 |

The 93 events that Silver had not yet received were all younger than 5 minutes, which is the normal in-flight lag of a
30-second micro-batch pipeline.

## Why it worked

- The Kafka offsets and the Silver progress live in the Spark checkpoint, so a restarted driver resumes exactly where
  the killed one stopped.
- The Bronze sink is transactional, so a batch killed halfway is never half-written.
- The Silver `MERGE` is insert-only on `event_id`, so a replayed batch cannot create duplicates.

## Limits worth knowing

- Kafka keeps data for 48 hours. An outage longer than that loses the oldest events (`failOnDataLoss` is `false`, so the
  stream continues instead of failing).
- In a real crash Docker restarts the container by itself (`restart: unless-stopped`). For this drill it was held down
  with `stop`, so the restart was manual.

## Follow-up

None needed. `scripts/lake_audit.py` was added so the same proof can be repeated at any time.
