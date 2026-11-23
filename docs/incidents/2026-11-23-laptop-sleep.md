# Incident: the host computer slept for 58 minutes

**Type:** real incident (unplanned). **Date:** 2026-11-23, all times UTC. **Result:** no data gap.

## What happened

The laptop running the whole stack went to sleep, which pauses Docker's virtual machine and every container in it.
Nothing was ingested while it slept. When it woke, everything resumed without any manual action.

## Detection

Nobody noticed at the time. It was found afterwards from three timestamps that did not fit:

| Evidence | What it showed |
|---|---|
| Airflow, `lakewire_marts_hourly` | the run for the 07:00 to 08:00 slot started at 08:54:49, not about 08:00 |
| Producer log | `stream error (Response ended prematurely)` at 07:56:59, then the next `connected` at 08:54:50 |
| Windows Task Scheduler | the hourly publish task ran at 08:54:49 (it is set to catch up after missed runs) |

The container clock jumped by about 58 minutes, which is what a suspended VM looks like from the inside.

## Did it lose data?

No. Silver event counts per 10-minute bucket of event time, around the sleep:

| Event time (UTC) | Events | First ingested | Last ingested |
|---|---|---|---|
| 07:50 | 1,912 | 07:50:30 | 08:55:00 |
| 08:00 | 2,573 | 08:55:00 | 08:55:00 |
| 08:10 | 2,370 | 08:55:00 | 08:55:30 |
| 08:20 | 1,964 | 08:55:30 | 08:55:30 |
| 08:30 | 1,803 | 08:55:30 | 08:55:30 |
| 08:40 | 1,682 | 08:55:30 | 08:55:30 |
| 08:50 | 1,745 | 08:55:30 | 09:00:30 |

Every bucket of the missing hour is present at a normal rate, and all of it was ingested within about 30 seconds of the
machine waking up. `scripts/lake_audit.py` afterwards: Bronze offsets contiguous (0 to 32,757 and 0 to 3,344, 0 gaps),
0 duplicate offsets, 0 duplicate events in Silver, nothing older than 5 minutes missing from Silver.

## Why it worked

- The producer remembers its position in Wikimedia's stream (the `Last-Event-ID` it last saw). On reconnect it asked for
  everything after that point, and Wikimedia replayed the missed hour.
- The Spark checkpoint resumed where it stopped, and the insert-only `MERGE` on `event_id` makes any overlap harmless.

## What did not go perfectly

- **The public dashboard went stale.** The publish task and the delayed marts run both started at 08:54:49, so the
  task published the old 07:00 snapshot, and the 09:00 snapshot was not published until the task was run by hand at about
  09:03. The dashboard stayed honest about it ("snapshot N h old"), but it was up to 2 hours behind for a while.
- **Publish and refresh are not coordinated.** The publish task fires on its own clock, so after a wake-up it can run
  before the marts job has produced a fresh snapshot.

## Limits

- This was one 58-minute sleep. Wikimedia's stream history is finite, so a much longer outage could leave a gap in the
  live data. I did not test beyond an hour.
- Kafka keeps 48 hours, which bounds how long the consumer side can be down.

## Follow-ups

- Move the publish schedule to a few minutes after the marts run so a normal hour publishes fresh data.
- Consider having the publish script check the snapshot's age and wait briefly for a fresh one before pushing.
