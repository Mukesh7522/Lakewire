# Incident: the Silver stream crashed during compaction

**Type:** real incident, found by looking at container uptime. **Date:** 2026-11-23, all times UTC.
**Impact:** the streaming driver restarted itself; no data lost.

## Detection

About 10 minutes after the stack started, `docker compose ps` showed `spark-stream` up for 6 minutes while every other
service had been up for 17. The container had restarted once (`RestartCount=1`, not OOM-killed, exit code 0).

## Timeline

| Time | Event |
|---|---|
| about 05:34 | Stack started. |
| 05:45:04 | Silver stream failed: `ConcurrentAppendException: [DELTA_CONCURRENT_APPEND] Files were added to partition [event_date=2026-11-23] by a concurrent update`. The driver exited. |
| 05:45:05 | Docker restarted the container (`restart: unless-stopped`). The checkpoint let it resume. |

## Root cause

`MaintenanceThread` waits 10 minutes after startup and then runs `OPTIMIZE` (compaction) on Bronze and Silver. The Silver
`MERGE` that runs every 30 seconds reads the table, and Delta aborts whichever of two overlapping commits lands second.
That time it was the merge, and an unhandled error in `foreachBatch` terminates the streaming query. It would have
recurred at every 6-hour compaction whenever the two overlapped.

## Reproduction

A throwaway Delta table on local disk, with a compaction loop hammering it while 40 merge batches ran:

| | Merges failed | Rows in Silver (expected 1,600) | Conflicts retried |
|---|---|---|---|
| No retry (old behaviour) | 17 of 40 | 920 | 0 |
| With the fix | 0 of 40 | 1,600 | 31 |

The compaction loop here is far more aggressive than the real one, which runs once every 6 hours.

## Fix

`lakewire/streaming/retry.py` retries a Delta commit that fails with a concurrent-modification error (5 attempts for the
merge, 3 for the compaction, linear backoff). Other errors are never retried. Retrying the merge is safe because it is
insert-only on `event_id`. Four unit tests cover conflict detection, retry then success, giving up, and not retrying
real errors. Deployed at 06:09:54. The first maintenance cycle afterwards completed at 06:21:14 with no restart.

## Limits

No conflict happened to occur during the live compaction after the fix, so the retry has been proven on the reproduction
but not yet observed live. Check the logs for `Delta commit conflict` over the first week.

## Prevention

Anything that shares a Delta table inside one driver must expect to lose commit races. The earlier test suite never ran
the maintenance thread, because it only starts 10 minutes after a continuous run begins, which is why CI missed it.
