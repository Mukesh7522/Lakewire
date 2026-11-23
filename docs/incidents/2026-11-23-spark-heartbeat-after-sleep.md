# Incident: Spark broke itself after the laptop woke up

**Type:** real incident, found while checking the aftermath of the laptop sleep. **Date:** 2026-11-23, all times UTC.
**Impact:** the streaming driver ran in a broken state for about 10 minutes, then crashed and restarted itself. No data lost.

This is a follow-on to [the laptop sleep](2026-11-23-laptop-sleep.md), which looked clean from the data side.

## Detection

A routine check after the sleep report showed `spark-stream` with `RestartCount=1`, although it had read 0 about ten minutes earlier.
The container's exit state was clean (`OOMKilled=false`, exit code 0), so the cause had to be in the log.

## Timeline

| Time | Event |
|---|---|
| 07:57 | Host goes to sleep; Docker's VM and every container pause. |
| 08:54:54 | Host wakes. Spark logs `HeartbeatReceiver: Removing executor driver with no recent heartbeats: 3475353 ms exceeds timeout 120000 ms`. |
| 08:54:57 onwards | Every 10 seconds: `Executor: Issue communicating with driver in heartbeater` and `Inbox: Ignoring error` (`Cannot find endpoint: spark://CoarseGrainedScheduler@...`). Data keeps flowing; the earlier audit shows events ingested up to at least 09:02. |
| 09:04:58 | The Spark JVM dies. Python reports `Py4JNetworkError: Answer from Java side is empty` in `awaitAnyTermination`. |
| 09:04:59 | Docker restarts the container; the checkpoint resumes the stream. |
| 09:11 | Audit after the restart: Bronze offsets contiguous (0 gaps), 0 duplicate offsets, 0 duplicate events in Silver, nothing older than 5 minutes missing. |

## Root cause

Spark checks its own executor by wall-clock heartbeats and gives up after `spark.network.timeout`, 120 seconds by
default. In local mode the "executor" lives inside the driver. When the host sleeps the clock jumps, so on waking Spark
sees an executor that has been silent for 58 minutes, removes it, and the driver is left half-working. A restart is the
only recovery. I did not diagnose exactly what killed the JVM at 09:04:58; it is most likely a consequence of that broken
state.

## Reproduction

`docker pause` freezes a container while the wall clock keeps running, which is what sleep looks like from inside.
Pausing `spark-stream` for 150 seconds (09:11:59 to 09:14:29) produced the same log line immediately on unpause:
`Removing executor driver with no recent heartbeats: 151821 ms exceeds timeout 120000 ms`, followed by the same heartbeat
errors.

## Fix

`lakewire/spark_utils.py` now sets `spark.network.timeout` to 2 hours (`NETWORK_TIMEOUT_SECONDS`). In local mode there is
no network and no remote executor, so a long timeout costs nothing. A unit test guards the value. It applies to every
Spark session, so it is deployed to both the streaming container and the Airflow image (which runs the pageview ingest).

## Verification

After the fix, the same freeze with a much longer pause: `spark-stream` paused for 300 seconds (09:20:58 to 09:25:58).
Afterwards there were no `Removing executor`, `Issue communicating` or `Ignoring error` lines, `RestartCount` stayed 0,
and Silver was processing about 3 rows per second with its last batch 10 seconds old.

## Limits

- The fix was tested with a 5-minute freeze, not a real sleep. A host that sleeps for more than 2 hours would still trip
  the timeout; the container would then crash and Docker would restart it, as it did here.
- Other timeouts may also be sensitive to a large clock jump. The Kafka and S3 clients recovered on their own in this
  incident, but a longer sleep has not been tested.
- The producer's own recovery from sleep was already covered by the laptop-sleep report.

## Prevention

Anything that measures liveness by wall-clock time will misfire on a host that sleeps. For a pipeline that runs on a
laptop, test with `docker pause`, not only with `docker stop`.
