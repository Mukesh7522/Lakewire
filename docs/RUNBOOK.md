# Runbook

## Is it healthy?
```bash
cd /opt/lakewire
python3 scripts/healthcheck.py        # services running, producer saw an event recently
docker compose ps
docker compose logs --tail=100 producer spark-stream
```

## Symptoms and first moves

| Symptom | Likely cause | Move |
|---|---|---|
| `publish-dashboard` red, "latest event is N minutes old" | producer disconnected, Spark crashed, or the hourly DAG failed | `healthcheck.py`; `docker compose logs spark-stream`; Airflow run history |
| `lakewire_last_event_age_seconds` climbing | Wikimedia stream down or blocked (check User-Agent) | `docker compose logs producer`; look for repeated reconnects |
| Dead-letter rate rising | upstream schema change | `kafka-console-consumer` on `wiki.edits.dlq`; update the contract |
| Spark container restarting | out of memory or Delta conflict | `docker stats`; lower `SPARK_DRIVER_MEMORY` or `WIKIS` |
| Disk above 80% | retention too long or too many wikis | lower `BRONZE_RETENTION_DAYS`, `SILVER_RETENTION_DAYS`, trim `WIKIS`, then `docker compose up -d` |
| Dashboard stale after the computer was off or asleep (own-computer setup) | nothing ingested while off; the producer resumes from its saved position, a long outage may leave a gap | start Docker, `python3 scripts/healthcheck.py`, then `powershell -NoProfile -File scripts\publish_local.ps1` |
| Airflow reports the scheduler unhealthy while the pageview ingest runs | Airflow standalone uses SQLite and therefore the sequential executor, which runs each task inside the scheduler, so the daily ingest (10 to 15 minutes, longer on a slow connection) pauses scheduling and delays the hourly marts run | Expected and self-healing. If the delay matters, give Airflow a Postgres database (standalone then uses the local executor) |
| Pageview DAG waiting | dump for that day not published yet | sensor retries every 15 minutes for 8 hours |

## Measure growth in week one
```bash
docker system df -v | head -30
docker compose exec s3 du -sh /data
```
Compare after 24 hours and project it against your disk. Adjust `WIKIS` and retention before it matters.

For the full picture in one command (events per day, duplicates and gaps, dead letters, reconnects, restarts, monitoring coverage, memory, disk): `python scripts/week_report.py`.

## Failure drills (each one is a portfolio story)
Prove the result with `docker compose exec -T airflow /opt/dbt-venv/bin/python - < scripts/lake_audit.py` (Bronze offset gaps, Silver duplicates, Silver vs Bronze, pageview fingerprints). Completed drills are written up in `docs/incidents/`.

1. **Stop the consumer.** `docker compose stop spark-stream`, wait 10 minutes, start it. Prove no gaps: Bronze offsets are contiguous and Silver has no duplicates.
2. **Poison message.** Produce a malformed event; it must land in the DLQ and the stream must keep running.
3. **Schema change.** Add a field (passes) then change `namespace` to a string (DLQ). Screenshot Grafana during both.
4. **Rebuild a day.** Delete one day of Silver pageviews and rerun the DAG for that date. Row counts must match the original.
5. Write each up in `docs/incidents/` with timeline, root cause, fix and prevention.
