"""Kafka -> Bronze (append-only Delta) -> Silver (deduplicated, MERGE-upserted Delta).

Continuous: python -m lakewire.streaming.edits_stream
One-shot:   python -m lakewire.streaming.edits_stream --once     (used by CI)
"""
from __future__ import annotations

import argparse
import logging
import sys
import time

from delta.tables import DeltaTable
from prometheus_client import Gauge, start_http_server
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQueryListener

from lakewire.config import Settings
from lakewire.spark_utils import build_spark
from lakewire.streaming.maintenance import MaintenanceThread
from lakewire.streaming.retry import retry_on_conflict
from lakewire.streaming.schemas import (
    BRONZE_SCHEMA,
    SILVER_SCHEMA,
    bronze_to_silver,
    dedupe_stream,
    kafka_to_bronze,
)

log = logging.getLogger("lakewire.edits_stream")

INPUT_RPS = Gauge("lakewire_stream_input_rows_per_second", "Input rows per second", ["query"])
BATCH_MS = Gauge("lakewire_stream_batch_duration_ms", "Trigger execution time in ms", ["query"])
LAST_BATCH = Gauge("lakewire_stream_last_batch_timestamp_seconds", "Wall clock of last batch", ["query"])


class MetricsListener(StreamingQueryListener):
    def onQueryStarted(self, event) -> None:  # noqa: N802
        pass

    def onQueryProgress(self, event) -> None:  # noqa: N802
        p = event.progress
        name = p.name or "unnamed"
        INPUT_RPS.labels(name).set(p.inputRowsPerSecond or 0)
        BATCH_MS.labels(name).set((p.durationMs or {}).get("triggerExecution", 0))
        LAST_BATCH.labels(name).set(time.time())

    def onQueryIdle(self, event) -> None:  # noqa: N802
        pass

    def onQueryTerminated(self, event) -> None:  # noqa: N802
        log.warning("query terminated: %s", event.id)


def ensure_tables(spark: SparkSession, bronze: str, silver: str) -> None:
    """Create empty Delta tables so downstream readers never race the first write."""
    for schema, part, path in ((BRONZE_SCHEMA, "ingest_date", bronze), (SILVER_SCHEMA, "event_date", silver)):
        spark.createDataFrame([], schema).write.format("delta").partitionBy(part).mode("ignore").save(path)


def make_merge(spark: SparkSession, silver: str, attempts: int = 5):
    def merge_batch(batch_df: DataFrame, batch_id: int) -> None:
        deduped = batch_df.dropDuplicates(["event_id"])

        def do_merge() -> None:
            (
                DeltaTable.forPath(spark, silver)
                .alias("t")
                .merge(deduped.alias("s"), "t.event_id = s.event_id AND t.event_date = s.event_date")
                .whenNotMatchedInsertAll()
                .execute()
            )

        # The periodic OPTIMIZE shares this table; losing a commit race must not kill the stream.
        retry_on_conflict(do_merge, attempts=attempts)

    return merge_batch


def run(once: bool) -> int:
    settings = Settings.from_env()
    spark = build_spark("lakewire-edits-stream", settings)
    spark.sparkContext.setLogLevel("WARN")
    bronze = settings.lake_path("bronze", "edits")
    silver = settings.lake_path("silver", "edits")
    ensure_tables(spark, bronze, silver)

    if not once:
        start_http_server(8001)
        spark.streams.addListener(MetricsListener())
        MaintenanceThread(
            spark, bronze, silver, settings.bronze_retention_days, settings.silver_retention_days
        ).start()

    trigger = {"availableNow": True} if once else {"processingTime": f"{settings.stream_trigger_seconds} seconds"}

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", settings.topic_raw)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 200_000)
        .option("failOnDataLoss", "false")
        .load()
    )
    bronze_q = (
        kafka_to_bronze(kafka_df)
        .writeStream.queryName("bronze")
        .format("delta")
        .option("checkpointLocation", settings.checkpoint_path("bronze_edits"))
        .trigger(**trigger)
        .start(bronze)
    )
    if once:
        bronze_q.awaitTermination()

    silver_in = bronze_to_silver(
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", 100)
        .option("ignoreDeletes", "true")  # Bronze retention deletes whole old partitions
        .load(bronze)
    )
    silver_q = (
        dedupe_stream(silver_in)
        .writeStream.queryName("silver")
        .foreachBatch(make_merge(spark, silver))
        .option("checkpointLocation", settings.checkpoint_path("silver_edits"))
        .trigger(**trigger)
        .start()
    )
    if once:
        silver_q.awaitTermination()
        spark.stop()
        return 0

    spark.streams.awaitAnyTermination()  # raises if a query fails; Docker restarts the container
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="process what is available, then exit")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    return run(args.once)


if __name__ == "__main__":
    sys.exit(main())
