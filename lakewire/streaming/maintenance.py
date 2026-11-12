"""Delta table maintenance. Runs inside the streaming driver so each table keeps ONE writer.

Delta on S3-compatible storage has no atomic put-if-absent by default, so concurrent writers from
different processes are unsafe. Compaction, retention and VACUUM therefore live in the same JVM as
the streaming queries that own the tables.
"""
from __future__ import annotations

import logging
import threading
import time

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

log = logging.getLogger("lakewire.maintenance")


def optimize(spark: SparkSession, path: str) -> None:
    DeltaTable.forPath(spark, path).optimize().executeCompaction()


def purge_older_than(spark: SparkSession, path: str, date_col: str, days: int) -> None:
    DeltaTable.forPath(spark, path).delete(f"{date_col} < date_sub(current_date(), {int(days)})")


def vacuum(spark: SparkSession, path: str, hours: int = 168) -> None:
    DeltaTable.forPath(spark, path).vacuum(hours)


class MaintenanceThread(threading.Thread):
    """Compacts every `optimize_every_s`; applies retention and VACUUM about once a day."""

    def __init__(self, spark, bronze: str, silver: str, bronze_days: int, silver_days: int,
                 optimize_every_s: int = 6 * 3600) -> None:
        super().__init__(daemon=True, name="delta-maintenance")
        self.spark, self.bronze, self.silver = spark, bronze, silver
        self.bronze_days, self.silver_days = bronze_days, silver_days
        self.optimize_every_s = optimize_every_s
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        last_daily = 0.0
        self._stop_event.wait(600)  # let the streams settle first
        while not self._stop_event.is_set():
            try:
                for path in (self.bronze, self.silver):
                    optimize(self.spark, path)
                if time.time() - last_daily > 23 * 3600:
                    purge_older_than(self.spark, self.bronze, "ingest_date", self.bronze_days)
                    purge_older_than(self.spark, self.silver, "event_date", self.silver_days)
                    for path in (self.bronze, self.silver):
                        vacuum(self.spark, path)
                    last_daily = time.time()
                log.info("maintenance cycle complete")
            except Exception:  # noqa: BLE001 - maintenance must never kill the streams
                log.exception("maintenance cycle failed")
            self._stop_event.wait(self.optimize_every_s)
