"""Hourly Wikimedia pageview files -> Silver Delta (idempotent per-day partition overwrite)."""
from __future__ import annotations

import logging
import re
import tempfile
from datetime import date, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lakewire.batch.dumps import download_hour, load_md5sums
from lakewire.config import Settings
from lakewire.streaming.schemas import PAGEVIEWS_SCHEMA

log = logging.getLogger("lakewire.pageviews")

LINE_RE = r"^(\S+) (.+) (\d+) (\d+)$"


def parse_pageviews(raw: DataFrame, domains: tuple[str, ...], top_n: int) -> DataFrame:
    """raw has columns `value` (text line) and `path` (source file). Returns PAGEVIEWS_SCHEMA rows."""
    parsed = (
        raw.select(
            F.regexp_extract("value", LINE_RE, 1).alias("domain_code"),
            F.regexp_extract("value", LINE_RE, 2).alias("page_title"),
            F.regexp_extract("value", LINE_RE, 3).cast("long").alias("views"),
            F.regexp_extract("path", r"pageviews-(\d{8}-\d{2})", 1).alias("stamp"),
        )
        .where((F.col("domain_code") != "") & F.col("domain_code").isin(list(domains)))
        .withColumn("view_hour", F.to_timestamp("stamp", "yyyyMMdd-HH"))
        .groupBy("view_hour", "page_title")
        .agg(F.sum("views").alias("views"))
    )
    ranked = parsed.withColumn(
        "rk", F.row_number().over(Window.partitionBy("view_hour").orderBy(F.col("views").desc(), "page_title"))
    ).where(F.col("rk") <= top_n)
    return (
        ranked.withColumn("view_date", F.to_date("view_hour"))
        .withColumn("domain_code", F.lit("+".join(domains)))
        .select(*[f.name for f in PAGEVIEWS_SCHEMA.fields])
    )


def ingest_day(spark: SparkSession, settings: Settings, day: date) -> int:
    """Download 24 hourly files, parse, and overwrite exactly this day's partition."""
    md5sums = load_md5sums(day, settings.user_agent)
    with tempfile.TemporaryDirectory(prefix="pageviews-") as tmp:
        tmp_path = Path(tmp)
        files = [download_hour(day, h, tmp_path, settings.user_agent, md5sums) for h in range(24)]
        raw = (
            spark.read.text([str(f) for f in files])
            .withColumn("path", F.input_file_name())
            .where(F.col("value").rlike(r"^(" + "|".join(re.escape(d) for d in settings.pageview_domains) + ") "))
        )
        out = parse_pageviews(raw, settings.pageview_domains, settings.pageview_top_n).cache()
        rows = out.count()
        (
            out.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"view_date = '{day:%Y-%m-%d}'")
            .partitionBy("view_date")
            .save(settings.lake_path("silver", "pageviews"))
        )
        out.unpersist()
    return rows


def run(day_str: str) -> int:
    from lakewire.spark_utils import build_spark
    from lakewire.streaming.maintenance import optimize, purge_older_than, vacuum

    settings = Settings.from_env()
    day = datetime.strptime(day_str, "%Y-%m-%d").date()
    spark = build_spark("lakewire-pageviews", settings)
    spark.sparkContext.setLogLevel("WARN")
    try:
        rows = ingest_day(spark, settings, day)
        path = settings.lake_path("silver", "pageviews")
        optimize(spark, path)
        purge_older_than(spark, path, "view_date", settings.silver_retention_days)
        vacuum(spark, path)
        log.info("pageviews %s: wrote %d rows", day, rows)
        return rows
    finally:
        spark.stop()


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)
    run(sys.argv[1])
