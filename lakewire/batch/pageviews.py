"""Hourly Wikimedia pageview files -> Silver Delta (idempotent per-day partition overwrite)."""
from __future__ import annotations

import gzip
import hashlib
import logging
import re
import tempfile
import time
from datetime import date, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lakewire.config import Settings
from lakewire.streaming.schemas import PAGEVIEWS_SCHEMA

log = logging.getLogger("lakewire.pageviews")

BASE_URL = "https://dumps.wikimedia.org/other/pageviews"
LINE_RE = r"^(\S+) (.+) (\d+) (\d+)$"


def file_name(day: date, hour: int) -> str:
    return f"pageviews-{day:%Y%m%d}-{hour:02d}0000.gz"


def file_url(day: date, hour: int) -> str:
    return f"{BASE_URL}/{day:%Y}/{day:%Y-%m}/{file_name(day, hour)}"


def dump_available(day: date, hour: int, user_agent: str) -> bool:
    import requests

    resp = requests.head(file_url(day, hour), headers={"User-Agent": user_agent}, timeout=30)
    return resp.status_code == 200


def load_md5sums(day: date, user_agent: str) -> dict[str, str]:
    """Best effort: the monthly md5sums.txt, used to verify downloads."""
    import requests

    try:
        resp = requests.get(f"{BASE_URL}/{day:%Y}/{day:%Y-%m}/md5sums.txt",
                            headers={"User-Agent": user_agent}, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.warning("md5sums unavailable (%s); falling back to gzip integrity check", exc)
        return {}
    sums = {}
    for line in resp.text.splitlines():
        parts = line.split()
        if len(parts) == 2:
            sums[parts[1].lstrip("*")] = parts[0]
    return sums


def _md5(path: Path) -> str:
    h = hashlib.md5()  # noqa: S324 - integrity check against the published checksum, not security
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _gzip_ok(path: Path) -> bool:
    try:
        with gzip.open(path, "rb") as fh:
            while fh.read(1 << 20):
                pass
        return True
    except (OSError, EOFError):
        return False


def download_hour(day: date, hour: int, dest: Path, user_agent: str, md5sums: dict[str, str],
                  attempts: int = 4) -> Path:
    import requests

    name = file_name(day, hour)
    target = dest / name
    for attempt in range(1, attempts + 1):
        try:
            with requests.get(file_url(day, hour), headers={"User-Agent": user_agent},
                              stream=True, timeout=(15, 120)) as resp:
                resp.raise_for_status()
                with target.open("wb") as fh:
                    # iter_content (not resp.raw) so a connection cut mid-body surfaces as a
                    # requests exception and is retried instead of killing the whole task.
                    for chunk in resp.iter_content(1 << 20):
                        fh.write(chunk)
            expected = md5sums.get(name)
            if (expected and _md5(target) != expected) or not _gzip_ok(target):
                raise OSError(f"checksum or gzip check failed for {name}")
            return target
        except (requests.RequestException, OSError) as exc:
            log.warning("download %s attempt %d/%d failed: %s", name, attempt, attempts, exc)
            time.sleep(min(2**attempt, 30))
    raise RuntimeError(f"could not download {name}")


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
