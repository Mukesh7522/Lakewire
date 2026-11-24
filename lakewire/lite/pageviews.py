"""Daily pageviews for the lightweight pipeline: download the 24 hourly dump files, keep the top pages per hour.

Same logic as the Spark job in lakewire/batch/pageviews.py, written in DuckDB so it runs on a plain GitHub runner:
keep the lines of the wanted domain codes, parse them with the same pattern, add desktop and mobile views together,
and keep the top N pages per hour (ties broken by title so the result is deterministic).
"""
from __future__ import annotations

import logging
import os
import tempfile
from datetime import date
from pathlib import Path

import duckdb

from lakewire.batch.dumps import download_hour, load_md5sums
from lakewire.lite.store import _literal

log = logging.getLogger("lakewire.lite.pageviews")

LINE_PATTERN = r"^(\S+) (.+) (\d+) (\d+)$"
HOURS_PER_DAY = 24


def day_path(root: Path, day: date) -> Path:
    return root / "silver" / "pageviews" / day.isoformat() / "part.parquet"


def already_ingested(root: Path, day: date) -> bool:
    return day_path(root, day).exists()


def _sql_text(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_day(files: list[Path], out: Path, domains: tuple[str, ...], top_n: int, workdir: Path,
              memory_limit: str = "8GB", threads: int = 4) -> int:
    """Turn the 24 downloaded files into one Parquet file. Returns the number of rows written."""
    out.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute(f"PRAGMA memory_limit={_sql_text(memory_limit)}")
        con.execute(f"PRAGMA threads={int(threads)}")
        con.execute(f"PRAGMA temp_directory={_sql_text(str(workdir / 'duckdb-spill'))}")
        file_list = "[" + ", ".join(_literal(f) for f in files) + "]"
        wanted = " OR ".join(f"starts_with(line, {_sql_text(d + ' ')})" for d in domains)
        # One column per line: the unit-separator character never appears in a dump, so lines are never split.
        con.execute(
            f"""
            CREATE TEMP TABLE parsed AS
            SELECT
                strptime(regexp_extract(filename, 'pageviews-(\\d{{8}}-\\d{{2}})', 1), '%Y%m%d-%H') AS view_hour,
                fields.page_title AS page_title,
                CAST(fields.views AS BIGINT) AS views
            FROM (
                SELECT filename, regexp_extract(line, '{LINE_PATTERN}', ['domain_code', 'page_title', 'views', 'bytes'])
                       AS fields, line
                FROM read_csv({file_list}, columns={{'line': 'VARCHAR'}}, delim=chr(31), quote='', escape='',
                              header=false, filename=true, compression='gzip')
                WHERE {wanted}
            )
            WHERE fields.views <> ''
            """
        )
        con.execute(
            f"""
            COPY (
                SELECT view_hour,
                       CAST(view_hour AS DATE) AS view_date,
                       {_sql_text('+'.join(domains))} AS domain_code,
                       page_title,
                       views
                FROM (
                    SELECT view_hour, page_title, views,
                           row_number() OVER (PARTITION BY view_hour ORDER BY views DESC, page_title) AS rk
                    FROM (SELECT view_hour, page_title, CAST(SUM(views) AS BIGINT) AS views FROM parsed GROUP BY 1, 2)
                )
                WHERE rk <= {int(top_n)}
                ORDER BY view_hour, views DESC, page_title
            ) TO {_literal(out.with_name('.part.tmp'))} (FORMAT parquet, COMPRESSION zstd)
            """
        )
        os.replace(out.with_name(".part.tmp"), out)
        return con.execute(f"SELECT count(*) FROM read_parquet({_literal(out)})").fetchone()[0]
    finally:
        con.close()


def ingest_day(day: date, root: Path, user_agent: str, domains: tuple[str, ...], top_n: int,
               workdir: Path | None = None) -> int:
    """Download the day's files (with checksum and retry), build the Parquet file, delete the downloads."""
    with tempfile.TemporaryDirectory(prefix="pageviews-", dir=workdir) as tmp:
        tmp_path = Path(tmp)
        md5sums = load_md5sums(day, user_agent)
        files = [download_hour(day, hour, tmp_path, user_agent, md5sums) for hour in range(HOURS_PER_DAY)]
        memory = os.getenv("LITE_DUCKDB_MEMORY", "8GB")  # a hosted runner has 16 GB; lower it on a small machine
        rows = build_day(files, day_path(root, day), domains, top_n, tmp_path, memory_limit=memory)
    log.info("pageviews %s: wrote %d rows", day, rows)
    return rows
