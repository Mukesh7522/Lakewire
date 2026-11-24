"""Entry points for the GitHub Actions pipeline.

    python -m lakewire.lite.run hourly      collect new edits, store them, rebuild the marts, publish the dashboard
    python -m lakewire.lite.run pageviews   load yesterday's pageviews (and the day before on a fresh start)

State lives in two git branches of this repository, replaced by one commit each run so history never grows:
`data` holds the Parquet files and the stream position, `gh-pages` holds the published site.
"""
from __future__ import annotations

import argparse
import logging
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from lakewire import export
from lakewire.batch.dumps import dump_available
from lakewire.config import Settings
from lakewire.lite import pageviews as lite_pageviews
from lakewire.lite import store
from lakewire.lite.collect import collect, gap_minutes
from lakewire.lite.gitstore import fetch_branch, push_tree

ROOT = Path(__file__).resolve().parents[2]
GAP_WARN_MINUTES = 15
log = logging.getLogger("lakewire.lite")


@dataclass(frozen=True)
class LiteConfig:
    remote: str
    data_branch: str = "data"
    pages_branch: str = "gh-pages"
    workdir: Path = Path(".lite")
    edits_retention_days: int = 7
    pageview_retention_days: int = 3
    pageview_top_n: int = 20000
    pageview_days: int = 2
    max_seconds: float = 1200
    grace_seconds: float = 45
    initial_hours: float = 3
    dbt_bin: str = "dbt"
    repo: str | None = None
    token: str | None = None
    publish: bool = True

    @classmethod
    def from_env(cls) -> LiteConfig:
        env = os.environ
        repo, token = env.get("GITHUB_REPOSITORY"), env.get("GITHUB_TOKEN")
        remote = env.get("LITE_REMOTE") or (
            f"https://x-access-token:{token}@github.com/{repo}.git" if repo and token else ""
        )
        if not remote:
            raise SystemExit("Set LITE_REMOTE, or GITHUB_REPOSITORY and GITHUB_TOKEN (set inside GitHub Actions).")
        return cls(
            remote=remote,
            data_branch=env.get("LITE_DATA_BRANCH", "data"),
            pages_branch=env.get("LITE_PAGES_BRANCH", "gh-pages"),
            workdir=Path(env.get("LITE_WORKDIR", ".lite")),
            edits_retention_days=int(env.get("EDITS_RETENTION_DAYS", "7")),
            pageview_retention_days=int(env.get("PAGEVIEW_RETENTION_DAYS", "3")),
            pageview_top_n=int(env.get("PAGEVIEW_TOP_N", "20000")),
            pageview_days=int(env.get("PAGEVIEW_DAYS", "2")),
            max_seconds=float(env.get("LITE_MAX_SECONDS", "1200")),
            grace_seconds=float(env.get("LITE_GRACE_SECONDS", "45")),
            initial_hours=float(env.get("LITE_INITIAL_HOURS", "3")),
            dbt_bin=env.get("DBT_BIN", "dbt"),
            repo=repo,
            token=token,
        )


def annotate(level: str, title: str, message: str) -> None:
    """A GitHub Actions annotation. It shows on the run page and in the API, so a run can be judged without its logs."""
    clean = message.replace("%", "%25").replace("\r", "").replace("\n", "%0A")
    print(f"::{level} title={title}::{clean}", flush=True)


def build_marts(cfg: LiteConfig, lake: Path) -> dict[str, int]:
    """Rebuild the dbt marts from the Parquet files into a throwaway DuckDB file and export the dashboard JSON."""
    database = (cfg.workdir / "lite.duckdb").resolve()
    for leftover in (database, database.with_name(database.name + ".wal")):
        leftover.unlink(missing_ok=True)
    env = {**os.environ, "LITE_DATA_DIR": lake.resolve().as_posix(), "DUCKDB_PATH": str(database),
           "PIPELINE_MODE": "lite"}
    proc = subprocess.run([cfg.dbt_bin, "build", "--target", "lite", "--profiles-dir", "."], cwd=ROOT / "dbt",
                          env=env, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if proc.returncode != 0:
        tail = "\n".join((proc.stdout + proc.stderr).strip().splitlines()[-25:])
        raise RuntimeError(f"dbt build failed:\n{tail}")
    os.environ["PIPELINE_MODE"] = "lite"
    return export.export(str(database), str(cfg.workdir / "exports"))


def request_pages_build(cfg: LiteConfig) -> None:
    """Ask GitHub Pages to rebuild now. Best effort: a push by the workflow token may not trigger a build by itself."""
    if not (cfg.repo and cfg.token):
        return
    import requests

    try:
        response = requests.post(
            f"https://api.github.com/repos/{cfg.repo}/pages/builds",
            headers={"Authorization": f"Bearer {cfg.token}", "Accept": "application/vnd.github+json",
                     "X-GitHub-Api-Version": "2022-11-28"},
            timeout=30,
        )
        if response.status_code >= 300:
            annotate("warning", "pages build", f"Pages build request returned HTTP {response.status_code}")
    except requests.RequestException as exc:  # the site still updates on the next push
        annotate("warning", "pages build", f"could not request a Pages build: {type(exc).__name__}")


def publish_site(cfg: LiteConfig, message: str) -> None:
    target = cfg.workdir / "site"
    if target.exists():
        shutil.rmtree(target)
    shutil.copytree(ROOT / "site", target, ignore=shutil.ignore_patterns("data", ".git"))
    shutil.copytree(cfg.workdir / "exports", target / "data")
    (target / ".nojekyll").write_text("", encoding="utf-8")
    push_tree(target, cfg.remote, cfg.pages_branch, message)
    request_pages_build(cfg)


def hourly(cfg: LiteConfig, settings: Settings | None = None, now: datetime | None = None) -> dict[str, Any]:
    settings = settings or Settings.from_env()
    now = now or store.utc_now()
    started = time.monotonic()
    lake = cfg.workdir / "lake"
    fetch_branch(cfg.remote, cfg.data_branch, lake)
    state = store.read_state(lake)
    previous = store.parse_ts(state["watermark"]) if state.get("watermark") else None

    result = collect(settings, lake / "state", now, max_seconds=cfg.max_seconds, grace_seconds=cfg.grace_seconds,
                     initial_hours=cfg.initial_hours)
    gap = gap_minutes(previous, result.first_bulk_dt) if result.resumed else None
    if gap is not None and gap > GAP_WARN_MINUTES:
        annotate("warning", "possible gap", f"the first event of this run is {gap:.0f} minutes after the previous "
                 "run's newest event; the saved stream position may have expired")
    if result.read == 0:
        annotate("warning", "no events", "the stream delivered nothing; check that Wikimedia is reachable")

    merged = store.merge_edits(lake, result.events, ingested_at=now)
    store.append_dlq(lake, result.dlq, now=now)
    removed = store.purge_older_than(lake, "edits", cfg.edits_retention_days, now.date())
    watermark = result.last_bulk_dt or previous
    store.write_state(lake, {
        "watermark": watermark.isoformat() if watermark else None,
        "last_run": now.isoformat(), "bulk_topic": result.bulk_topic,
    })
    # Persist progress before anything that can fail, so a broken dbt model never makes us re-read or lose events.
    push_tree(lake, cfg.remote, cfg.data_branch, f"Lakewire data {now:%Y-%m-%d %H:%M}Z")

    summary: dict[str, Any] = {
        "read": result.read, "filtered": result.filtered, "kept_new": merged.new_rows, "dlq": len(result.dlq),
        "caught_up": result.caught_up, "timed_out": result.timed_out, "resumed": result.resumed,
        "purged_days": len(removed), "collect_seconds": round(result.seconds),
    }
    if cfg.publish:
        counts = build_marts(cfg, lake)
        publish_site(cfg, f"Lakewire dashboard {now:%Y-%m-%d %H:%M}Z")
        summary["marts"] = counts
    summary["total_seconds"] = round(time.monotonic() - started)
    level = "warning" if result.timed_out else "notice"
    annotate(level, "lakewire-lite hourly", ", ".join(f"{k}={v}" for k, v in summary.items()))
    return summary


def pageviews(cfg: LiteConfig, settings: Settings | None = None, today: datetime | None = None) -> dict[str, Any]:
    settings = settings or Settings.from_env()
    today_date = (today or store.utc_now()).date()
    lake = cfg.workdir / "lake"
    fetch_branch(cfg.remote, cfg.data_branch, lake)
    loaded: dict[str, int] = {}
    for back in range(cfg.pageview_days, 0, -1):  # oldest first
        day = today_date - timedelta(days=back)
        if lite_pageviews.already_ingested(lake, day):
            continue
        if not dump_available(day, 23, settings.user_agent):
            annotate("warning", "pageviews", f"the dump for {day} is not complete yet; the next run will retry")
            continue
        loaded[day.isoformat()] = lite_pageviews.ingest_day(
            day, lake, settings.user_agent, settings.pageview_domains, cfg.pageview_top_n, cfg.workdir
        )
    removed = store.purge_older_than(lake, "pageviews", cfg.pageview_retention_days, today_date)
    if loaded or removed:
        push_tree(lake, cfg.remote, cfg.data_branch, f"Lakewire pageviews {today_date}")
    summary = {"loaded": loaded, "purged_days": len(removed)}
    annotate("notice", "lakewire-lite pageviews", str(summary))
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=["hourly", "pageviews"])
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        cfg = LiteConfig.from_env()
        (hourly if args.command == "hourly" else pageviews)(cfg)
    except Exception as exc:  # noqa: BLE001 - report it as an annotation, then fail the run
        annotate("error", f"lakewire-lite {args.command} failed", f"{type(exc).__name__}: {exc}"[:900])
        log.exception("run failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
