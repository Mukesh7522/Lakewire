"""Wikimedia hourly pageview dumps: URLs, availability check, checksummed download with retries.

Spark-free on purpose: the lightweight GitHub Actions pipeline uses it too, and must not install Spark.
"""
from __future__ import annotations

import gzip
import hashlib
import logging
import time
from datetime import date
from pathlib import Path

log = logging.getLogger("lakewire.pageviews")

BASE_URL = "https://dumps.wikimedia.org/other/pageviews"


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
