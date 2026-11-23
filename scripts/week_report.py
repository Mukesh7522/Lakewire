"""Summarise how the stack has been running, as numbers for the README and a resume.

Run from the repository root while the stack is up (needs Docker, and Python 3 on the host):

    python scripts/week_report.py            # markdown, ready to paste
    python scripts/week_report.py --json     # the raw numbers

It reads the lake through the Airflow container (DuckDB), Kafka offsets and container state through Docker, and
reconnects and monitoring coverage from Prometheus. Nothing is written anywhere. Any section that cannot be read
is reported as unavailable instead of failing the whole report.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROMETHEUS = "http://localhost:9090"
SCRAPE_SECONDS = 15  # observability/prometheus.yml: global scrape_interval

# Runs inside the Airflow container (DuckDB with the delta extension, S3 settings in the environment).
LAKE_SCRIPT = r"""
import json, os, duckdb
con = duckdb.connect()
con.execute("INSTALL httpfs; INSTALL delta; LOAD httpfs; LOAD delta; SET TimeZone='UTC';")
con.execute(
    "CREATE SECRET lake (TYPE s3, KEY_ID '%s', SECRET '%s', REGION 'us-east-1', ENDPOINT '%s', "
    "URL_STYLE 'path', USE_SSL false)"
    % (os.getenv("S3_ACCESS_KEY", "lakewire"), os.environ["S3_SECRET_KEY"], os.getenv("S3_HOSTPORT", "s3:9000"))
)
b = os.getenv("LAKE_BUCKET", "lake")
silver, bronze = f"delta_scan('s3://{b}/silver/edits')", f"delta_scan('s3://{b}/bronze/edits')"
pageviews = f"delta_scan('s3://{b}/silver/pageviews')"
out = {}
rows, distinct, first, last = con.execute(
    f"select count(*), count(distinct event_id), min(event_ts), max(event_ts) from {silver}").fetchone()
out["silver"] = {"rows": rows, "distinct": distinct, "first": str(first), "last": str(last)}
out["per_day"] = [
    {"day": str(d), "events": n, "bot_share": round(s, 3)}
    for d, n, s in con.execute(
        f"select event_date, count(*), avg(is_bot::int) from {silver} group by 1 order by 1").fetchall()
]
out["per_wiki"] = {w: n for w, n in con.execute(
    f"select wiki, count(*) from {silver} group by 1 order by 2 desc").fetchall()}
parts = con.execute(
    f"select kafka_partition, min(kafka_offset), max(kafka_offset), count(*) from {bronze} group by 1 order by 1"
).fetchall()
out["bronze"] = {
    "rows": sum(p[3] for p in parts),
    "gaps": sum((hi - lo + 1) - n for _, lo, hi, n in parts),
    "duplicate_offsets": con.execute(
        f"select count(*) from (select kafka_partition, kafka_offset from {bronze} group by 1, 2 having count(*) > 1)"
    ).fetchone()[0],
}
try:
    out["pageviews_by_day"] = {
        str(d): n for d, n in con.execute(
            f"select view_date, count(*) from {pageviews} group by 1 order by 1").fetchall()}
except duckdb.Error:
    out["pageviews_by_day"] = {}
print(json.dumps(out))
"""


# ---------------------------------------------------------------- small pure helpers (unit tested)
def parse_mem_mib(usage: str) -> float:
    """'2.105GiB / 4GiB' -> 2155.5 (the used part, in MiB)."""
    used = usage.split("/")[0].strip()
    for suffix, factor in (("GiB", 1024.0), ("MiB", 1.0), ("KiB", 1 / 1024), ("kB", 1 / 1024), ("B", 1 / 1048576)):
        if used.endswith(suffix):
            return float(used[: -len(suffix)]) * factor
    raise ValueError(f"cannot parse memory usage: {usage!r}")


def coverage(samples: int, window_seconds: float, step: int = SCRAPE_SECONDS) -> float:
    """Share of the window for which Prometheus has samples (gaps are when the host slept or the stack was down)."""
    if window_seconds <= 0:
        return 0.0
    return min(1.0, samples * step / window_seconds)


def find_gaps(timestamps: list[float], min_gap_seconds: float) -> list[tuple[float, float]]:
    """(start, length) of every hole between consecutive samples that is longer than min_gap_seconds."""
    ts = sorted(timestamps)
    return [(a, b - a) for a, b in zip(ts, ts[1:], strict=False) if b - a > min_gap_seconds]


def fmt_duration(seconds: float) -> str:
    seconds = int(seconds)
    if seconds >= 86400:
        return f"{seconds / 86400:.1f} days"
    if seconds >= 3600:
        return f"{seconds / 3600:.1f} h"
    return f"{seconds // 60} min"


# ---------------------------------------------------------------- data collection
def run(cmd: list[str], stdin: str | None = None) -> str:
    done = subprocess.run(
        cmd, input=stdin, capture_output=True, text=True, encoding="utf-8", errors="replace", cwd=ROOT, check=True
    )
    return done.stdout


def lake() -> dict:
    out = run(["docker", "compose", "exec", "-T", "airflow", "/opt/dbt-venv/bin/python", "-"], stdin=LAKE_SCRIPT)
    return json.loads(out[out.index("{"):])


def containers() -> dict:
    ids = run(["docker", "compose", "ps", "-q"]).split()
    rows = run(["docker", "inspect", "--format", "{{.Name}}|{{.RestartCount}}|{{.State.StartedAt}}", *ids]).splitlines()
    result = {}
    for line in rows:
        name, restarts, started = line.strip().split("|")
        result[name.lstrip("/").removeprefix("lakewire-").removesuffix("-1")] = {
            "restarts": int(restarts), "started": started[:19]}
    return result


def memory() -> dict:
    rows = run(["docker", "stats", "--no-stream", "--format", "{{.Name}}|{{.MemUsage}}"]).splitlines()
    per = {r.split("|")[0].removeprefix("lakewire-").removesuffix("-1"): parse_mem_mib(r.split("|")[1])
           for r in rows if r.startswith("lakewire-")}
    return {"per_container_mib": {k: round(v) for k, v in per.items()}, "total_mib": round(sum(per.values()))}


def disk() -> dict:
    def du(service: str, path: str) -> int:
        return int(run(["docker", "compose", "exec", "-T", service, "du", "-sk", path]).split()[0])

    return {"lake_kib": du("s3", "/data"), "kafka_kib": du("kafka", "/var/lib/kafka/data")}


def kafka_end_offset(topic: str) -> int:
    out = run(["docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh",
               "--bootstrap-server", "kafka:9092", "--topic", topic])
    return sum(int(line.rsplit(":", 1)[1]) for line in out.splitlines() if line.strip())


def prom_instant(query: str) -> float | None:
    url = f"{PROMETHEUS}/api/v1/query?" + urllib.parse.urlencode({"query": query})
    result = json.load(urllib.request.urlopen(url, timeout=20))["data"]["result"]  # noqa: S310 - localhost
    return float(result[0]["value"][1]) if result else None


def prom_timestamps(query: str, start: float, end: float, step: int) -> list[float]:
    qs = urllib.parse.urlencode({"query": query, "start": start, "end": end, "step": step})
    result = json.load(urllib.request.urlopen(f"{PROMETHEUS}/api/v1/query_range?{qs}", timeout=60))["data"]["result"]  # noqa: S310
    return [float(t) for t, _ in result[0]["values"]] if result else []


def monitoring(window_seconds: float) -> dict:
    window = int(min(window_seconds, 14 * 86400))  # Prometheus keeps 15 days
    now = datetime.now(UTC).timestamp()
    samples = prom_instant(f'count_over_time(up{{job="producer"}}[{window}s])') or 0
    reconnects = prom_instant(f"sum(increase(lakewire_sse_reconnects_total[{window}s]))")
    step = 60
    gaps = find_gaps(prom_timestamps('up{job="producer"}', now - window, now, step), 3 * step)
    longest = max(gaps, key=lambda g: g[1]) if gaps else None
    return {
        "window_seconds": window,
        "coverage": coverage(int(samples), window),
        "reconnects": None if reconnects is None else round(reconnects),
        "gaps_over_3min": len(gaps),
        "longest_gap_seconds": None if longest is None else round(longest[1]),
        "longest_gap_started": (
            None if longest is None else datetime.fromtimestamp(longest[0], UTC).strftime("%Y-%m-%d %H:%MZ")
        ),
    }


def collect() -> dict:
    report: dict = {"generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%MZ")}
    sections = {"lake": lake, "containers": containers, "memory": memory, "disk": disk,
                "dlq_messages": lambda: kafka_end_offset("wiki.edits.dlq"),
                "raw_messages": lambda: kafka_end_offset("wiki.edits.raw")}
    for name, fn in sections.items():
        try:
            report[name] = fn()
        except Exception as exc:  # noqa: BLE001 - a missing section must not hide the rest
            report[name] = {"unavailable": str(exc)[:120]}
    try:
        first = datetime.fromisoformat(report["lake"]["silver"]["first"]).replace(tzinfo=UTC)
        report["running_seconds"] = (datetime.now(UTC) - first).total_seconds()
        report["monitoring"] = monitoring(report["running_seconds"])
    except Exception as exc:  # noqa: BLE001
        report["monitoring"] = {"unavailable": str(exc)[:120]}
    return report


# ---------------------------------------------------------------- formatting
def ok(section) -> bool:
    return isinstance(section, dict) and "unavailable" not in section


def to_markdown(r: dict) -> str:
    lines = [f"## Lakewire run report (generated {r['generated_at']})", ""]
    rows: list[tuple[str, str]] = []
    if ok(r["lake"]):
        s, b = r["lake"]["silver"], r["lake"]["bronze"]
        days = r.get("running_seconds", 0) / 86400
        full = r["lake"]["per_day"][1:-1]  # drop the partial first and last day
        per_day_text = f"{round(sum(d['events'] for d in full) / len(full)):,}" if full else "n/a yet (needs 3+ days)"
        pageviews = r["lake"]["pageviews_by_day"]
        rows += [
            ("First event in the lake", f"{s['first'][:16]} UTC ({days:.1f} days ago)"),
            ("Events ingested (Silver, distinct)", f"{s['distinct']:,}"),
            ("Events per full day", per_day_text),
            ("Wikis", ", ".join(f"{w} {n:,}" for w, n in r["lake"]["per_wiki"].items())),
            ("Duplicate events in Silver", f"{s['rows'] - s['distinct']:,}"),
            ("Bronze offset gaps / duplicate offsets", f"{b['gaps']} / {b['duplicate_offsets']}"),
            ("Pageview days loaded", f"{len(pageviews)} ({sum(pageviews.values()):,} rows)"),
        ]
    if isinstance(r["dlq_messages"], int):
        rows.append(("Dead-letter messages (all time)", f"{r['dlq_messages']:,} of {r['raw_messages']:,} accepted"))
    m = r["monitoring"]
    if ok(m):
        rows += [
            ("Monitored coverage", f"{m['coverage'] * 100:.1f}% of the last {fmt_duration(m['window_seconds'])}"),
            ("Longest monitoring gap", "none over 3 minutes" if m["longest_gap_seconds"] is None
             else f"{fmt_duration(m['longest_gap_seconds'])} from {m['longest_gap_started']}"),
            ("Wikimedia stream reconnects", f"{m['reconnects']:,}"),
        ]
    if ok(r["containers"]):
        restarts = {k: v["restarts"] for k, v in r["containers"].items() if v["restarts"]}
        restart_text = ", ".join(f"{k} {v}" for k, v in restarts.items()) or "none"
        rows.append(("Container restarts (since last recreate)", restart_text))
    if ok(r["memory"]):
        rows.append(("Memory now (all containers)", f"{r['memory']['total_mib'] / 1024:.1f} GiB"))
    if ok(r["disk"]):
        lake_gb = r["disk"]["lake_kib"] / 1048576
        days = r.get("running_seconds", 0) / 86400
        per_day = f", about {lake_gb * 1024 / days:.0f} MiB/day" if days >= 0.1 else ""
        rough = " (rough: includes the one-off pageview day until 3+ days)" if 0.1 <= days < 3 else ""
        rows.append(("Lake size on disk", f"{lake_gb:.2f} GiB{per_day}{rough}"))
        rows.append(("Kafka data on disk", f"{r['disk']['kafka_kib'] / 1048576:.2f} GiB"))
    lines += ["| Metric | Value |", "|---|---|", *[f"| {k} | {v} |" for k, v in rows]]
    if ok(r["lake"]):
        lines += ["", "| Day (UTC) | Events | Bot share |", "|---|---|---|",
                  *[f"| {d['day']} | {d['events']:,} | {d['bot_share'] * 100:.0f}% |" for d in r["lake"]["per_day"]]]
    unavailable = [k for k, v in r.items() if isinstance(v, dict) and "unavailable" in v]
    if unavailable:
        lines += ["", "Unavailable: " + ", ".join(f"{k} ({r[k]['unavailable']})" for k in unavailable)]
    lines += [
        "",
        "Notes: the first and last day are partial. Coverage counts the time Prometheus could scrape the producer, "
        "so any period the computer slept or the stack was down shows as a gap. Restart counts reset whenever a "
        "container is recreated (for example by `docker compose up --build`), and the dead-letter count includes "
        "any test messages injected during failure drills.",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--json", action="store_true", help="print the raw numbers instead of markdown")
    args = parser.parse_args()
    report = collect()
    print(json.dumps(report, indent=2) if args.json else to_markdown(report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
