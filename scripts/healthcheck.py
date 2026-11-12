"""Run on the VM after a deploy (and by cron): are the containers up and is data actually flowing?"""
from __future__ import annotations

import json
import re
import subprocess
import sys
import time
import urllib.request

REQUIRED = ["kafka", "s3", "producer", "spark-stream", "airflow"]


def metric(url: str, name: str) -> float | None:
    text = urllib.request.urlopen(url, timeout=10).read().decode()  # noqa: S310 - localhost only
    m = re.search(rf"^{name}(?:{{[^}}]*}})?\s+([0-9.eE+-]+)$", text, re.M)
    return float(m.group(1)) if m else None


def main() -> int:
    cmd = ["docker", "compose", "ps", "--format", "json"]
    out = subprocess.run(cmd, capture_output=True, text=True, check=True).stdout
    running = {}
    for line in out.splitlines():
        if line.strip():
            c = json.loads(line)
            running[c["Service"]] = c["State"]
    problems = [f"{s} is {running.get(s, 'missing')}" for s in REQUIRED if running.get(s) != "running"]

    deadline = time.time() + 120  # allow a fresh deploy to connect
    age = None
    while time.time() < deadline:
        try:
            age = metric("http://127.0.0.1:8000/metrics", "lakewire_last_event_age_seconds")
            if age is not None and age < 120:
                break
        except OSError:
            pass
        time.sleep(5)
    if age is None or age >= 300:
        problems.append(f"producer last accepted event age: {age}")

    if problems:
        print("UNHEALTHY:", "; ".join(problems))
        return 1
    print(f"healthy: all services running, last event {age:.0f}s ago")
    return 0


if __name__ == "__main__":
    sys.exit(main())
