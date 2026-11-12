"""Generate deterministic test fixtures (committed to the repo; rerun only to change them)."""
from __future__ import annotations

import json
import random
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

OUT = Path(__file__).resolve().parent.parent / "tests" / "fixtures"
BASE = datetime(2026, 11, 10, 8, 0, 0)


def raw_event(i: int, wiki: str, etype: str, ts: datetime, user: str, bot: bool, title: str) -> dict:
    return {
        "$schema": "/mediawiki/recentchange/1.0.0",
        "meta": {"id": f"evt-{i:04d}", "dt": ts.strftime("%Y-%m-%dT%H:%M:%SZ"), "stream": "mediawiki.recentchange"},
        "id": 9000 + i,
        "type": etype,
        "namespace": 0,
        "title": title,
        "user": user,
        "bot": bot,
        "minor": False,
        "length": {"old": 1000 + i, "new": 1100 + 2 * i},
        "revision": {"old": 500 + i, "new": 501 + i},
        "wiki": wiki,
        "server_name": f"{wiki[:2]}.wikipedia.org",
    }


def make_sample_events() -> None:
    rng = random.Random(7)
    valid = []
    for i in range(12):
        wiki = "enwiki" if i % 3 else "dewiki"
        valid.append(raw_event(i, wiki, "new" if i == 5 else "edit", BASE + timedelta(minutes=7 * i),
                               f"User{i % 4}", i % 5 == 0, f"Article {i % 6}"))
    lines = [json.dumps(e) for e in valid]
    lines += [json.dumps(valid[2]), json.dumps(valid[7]), json.dumps(valid[7])]  # 3 duplicate deliveries
    lines += [json.dumps(raw_event(100, "enwiki", "log", BASE, "Admin", False, "Log entry")),
              json.dumps(raw_event(101, "enwiki", "log", BASE, "Admin", False, "Log entry 2")),
              json.dumps(raw_event(102, "wikidatawiki", "edit", BASE, "Bot", True, "Q42"))]  # skipped
    broken = raw_event(103, "enwiki", "edit", BASE, "Someone", False, "Broken")
    del broken["meta"]["id"]  # contract violation
    lines.append(json.dumps(broken))
    lines.append("{this is not json")  # contract violation
    rng.shuffle(lines)  # out-of-order delivery
    (OUT / "sample_events.jsonl").write_text("\n".join(lines) + "\n")
    (OUT / "expected.json").write_text(json.dumps(
        {"producer_ok": 15, "producer_skip": 3, "producer_dlq": 2, "silver_distinct": 12, "dlq_messages": 2}, indent=1))


def make_parquet_fixtures() -> None:
    rng = random.Random(11)
    users = [f"Editor{i}" for i in range(8)]
    rows, n = [], 0
    for day in (9, 10):
        start = datetime(2026, 11, day, 0, 0, 0)
        for _ in range(400):
            ts = start + timedelta(seconds=rng.randint(0, 86399))
            wiki = rng.choice(["enwiki", "enwiki", "dewiki", "frwiki"])
            n += 1
            rows.append((f"fx-{n:05d}", ts, ts.date(), wiki, rng.choice(["edit", "edit", "edit", "new"]),
                         f"Page_{rng.randint(1, 15)}".replace("_", " "), 0, rng.choice(users),
                         rng.random() < 0.3, rng.random() < 0.2, 1000, 1000 + rng.randint(-300, 600), 1, 2, ts))
        for k in range(8):  # a contested page: different editors, minutes apart
            ts = start + timedelta(hours=12, minutes=3 * k)
            n += 1
            rows.append((f"fx-{n:05d}", ts, ts.date(), "enwiki", "edit", "Contested page", 0, f"Rival{k % 3}",
                         False, False, 5000, 5000 + (800 if k % 2 else -800), 1, 2, ts))
    pv = []
    for day in (9, 10):
        for hour in range(24):
            for p in range(1, 16):
                pv.append((datetime(2026, 11, day, hour), datetime(2026, 11, day, hour).date(), "en+en.m",
                           f"Page_{p}", rng.randint(50, 5000) * (3 if (day == 10 and p == 4) else 1)))
    con = duckdb.connect()
    con.execute("""create table edits(event_id varchar, event_ts timestamp, event_date date, wiki varchar,
        event_type varchar, title varchar, namespace bigint, "user" varchar, is_bot boolean, is_minor boolean,
        length_old bigint, length_new bigint, rev_old bigint, rev_new bigint, ingested_at timestamp)""")
    con.executemany("insert into edits values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    con.execute(
        "create table pageviews(view_hour timestamp, view_date date, domain_code varchar, "
        "page_title varchar, views bigint)"
    )
    con.executemany("insert into pageviews values (?,?,?,?,?)", pv)
    for t in ("edits", "pageviews"):
        con.execute(f"copy {t} to '{OUT / (t + '.parquet')}' (format parquet)")


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    make_sample_events()
    make_parquet_fixtures()
    print("fixtures written to", OUT)
