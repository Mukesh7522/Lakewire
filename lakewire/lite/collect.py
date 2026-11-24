"""Pull the edits that arrived since the last run from Wikimedia's EventStreams, then stop.

The always-on pipeline keeps a connection open forever. Here a scheduled job connects, replays what it missed from the
saved stream position (`Last-Event-ID`; the first run uses `since`), and disconnects once it has caught up.

Correctness never depends on when this stops: the final stream position is saved and the next run continues from it
exactly, and duplicates are dropped by event_id. Stopping early only means the next run has more to read.
"""
from __future__ import annotations

import dataclasses
import re
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from lakewire import producer
from lakewire.config import Settings
from lakewire.lite.store import parse_ts

# Cheap pre-filters on the raw text so that events from wikis we do not keep are never JSON-parsed.
WIKI_RE = re.compile(r'"wiki"\s*:\s*"([^"]*)"')
DT_RE = re.compile(r'"dt"\s*:\s*"([^"]+)"')
TOPIC_RE = re.compile(r'"topic"\s*:\s*"([^"]+)"')


class MemorySink:
    """Collects what the producer's contract check would have sent to Kafka."""

    def __init__(self, dlq_topic: str) -> None:
        self.dlq_topic = dlq_topic
        self.events: list[dict[str, Any]] = []
        self.dlq: list[dict[str, Any]] = []

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        (self.dlq if topic == self.dlq_topic else self.events).append(value)

    def flush(self) -> None:
        pass


@dataclass
class CollectResult:
    events: list[dict[str, Any]] = field(default_factory=list)
    dlq: list[dict[str, Any]] = field(default_factory=list)
    read: int = 0                  # every event the stream delivered
    filtered: int = 0              # dropped by the wiki pre-filter without parsing
    seconds: float = 0.0
    caught_up: bool = False
    timed_out: bool = False
    resumed: bool = False          # a saved stream position was used
    bulk_topic: str | None = None  # the topic that carries most events (it is not always the same data centre)
    first_bulk_dt: datetime | None = None
    last_bulk_dt: datetime | None = None


def gap_minutes(previous_watermark: datetime | None, first_event: datetime | None) -> float | None:
    """How long after the previous run's newest event this run's first event is. Large means a possible gap."""
    if previous_watermark is None or first_event is None:
        return None
    return (first_event - previous_watermark).total_seconds() / 60


def collect(
    settings: Settings,
    state_dir: Path,
    now: datetime,
    *,
    max_seconds: float = 1200,
    grace_seconds: float = 45,
    initial_hours: float = 3,
    live_slack_seconds: float = 120,
    min_events_for_live: int = 200,
    clock: Callable[[], float] = time.monotonic,
) -> CollectResult:
    """Read the stream until it is caught up (plus a short grace period) or the time budget runs out."""
    state_dir.mkdir(parents=True, exist_ok=True)
    id_file = state_dir / "last_event_id"
    result = CollectResult(resumed=id_file.exists() and bool(id_file.read_text(encoding="utf-8").strip()))

    since = (now - timedelta(hours=initial_hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
    separator = "&" if "?" in settings.stream_url else "?"
    # The server prefers the Last-Event-ID header over `since`, so `since` only matters on the first run.
    config = dataclasses.replace(
        settings, stream_url=f"{settings.stream_url}{separator}since={since}", last_event_id_file=str(id_file)
    )
    sink = MemorySink(settings.topic_dlq)
    wikis = set(settings.wikis)
    live_after = now - timedelta(seconds=live_slack_seconds)
    per_topic: Counter[str] = Counter()
    first_dt: dict[str, datetime] = {}
    last_dt: dict[str, datetime] = {}
    started = clock()
    caught_up_at: float | None = None

    def handler(payload: str) -> None:
        nonlocal caught_up_at
        result.read += 1
        topic_match, dt_match = TOPIC_RE.search(payload), DT_RE.search(payload)
        if topic_match and dt_match:
            try:
                stamp = parse_ts(dt_match.group(1))
            except ValueError:
                stamp = None
            if stamp is not None:
                topic = topic_match.group(1)
                per_topic[topic] += 1
                first_dt.setdefault(topic, stamp)
                last_dt[topic] = max(last_dt.get(topic, stamp), stamp)
                if caught_up_at is None and result.read >= min_events_for_live:
                    bulk = per_topic.most_common(1)[0][0]
                    if last_dt[bulk] >= live_after:
                        caught_up_at = clock()
        wiki_match = WIKI_RE.search(payload)
        if wiki_match and wiki_match.group(1) not in wikis:
            result.filtered += 1
            return
        producer.process_message(payload, config, sink)

    def stop() -> bool:
        if clock() - started >= max_seconds:
            result.timed_out = True
            return True
        return caught_up_at is not None and clock() - caught_up_at >= grace_seconds

    producer.stream_live(config, handler, stop)

    result.seconds = clock() - started
    result.caught_up = caught_up_at is not None
    result.events, result.dlq = sink.events, sink.dlq
    if per_topic:
        result.bulk_topic = per_topic.most_common(1)[0][0]
        result.first_bulk_dt = first_dt[result.bulk_topic]
        result.last_bulk_dt = last_dt[result.bulk_topic]
    return result
