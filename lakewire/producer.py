"""Wikimedia EventStreams -> Kafka producer with contract validation and a dead-letter topic.

Live mode:   python -m lakewire.producer
Replay mode: python -m lakewire.producer --replay tests/fixtures/sample_events.jsonl
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path
from typing import Any, Protocol

from prometheus_client import Counter, Gauge, start_http_server

from lakewire.config import Settings
from lakewire.contract import classify

log = logging.getLogger("lakewire.producer")

EVENTS = Counter("lakewire_events_total", "Events seen, by outcome", ["outcome"])
RECONNECTS = Counter("lakewire_sse_reconnects_total", "SSE reconnects")
LAST_EVENT = Gauge("lakewire_last_event_timestamp_seconds", "Wall clock of last accepted event")
LAST_EVENT_AGE = Gauge("lakewire_last_event_age_seconds", "Seconds since last accepted event")


_STATE = {"last": time.time()}
SAVE_EVERY_SECONDS = 5.0


class Sink(Protocol):
    def send(self, topic: str, key: str, value: dict[str, Any]) -> None: ...
    def flush(self) -> None: ...


def iter_sse(lines: Iterable[str]) -> Iterator[tuple[str | None, str]]:
    """Minimal Server-Sent Events parser. Yields (last_event_id, data) per dispatched event."""
    event_id: str | None = None
    data: list[str] = []
    for line in lines:
        if line == "":
            if data:
                yield event_id, "\n".join(data)
            data = []
            continue
        if line.startswith(":"):
            continue
        field, _, value = line.partition(":")
        value = value[1:] if value.startswith(" ") else value
        if field == "data":
            data.append(value)
        elif field == "id":
            event_id = value


def process_message(payload: str, settings: Settings, sink: Sink) -> str:
    """Handle one raw JSON payload. Returns the outcome label."""
    try:
        raw = json.loads(payload)
    except json.JSONDecodeError as exc:
        sink.send(settings.topic_dlq, "invalid-json", {"errors": [str(exc)], "payload": payload[:2000]})
        EVENTS.labels("dlq").inc()
        return "dlq"
    outcome, result = classify(raw, settings.wikis)
    if outcome == "ok":
        sink.send(settings.topic_raw, result["wiki"], result)
        _STATE["last"] = time.time()
        LAST_EVENT.set(_STATE["last"])
    elif outcome == "dlq":
        sink.send(settings.topic_dlq, "contract", {"errors": result, "payload": raw})
    EVENTS.labels(outcome).inc()
    return outcome


class KafkaSink:
    def __init__(self, bootstrap: str) -> None:
        from confluent_kafka import Producer  # imported lazily so unit tests need no broker client

        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "linger.ms": 200,
                "compression.type": "lz4",
                "enable.idempotence": True,
                "client.id": "lakewire-producer",
            }
        )

    @staticmethod
    def _on_delivery(err, _msg) -> None:
        if err is not None:
            EVENTS.labels("delivery_error").inc()
            log.error("delivery failed: %s", err)

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        data = json.dumps(value, separators=(",", ":")).encode()
        while True:
            try:
                self._producer.produce(topic, data, key=key, on_delivery=self._on_delivery)
                break
            except BufferError:
                self._producer.poll(1)
        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush(30)


def _load_last_id(path: str) -> str | None:
    if not path:
        return None
    try:
        value = Path(path).read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return value or None


def _save_last_id(path: str, value: str | None) -> None:
    """Atomically remember where the stream stopped, so a restart resumes instead of losing events."""
    if not path or not value:
        return
    try:
        target = Path(path)
        tmp = target.with_suffix(".tmp")
        tmp.write_text(value, encoding="utf-8")
        os.replace(tmp, target)
    except OSError as exc:
        log.warning("could not save last event id: %s", exc)


def _forget_last_id(path: str) -> None:
    if path:
        Path(path).unlink(missing_ok=True)


def stream_live(settings: Settings, handler: Callable[[str], None], stop: Callable[[], bool]) -> None:
    import requests

    state_file = settings.last_event_id_file
    last_id: str | None = _load_last_id(state_file)
    if last_id:
        log.info("resuming the stream from the saved position")
    saved_at = time.monotonic()
    backoff = 1.0
    try:
        while not stop():
            headers = {"User-Agent": settings.user_agent, "Accept": "text/event-stream"}
            if last_id:
                headers["Last-Event-ID"] = last_id
            try:
                with requests.get(settings.stream_url, headers=headers, stream=True, timeout=(10, 90)) as resp:
                    resp.raise_for_status()
                    log.info("connected to %s", settings.stream_url)
                    backoff = 1.0
                    for event_id, data in iter_sse(resp.iter_lines(chunk_size=1024, decode_unicode=True)):
                        last_id = event_id or last_id
                        handler(data)
                        if time.monotonic() - saved_at >= SAVE_EVERY_SECONDS:
                            _save_last_id(state_file, last_id)
                            saved_at = time.monotonic()
                        if stop():
                            return
            except requests.RequestException as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if last_id and status is not None and 400 <= status < 500:
                    # The server no longer accepts our position (too old). Start from live instead of looping.
                    log.warning("saved stream position rejected (HTTP %s); starting from live", status)
                    last_id = None
                    _forget_last_id(state_file)
                    continue
                RECONNECTS.inc()
                log.warning("stream error (%s); reconnecting in %.0fs", exc, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 60.0)
    finally:
        _save_last_id(state_file, last_id)


def replay(path: str, settings: Settings, sink: Sink) -> dict[str, int]:
    counts: dict[str, int] = {"ok": 0, "skip": 0, "dlq": 0}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                counts[process_message(line, settings, sink)] += 1
    sink.flush()
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--replay", help="replay a JSONL file of raw events, then exit")
    parser.add_argument("--metrics-port", type=int, default=8000)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    settings = Settings.from_env()
    sink = KafkaSink(settings.kafka_bootstrap)

    if args.replay:
        counts = replay(args.replay, settings, sink)
        log.info("replay finished: %s", counts)
        return 0

    start_http_server(args.metrics_port)
    LAST_EVENT.set(_STATE["last"])
    LAST_EVENT_AGE.set_function(lambda: time.time() - _STATE["last"])

    stopping = {"flag": False}
    signal.signal(signal.SIGTERM, lambda *_: stopping.update(flag=True))
    signal.signal(signal.SIGINT, lambda *_: stopping.update(flag=True))

    def handler(payload: str) -> None:
        process_message(payload, settings, sink)

    try:
        stream_live(settings, handler, lambda: stopping["flag"])
    finally:
        sink.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
