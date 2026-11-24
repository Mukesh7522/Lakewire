"""Shared helpers for the lightweight-pipeline tests: a fake Wikimedia stream and event builders."""
import gzip
import json
import threading
import time
from datetime import UTC, date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from lakewire.config import Settings

BULK, SPARSE = "codfw.mediawiki.recentchange", "eqiad.mediawiki.recentchange"


def make_event(i, when, wiki="enwiki", topic=BULK, **overrides):
    event = {"$schema": "/mediawiki/recentchange/1.0.1",
             "meta": {"id": f"evt-{i}", "dt": when.strftime("%Y-%m-%dT%H:%M:%S.000Z"), "topic": topic},
             "type": "edit", "namespace": 0, "title": f"Page {i}", "user": "U", "bot": False, "minor": False,
             "length": {"old": 1, "new": 2}, "revision": {"old": 1, "new": 2}, "wiki": wiki}
    event.update(overrides)
    return event


def serve(history, live_wiki="enwiki"):
    """A fake Wikimedia stream: replay `history` quickly, then a steady trickle of live events (like production)."""
    seen = {"headers": [], "paths": []}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            seen["headers"].append(self.headers.get("Last-Event-ID"))
            seen["paths"].append(self.path)
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            start = int(self.headers.get("Last-Event-ID") or 0)
            try:
                for n, event in enumerate(history[start:], start=start + 1):
                    self.wfile.write(f"id: {n}\ndata: {json.dumps(event)}\n\n".encode())
                    self.wfile.flush()
                n = max(len(history), start)
                while True:  # live phase
                    n += 1
                    live = make_event(10_000 + n, datetime.now(UTC).replace(tzinfo=None), wiki=live_wiki)
                    self.wfile.write(f"id: {n}\ndata: {json.dumps(live)}\n\n".encode())
                    self.wfile.flush()
                    time.sleep(0.02)
            except OSError:
                return

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, seen


def settings_for(server):
    base = Settings.from_env()
    return Settings(**{**base.__dict__, "stream_url": f"http://127.0.0.1:{server.server_port}/stream",
                       "wikis": ("enwiki", "dewiki"), "last_event_id_file": ""})


def naive_now():
    return datetime.now(UTC).replace(tzinfo=None)


def history_two_hours_ago(count=300):
    start = naive_now() - timedelta(hours=2)
    wikis = ["enwiki", "dewiki", "commonswiki"]
    events = [make_event(i, start + timedelta(seconds=20 * i), wiki=wikis[i % 3]) for i in range(count)]
    events.insert(5, make_event(900, start, topic=SPARSE))  # the other, sparse data centre
    return events


DAY = date(2026, 11, 10)


def hourly_file(hour):
    lines = [
        f"en Alpha {10 + hour} 0",
        "en.m Alpha 5 0",   # mobile views are added to desktop views
        "en Beta 7 0",
        "en Gamma 3 0",
        "en Foo bar 4 0",    # a title with a space still parses
        "de Wurst 999 0",    # another language is ignored
        "en.d Wiktionary 50 0",  # a similar-looking code that is not wanted
        "garbage line without numbers",
    ]
    return gzip.compress(("\n".join(lines) + "\n").encode())


def serve_dumps():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            name = self.path.rsplit("/", 1)[-1]
            if name.startswith("pageviews-20261110-") and name.endswith("0000.gz"):
                body = hourly_file(int(name[len("pageviews-20261110-"):][:2]))
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:  # md5sums.txt and anything else: not published
                self.send_response(404)
                self.end_headers()

        def do_HEAD(self):  # noqa: N802 - the "is the last hour published yet?" check
            name = self.path.rsplit("/", 1)[-1]
            self.send_response(200 if name.startswith("pageviews-20261110-") else 404)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server
