import json

from lakewire import producer
from lakewire.config import Settings


class FakeSink:
    def __init__(self):
        self.sent = []

    def send(self, topic, key, value):
        self.sent.append((topic, key, value))

    def flush(self):
        pass


def test_sse_parser_handles_comments_ids_and_multiline_data():
    lines = [":ok", "", "id: 42", "data: {\"a\":", "data: 1}", "", "event: message", "data: second", ""]
    events = list(producer.iter_sse(lines))
    assert events == [("42", '{"a":\n1}'), ("42", "second")]


def test_heartbeat_only_stream_yields_nothing():
    assert list(producer.iter_sse([":heartbeat", "", ":heartbeat", ""])) == []


def test_process_message_routes_ok_skip_and_dlq():
    settings = Settings.from_env()
    sink = FakeSink()
    ok = {"meta": {"id": "x", "dt": "2026-11-10T00:00:00Z"}, "type": "edit", "wiki": "enwiki",
          "title": "T", "user": "U"}
    assert producer.process_message(json.dumps(ok), settings, sink) == "ok"
    assert producer.process_message(json.dumps({**ok, "type": "log"}), settings, sink) == "skip"
    assert producer.process_message("{not json", settings, sink) == "dlq"
    topics = [t for t, _, _ in sink.sent]
    assert topics == [settings.topic_raw, settings.topic_dlq]


def test_replay_counts_match_fixture(tmp_path):
    settings = Settings.from_env()
    sink = FakeSink()
    fixture = "tests/fixtures/sample_events.jsonl"
    counts = producer.replay(fixture, settings, sink)
    expected = json.load(open("tests/fixtures/expected.json"))
    assert counts["ok"] == expected["producer_ok"] and counts["dlq"] == expected["producer_dlq"]


def test_live_stream_survives_a_dropped_connection_and_resumes_with_last_event_id():
    """Local SSE server: first connection dies mid-stream, second must send Last-Event-ID."""
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer

    seen_headers = []
    ev = {"meta": {"id": "a", "dt": "2026-11-10T00:00:00Z"}, "type": "edit", "wiki": "enwiki",
          "title": "T", "user": "U"}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            seen_headers.append(self.headers.get("Last-Event-ID"))
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            n = len(seen_headers)
            body = json.dumps({**ev, "meta": {**ev["meta"], "id": f"e{n}"}})
            self.wfile.write(f": hello\n\nid: id-{n}\ndata: {body}\n\n".encode())
            self.wfile.flush()

        def log_message(self, *a):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = Settings.from_env()
    settings = Settings(**{**base.__dict__, "stream_url": f"http://127.0.0.1:{server.server_port}/s"})
    sink, got = FakeSink(), []

    def handler(payload):
        got.append(producer.process_message(payload, settings, sink))

    producer.stream_live(settings, handler, lambda: len(got) >= 2)
    server.shutdown()
    assert got == ["ok", "ok"]
    assert seen_headers[0] is None and seen_headers[1] == "id-1"


def _sse_server(reject_id=None):
    """Local SSE server. Records Last-Event-ID headers; answers 400 when the id equals reject_id."""
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer

    seen = []
    ev = {"meta": {"id": "a", "dt": "2026-11-10T00:00:00Z"}, "type": "edit", "wiki": "enwiki",
          "title": "T", "user": "U"}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            seen.append(self.headers.get("Last-Event-ID"))
            if reject_id is not None and seen[-1] == reject_id:
                self.send_response(400)
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            body = json.dumps({**ev, "meta": {**ev["meta"], "id": f"e{len(seen)}"}})
            self.wfile.write(f"id: id-{len(seen)}\ndata: {body}\n\n".encode())
            self.wfile.flush()

        def log_message(self, *a):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, seen


def _settings_for(server, state_file):
    base = Settings.from_env()
    return Settings(**{**base.__dict__, "stream_url": f"http://127.0.0.1:{server.server_port}/s",
                       "last_event_id_file": str(state_file)})


def test_stream_position_survives_a_restart(tmp_path):
    """A new process must resume from the id the previous one saved, not start from live."""
    server, seen = _sse_server()
    state = tmp_path / "last_event_id"
    settings = _settings_for(server, state)
    def one_process_lifetime():
        got = []
        producer.stream_live(settings, lambda p: got.append(producer.process_message(p, settings, FakeSink())),
                             lambda: len(got) >= 1)

    one_process_lifetime()
    one_process_lifetime()
    server.shutdown()
    assert seen[0] is None and seen[1] == "id-1"
    assert state.read_text() == "id-2"


def test_a_rejected_saved_position_falls_back_to_live(tmp_path):
    server, seen = _sse_server(reject_id="too-old")
    state = tmp_path / "last_event_id"
    state.write_text("too-old")
    settings = _settings_for(server, state)
    got = []
    producer.stream_live(settings, lambda p: got.append(producer.process_message(p, settings, FakeSink())),
                         lambda: len(got) >= 1)
    server.shutdown()
    assert got == ["ok"]
    assert seen == ["too-old", None]  # rejected once, then retried from live
    assert state.read_text() == "id-2"
