import gzip
import threading
from datetime import date
from http.server import BaseHTTPRequestHandler, HTTPServer

from lakewire.batch import dumps

BODY = gzip.compress(b"en Main_Page 5 0\n" * 200_000)


def test_download_retries_when_the_connection_drops_mid_body(tmp_path, monkeypatch):
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            calls.append(1)
            self.send_response(200)
            self.send_header("Content-Length", str(len(BODY)))
            self.end_headers()
            if len(calls) == 1:  # first attempt: promise the full body, send half, hang up
                self.wfile.write(BODY[: len(BODY) // 2])
                self.wfile.flush()
                self.close_connection = True
                return
            self.wfile.write(BODY)

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setattr(dumps, "BASE_URL", f"http://127.0.0.1:{server.server_port}")
    monkeypatch.setattr(dumps.time, "sleep", lambda _s: None)
    try:
        out = dumps.download_hour(date(2026, 11, 10), 5, tmp_path, "test-agent", {})
    finally:
        server.shutdown()
    assert len(calls) == 2
    assert out.read_bytes() == BODY
