import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from types import SimpleNamespace

from lakewire.alerts import notify_failure

CONTEXT = {"task_instance": SimpleNamespace(dag_id="d", task_id="t", try_number=2, log_url="http://airflow/log")}


def test_alert_is_a_no_op_without_a_webhook(monkeypatch):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    notify_failure(CONTEXT)  # must not raise or try the network


def test_alert_posts_the_failure_to_the_webhook(monkeypatch):
    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            received.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setenv("SLACK_WEBHOOK_URL", f"http://127.0.0.1:{server.server_port}/hook")
    try:
        notify_failure(CONTEXT)
    finally:
        server.shutdown()
    assert len(received) == 1
    assert "d.t" in received[0]["text"] and "try 2" in received[0]["text"]


def test_alert_swallows_webhook_errors(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "http://127.0.0.1:9/unreachable")
    notify_failure(CONTEXT)  # a dead webhook must never fail the Airflow callback
