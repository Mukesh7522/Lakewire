"""Optional Slack alerts for Airflow failures. No-ops when SLACK_WEBHOOK_URL is unset."""
from __future__ import annotations

import logging

from lakewire.config import Settings

log = logging.getLogger("lakewire.alerts")


def notify_failure(context: dict) -> None:
    url = Settings.from_env().slack_webhook_url
    if not url:
        return
    import requests

    ti = context.get("task_instance")
    text = f":rotating_light: Lakewire task failed: {ti.dag_id}.{ti.task_id} (try {ti.try_number}) {ti.log_url}"
    try:
        requests.post(url, json={"text": text}, timeout=10)
    except requests.RequestException:
        log.exception("could not post Slack alert")
