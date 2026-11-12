"""Daily pageview backfill: wait for the dump, download, validate, parse, overwrite the day's partition."""
import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from lakewire.alerts import notify_failure

START = pendulum.parse(os.getenv("PAGEVIEWS_START_DATE", "2026-11-10"))


def _dump_ready(ds: str) -> bool:
    from datetime import datetime

    from lakewire.batch.pageviews import dump_available
    from lakewire.config import Settings

    day = datetime.strptime(ds, "%Y-%m-%d").date()
    return dump_available(day, 23, Settings.from_env().user_agent)


def _ingest(ds: str) -> int:
    from lakewire.batch.pageviews import run

    return run(ds)


@dag(
    dag_id="lakewire_pageviews_daily",
    schedule="@daily",
    start_date=START,
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=20),
        "on_failure_callback": notify_failure,
    },
    tags=["lakewire", "batch"],
)
def pageviews_daily():
    wait = PythonSensor(
        task_id="wait_for_dump",
        python_callable=_dump_ready,
        op_kwargs={"ds": "{{ ds }}"},
        mode="reschedule",
        poke_interval=900,
        timeout=8 * 3600,
    )
    ingest = PythonOperator(
        task_id="ingest_pageviews",
        python_callable=_ingest,
        op_kwargs={"ds": "{{ ds }}"},
        execution_timeout=timedelta(hours=2),
    )
    wait >> ingest


pageviews_daily()
