"""Hourly: dbt builds the Gold marts in DuckDB, then a JSON snapshot is exported for the dashboard."""
from datetime import timedelta

import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

from lakewire.alerts import notify_failure

DBT = "cd /opt/lakewire/dbt && /opt/dbt-venv/bin/dbt"


@dag(
    dag_id="lakewire_marts_hourly",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 11, 10, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": notify_failure,
    },
    tags=["lakewire", "marts"],
)
def marts_hourly():
    build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build --target prod --profiles-dir .",
        execution_timeout=timedelta(minutes=40),
    )
    export = BashOperator(
        task_id="export_marts",
        bash_command="cd /opt/lakewire && python -m lakewire.export",
    )
    build >> export


marts_hourly()
