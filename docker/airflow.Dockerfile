FROM apache/airflow:2.10.5-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /data/warehouse /data/exports /opt/ivy /opt/dbt-venv \
    && chown -R airflow /data /opt/ivy /opt/dbt-venv
USER airflow
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt
# dbt lives in its own virtualenv so its dependencies cannot clash with Airflow's.
RUN python -m venv /opt/dbt-venv && /opt/dbt-venv/bin/pip install --no-cache-dir dbt-duckdb==1.11.1 duckdb==2.0.0
ENV PYTHONPATH=/opt/lakewire SPARK_IVY_DIR=/opt/ivy \
    AIRFLOW__CORE__LOAD_EXAMPLES=false AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False AIRFLOW__CORE__DAGS_FOLDER=/opt/lakewire/dags \
    DUCKDB_PATH=/data/warehouse/lakewire.duckdb EXPORT_DIR=/data/exports
COPY --chown=airflow lakewire /opt/lakewire/lakewire
COPY --chown=airflow dbt /opt/lakewire/dbt
COPY --chown=airflow dags /opt/lakewire/dags
RUN S3_SECRET_KEY=build-time-only python -c "from lakewire.spark_utils import build_spark; s=build_spark('warm'); s.range(1).count(); s.stop()"
