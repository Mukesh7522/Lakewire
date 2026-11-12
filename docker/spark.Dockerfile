FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements-runtime.txt .
RUN pip install --no-cache-dir -r requirements-runtime.txt
COPY lakewire ./lakewire
COPY scripts ./scripts
COPY tests/fixtures ./tests/fixtures
ENV PYTHONPATH=/app SPARK_IVY_DIR=/opt/ivy PYTHONUNBUFFERED=1
# Run unprivileged. Spark needs a writable home, the Ivy cache and the working directory.
RUN useradd --system --create-home --uid 10001 app && mkdir -p /opt/ivy && chown -R app:app /app /opt/ivy
USER app
# Warm the Ivy cache at build time so containers start without downloading jars. Any non-empty
# credential works here: the warm-up never touches storage.
RUN S3_SECRET_KEY=build-time-only python -c "from lakewire.spark_utils import build_spark; s=build_spark('warm'); s.range(1).count(); s.stop()"
CMD ["python", "-m", "lakewire.streaming.edits_stream"]
