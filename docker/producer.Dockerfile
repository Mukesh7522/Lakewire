FROM python:3.11-slim-bookworm
WORKDIR /app
COPY requirements-producer.txt .
RUN pip install --no-cache-dir -r requirements-producer.txt
COPY lakewire ./lakewire
COPY tests/fixtures ./tests/fixtures
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1
# Run unprivileged: the producer only reads files and talks to the network.
# /state holds the stream resume position; a named volume mounted there inherits this ownership.
RUN useradd --system --create-home --uid 10001 app && mkdir /state && chown app:app /state
USER app
CMD ["python", "-m", "lakewire.producer"]
