FROM python:3.11-slim-bookworm
WORKDIR /app
COPY requirements-producer.txt .
RUN pip install --no-cache-dir -r requirements-producer.txt
COPY lakewire ./lakewire
COPY tests/fixtures ./tests/fixtures
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1
CMD ["python", "-m", "lakewire.producer"]
