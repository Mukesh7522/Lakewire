"""Runtime settings, read from environment variables at call time."""
from __future__ import annotations

import os
from dataclasses import dataclass


def _csv(name: str, default: str) -> tuple[str, ...]:
    return tuple(x.strip() for x in os.getenv(name, default).split(",") if x.strip())


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str
    topic_raw: str
    topic_dlq: str
    wikis: tuple[str, ...]
    user_agent: str
    stream_url: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    lake_bucket: str
    pageview_domains: tuple[str, ...]
    pageview_top_n: int
    bronze_retention_days: int
    silver_retention_days: int
    stream_trigger_seconds: int
    slack_webhook_url: str

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            kafka_bootstrap=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
            topic_raw=os.getenv("TOPIC_RAW", "wiki.edits.raw"),
            topic_dlq=os.getenv("TOPIC_DLQ", "wiki.edits.dlq"),
            wikis=_csv("WIKIS", "enwiki,dewiki,frwiki,eswiki,jawiki"),
            user_agent=os.getenv("USER_AGENT", "Lakewire/1.0 (set USER_AGENT in .env)"),
            stream_url=os.getenv(
                "STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange"
            ),
            s3_endpoint=os.getenv("S3_ENDPOINT", "http://s3:9000"),
            s3_access_key=os.getenv("S3_ACCESS_KEY", "lakewire"),
            s3_secret_key=os.getenv("S3_SECRET_KEY", ""),
            lake_bucket=os.getenv("LAKE_BUCKET", "lake"),
            pageview_domains=_csv("PAGEVIEW_DOMAINS", "en,en.m"),
            pageview_top_n=int(os.getenv("PAGEVIEW_TOP_N", "50000")),
            bronze_retention_days=int(os.getenv("BRONZE_RETENTION_DAYS", "14")),
            silver_retention_days=int(os.getenv("SILVER_RETENTION_DAYS", "120")),
            stream_trigger_seconds=int(os.getenv("STREAM_TRIGGER_SECONDS", "30")),
            slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL", ""),
        )

    def lake_path(self, layer: str, table: str) -> str:
        return f"s3a://{self.lake_bucket}/{layer}/{table}"

    def checkpoint_path(self, name: str) -> str:
        return f"s3a://{self.lake_bucket}/_checkpoints/{name}"
