"""Table schemas and pure DataFrame transforms (testable without Delta or Kafka)."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# What the producer writes to Kafka (flat, validated against the contract).
EVENT_JSON_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_ts", StringType()),
        StructField("wiki", StringType()),
        StructField("event_type", StringType()),
        StructField("title", StringType()),
        StructField("namespace", LongType()),
        StructField("user", StringType()),
        StructField("is_bot", BooleanType()),
        StructField("is_minor", BooleanType()),
        StructField("length_old", LongType()),
        StructField("length_new", LongType()),
        StructField("rev_old", LongType()),
        StructField("rev_new", LongType()),
    ]
)

BRONZE_SCHEMA = StructType(
    [
        StructField("raw_json", StringType()),
        StructField("kafka_topic", StringType()),
        StructField("kafka_partition", LongType()),
        StructField("kafka_offset", LongType()),
        StructField("kafka_ts", TimestampType()),
        StructField("ingested_at", TimestampType()),
        StructField("ingest_date", DateType()),
    ]
)

SILVER_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_ts", TimestampType()),
        StructField("event_date", DateType()),
        StructField("wiki", StringType()),
        StructField("event_type", StringType()),
        StructField("title", StringType()),
        StructField("namespace", LongType()),
        StructField("user", StringType()),
        StructField("is_bot", BooleanType()),
        StructField("is_minor", BooleanType()),
        StructField("length_old", LongType()),
        StructField("length_new", LongType()),
        StructField("rev_old", LongType()),
        StructField("rev_new", LongType()),
        StructField("ingested_at", TimestampType()),
    ]
)

PAGEVIEWS_SCHEMA = StructType(
    [
        StructField("view_hour", TimestampType()),
        StructField("view_date", DateType()),
        StructField("domain_code", StringType()),
        StructField("page_title", StringType()),
        StructField("views", LongType()),
    ]
)


def kafka_to_bronze(df: DataFrame) -> DataFrame:
    """Shape a Kafka source DataFrame into the Bronze table layout (value kept as received)."""
    return (
        df.select(
            F.col("value").cast("string").alias("raw_json"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").cast("long").alias("kafka_partition"),
            F.col("offset").cast("long").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("ingest_date", F.to_date("ingested_at"))
    )


def bronze_to_silver(df: DataFrame) -> DataFrame:
    """Parse Bronze JSON into typed Silver columns. Rows without id or timestamp are dropped."""
    parsed = df.select(F.from_json("raw_json", EVENT_JSON_SCHEMA).alias("e"), "ingested_at").select(
        "e.*", "ingested_at"
    )
    return (
        parsed.withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("event_date", F.to_date("event_ts"))
        .where(F.col("event_id").isNotNull() & F.col("event_ts").isNotNull())
        .select(*[f.name for f in SILVER_SCHEMA.fields])
    )


def dedupe_stream(df: DataFrame, watermark: str = "10 minutes") -> DataFrame:
    """Bound state with a watermark and drop repeats of the same event id inside it."""
    return df.withWatermark("event_ts", watermark).dropDuplicatesWithinWatermark(["event_id"])
