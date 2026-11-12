"""Spark session factory: Delta Lake + S3A (S3-compatible store) + Kafka source, UTC everywhere."""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

from lakewire.config import Settings

SPARK_VERSION = "3.5.1"
DELTA_VERSION = "3.2.0"
PACKAGES = ",".join(
    [
        f"io.delta:delta-spark_2.12:{DELTA_VERSION}",
        f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ]
)


def build_spark(app_name: str, settings: Settings | None = None, shuffle_partitions: int = 8) -> SparkSession:
    settings = settings or Settings.from_env()
    if not settings.s3_secret_key:
        raise RuntimeError("S3_SECRET_KEY is not set; refusing to start Spark without storage credentials")
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.jars.packages", PACKAGES)
        .config("spark.jars.ivy", os.getenv("SPARK_IVY_DIR", "/opt/ivy"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )
    return builder.getOrCreate()
