"""CI end-to-end assertions. Run inside the spark image after producer replay + one-shot stream."""
from __future__ import annotations

import json
import sys
from pathlib import Path

from lakewire.config import Settings
from lakewire.spark_utils import build_spark


def main() -> int:
    expected = json.loads(Path(sys.argv[1]).read_text())
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 1  # how many times the fixture was replayed
    s = Settings.from_env()
    spark = build_spark("smoke-check", s)
    spark.sparkContext.setLogLevel("ERROR")

    bronze = spark.read.format("delta").load(s.lake_path("bronze", "edits")).count()
    silver_df = spark.read.format("delta").load(s.lake_path("silver", "edits"))
    silver, distinct = silver_df.count(), silver_df.select("event_id").distinct().count()
    dlq = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", s.kafka_bootstrap)
        .option("subscribe", s.topic_dlq)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .count()
    )
    results = {
        "bronze_rows": (bronze, expected["producer_ok"] * runs),
        "silver_rows": (silver, expected["silver_distinct"]),
        "silver_distinct_ids": (distinct, expected["silver_distinct"]),
        "dlq_messages": (dlq, expected["dlq_messages"] * runs),
    }
    failed = {k: v for k, v in results.items() if v[0] != v[1]}
    for name, (got, want) in results.items():
        print(f"{'FAIL' if name in failed else 'ok  '} {name}: got {got}, expected {want}")
    spark.stop()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
