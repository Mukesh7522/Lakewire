"""CI helper: load the pageview parquet fixture into the Silver pageviews Delta table on MinIO."""
from __future__ import annotations

import sys

from lakewire.config import Settings
from lakewire.spark_utils import build_spark

s = Settings.from_env()
spark = build_spark("seed-pageviews", s)
df = spark.read.parquet(sys.argv[1])
df.write.format("delta").mode("overwrite").partitionBy("view_date").save(s.lake_path("silver", "pageviews"))
print("seeded", df.count(), "pageview rows")
spark.stop()
