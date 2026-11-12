import json
import tempfile

from lakewire.batch.pageviews import parse_pageviews
from lakewire.streaming.schemas import SILVER_SCHEMA, bronze_to_silver, dedupe_stream


def _bronze_rows(spark, events):
    rows = [(json.dumps(e), "2026-11-10 08:05:00") for e in events]
    df = spark.createDataFrame(rows, ["raw_json", "ingested_at"])
    return df.selectExpr("raw_json", "to_timestamp(ingested_at) as ingested_at")


def event(i, ts="2026-11-10T08:00:00Z", **over):
    base = {"event_id": f"e{i}", "event_ts": ts, "wiki": "enwiki", "event_type": "edit", "title": "T",
            "namespace": 0, "user": "U", "is_bot": False, "is_minor": False,
            "length_old": 1, "length_new": 2, "rev_old": 1, "rev_new": 2}
    return {**base, **over}


def test_bronze_to_silver_types_and_drops_bad_rows(spark):
    df = _bronze_rows(spark, [event(1), event(2, ts="not a date"), {"title": "no id"}])
    out = bronze_to_silver(df)
    assert [f.name for f in out.schema.fields] == [f.name for f in SILVER_SCHEMA.fields]
    rows = out.collect()
    assert len(rows) == 1 and rows[0].event_id == "e1" and str(rows[0].event_date) == "2026-11-10"


def test_streaming_dedupe_removes_redelivered_events_and_keeps_late_ones(spark):
    src, ckpt = tempfile.mkdtemp(), tempfile.mkdtemp()
    events = [event(1), event(2, "2026-11-10T08:01:00Z"), event(1), event(1), event(2, "2026-11-10T08:01:00Z"),
              event(3, "2026-11-10T06:00:00Z")]  # e3 is hours late relative to the others
    _bronze_rows(spark, events).write.mode("overwrite").json(src)
    schema = "raw_json string, ingested_at timestamp"
    collected = []

    def sink(batch, _id):
        collected.extend(r.event_id for r in batch.collect())

    stream = dedupe_stream(bronze_to_silver(spark.readStream.schema(schema).json(src)))
    q = stream.writeStream.foreachBatch(sink).option("checkpointLocation", ckpt).trigger(availableNow=True).start()
    q.awaitTermination()
    assert sorted(set(collected)) == ["e1", "e2", "e3"]
    assert collected.count("e1") == 1 and collected.count("e2") == 1


def test_parse_pageviews_filters_domains_merges_mobile_and_keeps_top_n(spark):
    path = "file:///tmp/pageviews-20261110-050000.gz"
    lines = ["en Main_Page 100 0", "en.m Main_Page 50 0", "de Hauptseite 999 0", "en Other 10 0", "en.d Wiktionary 5 0",
             "garbage line"]
    raw = spark.createDataFrame([(ln, path) for ln in lines], ["value", "path"])
    out = parse_pageviews(raw, ("en", "en.m"), top_n=1).collect()
    assert len(out) == 1
    assert out[0].page_title == "Main_Page" and out[0].views == 150
    assert str(out[0].view_hour) == "2026-11-10 05:00:00" and str(out[0].view_date) == "2026-11-10"
