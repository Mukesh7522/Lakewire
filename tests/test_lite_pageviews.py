import duckdb
from lite_helpers import DAY, serve_dumps

from lakewire.batch import dumps
from lakewire.lite import pageviews


def test_a_day_is_reduced_to_the_top_pages_per_hour(tmp_path, monkeypatch):
    server = serve_dumps()
    monkeypatch.setattr(dumps, "BASE_URL", f"http://127.0.0.1:{server.server_port}")
    monkeypatch.setattr(dumps.time, "sleep", lambda _s: None)
    try:
        assert not pageviews.already_ingested(tmp_path, DAY)
        rows = pageviews.ingest_day(DAY, tmp_path, "test-agent", ("en", "en.m"), top_n=2, workdir=tmp_path)
    finally:
        server.shutdown()
    assert rows == 48  # 24 hours x the top 2 pages
    assert pageviews.already_ingested(tmp_path, DAY)
    con = duckdb.connect()
    path = pageviews.day_path(tmp_path, DAY).as_posix()
    hour5 = con.execute(
        f"select page_title, views, domain_code, view_date from read_parquet('{path}') "
        "where view_hour = timestamp '2026-11-10 05:00:00' order by views desc").fetchall()
    assert hour5 == [("Alpha", 20, "en+en.m", DAY), ("Beta", 7, "en+en.m", DAY)]  # 15 + 5 mobile; Gamma cut off
    assert con.execute(f"select count(*) from read_parquet('{path}') where page_title in ('Wurst','Wiktionary')"
                       ).fetchone()[0] == 0
    columns = [(c[0], c[1]) for c in con.execute(f"describe select * from read_parquet('{path}')").fetchall()]
    # What dbt's stg_pageviews reads. views must stay BIGINT: DuckDB's SUM is a 128-bit integer that Parquet can only
    # store as a float, which silently changed the column type until a comparison with the Spark output caught it.
    assert columns == [("view_hour", "TIMESTAMP"), ("view_date", "DATE"), ("domain_code", "VARCHAR"),
                       ("page_title", "VARCHAR"), ("views", "BIGINT")]


def test_titles_with_spaces_are_kept_when_there_is_room(tmp_path, monkeypatch):
    server = serve_dumps()
    monkeypatch.setattr(dumps, "BASE_URL", f"http://127.0.0.1:{server.server_port}")
    monkeypatch.setattr(dumps.time, "sleep", lambda _s: None)
    try:
        pageviews.ingest_day(DAY, tmp_path, "test-agent", ("en", "en.m"), top_n=10, workdir=tmp_path)
    finally:
        server.shutdown()
    con = duckdb.connect()
    path = pageviews.day_path(tmp_path, DAY).as_posix()
    titles = {r[0] for r in con.execute(f"select page_title from read_parquet('{path}')").fetchall()}
    assert titles == {"Alpha", "Beta", "Gamma", "Foo bar"}
