/* Words and structure of the two pipeline diagrams (full stack and lite) and the engineering highlights. */
(function () {
  'use strict';
  const FULL_NODES = {
    src:   {x: 20,  y: 60,  t: 'EventStreams', s: 'Wikimedia SSE', c: '#8ab4ff', layer: 'Source', h: 'Wikimedia EventStreams',
      p: 'A public Server-Sent Events feed of every change on every wiki. Lakewire keeps only edits and page creations on the configured wikis and sends a descriptive User-Agent, as Wikimedia asks.', d: 'Be a polite client: identify yourself, filter early, never hammer the source.', st: ['SSE', 'HTTPS']},
    prod:  {x: 200, y: 60,  t: 'Producer', s: 'Python · contract', c: '#3ddbc0', layer: 'Ingest', h: 'Producer with a data contract',
      p: 'A hand-written SSE parser reconnects with exponential backoff (1 to 60 s) and resumes from the Last-Event-ID. Every event is validated against a JSON Schema before it is allowed anywhere near Kafka. The Kafka producer is idempotent and compresses with lz4.', d: 'A bad event must never block the stream. It goes to the dead-letter topic instead.', st: ['Python', 'jsonschema', 'confluent-kafka', 'Prometheus']},
    dlq:   {x: 200, y: 200, t: 'Dead-letter topic', s: 'wiki.edits.dlq', c: '#ff8a80', layer: 'Ingest', h: 'Dead-letter queue',
      p: 'Invalid JSON and contract violations are kept with their error list and original payload for seven days so they can be inspected and replayed. Additive fields pass the contract; type changes and missing required fields do not.', d: 'Schema drift becomes a visible, countable event instead of a silent corruption.', st: ['Kafka topic', '7 d retention']},
    kafka: {x: 380, y: 60,  t: 'Kafka', s: 'KRaft · 3 partitions', c: '#ffb547', layer: 'Transport', h: 'Kafka (single-broker KRaft)',
      p: 'Topic wiki.edits.raw has three partitions, is keyed by wiki and keeps 48 hours, enough to survive a long consumer outage and replay.', d: 'Kafka is at-least-once, so everything downstream is built to tolerate duplicates.', st: ['Kafka 3.8', 'KRaft']},
    spark: {x: 560, y: 60,  t: 'Spark Streaming', s: 'Structured · Delta', c: '#c792ea', layer: 'Process', h: 'Spark Structured Streaming',
      p: 'Reads Kafka into Bronze, then Bronze into Silver. A watermark plus dropDuplicatesWithinWatermark bounds state, and an insert-only MERGE on event_id makes any redelivery harmless. Table compaction, retention and VACUUM run in the same JVM.', d: 'One writer per Delta table. S3-compatible storage has no atomic put-if-absent, so maintenance lives inside the streaming driver.', st: ['Spark 3.5', 'Delta 3.2', 'foreachBatch MERGE']},
    bronze:{x: 740, y: 60,  t: 'Bronze', s: 'raw · 14 days', c: '#b08968', layer: 'Storage', h: 'Bronze: raw and replayable',
      p: 'Append-only Delta. The JSON payload is stored exactly as received next to its Kafka topic, partition and offset, partitioned by ingest date.', d: 'Keep the raw truth so Silver can always be rebuilt.', st: ['Delta', 'SeaweedFS (S3)']},
    silver:{x: 920, y: 60,  t: 'Silver', s: 'typed · 120 days', c: '#cfd8e3', layer: 'Storage', h: 'Silver: typed and deduplicated',
      p: 'Parsed columns, partitioned by event date. CI replays the same fixture events twice and asserts that Silver does not change while Bronze and the DLQ double.', d: 'Effectively-once is proven by a test, not claimed in a README.', st: ['Delta', 'MERGE on event_id']},
    dumps: {x: 20,  y: 320, t: 'Pageview dumps', s: 'hourly .gz files', c: '#8ab4ff', layer: 'Source', h: 'Wikimedia pageview dumps',
      p: 'One compressed file per hour of aggregate pageviews, published with a delay of a few hours.', d: 'The dashboard says so: trending data lags the live edit stream.', st: ['HTTPS', 'gzip']},
    air:   {x: 200, y: 320, t: 'Airflow + PySpark', s: 'daily DAG', c: '#3ddbc0', layer: 'Batch', h: 'Airflow daily pageview backfill',
      p: 'Waits for the dump (retrying every 15 minutes for up to 8 hours), downloads with retries, checks md5 and gzip integrity, keeps the top N pages for the configured domains and overwrites exactly that day.', d: 'Idempotent per-day partition overwrite: rerunning a day gives the same rows.', st: ['Airflow 2.10', 'PySpark', 'replaceWhere']},
    pvs:   {x: 380, y: 320, t: 'Silver pageviews', s: 'by view_date', c: '#cfd8e3', layer: 'Storage', h: 'Silver pageviews',
      p: 'Delta table partitioned by view date, with the same retention and compaction treatment as the edits tables.', d: 'Airflow only ever writes this table, which keeps the single-writer rule intact.', st: ['Delta']},
    dbt:   {x: 560, y: 320, t: 'dbt + DuckDB', s: 'hourly via Airflow', c: '#ff8fa3', layer: 'Transform', h: 'dbt on DuckDB',
      p: 'DuckDB reads the Delta tables straight from the S3-compatible store with delta_scan. Staging models feed four incremental marts. CI builds everything on parquet fixtures, twice, to exercise the incremental path and the data tests.', d: 'Marts are small enough to keep forever and cheap enough to rebuild hourly.', st: ['dbt-duckdb', 'delta_scan', 'incremental']},
    gold:  {x: 740, y: 320, t: 'Gold marts', s: 'JSON snapshots', c: '#ffd166', layer: 'Serve', h: 'Gold marts and JSON export',
      p: 'Pipeline health, bot vs human, trending articles and edit-conflict signals. A small export writes JSON atomically (temp file, then rename) so a reader never sees half a file.', d: 'The public site never touches the lake. It only reads four tiny files.', st: ['DuckDB', 'JSON']},
    pages: {x: 920, y: 320, t: 'This dashboard', s: 'GitHub Pages', c: '#3ddbc0', layer: 'Serve', h: 'Publish to GitHub Pages',
      p: 'Every hour the snapshot is pushed to GitHub Pages: by a GitHub Actions workflow that pulls it from the server, or by a small script when the stack runs on a laptop. The page always shows how old the snapshot is, and the server workflow fails loudly if the newest event is more than 45 minutes old.', d: 'Honest freshness: show the staleness banner and make the alarm noisy.', st: ['GitHub Actions', 'Pages']}
  };

  const FULL_EDGES = [
    ['src', 'prod', 'a'], ['prod', 'kafka', 'a'], ['kafka', 'spark', 'a'], ['spark', 'bronze', 'a'], ['bronze', 'silver', 'a'],
    ['dumps', 'air', 'b'], ['air', 'pvs', 'b'], ['pvs', 'dbt', 'b'], ['dbt', 'gold', 'b'], ['gold', 'pages', 'b']
  ];

  const LITE_NODES = {
    src:    {x: 20,  y: 60,  t: `EventStreams`, s: `Wikimedia SSE`, c: '#8ab4ff', layer: 'Source', h: 'Wikimedia EventStreams',
      p: `A public Server-Sent Events feed of every change on every wiki. The collector keeps only edits and page creations on the configured wikis and sends a descriptive User-Agent, as Wikimedia asks. The feed can be replayed from a saved position for days, which is what makes a scheduled job possible.`, d: `Be a polite client: identify yourself, filter early, and let the source remember your place.`, st: ['SSE', 'HTTPS']},
    prod:   {x: 245, y: 60,  t: `Collector`, s: `Python · contract`, c: '#3ddbc0', layer: 'Ingest', h: 'Collector with a data contract',
      p: `Every hour a GitHub Actions job connects, replays whatever it missed from the position saved by the last run, and disconnects once it has caught up. Wikis it does not keep are dropped before parsing, and every kept event is validated against a JSON Schema.`, d: `Correctness never depends on when the job stops: the next run continues from the exact saved position.`, st: ['Python', 'jsonschema', 'GitHub Actions']},
    dlq:    {x: 245, y: 200, t: `Dead-letter log`, s: `recent.jsonl`, c: '#ff8a80', layer: 'Ingest', h: 'Dead-letter log',
      p: `Events that break the contract are kept with their error list and a copy of the payload (the latest 500), so schema drift is visible and countable. Additive fields pass; type changes and missing required fields do not.`, d: `A bad event never fails a run. It is recorded and skipped.`, st: ['JSONL', 'last 500']},
    merge:  {x: 470, y: 60,  t: `Dedupe + merge`, s: `DuckDB · event_id`, c: '#c792ea', layer: 'Process', h: 'Deduplicate and merge',
      p: `New events are merged into one Parquet file per hour of event time, and any event_id that is already stored is dropped. Merging the same events twice leaves the stored data unchanged, and a test replays the same events to prove it.`, d: `Idempotent writes make retries harmless, so a failed run can simply be run again.`, st: ['DuckDB', 'Parquet', 'zstd']},
    silver: {x: 695, y: 60,  t: `Silver edits`, s: `Parquet · 7 days`, c: '#cfd8e3', layer: 'Storage', h: 'Silver edits: typed Parquet files',
      p: `One file per hour, in a folder per day, with the typed columns the dbt models read. Days older than the retention window are deleted on every run. A week of five wikis is a little over 100 MB.`, d: `Keep only what the dashboard needs, so git stays a fine place to store it.`, st: ['Parquet', '7 day window']},
    branch: {x: 920, y: 60,  t: `Data branch`, s: `git · 1 commit`, c: '#b08968', layer: 'Storage', h: 'Storage in a git branch',
      p: `The dataset and the saved stream position live in a branch that every run replaces with a single commit, so history never grows and git uploads only the files that changed. There is no server and no database to run.`, d: `Use the platform you already have as the storage layer.`, st: ['git', 'GitHub']},
    dumps:  {x: 20,  y: 320, t: `Pageview dumps`, s: `hourly .gz files`, c: '#8ab4ff', layer: 'Source', h: 'Wikimedia pageview dumps',
      p: `One compressed file per hour of aggregate pageviews, published with a delay of a few hours.`, d: `The dashboard says so: trending data lags the live edit stream.`, st: ['HTTPS', 'gzip']},
    air:    {x: 200, y: 320, t: `Pageview job`, s: `daily · DuckDB`, c: '#3ddbc0', layer: 'Batch', h: 'Daily pageview job',
      p: `Once a day a second workflow downloads the previous day (about 1.2 GB, checked for integrity, with retries), keeps the top pages per hour, and writes one Parquet file for that day. Re-running a day gives the same rows.`, d: `Idempotent per day: a missed or repeated run cannot corrupt anything.`, st: ['GitHub Actions', 'DuckDB', 'top N per hour']},
    pvs:    {x: 380, y: 320, t: `Silver pageviews`, s: `Parquet · 3 days`, c: '#cfd8e3', layer: 'Storage', h: 'Silver pageviews',
      p: `One Parquet file per day, kept for three days: enough to compare an hour with the same hour yesterday.`, d: `Retention is sized to what the trending mart actually needs.`, st: ['Parquet']},
    dbt:    {x: 560, y: 320, t: `dbt + DuckDB`, s: `hourly · rebuilt`, c: '#ff8fa3', layer: 'Transform', h: 'dbt on DuckDB',
      p: `Every run rebuilds the marts from scratch in a throwaway DuckDB file, straight from the Parquet files, and runs all of the dbt models and data tests. If a test fails the run fails, and the site keeps its last good snapshot.`, d: `Rebuilding is cheap at this size, which removes a whole class of incremental-state bugs.`, st: ['dbt-duckdb', 'read_parquet', 'data tests']},
    gold:   {x: 740, y: 320, t: `Gold marts`, s: `JSON snapshots`, c: '#ffd166', layer: 'Serve', h: 'Gold marts and JSON export',
      p: `Pipeline health, bot vs human, trending articles and edit-conflict signals. A small export writes JSON atomically (temp file, then rename) so a reader never sees half a file.`, d: `The public site never touches the lake. It only reads four tiny files.`, st: ['DuckDB', 'JSON']},
    pages:  {x: 920, y: 320, t: `This dashboard`, s: `GitHub Pages`, c: '#3ddbc0', layer: 'Serve', h: 'Publish to GitHub Pages',
      p: `The job pushes the site and the four JSON files to the gh-pages branch and asks GitHub Pages to rebuild. The page always shows how old its snapshot is and warns when it is stale.`, d: `Honest freshness: show the banner, and let a failed run email the owner.`, st: ['GitHub Actions', 'Pages']}
  };

  const LITE_EDGES = [
    ['src', 'prod', 'a'], ['prod', 'merge', 'a'], ['merge', 'silver', 'a'], ['silver', 'branch', 'a'],
    ['dumps', 'air', 'b'], ['air', 'pvs', 'b'], ['pvs', 'dbt', 'b'], ['dbt', 'gold', 'b'], ['gold', 'pages', 'b']
  ];

  const PIPES = {
    full: {nodes: FULL_NODES, edges: FULL_EDGES, curveFrom: 'silver', first: 'spark',
      lanes: ['STREAMING · ALWAYS ON', 'BATCH + SERVING · SCHEDULED'],
      opsBar: 'Prometheus · Grafana · Slack alerts · GitHub Actions CI/CD · health checks',
      ops: {c: '#8be28b', layer: 'Operations', h: 'Observability and delivery', p: 'The producer and the streaming driver export Prometheus metrics (last event age, input rate, batch time). Grafana is provisioned from the repo and Airflow failures can post to Slack. CI lints, runs unit tests, builds dbt twice, and runs a Kafka, SeaweedFS and Spark end-to-end job on every push. Failure drills and real incidents are written up in the repository.', d: 'If you cannot see it fail, you cannot call it production.', st: ['Prometheus', 'Grafana', 'GitHub Actions', 'Docker Compose']}},
    lite: {nodes: LITE_NODES, edges: LITE_EDGES, curveFrom: 'branch', first: 'prod',
      lanes: ['HOURLY · SCHEDULED ON GITHUB ACTIONS', 'DAILY + SERVING · SCHEDULED'],
      opsBar: 'Run history · failure emails · dbt data tests every hour · CI on every push',
      ops: {c: '#8be28b', layer: 'Operations', h: 'Observability and delivery', p: `Each run leaves a summary annotation (events read, kept and dead-lettered), a failed run emails the owner, and the hourly build runs every dbt data test. CI runs the whole pipeline on every push: a fake Wikimedia stream, real git and real dbt, with only the pipeline's own small set of dependencies installed.`, d: `If you cannot see it fail, you cannot call it production.`, st: ['GitHub Actions', 'dbt tests', 'CI']}}
  };

  const HLS = {
    full: [
      ['Effectively-once', 'Kafka delivers at least once. A watermark dedupe and an insert-only MERGE on event_id make redelivery harmless, and CI proves it by replaying the same events twice.'],
      ['Data contract at the edge', 'Events are validated against a JSON Schema before Kafka. Violations land in a dead-letter topic with their errors instead of breaking the stream.'],
      ['One writer per table', 'Delta on S3-compatible storage lacks atomic put-if-absent, so compaction, retention and VACUUM run inside the streaming driver rather than a second process.'],
      ['Idempotent backfills', 'Pageviews are checked for integrity, filtered, and written with a per-day partition overwrite. Re-running a day yields the same rows.'],
      ['Honest freshness', 'The dashboard states how old its snapshot is and shows a banner when data is stale, so silence never looks like success.'],
      ['Tested like production', 'Lint, unit tests, dbt built twice, and a real Kafka, SeaweedFS and Spark end-to-end job on every push. Failure drills and real incidents are written up in the repository.']
    ],
    lite: [
      ['Effectively-once', `Each run resumes from the stream position saved by the last one. If a run is late, missed or repeated, events are deduplicated by id when they are stored, and tests replay the same events to prove nothing is counted twice.`],
      ['Data contract at the edge', `Events are validated against a JSON Schema before they are stored. Violations are kept with their errors in a dead-letter log instead of breaking the run.`],
      ['State in git, history stays small', `The Parquet files and the stream position live in a branch that is replaced by one commit per run, so the repository never grows and nothing needs a server or a database.`],
      ['Idempotent backfills', `Pageviews are checked for integrity, filtered, and written one day at a time. Re-running a day yields the same rows.`],
      ['Honest freshness', `The dashboard states how old its snapshot is and shows a banner when data is stale, so silence never looks like success.`],
      ['Tested like production', `Lint, unit tests, and an end-to-end test that runs a fake Wikimedia stream through real git and real dbt on every push. The hourly build runs every dbt data test.`]
    ]
  };

  LW.PIPES = PIPES;
  LW.HLS = HLS;
})();
