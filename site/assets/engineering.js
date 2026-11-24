/* Engineering page: the guarantees, the evidence, and how the two ways of running it differ. */
(function () {
  'use strict';
  const {$, el} = LW;

  const STATS = {
    lite: [['2', 'scheduled workflows'], ['4', 'published marts'], ['54', 'automated tests'], ['0', 'servers to run']],
    full: [['2', 'Airflow DAGs'], ['4', 'published marts'], ['54', 'automated tests'], ['7', 'recorded incidents and drills']]
  };

  const INCIDENTS = [
    ['Silver stream crashed during compaction', 'real incident', 'docs/incidents/2026-11-23-silver-compaction-crash.md'],
    ['Kafka data was never in its volume', 'defect found early', 'docs/incidents/2026-11-23-kafka-data-not-persisted.md'],
    ['Spark broke itself after the laptop woke', 'real incident', 'docs/incidents/2026-11-23-spark-heartbeat-after-sleep.md'],
    ['Laptop slept for 58 minutes', 'real incident', 'docs/incidents/2026-11-23-laptop-sleep.md'],
    ['Consumer down for 13 minutes', 'drill', 'docs/incidents/2026-11-23-consumer-outage.md'],
    ['Poison messages and a schema change', 'drill', 'docs/incidents/2026-11-23-poison-messages-and-schema-change.md'],
    ['Delete a day of pageviews and rebuild it', 'drill', 'docs/incidents/2026-11-23-pageview-day-rebuild.md']
  ];

  const COMPARE = [
    ['Where it runs', 'Docker Compose on a computer or a VM', 'Scheduled GitHub Actions jobs'],
    ['Cost', 'A machine that stays on', 'Free for a public repository'],
    ['Freshness', 'Seconds', 'About an hour, depending on GitHub scheduling'],
    ['Ingestion', 'Kafka and Spark Structured Streaming', 'A pull-based collector that resumes from a saved position'],
    ['Storage', 'Delta Lake, bronze and silver layers', 'Hourly Parquet files kept in a git branch'],
    ['Scheduling', 'Airflow', 'GitHub Actions cron'],
    ['Monitoring', 'Prometheus and Grafana', 'Workflow annotations and the status pill on this site'],
    ['Delivery guarantee', 'Effectively-once: watermark dedupe and an insert-only MERGE', 'Effectively-once: deduplication by event id when files are written']
  ];

  LW.boot(S => {
    $('#hl').replaceChildren(...LW.HLS[S.mode].map(([t, p], i) => el('div', {className: 'card guar reveal'},
      el('div', {className: 'no', textContent: String(i + 1).padStart(2, '0')}), el('h3', {textContent: t}), el('p', {textContent: p}))));

    $('#stats').replaceChildren(...STATS[S.mode].map(([n, l]) => el('div', {}, el('b', {textContent: n}), el('span', {textContent: l}))));

    $('#incidents').replaceChildren(...INCIDENTS.map(([t, kind, path]) => el('li', {},
      el('a', {href: LW.docUrl(path), target: '_blank', rel: 'noopener noreferrer', textContent: t}),
      el('span', {className: 'what', textContent: kind}))));

    const idx = S.mode === 'lite' ? 2 : 1;
    $('#cmp-body').replaceChildren(...COMPARE.map(row => {
      const tr = el('tr', {}, ...row.map(c => el('td', {textContent: c})));
      tr.children[idx].className = 'on';
      return tr;
    }));
    $('#cmp-full').className = S.mode === 'full' ? 'on' : '';
    $('#cmp-lite').className = S.mode === 'lite' ? 'on' : '';
  });
})();
