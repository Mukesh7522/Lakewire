/* Overview page: the headline numbers, one chart, and a taste of each other page. */
(function () {
  'use strict';
  const {$, el} = LW;
  let chart = null, ticking = false;

  const STEPS = {
    lite: [['Collect', 'Wikimedia stream, replayed each hour'], ['Check', 'Data contract and deduplication'], ['Store', 'Parquet files in a git branch'],
      ['Model', 'dbt and DuckDB, every test'], ['Publish', 'GitHub Pages']],
    full: [['Stream', 'Wikimedia into Kafka'], ['Process', 'Spark Structured Streaming'], ['Store', 'Delta Lake, bronze and silver'],
      ['Model', 'dbt and DuckDB marts'], ['Publish', 'GitHub Pages']]
  };

  function lede(S) {
    const n = S.wikis.length || 'several';
    return S.mode === 'lite'
      ? `Every hour a GitHub Actions job collects the new edits from ${n} Wikipedia editions, checks them against a data contract, and rebuilds the charts on these pages. No server runs it.`
      : `Every edit on ${n} Wikipedia editions flows through Kafka and Spark into a lakehouse and out to the charts on these pages.`;
  }

  function miniList(host, items, empty) {
    host.replaceChildren();
    if (!items.length) { host.append(el('li', {}, el('span', {className: 't muted', textContent: empty}))); return; }
    items.forEach((it, i) => host.append(el('li', {},
      el('span', {className: 'n', textContent: String(i + 1)}),
      el('span', {className: 't'}, el('a', {href: it.href, target: '_blank', rel: 'noopener noreferrer', textContent: it.title})),
      el('span', {className: 'v', textContent: it.value}))));
  }

  LW.boot(S => {
    const H = S.health;
    $('#hero-lede').textContent = lede(S);

    // headline numbers
    const series = S.hours.map(h => S.perHour[h].total);
    LW.countUp($('#k-hour'), H.events_last_hour);
    LW.countUp($('#k-day'), H.events_last_24h);
    LW.countUp($('#k-total'), H.events_total);
    LW.charts.spark($('#sp-hour'), series.slice(-48), 'var(--w2)');
    LW.charts.spark($('#sp-day'), series.map((_, i) => series.slice(Math.max(0, i - 23), i + 1).reduce((a, b) => a + b, 0)).slice(-48), 'var(--w1)');
    let run = 0;
    LW.charts.spark($('#sp-total'), series.map(v => (run += v)), 'var(--w5)');
    $('#f-hour').textContent = `${S.wikis.length} wikis, ${S.hours.length} hours of history`;
    $('#f-day').textContent = 'rolling 24 hour window';
    $('#f-total').textContent = S.mode === 'lite' ? 'kept for the last 7 days' : 'inside the retention window';
    $('#f-latest').textContent = S.lag == null ? 'no timestamp in the snapshot' : `${LW.ago(S.lag)} behind when the snapshot was built`;
    if (!ticking) {
      ticking = true;
      LW.onTick(s => { $('#k-latest').textContent = s.lag == null ? 'n/a' : LW.ago(s.lag + (Date.now() - s.genAt) / 1000) + ' ago'; });
    }

    // edits per hour, stacked by wiki
    const hours = S.hours.slice(-72);
    const totals = Object.fromEntries(S.wikis.map(w => [w, hours.reduce((s, h) => s + (S.idx[w][h] ? S.idx[w][h].edits_total : 0), 0)]));
    const order = [...S.wikis].sort((a, b) => totals[b] - totals[a]);
    const chartSeries = order.map(w => ({key: w, label: LW.wikiName(w), color: S.color(w), values: hours.map(h => (S.idx[w][h] ? S.idx[w][h].edits_total : 0))}));
    const cfg = {x: hours, series: chartSeries, mode: 'stacked', ariaLabel: 'Edits per hour, stacked by wiki'};
    if (chart) chart.update(cfg); else chart = LW.charts.timeSeries($('#chart'), cfg);
    $('#legend').innerHTML = LW.charts.legend(chartSeries);
    LW.charts.collecting($('#collecting'), S.hours.length, 24);

    // a taste of the other pages
    const trend = (S.data.trending.rows || []).slice(0, 5).map(r => ({
      title: String(r.page_title).replaceAll('_', ' '), href: LW.pageUrl('en.wikipedia.org', r.page_title), value: LW.compact(r.views) + ' views'}));
    miniList($('#mini-trend'), trend, S.mode === 'lite' ? 'Pageviews load once a day. Check back after the first daily run.' : 'Pageview data arrives a few hours after each deploy.');
    const hot = (S.data.conflicts.rows || []).slice(0, 5).map(r => ({
      title: r.title, href: LW.pageUrl(LW.wikiHost(r.wiki), r.title), value: `score ${r.conflict_score}`}));
    miniList($('#mini-conf'), hot, 'No page is being contested right now.');

    $('#steps').replaceChildren(...STEPS[S.mode].map(([t, d]) => el('div', {className: 'step'}, el('b', {textContent: t}), el('span', {textContent: d}))));
  });
})();
