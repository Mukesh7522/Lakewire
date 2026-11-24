/* Trending page: a podium for the top three, then a searchable, sortable table of everything ranked. */
(function () {
  'use strict';
  const {$, $$, el} = LW;
  const st = {k: 'trend_score', dir: -1, q: ''};
  let wired = false, last = null;

  const cmp = (k, dir) => (a, b) => {
    const x = a[k], y = b[k];
    if (x == null && y == null) return 0;
    if (x == null) return 1;
    if (y == null) return -1;
    return (typeof x === 'string' ? x.localeCompare(y) : x - y) * dir;
  };
  const title = r => String(r.page_title).replaceAll('_', ' ');
  const link = (r, cls) => el('a', {href: LW.pageUrl('en.wikipedia.org', r.page_title), target: '_blank', rel: 'noopener noreferrer', textContent: title(r), className: cls || ''});

  function growthBadge(g) {
    if (g == null) return el('span', {className: 'badge na', textContent: 'n/a'});
    const cls = g >= 1.5 ? 'hot' : g >= 1 ? 'up' : 'dn';
    return el('span', {className: 'badge ' + cls, textContent: (g >= 1.5 ? '▲ ' : g < 1 ? '▼ ' : '') + g + '×'});
  }

  function render(S) {
    const all = S.data.trending.rows || [];
    const q = st.q.trim().toLowerCase();
    const rows = all.filter(r => !q || title(r).toLowerCase().includes(q)).sort(cmp(st.k, st.dir));
    const maxViews = Math.max(1, ...all.map(r => r.views));

    const vh = all[0] && all[0].view_hour;
    $('#trend-lede').textContent = vh
      ? `English Wikipedia articles for the pageview hour starting ${LW.fmtHour(vh)}, ranked by trend score. Pageview files are published with a delay, so this lags the live edit counts.`
      : 'English Wikipedia articles ranked by trend score: the log of views, scaled by growth against the same hour the day before.';

    // empty state: explain what fills this page and when
    $('#empty').hidden = all.length > 0;
    $('#trend-card').hidden = all.length === 0;
    $('#podium').hidden = all.length === 0;
    if (!all.length) {
      $('#empty-body').textContent = S.mode === 'lite'
        ? 'Pageviews are loaded once a day by their own GitHub Actions job (lakewire-lite-pageviews). Run it from the Actions tab, or wait for the next scheduled run, and this page fills in.'
        : 'The first daily pageview file is loaded a few hours after a fresh deploy.';
      return;
    }

    // podium: the three highest scores
    const top = [...all].sort((a, b) => b.trend_score - a.trend_score).slice(0, 3);
    $('#podium').replaceChildren(...top.map((r, i) => {
      const g = r.growth_vs_prev_day;
      return el('div', {className: 'card'},
        el('div', {className: 'n', textContent: String(i + 1)}),
        el('h3', {}, link(r)),
        el('p', {className: 'sub num', textContent: `${LW.fmt(r.views)} views · ${g == null ? 'no comparison yet' : g + '× the same hour yesterday'}`}));
    }));

    // table
    $('#trend-count').textContent = `${rows.length} of ${all.length} articles`;
    const body = $('#trend tbody');
    body.replaceChildren();
    if (!rows.length) body.append(el('tr', {}, el('td', {colSpan: 6, className: 'empty', textContent: 'No articles match your filter.'})));
    rows.forEach((r, i) => {
      const meter = el('div', {className: 'meter'}, el('i'));
      meter.firstChild.style.width = (r.views / maxViews) * 100 + '%';
      body.append(el('tr', {},
        el('td', {className: 'rk', textContent: String(i + 1).padStart(2, '0')}),
        el('td', {className: 'title'}, link(r), meter),
        el('td', {className: 'r', textContent: LW.fmt(r.views)}),
        el('td', {className: 'r'}, growthBadge(r.growth_vs_prev_day)),
        el('td', {className: 'r', textContent: LW.fmt(r.edits_1h)}),
        el('td', {className: 'r', textContent: r.trend_score == null ? 'n/a' : Number(r.trend_score).toFixed(1)})));
    });
    $$('#trend th[data-k]').forEach(th => {
      const on = th.dataset.k === st.k;
      th.classList.toggle('sorted', on);
      th.dataset.dir = on ? (st.dir < 0 ? '↓' : '↑') : '';
      th.setAttribute('aria-sort', on ? (st.dir < 0 ? 'descending' : 'ascending') : 'none');
    });
  }

  LW.boot(S => {
    last = S;
    if (!wired) {
      wired = true;
      $$('#trend th[data-k]').forEach(th => {
        const go = () => {
          st.dir = st.k === th.dataset.k ? -st.dir : (th.dataset.k === 'page_title' ? 1 : -1);
          st.k = th.dataset.k;
          render(last);
        };
        th.tabIndex = 0;
        th.addEventListener('click', go);
        th.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); go(); } });
      });
      $('#q').addEventListener('input', e => { st.q = e.target.value; render(last); });
      addEventListener('keydown', e => {
        if (e.key === '/' && !/INPUT|TEXTAREA/.test(document.activeElement.tagName)) { e.preventDefault(); $('#q').focus(); }
      });
    }
    render(S);
  });
})();
