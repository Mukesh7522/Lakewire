/* Activity page: who is editing. Volume by wiki or bot share, with a humans-versus-bots split and a heatmap. */
(function () {
  'use strict';
  const {$, $$, el} = LW;
  const st = {metric: 'volume', range: 72, sel: null};
  let chart = null, wired = false, last = null;

  function niceShareMax(v) { return v <= .1 ? .1 : v <= .2 ? .2 : v <= .25 ? .25 : v <= .5 ? .5 : 1; }

  function render(S) {
    if (!st.sel) st.sel = new Set(S.wikis);
    st.sel = new Set([...st.sel].filter(w => S.wikis.includes(w)));
    if (!st.sel.size) st.sel = new Set(S.wikis);
    const hours = st.range ? S.hours.slice(-st.range) : S.hours;
    const wikis = S.wikis.filter(w => st.sel.has(w));
    const val = (w, h, k) => (S.idx[w][h] ? S.idx[w][h][k] : null);

    // wiki toggles
    $('#chips').replaceChildren(...S.wikis.map(w => {
      const b = el('button', {className: 'chip', type: 'button', textContent: LW.wikiName(w)});
      b.prepend(el('i'));
      b.style.setProperty('--c', S.color(w));
      b.setAttribute('aria-pressed', String(st.sel.has(w)));
      b.addEventListener('click', () => {
        if (st.sel.has(w)) { if (st.sel.size > 1) st.sel.delete(w); } else st.sel.add(w);
        render(S);
      });
      return b;
    }));

    // main chart
    const totals = Object.fromEntries(wikis.map(w => [w, hours.reduce((s, h) => s + (val(w, h, 'edits_total') || 0), 0)]));
    const order = [...wikis].sort((a, b) => totals[b] - totals[a]);
    let cfg;
    if (st.metric === 'volume') {
      cfg = {x: hours, mode: 'stacked', ariaLabel: 'Edits per hour by wiki',
        series: order.map(w => ({key: w, label: LW.wikiName(w), color: S.color(w), values: hours.map(h => val(w, h, 'edits_total') || 0)}))};
    } else {
      const series = order.map(w => ({key: w, label: LW.wikiName(w), color: S.color(w), values: hours.map(h => val(w, h, 'bot_share'))}));
      const peak = Math.max(...series.flatMap(s => s.values.map(v => v || 0)), .01);
      cfg = {x: hours, mode: 'lines', series, yMax: niceShareMax(peak), yFormat: v => LW.pct(v, 0), tipFormat: v => LW.pct(v, 1), ariaLabel: 'Share of edits made by bots, by wiki'};
    }
    if (chart) chart.update(cfg); else chart = LW.charts.timeSeries($('#chart'), cfg);
    $('#legend').innerHTML = LW.charts.legend(cfg.series);
    LW.charts.collecting($('#collecting'), S.hours.length, 24);
    $('#chart-title').textContent = st.metric === 'volume' ? 'Edits per hour' : 'Share of edits made by bots';
    $('#chart-sub').textContent = `${wikis.length} of ${S.wikis.length} wikis, ${hours.length} hours, all times UTC`;

    // humans versus bots
    let bot = 0, human = 0;
    wikis.forEach(w => hours.forEach(h => { bot += val(w, h, 'bot_edits') || 0; human += val(w, h, 'human_edits') || 0; }));
    const all = bot + human || 1;
    LW.charts.donut($('#donut'), [{label: 'Human', value: human, color: 'var(--human)'}, {label: 'Bot', value: bot, color: 'var(--bot)'}],
      {center: LW.pct(bot / all, 0), sub: 'bot edits', ariaLabel: 'Share of edits made by bots'});
    $('#split').replaceChildren(
      el('span', {}, el('i'), `Human ${LW.compact(human)}`), el('span', {}, el('i'), `Bot ${LW.compact(bot)}`));
    $$('#split span')[0].firstChild.style.cssText = 'background:var(--human)';
    $$('#split span')[1].firstChild.style.cssText = 'background:var(--bot)';

    // per wiki
    const per = order.map(w => {
      const b = hours.reduce((s, h) => s + (val(w, h, 'bot_edits') || 0), 0);
      return {label: LW.wikiName(w), value: totals[w], note: LW.pct(totals[w] ? b / totals[w] : 0, 0) + ' bots', color: S.color(w)};
    });
    LW.charts.hbars($('#wikibars'), per, {format: LW.compact});

    // heatmap
    LW.charts.heat($('#heat'), hours, order.map(w => ({label: LW.wikiName(w), color: S.color(w), values: hours.map(h => val(w, h, 'edits_total'))})));
  }

  LW.boot(S => {
    last = S;
    if (!wired) {
      wired = true;
      $$('#seg-metric button').forEach(b => b.addEventListener('click', () => {
        st.metric = b.dataset.m;
        $$('#seg-metric button').forEach(x => x.setAttribute('aria-pressed', String(x === b)));
        render(last);
      }));
      $$('#seg-range button').forEach(b => b.addEventListener('click', () => {
        st.range = +b.dataset.r;
        $$('#seg-range button').forEach(x => x.setAttribute('aria-pressed', String(x === b)));
        render(last);
      }));
    }
    render(S);
  });
})();
