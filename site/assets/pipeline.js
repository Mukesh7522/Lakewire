/* Pipeline page: an explorer for the diagram of whichever mode is running (full stack or lite). */
(function () {
  'use strict';
  const {$, $$, esc, el} = LW;
  const W = 150, H = 76;
  let selected = null;

  function render(S) {
    const P = LW.PIPES[S.mode], N = P.nodes;
    const svg = $('#pipesvg');
    let paths = '', nodes = '';
    const edge = (id, d, cls = '') => { paths += `<path id="p-${id}" class="pl flow ${cls}" d="${d}"/>`; };
    P.edges.forEach(([a, b]) => edge(`${a}-${b}`, `M${N[a].x + W} ${N[a].y + H / 2} L${N[b].x} ${N[b].y + H / 2}`));
    edge('prod-dlq', `M${N.prod.x + W / 2} ${N.prod.y + H} L${N.dlq.x + W / 2} ${N.dlq.y}`, 'bad');
    const from = N[P.curveFrom];
    edge(`${P.curveFrom}-dbt`, `M${from.x + W / 2} ${from.y + H} C${from.x + W / 2} 232 ${N.dbt.x + W / 2} 232 ${N.dbt.x + W / 2} ${N.dbt.y}`);
    Object.entries(N).forEach(([k, n]) => {
      if (k === 'ops') return;
      nodes += `<g class="pn" data-k="${esc(k)}" tabindex="0" role="button" aria-label="${esc(n.h)}" style="--c:${n.c}">` +
        `<rect class="bx" x="${n.x}" y="${n.y}" width="${W}" height="${H}" rx="12"/>` +
        `<rect class="accent" x="${n.x}" y="${n.y + 10}" width="4" height="${H - 20}" rx="2"/>` +
        `<text class="t" x="${n.x + 18}" y="${n.y + 31}">${esc(n.t)}</text><text class="s" x="${n.x + 18}" y="${n.y + 54}">${esc(n.s)}</text></g>`;
    });
    const ops = `<g class="pn" data-k="ops" tabindex="0" role="button" aria-label="Observability and delivery" style="--c:#2f9e5f">` +
      `<rect class="bx" x="20" y="425" width="1050" height="38" rx="12"/><rect class="accent" x="20" y="433" width="4" height="22" rx="2"/>` +
      `<text class="t" x="40" y="449">Observability &amp; delivery</text><text class="s" x="262" y="449">${esc(P.opsBar)}</text></g>`;
    svg.innerHTML = `<text class="lane" x="20" y="30">${esc(P.lanes[0])}</text><text class="lane" x="20" y="290">${esc(P.lanes[1])}</text>` + paths + nodes + ops;
    N.ops = P.ops;

    const show = k => {
      selected = k;
      $$('.pn', svg).forEach(g => g.classList.toggle('sel', g.dataset.k === k));
      const n = N[k], box = $('#pd');
      box.style.setProperty('--c', n.c);
      const stack = el('div', {className: 'stack'}, ...n.st.map(t => el('span', {className: 'tag', textContent: t})));
      box.replaceChildren(
        el('div', {}, el('div', {className: 'layer', textContent: n.layer}), el('h3', {textContent: n.h}), el('p', {textContent: n.p}), stack),
        el('div', {className: 'dec'}, el('small', {textContent: 'Design decision'}), el('span', {textContent: n.d})));
    };
    $$('.pn', svg).forEach(g => {
      g.addEventListener('click', () => show(g.dataset.k));
      g.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); show(g.dataset.k); } });
    });
    show(selected && N[selected] ? selected : P.first);
  }

  const RUNS = {
    lite: [
      ['Collect new edits, store, rebuild the marts, publish', 'GitHub Actions: lakewire-lite-hourly', 'every hour, at :23'],
      ['Load the previous day of pageviews', 'GitHub Actions: lakewire-lite-pageviews', 'once a day'],
      ['Data and the published site', 'two git branches, one commit each', 'replaced on every run']
    ],
    full: [
      ['Producer, Kafka, Spark streaming, object store, Airflow, Prometheus, Grafana', 'Docker, on your computer or a VM', 'always on, restarted by Docker'],
      ['Rebuild the marts and export JSON', 'Airflow: lakewire_marts_hourly', 'every hour'],
      ['Load the previous day of pageviews', 'Airflow: lakewire_pageviews_daily', 'daily, waits for the dump']
    ]
  };

  LW.boot(S => {
    $('#pipe-lede').textContent = S.mode === 'lite'
      ? 'The version you are looking at has no servers. Scheduled GitHub Actions jobs do the work, and the state lives in git. Click any stage to see what it does and why it was built that way.'
      : 'The version you are looking at runs as containers: a streaming path that is always on and a scheduled batch path. Click any stage to see what it does and why it was built that way.';
    render(S);
    const body = $('#runs');
    body.replaceChildren(...RUNS[S.mode].map(r => el('tr', {}, ...r.map(c => el('td', {textContent: c})))));
  });
})();
