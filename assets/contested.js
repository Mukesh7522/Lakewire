/* Contested page: pages where editors keep reversing each other. A heuristic signal, never a verdict. */
(function () {
  'use strict';
  const {$, $$, el} = LW;
  const st = {k: 'conflict_score', dir: -1};
  let wired = false, last = null;

  const cmp = (k, dir) => (a, b) => {
    const x = a[k], y = b[k];
    if (x == null && y == null) return 0;
    if (x == null) return 1;
    if (y == null) return -1;
    return (typeof x === 'string' ? x.localeCompare(y) : x - y) * dir;
  };

  function render(S) {
    const all = S.data.conflicts.rows || [];
    const rows = [...all].sort(cmp(st.k, st.dir));
    const max = Math.max(1, ...all.map(r => r.conflict_score));
    const body = $('#conf tbody');
    body.replaceChildren();
    $('#conf-count').textContent = `${all.length} flagged`;
    if (!rows.length) body.append(el('tr', {}, el('td', {colSpan: 7, className: 'empty'}, el('b', {textContent: 'Nothing flagged right now'}), 'Quiet is good. Pages appear here when editors alternate quickly, at least three times in a day.')));

    rows.forEach(r => {
      const meter = el('div', {className: 'meter'}, el('i'));
      meter.firstChild.style.width = (r.conflict_score / max) * 100 + '%';
      meter.firstChild.style.background = 'var(--bot)';
      const tr = el('tr', {className: 'clickable', tabIndex: 0},
        el('td', {}, el('span', {className: 'tag', textContent: LW.wikiName(r.wiki)})),
        el('td', {className: 'title'}, el('a', {href: LW.pageUrl(LW.wikiHost(r.wiki), r.title), target: '_blank', rel: 'noopener noreferrer', textContent: r.title})),
        el('td', {className: 'r', textContent: r.event_date}),
        el('td', {className: 'r', textContent: LW.fmt(r.edits)}),
        el('td', {className: 'r', textContent: LW.fmt(r.distinct_editors)}),
        el('td', {className: 'r', textContent: LW.fmt(r.rapid_alternations)}),
        el('td', {}, el('span', {className: 'num', textContent: LW.fmt(r.conflict_score)}), meter));
      const toggle = e => {
        if (e.target.closest && e.target.closest('a')) return;
        const next = tr.nextElementSibling;
        if (next && next.classList.contains('detail')) { next.remove(); tr.setAttribute('aria-expanded', 'false'); return; }
        const capped = Math.min(r.distinct_editors, 5);
        tr.after(el('tr', {className: 'detail'}, el('td', {colSpan: 7,
          textContent: `${r.rapid_alternations} rapid reversals × ${capped} (editors, capped at 5) = score ${r.conflict_score}. These edits moved ${LW.fmt(r.bytes_swing)} bytes in total. A high score means a lot of back-and-forth, not that anyone is right or wrong.`})));
        tr.setAttribute('aria-expanded', 'true');
      };
      tr.setAttribute('aria-expanded', 'false');
      tr.addEventListener('click', toggle);
      tr.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); toggle(e); } });
      body.append(tr);
    });

    $$('#conf th[data-k]').forEach(th => {
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
      $$('#conf th[data-k]').forEach(th => {
        const go = () => {
          st.dir = st.k === th.dataset.k ? -st.dir : (['wiki', 'title'].includes(th.dataset.k) ? 1 : -1);
          st.k = th.dataset.k;
          render(last);
        };
        th.tabIndex = 0;
        th.addEventListener('click', go);
        th.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); go(); } });
      });
    }
    render(S);
  });
})();
