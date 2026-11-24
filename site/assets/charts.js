/* Lakewire dashboard: small hand-written SVG charts. No libraries. Every label that comes from data is escaped. */
(function () {
  'use strict';
  const C = LW.charts = {};
  const {esc, el} = LW;
  const r1 = n => Math.round(n * 10) / 10;

  function niceMax(v) {
    if (!(v > 0)) return 1;
    const p = Math.pow(10, Math.floor(Math.log10(v))), f = v / p;
    return (f <= 1 ? 1 : f <= 2 ? 2 : f <= 2.5 ? 2.5 : f <= 5 ? 5 : 10) * p;
  }
  const hourLabel = iso => String(new Date(iso).getUTCHours()).padStart(2, '0') + ':00';
  const dayLabel = iso => new Date(iso).toLocaleDateString('en', {month: 'short', day: 'numeric', timeZone: 'UTC'});

  C.legend = series => `<div class="legend">${series.map(s => `<span><i style="--c:${s.color}"></i>${esc(s.label)}</span>`).join('')}</div>`;

  /* A stacked time series. Few points are drawn as capped-width bars, more as stacked areas; "lines" mode draws lines. */
  C.timeSeries = function (host, initial) {
    let cfg = initial;
    function draw() {
      const x = cfg.x, series = cfg.series, n = x.length;
      if (!n) { host.innerHTML = '<div class="empty"><b>No data yet</b>The first snapshot has not been built.</div>'; return; }
      const W = Math.max(300, host.clientWidth || 640), H = cfg.height || (W < 520 ? 250 : 330);
      const m = {l: 46, r: 12, t: 14, b: 34}, iw = W - m.l - m.r, ih = H - m.t - m.b;
      const lines = cfg.mode === 'lines', bars = !lines && n <= 8;
      const totals = x.map((_, i) => series.reduce((s, sr) => s + (sr.values[i] || 0), 0));
      const peak = lines ? Math.max(...series.flatMap(s => s.values.map(v => v || 0))) : Math.max(...totals);
      const yMax = cfg.yMax || niceMax(peak), fmtY = cfg.yFormat || LW.compact;
      const y = v => r1(m.t + ih - (v / yMax) * ih);
      const band = iw / n;
      const px = i => bars ? r1(m.l + band * (i + .5)) : r1(n === 1 ? m.l + iw / 2 : m.l + (iw * i) / (n - 1));
      let g = '';
      for (let k = 0; k <= 4; k++) {
        const v = (yMax * k) / 4;
        g += `<line class="gl${k ? '' : ' zero'}" x1="${m.l}" x2="${W - m.r}" y1="${y(v)}" y2="${y(v)}"/><text class="ax" x="${m.l - 8}" y="${y(v) + 4}" text-anchor="end">${esc(fmtY(v))}</text>`;
      }
      const maxLabels = Math.max(2, Math.floor(iw / 78)), step = Math.max(1, Math.ceil(n / maxLabels));
      for (let i = 0; i < n; i += step) {
        const midnight = new Date(x[i]).getUTCHours() === 0;
        g += `<text class="ax" x="${px(i)}" y="${H - 16}" text-anchor="middle">${hourLabel(x[i])}</text>`;
        if (midnight || i === 0) g += `<text class="ax" x="${px(i)}" y="${H - 3}" text-anchor="middle" style="font-weight:600">${dayLabel(x[i])}</text>`;
      }
      let body = '';
      if (lines) {
        series.forEach(s => {
          const pts = s.values.map((v, i) => (v == null ? null : [px(i), y(v)])).filter(Boolean);
          body += `<polyline points="${pts.map(p => p.join(',')).join(' ')}" fill="none" style="stroke:${s.color}" stroke-width="2.2" stroke-linejoin="round" stroke-linecap="round"/>`;
          if (n <= 24) pts.forEach(p => { body += `<circle cx="${p[0]}" cy="${p[1]}" r="3" style="fill:${s.color}"/>`; });
        });
      } else {
        const base = new Array(n).fill(0);
        series.forEach(s => {
          const top = s.values.map((v, i) => base[i] + (v || 0));
          if (bars) {
            const bw = Math.min(46, band * .62);
            top.forEach((t, i) => {
              if (t === base[i]) return;
              body += `<rect x="${r1(px(i) - bw / 2)}" y="${y(t)}" width="${r1(bw)}" height="${r1(Math.max(0, y(base[i]) - y(t)))}" rx="3" style="fill:${s.color}"/>`;
            });
          } else {
            const upper = top.map((t, i) => `${px(i)},${y(t)}`);
            const lower = base.map((b, i) => `${px(i)},${y(b)}`).reverse();
            body += `<path d="M${upper.join('L')}L${lower.join('L')}Z" style="fill:${s.color}" fill-opacity=".82"/>`;
            body += `<polyline points="${upper.join(' ')}" fill="none" style="stroke:${s.color}" stroke-width="1.5" stroke-linejoin="round"/>`;
          }
          top.forEach((t, i) => { base[i] = t; });
        });
      }
      host.innerHTML = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(cfg.ariaLabel || 'Chart')}" style="touch-action:pan-y">${g}${body}` +
        `<line class="xh" id="xh" y1="${m.t}" y2="${m.t + ih}" style="display:none"/><rect id="hit" x="${m.l}" y="${m.t}" width="${iw}" height="${ih}" fill="transparent"/></svg>`;
      const svg = host.querySelector('svg'), xh = svg.querySelector('#xh'), hit = svg.querySelector('#hit');
      const at = ev => {
        const box = svg.getBoundingClientRect(), local = ((ev.clientX - box.left) / box.width) * W;
        const i = bars ? Math.floor((local - m.l) / band) : Math.round(((local - m.l) / iw) * (n - 1));
        return Math.max(0, Math.min(n - 1, i));
      };
      hit.addEventListener('pointermove', ev => {
        const i = at(ev);
        xh.setAttribute('x1', px(i)); xh.setAttribute('x2', px(i)); xh.style.display = '';
        const rows = series.filter(s => s.values[i] != null).map(s => `<div class="tr"><span><i style="--c:${s.color}"></i>${esc(s.label)}</span><b style="display:inline">${esc((cfg.tipFormat || LW.fmt)(s.values[i]))}</b></div>`).join('');
        const total = !lines && series.length > 1 ? `<div class="tr" style="margin-top:4px;border-top:1px solid rgba(255,255,255,.25);padding-top:4px"><span>Total</span><b style="display:inline">${esc(LW.fmt(totals[i]))}</b></div>` : '';
        LW.tip.show(`<b>${esc(LW.fmtHour(x[i]))}</b>${rows}${total}`, ev.clientX, ev.clientY);
      });
      hit.addEventListener('pointerleave', () => { xh.style.display = 'none'; LW.tip.hide(); });
    }
    let raf = 0;
    new ResizeObserver(() => { cancelAnimationFrame(raf); raf = requestAnimationFrame(draw); }).observe(host);
    document.addEventListener('lw-theme', draw);
    draw();
    return {update(next) { cfg = next; draw(); }};
  };

  C.donut = function (host, parts, opts = {}) {
    const total = parts.reduce((s, p) => s + p.value, 0) || 1, R = 70, CIRC = 2 * Math.PI * R;
    let off = 0, arcs = '';
    parts.forEach(p => {
      const len = Math.max(0, (p.value / total) * CIRC - 2);
      arcs += `<circle cx="100" cy="100" r="${R}" fill="none" stroke-width="24" stroke-dasharray="${r1(len)} ${r1(CIRC - len)}" stroke-dashoffset="${r1(-off)}" transform="rotate(-90 100 100)" style="stroke:${p.color}"/>`;
      off += (p.value / total) * CIRC;
    });
    host.innerHTML = `<svg class="donut" viewBox="0 0 200 200" role="img" aria-label="${esc(opts.ariaLabel || 'Share')}">` +
      `<circle cx="100" cy="100" r="${R}" fill="none" stroke-width="24" style="stroke:var(--surface-2)"/>${arcs}` +
      `<text class="center" x="100" y="106" text-anchor="middle">${esc(opts.center || '')}</text>` +
      `<text class="centersub" x="100" y="126" text-anchor="middle">${esc(opts.sub || '')}</text></svg>`;
  };

  C.hbars = function (host, rows, opts = {}) {
    const top = Math.max(...rows.map(r => r.value), 1);
    host.replaceChildren(el('div', {className: 'hbars'}, ...rows.map(r => {
      const bar = el('i'); bar.style.width = Math.max(2, (r.value / top) * 100) + '%';
      const wrap = el('div', {className: 'hbar'},
        el('div', {className: 'row'}, el('span', {textContent: r.label}), el('b', {className: 'num', textContent: (opts.format || LW.fmt)(r.value) + (r.note ? ' · ' + r.note : '')})),
        el('div', {className: 'track'}, bar));
      wrap.style.setProperty('--c', r.color);
      return wrap;
    })));
  };

  /* Rows of cells, one per hour, shaded by each row's own range so every wiki's daily rhythm shows. */
  C.heat = function (host, hours, rows) {
    const grid = el('div', {className: 'heat'});
    grid.style.gridTemplateColumns = `max-content repeat(${hours.length}, minmax(9px, 1fr))`;
    const every = Math.max(1, Math.ceil(hours.length / 8));
    grid.append(el('span'));
    hours.forEach((h, i) => grid.append(el('span', {className: 'ht', textContent: i % every === 0 ? hourLabel(h) : ''})));
    rows.forEach(r => {
      const peak = Math.max(...r.values.map(v => v || 0), 1);
      grid.append(el('span', {className: 'hl', textContent: r.label}));
      r.values.forEach((v, i) => {
        const c = el('span', {className: 'hc', title: `${r.label} · ${LW.fmtHour(hours[i])} · ${v == null ? 'no data' : LW.fmt(v) + ' edits'}`});
        c.style.setProperty('--c', r.color); c.style.setProperty('--v', v == null ? 0 : Math.max(.06, v / peak));
        grid.append(c);
      });
    });
    host.replaceChildren(grid);
  };

  C.spark = function (host, values, color = 'var(--accent)') {
    const v = values.filter(x => x != null);
    if (v.length < 2) { host.innerHTML = ''; return; }
    const lo = Math.min(...v), hi = Math.max(...v), span = hi - lo || 1;
    const pts = v.map((val, i) => `${r1((i / (v.length - 1)) * 100)},${r1(28 - ((val - lo) / span) * 24)}`);
    host.innerHTML = `<svg viewBox="0 0 100 32" preserveAspectRatio="none" aria-hidden="true" style="width:100%;height:100%">` +
      `<path d="M0,32L${pts.join('L')}L100,32Z" style="fill:${color}" fill-opacity=".12"/>` +
      `<polyline points="${pts.join(' ')}" fill="none" style="stroke:${color}" stroke-width="2" vector-effect="non-scaling-stroke" stroke-linejoin="round" stroke-linecap="round"/></svg>`;
  };

  /* "Collecting data" note for a young pipeline: the charts fill in as more hours arrive. */
  C.collecting = function (host, have, want) {
    host.replaceChildren();
    if (have >= want) return;
    const bar = el('b'); bar.style.width = Math.max(4, (have / want) * 100) + '%';
    const wrap = el('div', {className: 'collecting'}, el('span', {textContent: `${have} of ${want} hours collected so far. The charts fill in as the pipeline runs.`}),
      el('span', {className: 'bar-bg'}, bar));
    host.append(wrap);
  };
})();
