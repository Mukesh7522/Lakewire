/* Lakewire dashboard: shared shell. Theme, data loading, status, formatting, tooltip. Every page loads this first. */
(function () {
  'use strict';
  const LW = window.LW = {};
  document.documentElement.classList.add('js');

  LW.REPO_URL = 'https://github.com/Mukesh7522/Lakewire';
  LW.WIKI_NAMES = {enwiki: 'English', dewiki: 'German', frwiki: 'French', eswiki: 'Spanish', jawiki: 'Japanese',
    commonswiki: 'Commons', wikidatawiki: 'Wikidata'};
  const STALE_PIPELINE_S = 1800;      // the freshness promise: a newest event older than this is stale
  const STALE_SNAPSHOT_S = 3 * 3600;  // published hourly, so allow for scheduling delays

  /* ---------- helpers ---------- */
  const $ = (s, r = document) => r.querySelector(s);
  const $$ = (s, r = document) => [...r.querySelectorAll(s)];
  const esc = s => String(s).replace(/[&<>"']/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]));
  const el = (tag, props = {}, ...kids) => { const e = document.createElement(tag); Object.assign(e, props); kids.forEach(k => e.append(k)); return e; };
  Object.assign(LW, {$, $$, esc, el});
  LW.reduceMotion = matchMedia('(prefers-reduced-motion: reduce)').matches;
  LW.fmt = n => (n == null || Number.isNaN(Number(n))) ? 'n/a' : Number(n).toLocaleString('en');
  LW.compact = n => new Intl.NumberFormat('en', {notation: 'compact', maximumFractionDigits: 1}).format(n);
  LW.pct = (n, d = 0) => (n * 100).toFixed(d) + '%';
  LW.ago = s => { s = Math.max(0, Math.round(s)); return s < 90 ? s + ' s' : s < 5400 ? Math.round(s / 60) + ' min' : s < 172800 ? (s / 3600).toFixed(1) + ' h' : Math.round(s / 86400) + ' d'; };
  const hourFmt = new Intl.DateTimeFormat('en', {month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false, timeZone: 'UTC'});
  LW.fmtHour = iso => hourFmt.format(new Date(iso)) + ' UTC';
  LW.wikiName = w => LW.WIKI_NAMES[w] || w;
  const SPECIAL_HOSTS = {commonswiki: 'commons.wikimedia.org', wikidatawiki: 'www.wikidata.org', metawiki: 'meta.wikimedia.org',
    mediawikiwiki: 'www.mediawiki.org', specieswiki: 'species.wikimedia.org'};
  LW.wikiHost = w => SPECIAL_HOSTS[w] || w.replace(/wiki$/, '').replace(/_/g, '-') + '.wikipedia.org';
  LW.pageUrl = (host, title) => `https://${host}/wiki/${encodeURIComponent(String(title).replaceAll(' ', '_')).replace(/%2F/g, '/').replace(/%3A/g, ':')}`;
  LW.docUrl = path => `${LW.REPO_URL}/blob/main/${path}`;

  /* ---------- theme ---------- */
  const THEME_KEY = 'lw-theme';
  function initTheme() {
    const btn = $('#theme');
    if (!btn) return;
    btn.addEventListener('click', () => {
      const next = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
      document.documentElement.dataset.theme = next;
      try { localStorage.setItem(THEME_KEY, next); } catch (e) { /* private mode: the choice just is not remembered */ }
      document.dispatchEvent(new CustomEvent('lw-theme'));
    });
  }

  /* ---------- tooltip ---------- */
  let tipEl = null;
  LW.tip = {
    show(html, x, y) {
      tipEl = tipEl || document.body.appendChild(el('div', {className: 'tip', role: 'presentation'}));
      tipEl.innerHTML = html;  // callers pass markup built from escaped values
      tipEl.classList.add('on');
      const w = tipEl.offsetWidth, h = tipEl.offsetHeight;
      tipEl.style.left = Math.max(8, Math.min(innerWidth - w - 8, x + 14)) + 'px';
      tipEl.style.top = Math.max(8, Math.min(innerHeight - h - 8, y + 14)) + 'px';
    },
    hide() { if (tipEl) tipEl.classList.remove('on'); }
  };

  /* ---------- count-up ---------- */
  LW.countUp = (node, to, fmt = LW.fmt) => {
    if (!node) return;
    const from = Number(node.dataset.v) || 0;
    node.dataset.v = to;
    if (to == null || Number.isNaN(Number(to)) || LW.reduceMotion || from === to) { node.textContent = fmt(to); return; }
    const t0 = performance.now(), dur = 700;
    const step = t => {
      const k = Math.min(1, (t - t0) / dur), e = 1 - Math.pow(1 - k, 3);
      node.textContent = fmt(Math.round(from + (to - from) * e));
      if (k < 1) requestAnimationFrame(step);
    };
    requestAnimationFrame(step);
  };

  LW.reveal = () => {
    const io = new IntersectionObserver(es => es.forEach(e => { if (e.isIntersecting) { e.target.classList.add('in'); io.unobserve(e.target); } }), {threshold: .05});
    $$('.reveal:not(.in)').forEach((n, i) => { n.style.transitionDelay = Math.min(i % 4, 3) * 60 + 'ms'; io.observe(n); });
  };

  /* ---------- data ---------- */
  const loadScript = src => new Promise((ok, fail) => {
    const s = document.createElement('script');
    s.src = src; s.onload = ok; s.onerror = () => fail(new Error('could not load ' + src));
    document.head.append(s);
  });
  LW.load = async () => {
    const force = new URLSearchParams(location.search).has('demo');
    if (!force) {
      try {
        const get = n => fetch(`data/${n}.json`, {cache: 'no-store'}).then(r => { if (!r.ok) throw new Error(n + ' ' + r.status); return r.json(); });
        const [health, trending, bot_share, conflicts] = await Promise.all(['health', 'trending', 'bot_share', 'conflicts'].map(get));
        if (!health.rows || !bot_share.rows) throw new Error('unexpected shape');
        return {demo: false, health, trending, bot_share, conflicts};
      } catch (e) { console.info('Live snapshot unavailable, using demo data:', e.message); }
    }
    await loadScript('assets/demo.js');
    return LW.makeDemo();
  };

  LW.prepare = d => {
    const S = {data: d, demo: !!d.demo, mode: d.health.mode === 'lite' ? 'lite' : 'full'};
    S.health = d.health.rows[0] || {};
    S.genAt = Date.parse(d.health.generated_at) || Date.now();
    S.lag = d.health.lag_seconds;
    S.rows = d.bot_share.rows;
    S.wikis = [...new Set(S.rows.map(r => r.wiki))].sort((a, b) => a === 'enwiki' ? -1 : b === 'enwiki' ? 1 : a.localeCompare(b));
    S.hours = [...new Set(S.rows.map(r => r.hour_ts))].sort();
    S.idx = {};
    S.rows.forEach(r => (S.idx[r.wiki] = S.idx[r.wiki] || {})[r.hour_ts] = r);
    S.perHour = {};
    S.rows.forEach(r => {
      const h = S.perHour[r.hour_ts] = S.perHour[r.hour_ts] || {total: 0, bot: 0, human: 0};
      h.total += r.edits_total; h.bot += r.bot_edits; h.human += r.human_edits;
    });
    S.color = w => `var(--w${(Math.max(0, S.wikis.indexOf(w)) % 7) + 1})`;
    return S;
  };

  /* ---------- shell: status pill, banners, footer ---------- */
  const MODES = {
    full: {label: 'Full stack', blurb: 'Kafka, Spark and Airflow'},
    lite: {label: 'Lite', blurb: 'scheduled GitHub Actions'}
  };
  LW.modeInfo = m => MODES[m];
  const tickers = [];
  LW.onTick = fn => tickers.push(fn);

  function tick() {
    const S = LW.S;
    if (!S) return;
    const snapAge = (Date.now() - S.genAt) / 1000, lag = S.lag;
    const pill = $('#status'), text = $('#status-t'), warn = $('#n-stale'), warnText = $('#n-stale-t');
    let state = 'live', msg = '';
    if (S.demo) state = 'demo';
    else if (lag == null || lag > STALE_PIPELINE_S) { state = 'stale'; msg = 'The pipeline had not processed a new event recently when this snapshot was built, so these numbers may be out of date.'; }
    else if (snapAge > STALE_SNAPSHOT_S) { state = 'stale'; msg = `This snapshot is ${LW.ago(snapAge)} old. The publish job may have stopped, so these numbers may be out of date.`; }
    if (pill) {
      pill.dataset.s = state;
      text.textContent = state === 'demo' ? 'Demo data' : `${state === 'live' ? 'Live' : 'Stale'} · ${LW.ago(snapAge)} old`;
    }
    if (warn) { warn.classList.toggle('show', !!msg); if (warnText) warnText.textContent = msg; }
    tickers.forEach(fn => fn(S, snapAge));
  }

  LW.renderShell = S => {
    document.body.dataset.mode = S.mode;
    $$('[data-mode-label]').forEach(n => { n.textContent = MODES[S.mode].label; });
    $$('[data-mode-blurb]').forEach(n => { n.textContent = MODES[S.mode].blurb; });
    const demo = $('#n-demo');
    if (demo) demo.classList.toggle('show', S.demo);
    const gen = $('#foot-gen');
    if (gen) gen.textContent = 'Snapshot generated ' + new Date(S.genAt).toUTCString() + '.';
    tick();
  };

  /* Keep ?demo and ?mode when moving between pages, so the demo can be browsed. */
  function carryQuery() {
    const q = location.search;
    if (!/(^|[?&])(demo|mode)=?/.test(q)) return;
    $$('.nav a, .brand').forEach(a => { a.href = a.getAttribute('href').split('?')[0] + q; });
  }

  LW.boot = init => {
    initTheme();
    carryQuery();
    const repo = $('#repo-link');
    if (repo) repo.href = LW.REPO_URL;
    let first = true;
    async function run() {
      try {
        const S = LW.prepare(await LW.load());
        LW.S = S;
        LW.renderShell(S);
        init(S, first);
        first = false;
        LW.reveal();
      } catch (err) {
        console.error(err);
        const pill = $('#status');
        if (pill) { pill.dataset.s = 'stale'; $('#status-t').textContent = 'Could not load data'; }
        const warn = $('#n-stale');
        if (warn) { $('#n-stale-t').textContent = 'Could not load data: ' + err.message; warn.classList.add('show'); }
      }
    }
    run();
    setInterval(tick, 1000);
    setInterval(() => { if (LW.S && !LW.S.demo) run(); }, 5 * 60 * 1000);
  };
})();
