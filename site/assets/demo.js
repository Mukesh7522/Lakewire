/* Simulated data, used only when no published snapshot exists or when the URL has ?demo. Every page labels it. */
(function () {
  'use strict';
  function mulberry32(a) { return () => { a |= 0; a = a + 0x6D2B79F5 | 0; let t = Math.imul(a ^ a >>> 15, 1 | a); t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t; return ((t ^ t >>> 14) >>> 0) / 4294967296; }; }
  function makeDemo() {
    const rnd = mulberry32(20261110), now = new Date();
    const end = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), now.getUTCHours());
    const wk = [['enwiki', 9200, .19, 14], ['dewiki', 2400, .09, 13], ['frwiki', 1750, .12, 14], ['eswiki', 1150, .08, 20], ['jawiki', 1550, .15, 11]];
    const bot = [];
    for (let h = 71; h >= 0; h--) {
      const ts = new Date(end - h * 3600e3);
      for (const [wiki, base, bs, peak] of wk) {
        const d = .62 + .38 * Math.cos((ts.getUTCHours() - peak) / 24 * 2 * Math.PI);
        const total = Math.round(base * d * (.86 + .28 * rnd()));
        const share = Math.min(.6, Math.max(.02, bs + .04 * Math.sin(h / 6 + base) + (rnd() - .5) * .03));
        const b = Math.round(total * share);
        bot.push({wiki, hour_ts: ts.toISOString(), edits_total: total, bot_edits: b, human_edits: total - b, bot_share: +(b / total).toFixed(4)});
      }
    }
    const tot = {}; bot.forEach(r => tot[r.hour_ts] = (tot[r.hour_ts] || 0) + r.edits_total);
    const hrs = Object.keys(tot).sort(), last = tot[hrs.at(-1)];
    const titles = ['Solar_eclipse','Artemis_program','Quantum_computing','Tour_de_France','Bitcoin','Mount_Everest','Jazz','Marie_Curie','Great_Barrier_Reef',"Rubik's_Cube",'Octopus','Voyager_1','Chess','Black_hole','Mars','Photosynthesis','Silk_Road','Ada_Lovelace','Coffee','Aurora','Tardigrade','Python_(programming_language)','Apollo_11','Pizza','Sourdough'];
    const vh = new Date(end - 5 * 3600e3).toISOString();
    const trending = titles.map((t, i) => {
      const views = Math.round(90000 / Math.pow(i + 1.4, .9) * (.8 + .4 * rnd())), prev = Math.round(views / (.6 + 2.2 * rnd()));
      const g = +(views / prev).toFixed(3);
      return {page_title: t, view_hour: vh, views, views_prev_day: prev, growth_vs_prev_day: g, edits_1h: Math.round(rnd() * rnd() * 40), trend_score: +(Math.log(1 + views) * g).toFixed(3)};
    }).sort((a, b) => b.trend_score - a.trend_score);
    const ct = ['Jupiter','List_of_largest_cities','Honey_bee','Roman_Empire','Cheese','Nile','Periodic_table','Lighthouse','Tea','Volcano','Origami','Glacier'];
    const today = new Date(end).toISOString().slice(0, 10), yday = new Date(end - 864e5).toISOString().slice(0, 10);
    const conflicts = ct.map((t, i) => {
      const rapid = 3 + Math.floor(rnd() * 11), ed = 2 + Math.floor(rnd() * 8);
      return {wiki: wk[Math.floor(rnd() * 5)][0], title: t.replaceAll('_', ' '), event_date: rnd() > .4 ? today : yday, edits: rapid + 2 + Math.floor(rnd() * 25), distinct_editors: ed, rapid_alternations: rapid, bytes_swing: Math.floor(rnd() * 9000) + 200, conflict_score: rapid * Math.min(ed, 5)};
    }).sort((a, b) => b.conflict_score - a.conflict_score);
    const iso = now.toISOString();
    return {
      demo: true,
      health: {generated_at: iso, lag_seconds: 180, mode: new URLSearchParams(location.search).get('mode') === 'lite' ? 'lite' : 'full', rows: [{latest_event_ts: new Date(now - 180e3).toISOString(), events_last_hour: last, events_last_24h: hrs.slice(-24).reduce((s, h) => s + tot[h], 0), events_total: hrs.reduce((s, h) => s + tot[h], 0) * 9, wikis: 5}]},
      trending: {generated_at: iso, rows: trending}, bot_share: {generated_at: iso, rows: bot}, conflicts: {generated_at: iso, rows: conflicts}
    };
  }
  LW.makeDemo = makeDemo;
})();
