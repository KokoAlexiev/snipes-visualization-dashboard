# Snipes visualization dashboard

**Production:** [exodia-lkan-sniper-dashboard.netlify.app](https://exodia-lkan-sniper-dashboard.netlify.app/)  
**Upstream repo (Netlify):** [github.com/KaloyanAleksiev4/snipes-visualization-dashboard](https://github.com/KaloyanAleksiev4/snipes-visualization-dashboard)

Netlify serves `public/` and `/.netlify/functions/snipes-html` (Discord + Supabase cache). **Deploys rebuild when you push to the branch Netlify watches** (usually `main` on Kaloyan’s repo).

---

## How it works

```
Browser  →  public/index.html  (date picker UI)
              ↓  fetch ?start=YYYY-MM-DD&end=YYYY-MM-DD
         →  /.netlify/functions/snipes-html
              1. Discord watermark check per channel
              2. Supabase cache HIT/MISS → generateHTML
              ↓
         →  X-Snipes-Cache: HIT | MISS
```

## Prerequisites

- Node.js 18+
- Discord bot (Message Content Intent), read trade-success + create-trades
- Supabase project

## Setup

1. Run `sql/snipes_dashboard_cache.sql` in Supabase SQL editor.
2. Copy `.env.example` → `.env` with `DISCORD_BOT_TOKEN`, `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`.
3. `npm install` then `npm run dev` → http://localhost:8888

## Offline chart generator

`visualizations/generate_snipes_chart.js` writes a standalone HTML report (hostname, got/missed/blacklist/not-eligible, tier heartbeats, ≤2% pool). Not used by the Netlify page directly.

```bash
npm run generate-chart -- 7
```

Needs `DISCORD_BOT_TOKEN` and optional Supabase env for blacklist.

## Netlify deploy

- **Publish:** `public`
- **Functions:** `netlify/functions` (see `netlify.toml`, `snipes-html` timeout 900s)
- Set the three env vars in Netlify UI.

## Security

Never commit `.env` or service role keys.

## API

`GET /.netlify/functions/snipes-html?start=YYYY-MM-DD&end=YYYY-MM-DD` (max 30 days). Legacy `?days=N` supported.
