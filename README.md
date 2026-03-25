# Snipes dashboard

Interactive snipes-by-hostname dashboard backed by Discord trade data, served as a Netlify web app with a Supabase cache for fast repeat loads.

## How it works

```
Browser  →  public/index.html  (date picker UI)
              ↓  fetch ?start=YYYY-MM-DD&end=YYYY-MM-DD
         →  /.netlify/functions/snipes-html
              1. Cheap Discord watermark check (newest message snowflake per channel)
              2. Load Supabase cache row for this exact date range
                 • HIT (watermarks match or range is fully in the past) → generateHTML from JSON
                 • MISS / STALE → full Discord pagination → generateHTML → upsert cache
              ↓
         →  X-Snipes-Cache: HIT | MISS   (visible in browser DevTools → Network tab)
```

**Past ranges** (end date before today UTC) are cached **permanently** — no watermark check needed. Today's data is refreshed when new Discord messages arrive (detected via the cheap watermark query).

## Prerequisites

- Node.js 18+
- A Discord bot with **Message Content Intent**, in your server, with read access to trade-success and create-trades channels.
- A Supabase project (free tier is fine).

## Setup

### 1. Apply Supabase SQL migration

In the Supabase SQL editor, run the contents of:

```
snipes-dashboard/sql/snipes_dashboard_cache.sql
```

### 2. Get env vars

| Variable | Where to find it |
|----------|-----------------|
| `DISCORD_BOT_TOKEN` | Discord Developer Portal → your app → **Bot** → copy/reset token |
| `SUPABASE_URL` | Supabase project **Settings → API → Project URL** |
| `SUPABASE_SERVICE_ROLE_KEY` | Supabase project **Settings → API → service_role** (keep secret) |

### 3. Local dev

```bash
cd snipes-dashboard
npm install
cp .env.example .env
# Fill in DISCORD_BOT_TOKEN, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
npm run dev
```

Open **http://localhost:8888**. The shell page loads with yesterday–today selected by default.

## UI

- **Start date / End date pickers**: pick any calendar range up to **30 days** (inclusive).
- Default on load: **yesterday → today** (approximately the last 24 hours as a UTC calendar window).
- A **Cached** (green) or **Live fetch** (red) badge appears after each load.
- Inspect `X-Snipes-Cache: HIT | MISS` in DevTools → Network → Response Headers.

## Deploy on Netlify

1. Push `snipes-dashboard/` as its own Git repository **or** keep it inside a monorepo (set **Base directory** to `snipes-dashboard` if so).
2. Build command: `npm run build` (or leave empty).
3. Publish directory: `public` (relative to base).
4. Add env vars in **Site configuration → Environment variables**:
   - `DISCORD_BOT_TOKEN`
   - `SUPABASE_URL`
   - `SUPABASE_SERVICE_ROLE_KEY`
5. **Check function timeout** — Site configuration → Functions. Fetching many days of Discord history can take minutes. Set the timeout as high as your plan allows. The `netlify.toml` requests `timeout = 900` for the `snipes-html` function.

## Caching behaviour

| Scenario | What happens |
|----------|-------------|
| Second load of the same past date range | Instant — served from Supabase, no Discord calls |
| Second load of a range that includes today, no new Discord messages | HIT — watermarks match |
| Load of today's date after a new trade fires in Discord | MISS — refetches, updates cache |
| `SUPABASE_*` env vars missing | Graceful fallback — live Discord fetch every time |

## Security

- `SUPABASE_SERVICE_ROLE_KEY` is server-side only. Never add it to `public/`, client code, or commit it.
- The `snipes_dashboard_cache` table has RLS enabled with no anon policies. Only the service role key (which bypasses RLS) can read/write it.
- Discord bot token: if ever committed to git, rotate it in the Discord Developer Portal immediately.

## API

```
GET /.netlify/functions/snipes-html?start=2026-01-01&end=2026-01-07
```

- `start` / `end`: UTC calendar dates (YYYY-MM-DD), inclusive.
- Response headers: `X-Snipes-Cache: HIT | MISS`.
- Max span: 30 days.
- Legacy: `?days=N` still works (mapped to `end=today`, `start=today-(N-1)`).
