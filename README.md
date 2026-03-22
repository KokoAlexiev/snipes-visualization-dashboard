# Snipes dashboard

Interactive snipes-by-hostname dashboard (same output as `visualizations/generate_snipes_chart.js`), served as a small web app:

- On load, the **last 24 hours** (`1` day) are fetched and rendered.
- A **slider** from **1–30** selects how many full days of history to pull (1 = 24 hours, 30 = 30 days).
- A **loading** overlay stays visible until the chart iframe finishes loading.
- **Netlify**: static `public/` site + serverless function that talks to Discord (token stays server-side).

## Prerequisites

- Node.js 18+
- A Discord bot with **Message Content Intent** enabled, in your server, with read access to the trade-success and create-trades channels.

## Local development

```bash
cd snipes-dashboard
npm install
cp .env.example .env
# Edit .env — set DISCORD_BOT_TOKEN
npm run dev
```

Open the URL Netlify prints (usually `http://localhost:8888`). The shell page is `public/index.html`; chart HTML is loaded from `/.netlify/functions/snipes-html?days=N`.

## Deploy on Netlify

1. Push this folder as its own Git repository (recommended), **or** keep it inside a larger monorepo.
2. New site from Git → if the repo root is this app, leave defaults; if the app lives in a subfolder, set **Base directory** in Netlify to `snipes-dashboard`.
3. Build command: `npm run build` (or leave empty).
4. Publish directory: `public` (relative to the base directory).
5. Add environment variable **`DISCORD_BOT_TOKEN`** (and optional channel/guild overrides from `.env.example`).

Always run `npm install` and `npm run dev` from the same directory that contains this app’s `package.json` and `netlify.toml`.

Functions are picked up from `netlify/functions` via `netlify.toml`.

### Timeouts

Discord history for many days can take longer than the default **10s** function limit on the free tier. If requests time out, shorten the range or use a higher function timeout on a paid Netlify plan.

## Security

- Never commit `.env` or real tokens.
- If a bot token was ever committed elsewhere, **rotate it** in the Discord developer portal.

## CLI (optional)

From `snipes-dashboard/netlify/functions/lib`, you can still run the embedded generator as a script if `require.main === module` (original `main()`): set `DISCORD_BOT_TOKEN`, then `node snipesCore.cjs 7` from that directory (writes HTML next to the file). Prefer the web app for day-to-day use.
