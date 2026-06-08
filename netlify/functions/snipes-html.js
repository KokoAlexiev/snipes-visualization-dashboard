'use strict';

const { getHtmlForDateRange } = require('./lib/snipesCore.cjs');
const { createSupabaseClient, todayUTC } = require('./lib/snipesCache.cjs');

const COMMON_HEADERS = {
  'Content-Type':   'text/html; charset=utf-8',
  'X-Frame-Options':'SAMEORIGIN',
  'Cache-Control':  'private, no-cache'
};

/** Coalesce concurrent identical range requests (cheap cache reads, but avoids piling up). */
const inflightRequests = new Map();

/** Throttle background warm triggers per range so we don't spam the background function. */
const lastWarmTrigger = new Map();
const WARM_TRIGGER_THROTTLE_MS = 30 * 1000;

function getHtmlForDateRangeDeduped(startDateStr, endDateStr, supabase) {
  const key = `${startDateStr}|${endDateStr}`;
  if (inflightRequests.has(key)) {
    return inflightRequests.get(key);
  }
  // Request path: read cache only, never run Discord (so it can't time out).
  const promise = getHtmlForDateRange(startDateStr, endDateStr, supabase, { noBuild: true }).finally(() => {
    inflightRequests.delete(key);
  });
  inflightRequests.set(key, promise);
  return promise;
}

/** Fire a background rebuild of the cache for this range (does not block the response). */
async function triggerBackgroundWarm(startDateStr, endDateStr) {
  const key = `${startDateStr}|${endDateStr}`;
  const now = Date.now();
  const last = lastWarmTrigger.get(key) || 0;
  if (now - last < WARM_TRIGGER_THROTTLE_MS) return;
  lastWarmTrigger.set(key, now);

  const base = process.env.URL || process.env.DEPLOY_PRIME_URL || process.env.DEPLOY_URL || '';
  if (!base) {
    console.warn('[snipes-html] no site URL env — cannot trigger background warm');
    return;
  }
  try {
    // Background functions return 202 immediately, so awaiting this is fast.
    await fetch(`${base}/.netlify/functions/snipes-warm-background`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ start: startDateStr, end: endDateStr })
    });
    console.log(`[snipes-html] triggered background warm ${key}`);
  } catch (err) {
    console.warn('[snipes-html] background warm trigger failed:', err && err.message);
  }
}

function warmingPage(startDateStr, endDateStr) {
  return `<!DOCTYPE html><html><head><meta charset="utf-8"><title>Preparing…</title></head>` +
    `<body style="font-family:sans-serif;padding:2rem;background:#1a1b26;color:#c0caf5;">` +
    `<h1>Preparing snipes data…</h1><p>Building the cache for ${startDateStr} → ${endDateStr}. ` +
    `This page will refresh automatically.</p></body></html>`;
}

/** Validate a YYYY-MM-DD string; return true if well-formed. */
function isValidDate(s) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return false;
  const d = new Date(s + 'T00:00:00Z');
  return !isNaN(d.getTime());
}

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 204,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type'
      },
      body: ''
    };
  }

  if (event.httpMethod !== 'GET') {
    return { statusCode: 405, body: 'Method Not Allowed' };
  }

  try {
    const params = event.queryStringParameters || {};
    const today  = todayUTC();

    // Parse start / end — fall back to converting legacy ?days=N
    let startDateStr = params.start;
    let endDateStr   = params.end;

    if (!startDateStr || !endDateStr) {
      // Legacy backward-compat: ?days=N  →  end = today, start = today - (N-1) days
      const raw    = params.days ?? '1';
      const days   = Math.min(30, Math.max(1, parseInt(raw, 10) || 1));
      endDateStr   = today;
      const d      = new Date(today + 'T00:00:00Z');
      d.setUTCDate(d.getUTCDate() - (days - 1));
      startDateStr = d.toISOString().slice(0, 10);
    }

    // Validate
    if (!isValidDate(startDateStr) || !isValidDate(endDateStr)) {
      return { statusCode: 400, body: 'Invalid start or end date (expected YYYY-MM-DD)' };
    }
    if (startDateStr > endDateStr) {
      return { statusCode: 400, body: 'start must be <= end' };
    }
    // Clamp to 30 calendar days max
    const [sy, sm, sd] = startDateStr.split('-').map(Number);
    const [ey, em, ed] = endDateStr.split('-').map(Number);
    const span = Math.round((Date.UTC(ey, em-1, ed) - Date.UTC(sy, sm-1, sd)) / 86400000);
    if (span > 29) {
      return { statusCode: 400, body: 'Date range too wide (max 30 days inclusive)' };
    }

    const supabase = createSupabaseClient();
    const result = await getHtmlForDateRangeDeduped(startDateStr, endDateStr, supabase);

    // Cache missing for one or more days — kick off a background build and tell the client to retry.
    if (result.needsBuild) {
      await triggerBackgroundWarm(startDateStr, endDateStr);
      return {
        statusCode: 200,
        headers: { ...COMMON_HEADERS, 'X-Snipes-Cache': 'WARMING' },
        body: warmingPage(startDateStr, endDateStr)
      };
    }

    // Served from cache. If today's slice is stale, refresh it in the background (non-blocking).
    if (result.stale) {
      await triggerBackgroundWarm(startDateStr, endDateStr);
    }

    return {
      statusCode: 200,
      headers: {
        ...COMMON_HEADERS,
        'X-Snipes-Cache': result.cacheHit ? 'HIT' : 'STALE'
      },
      body: result.html
    };
  } catch (err) {
    console.error('snipes-html error:', err);
    const message = err && err.message ? String(err.message) : 'Unknown error';
    return {
      statusCode: 500,
      headers: {
        'Content-Type':   'text/html; charset=utf-8',
        'X-Frame-Options':'SAMEORIGIN'
      },
      body: `<!DOCTYPE html><html><head><meta charset="utf-8"><title>Error</title></head><body style="font-family:sans-serif;padding:2rem;"><h1>Dashboard error</h1><p>${message.replace(/</g, '&lt;')}</p><p>Check Netlify function logs and environment variables.</p></body></html>`
    };
  }
};
