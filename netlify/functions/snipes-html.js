'use strict';

const { getHtmlForDateRange } = require('./lib/snipesCore.cjs');
const { createSupabaseClient, todayUTC } = require('./lib/snipesCache.cjs');

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
    const { html, cacheHit } = await getHtmlForDateRange(startDateStr, endDateStr, supabase);

    return {
      statusCode: 200,
      headers: {
        'Content-Type':   'text/html; charset=utf-8',
        'X-Frame-Options':'SAMEORIGIN',
        'Cache-Control':  'private, no-cache',
        'X-Snipes-Cache': cacheHit ? 'HIT' : 'MISS'
      },
      body: html
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
