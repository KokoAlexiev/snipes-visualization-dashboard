'use strict';

// Background function (15-minute limit). Rebuilds the Supabase cache for a date
// range by fetching from Discord, so the synchronous snipes-html function only
// ever needs to read the cache. Invoked by snipes-html when the cache is missing
// or stale, and can also be called directly / on a schedule.

const { getHtmlForDateRange } = require('./lib/snipesCore.cjs');
const { createSupabaseClient, todayUTC } = require('./lib/snipesCache.cjs');

function isValidDate(s) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return false;
  const d = new Date(s + 'T00:00:00Z');
  return !isNaN(d.getTime());
}

exports.handler = async (event) => {
  let start, end;
  try {
    const body = event.body ? JSON.parse(event.body) : {};
    start = body.start;
    end = body.end;
  } catch (_) {
    return { statusCode: 400, body: 'Invalid JSON body' };
  }

  // Default to today if not provided.
  if (!start || !end) {
    const today = todayUTC();
    start = start || today;
    end = end || today;
  }

  if (!isValidDate(start) || !isValidDate(end) || start > end) {
    return { statusCode: 400, body: 'Invalid start/end' };
  }

  const supabase = createSupabaseClient();
  if (!supabase) {
    console.error('[warm-background] missing Supabase env — cannot cache');
    return { statusCode: 500, body: 'Supabase not configured' };
  }

  console.log(`[warm-background] rebuilding cache ${start}..${end}`);
  const t0 = Date.now();
  try {
    // forceToday: rebuild today even if a (stale) row exists. Lower concurrency
    // to keep memory well under the function limit — we have up to 15 minutes.
    await getHtmlForDateRange(start, end, supabase, { forceToday: true, concurrency: 6 });
    console.log(`[warm-background] done ${start}..${end} in ${Date.now() - t0}ms`);
    return { statusCode: 200, body: 'ok' };
  } catch (err) {
    console.error('[warm-background] error:', err && err.message);
    return { statusCode: 500, body: String(err && err.message) };
  }
};
