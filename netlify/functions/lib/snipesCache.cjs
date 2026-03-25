'use strict';

const { createClient } = require('@supabase/supabase-js');

// Bump this when the event schema or generateHTML logic changes so old rows are ignored.
const CACHE_SCHEMA_VERSION = 1;

const TABLE = 'snipes_dashboard_cache';

/**
 * Parse a YYYY-MM-DD string into UTC start-of-day and end-of-day milliseconds.
 */
function dateStringToRange(dateStr) {
  const [y, m, d] = dateStr.split('-').map(Number);
  const startMs = Date.UTC(y, m - 1, d, 0, 0, 0, 0);
  const endMs   = Date.UTC(y, m - 1, d, 23, 59, 59, 999);
  return { startMs, endMs };
}

/**
 * Return today's date as a UTC YYYY-MM-DD string.
 */
function todayUTC() {
  return new Date().toISOString().slice(0, 10);
}

/**
 * Return true if the range end is strictly before today UTC (past-only range → cache is permanent).
 */
function isFullyInPast(endDateStr) {
  return endDateStr < todayUTC();
}

/**
 * Create a Supabase client using the service role key (bypasses RLS).
 * Falls back gracefully if env vars are not set.
 */
function createSupabaseClient() {
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) return null;
  return createClient(url.replace(/\/$/, ''), key, {
    auth: { persistSession: false }
  });
}

/**
 * Serialize the in-memory payload into a JSON-safe object.
 * Strips `date` (Date objects, not needed by generateHTML) and truncates rawContent.
 */
function serializePayload(events, missedSnipes, marketFeedUnder3, allCreateTrades, chartStartMs, chartEndMs) {
  const stripEvent = (e) => {
    const { date: _d, ...rest } = e;
    if (rest.rawContent) rest.rawContent = String(rest.rawContent).substring(0, 300);
    return rest;
  };
  return {
    schemaVersion: CACHE_SCHEMA_VERSION,
    chartStartMs,
    chartEndMs,
    events:          (events          || []).map(stripEvent),
    missedSnipes:    (missedSnipes    || []).map(stripEvent),
    marketFeedUnder3:(marketFeedUnder3|| []).map(stripEvent),
    allCreateTrades: (allCreateTrades || []).map(stripEvent)
  };
}

/**
 * Deserialize and validate a cached payload.
 * Returns null if schema version mismatch.
 */
function deserializePayload(raw) {
  if (!raw || raw.schemaVersion !== CACHE_SCHEMA_VERSION) return null;
  return {
    chartStartMs:     raw.chartStartMs,
    chartEndMs:       raw.chartEndMs,
    events:           raw.events           || [],
    missedSnipes:     raw.missedSnipes     || [],
    marketFeedUnder3: raw.marketFeedUnder3 || [],
    allCreateTrades:  raw.allCreateTrades  || []
  };
}

/**
 * Load a cache row for the given date range.
 * Returns { payload, watermarkTradeSuccess, watermarkCreateTrades } or null on miss/error.
 */
async function loadCache(supabase, startDateStr, endDateStr) {
  if (!supabase) return null;
  try {
    const { data, error } = await supabase
      .from(TABLE)
      .select('payload, watermark_trade_success, watermark_create_trades, content_version')
      .eq('start_date', startDateStr)
      .eq('end_date',   endDateStr)
      .single();
    if (error || !data) return null;
    const payload = deserializePayload(data.payload);
    if (!payload) return null; // schema version mismatch
    return {
      payload,
      watermarkTradeSuccess: data.watermark_trade_success,
      watermarkCreateTrades: data.watermark_create_trades
    };
  } catch (err) {
    console.warn('[snipesCache] loadCache error:', err && err.message);
    return null;
  }
}

/**
 * Upsert a cache row.
 */
async function upsertCache(supabase, startDateStr, endDateStr, payloadObj, wmTradeSuccess, wmCreateTrades) {
  if (!supabase) return;
  try {
    const serialized = serializePayload(
      payloadObj.events,
      payloadObj.missedSnipes,
      payloadObj.marketFeedUnder3,
      payloadObj.allCreateTrades,
      payloadObj.chartStartMs,
      payloadObj.chartEndMs
    );
    const { error } = await supabase
      .from(TABLE)
      .upsert({
        start_date:                startDateStr,
        end_date:                  endDateStr,
        watermark_trade_success:   wmTradeSuccess,
        watermark_create_trades:   wmCreateTrades,
        payload:                   serialized,
        content_version:           CACHE_SCHEMA_VERSION,
        built_at:                  new Date().toISOString()
      }, { onConflict: 'start_date,end_date' });
    if (error) console.warn('[snipesCache] upsertCache error:', error.message);
    else console.log(`[snipesCache] UPSERT ${startDateStr}..${endDateStr}`);
  } catch (err) {
    console.warn('[snipesCache] upsertCache threw:', err && err.message);
  }
}

/**
 * Get the newest message ID (snowflake) from a Discord channel as a watermark.
 * Returns empty string on error.
 */
async function getChannelWatermark(channel) {
  try {
    const msgs = await channel.messages.fetch({ limit: 1 });
    if (!msgs || msgs.size === 0) return '';
    return msgs.first().id;
  } catch (err) {
    console.warn('[snipesCache] getChannelWatermark error:', err && err.message);
    return '';
  }
}

module.exports = {
  CACHE_SCHEMA_VERSION,
  createSupabaseClient,
  dateStringToRange,
  todayUTC,
  isFullyInPast,
  loadCache,
  upsertCache,
  getChannelWatermark
};
