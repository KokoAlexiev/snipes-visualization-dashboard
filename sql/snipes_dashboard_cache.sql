-- Snipes dashboard cache table
-- Each row caches the full generateHTML() input payload for an explicit calendar date range (UTC).
-- PK: (start_date, end_date) — exact match on the range the user requested.
-- Watermarks: newest Discord message snowflake in each channel at cache-build time.
--   For past-only ranges (end_date < today UTC), the cache is treated as permanent.
--   For ranges including today, the function re-checks watermarks to detect new messages.
-- Apply this in: Supabase SQL editor → New query → paste & run.

CREATE TABLE IF NOT EXISTS public.snipes_dashboard_cache (
  -- Date range key (UTC calendar days, inclusive on both sides)
  start_date            date        NOT NULL,
  end_date              date        NOT NULL,

  -- Discord channel watermarks (newest message snowflake at build time)
  watermark_trade_success   text    NOT NULL,
  watermark_create_trades   text    NOT NULL,

  -- Serialized inputs for generateHTML():
  -- { schemaVersion, chartStartMs, chartEndMs, events, missedSnipes, marketFeedUnder3, allCreateTrades }
  payload               jsonb       NOT NULL,

  -- Schema version — bump this constant in snipesCache.cjs when the parser or chart logic changes
  -- so stale cached rows are automatically ignored.
  content_version       smallint    NOT NULL DEFAULT 1,

  built_at              timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT snipes_cache_pkey PRIMARY KEY (start_date, end_date),
  CONSTRAINT snipes_cache_date_order CHECK (start_date <= end_date),
  CONSTRAINT snipes_cache_max_span   CHECK ((end_date - start_date) <= 29)
);

-- Index for housekeeping (e.g. DELETE WHERE built_at < now() - interval '90 days')
CREATE INDEX IF NOT EXISTS snipes_cache_built_at_idx ON public.snipes_dashboard_cache (built_at);

-- Enable RLS — service_role bypasses it automatically (no explicit policy needed for server-side access)
ALTER TABLE public.snipes_dashboard_cache ENABLE ROW LEVEL SECURITY;

-- No public/anon policies intentionally — the Netlify function must use SUPABASE_SERVICE_ROLE_KEY.

COMMENT ON TABLE public.snipes_dashboard_cache IS
  'Cache of Snipes dashboard chart payloads keyed by UTC calendar date range. '
  'Populated and read by the Netlify snipes-html function. '
  'Use SUPABASE_SERVICE_ROLE_KEY (service_role bypasses RLS).';

COMMENT ON COLUMN public.snipes_dashboard_cache.start_date IS 'Inclusive start of UTC calendar day range';
COMMENT ON COLUMN public.snipes_dashboard_cache.end_date   IS 'Inclusive end of UTC calendar day range';
COMMENT ON COLUMN public.snipes_dashboard_cache.watermark_trade_success IS 'Newest Discord message snowflake in trade-success at build time';
COMMENT ON COLUMN public.snipes_dashboard_cache.watermark_create_trades IS 'Newest Discord message snowflake in create-trades at build time';
COMMENT ON COLUMN public.snipes_dashboard_cache.payload IS
  'JSON blob: { schemaVersion, chartStartMs, chartEndMs, events[], missedSnipes[], marketFeedUnder3[], allCreateTrades[] }';
COMMENT ON COLUMN public.snipes_dashboard_cache.content_version IS
  'Bumped in code (CACHE_SCHEMA_VERSION) when parser/chart logic changes — rows with lower version are ignored';
