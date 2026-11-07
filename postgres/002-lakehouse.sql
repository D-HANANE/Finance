SELECT pg_advisory_xact_lock(487200);
-- Additive migration. Existing candles, archives and old alert history are preserved.
CREATE SCHEMA IF NOT EXISTS iceberg_catalog;
DO $$ BEGIN
 IF to_regclass('public.alerts') IS NOT NULL AND NOT EXISTS (
  SELECT 1 FROM information_schema.columns WHERE table_schema='public'
  AND table_name='alerts' AND column_name='alert_id') THEN
  ALTER TABLE alerts RENAME TO alerts_legacy_v1;
 END IF;
END $$;
CREATE TABLE IF NOT EXISTS alerts (
 alert_id uuid PRIMARY KEY, symbol text NOT NULL, alert_type text NOT NULL,
 description text NOT NULL, rule text NOT NULL,
 observed_value numeric NOT NULL, threshold numeric NOT NULL DEFAULT 0.05,
 opened_at timestamptz NOT NULL, last_observed_at timestamptz NOT NULL,
 updated_at timestamptz NOT NULL DEFAULT now(), resolved_at timestamptz, closed_at timestamptz,
 status text NOT NULL CHECK(status IN ('active','resolved','closed')),
 price numeric, reference_price numeric, reference_time timestamptz,
 origin text NOT NULL DEFAULT 'gold', fingerprint text NOT NULL,
 UNIQUE(symbol,alert_type,opened_at)
);
CREATE INDEX IF NOT EXISTS alerts_status_time_v2 ON alerts(status,last_observed_at DESC);
CREATE TABLE IF NOT EXISTS alert_history (
 history_id bigserial PRIMARY KEY, alert_id uuid NOT NULL REFERENCES alerts(alert_id),
 status text NOT NULL, action text NOT NULL, occurred_at timestamptz NOT NULL,
 recorded_at timestamptz NOT NULL DEFAULT now(), details jsonb NOT NULL,
 fingerprint text NOT NULL, UNIQUE(alert_id,fingerprint,action)
);
CREATE TABLE IF NOT EXISTS alert_observations (
 symbol text NOT NULL, observed_at timestamptz NOT NULL,
 price numeric NOT NULL, reference_price numeric, reference_time timestamptz,
 change_fraction numeric, source_updated_at timestamptz NOT NULL,
 fingerprint text NOT NULL, PRIMARY KEY(symbol,observed_at)
);
CREATE TABLE IF NOT EXISTS alert_state (
 name text PRIMARY KEY, source_cursor timestamptz NOT NULL DEFAULT '1970-01-01 UTC',
 source_snapshot text, updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO alert_state(name) VALUES ('gold_minute') ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS lakehouse_files (
 manifest_key text PRIMARY KEY, etag text NOT NULL, parquet_key text NOT NULL,
 row_count bigint NOT NULL, processed_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS lakehouse_dirty (
 symbol text NOT NULL, minute_start timestamptz NOT NULL,
 changed_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(symbol,minute_start)
);
CREATE TABLE IF NOT EXISTS lakehouse_runs (
 request_id text PRIMARY KEY, stage text NOT NULL, status text NOT NULL,
 started_at timestamptz NOT NULL DEFAULT now(), completed_at timestamptz,
 result jsonb, error text
);
-- Historical legacy detections are archived, not falsely inferred to be currently active.
DO $$ BEGIN
 IF to_regclass('public.alerts_legacy_v1') IS NOT NULL THEN
  INSERT INTO alerts(alert_id,symbol,alert_type,description,rule,observed_value,
   opened_at,last_observed_at,updated_at,closed_at,status,price,reference_price,
   reference_time,origin,fingerprint)
  SELECT md5(symbol||bucket::text)::uuid,symbol,'price_change_5m',
   'Détection historique migrée, état actif non établi',
   'abs(close/reference_close-1)>0.05 sur 5 minutes',change_fraction,
   event_time,event_time,detected_at,now(),'closed',price,reference_price,
   reference_time,'legacy',md5(symbol||bucket::text)
  FROM alerts_legacy_v1 ON CONFLICT DO NOTHING;
 END IF;
END $$;

ALTER TABLE alerts ADD COLUMN IF NOT EXISTS trigger_value numeric;
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS peak_value numeric;
CREATE TABLE IF NOT EXISTS lakehouse_publication (id integer PRIMARY KEY CHECK(id=1), snapshots jsonb NOT NULL,published_at timestamptz NOT NULL DEFAULT now());

-- Serving projection of Iceberg Gold. Reuse existing v1 tables, without recomputing in PostgreSQL.
CREATE TABLE IF NOT EXISTS gold_minute (
 symbol text NOT NULL,minute_start timestamptz NOT NULL,close double precision,
 ma20 double precision,ma50 double precision,PRIMARY KEY(symbol,minute_start));
ALTER TABLE gold_minute ADD COLUMN IF NOT EXISTS last_event_time timestamptz;
ALTER TABLE gold_minute ADD COLUMN IF NOT EXISTS log_return double precision;
ALTER TABLE gold_minute ADD COLUMN IF NOT EXISTS vwap_day double precision;
ALTER TABLE gold_minute ADD COLUMN IF NOT EXISTS reference_price double precision;
ALTER TABLE gold_minute ADD COLUMN IF NOT EXISTS reference_time timestamptz;
ALTER TABLE gold_minute ADD COLUMN IF NOT EXISTS change_5m double precision;
ALTER TABLE gold_minute ADD COLUMN IF NOT EXISTS metric_updated_at timestamptz;
ALTER TABLE gold_minute ADD COLUMN IF NOT EXISTS source_snapshot text;
ALTER TABLE gold_minute ADD COLUMN IF NOT EXISTS published_at timestamptz;
CREATE TABLE IF NOT EXISTS gold_daily (
 symbol text NOT NULL,day date NOT NULL,close double precision,daily_return double precision,
 daily_volatility double precision,vwap double precision,minutes bigint,is_complete boolean,
 PRIMARY KEY(symbol,day));
ALTER TABLE gold_daily ADD COLUMN IF NOT EXISTS source_snapshot text;
ALTER TABLE gold_daily ADD COLUMN IF NOT EXISTS published_at timestamptz;
CREATE TABLE IF NOT EXISTS gold_correlations (
 symbol_a text NOT NULL,symbol_b text NOT NULL,correlation double precision,observations bigint,
 PRIMARY KEY(symbol_a,symbol_b));
ALTER TABLE gold_correlations ADD COLUMN IF NOT EXISTS source_snapshot text;
ALTER TABLE gold_correlations ADD COLUMN IF NOT EXISTS published_at timestamptz;
CREATE TABLE IF NOT EXISTS gold_serving_publication (
 id integer PRIMARY KEY CHECK(id=1),snapshots jsonb NOT NULL,
 source_cursor timestamptz NOT NULL,row_counts jsonb NOT NULL,published_at timestamptz NOT NULL DEFAULT now());

CREATE OR REPLACE VIEW alert_symbol_coverage AS
WITH symbols AS (
 SELECT symbol FROM latest_prices UNION SELECT symbol FROM alert_observations
 UNION SELECT symbol FROM gold_minute WHERE source_snapshot IS NOT NULL
), coverage AS (
 SELECT symbol,count(*) observations,count(change_fraction) eligible_observations,
 count(*) FILTER(WHERE abs(change_fraction)>0.05) breaches,max(observed_at) last_observation
 FROM alert_observations GROUP BY symbol
), active AS (
 SELECT symbol,count(*) active_alerts FROM alerts WHERE status='active' AND origin='gold' GROUP BY symbol
)
SELECT s.symbol,coalesce(c.observations,0) observations,
 coalesce(c.eligible_observations,0) eligible_observations,coalesce(c.breaches,0) breaches,
 coalesce(a.active_alerts,0) active_alerts,c.last_observation
FROM symbols s LEFT JOIN coverage c USING(symbol) LEFT JOIN active a USING(symbol);

ALTER TABLE gold_serving_publication ADD COLUMN IF NOT EXISTS formula_version text NOT NULL DEFAULT '';
