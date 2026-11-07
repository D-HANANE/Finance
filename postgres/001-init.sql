CREATE TABLE candles (
 symbol text NOT NULL, minute_start timestamptz NOT NULL, minute_end timestamptz NOT NULL,
 open numeric(18,4) NOT NULL, high numeric(18,4) NOT NULL, low numeric(18,4) NOT NULL,
 close numeric(18,4) NOT NULL, volume bigint NOT NULL CHECK (volume >= 0),
 notional numeric(38,4) NOT NULL, trade_count bigint NOT NULL CHECK (trade_count > 0),
 last_event_time timestamptz NOT NULL, last_trade_id text NOT NULL,
 is_final boolean NOT NULL DEFAULT false, updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY (symbol, minute_start),
 CHECK (low <= open AND open <= high AND low <= close AND close <= high),
 CHECK (minute_end = minute_start + interval '1 minute')
);
CREATE INDEX candles_time ON candles (minute_start DESC);
CREATE INDEX candles_reference ON candles (symbol, last_event_time DESC);
CREATE TABLE latest_prices (
 symbol text PRIMARY KEY, price numeric(18,4) NOT NULL,
 event_time timestamptz NOT NULL, trade_id text NOT NULL,
 updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE VIEW live_indicators AS
SELECT symbol, minute_start, close, is_final,
 CASE WHEN count(*) OVER w20 = 20 THEN avg(close) OVER w20 END AS ma20,
 CASE WHEN count(*) OVER w50 = 50 THEN avg(close) OVER w50 END AS ma50,
 sum(notional) OVER day / nullif(sum(volume) OVER day, 0) AS vwap_day
FROM candles
WINDOW w20 AS (PARTITION BY symbol ORDER BY minute_start ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
 w50 AS (PARTITION BY symbol ORDER BY minute_start ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),
 day AS (PARTITION BY symbol, (minute_start AT TIME ZONE 'UTC')::date ORDER BY minute_start);
