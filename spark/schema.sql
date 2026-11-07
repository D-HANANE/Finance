CREATE NAMESPACE IF NOT EXISTS lake.silver;
CREATE NAMESPACE IF NOT EXISTS lake.gold;
CREATE TABLE IF NOT EXISTS lake.silver.trades (
 trade_id STRING,symbol STRING,price DECIMAL(18,4),quantity BIGINT,event_time TIMESTAMP,
 source_topic STRING,source_partition INT,source_offset BIGINT)
 USING iceberg PARTITIONED BY(days(event_time))
 LOCATION 's3://silver/lakehouse-v2/trades'
 TBLPROPERTIES('format-version'='2');
CREATE TABLE IF NOT EXISTS lake.silver.candles (
 symbol STRING,minute_start TIMESTAMP,minute_end TIMESTAMP,open DECIMAL(18,4),
 high DECIMAL(18,4),low DECIMAL(18,4),close DECIMAL(18,4),volume BIGINT,
 notional DECIMAL(38,4),trade_count BIGINT,last_event_time TIMESTAMP,last_trade_id STRING)
 USING iceberg PARTITIONED BY(days(minute_start))
 LOCATION 's3://silver/lakehouse-v2/candles'
 TBLPROPERTIES('format-version'='2');
CREATE TABLE IF NOT EXISTS lake.silver.rejected (
 source_object STRING,trade_id STRING,symbol STRING,event_time TIMESTAMP,reason STRING)
 USING iceberg LOCATION 's3://silver/lakehouse-v2/rejected'
 TBLPROPERTIES('format-version'='2');
CREATE TABLE IF NOT EXISTS lake.gold.minute_metrics (
 symbol STRING,minute_start TIMESTAMP,close DOUBLE,last_event_time TIMESTAMP,
 ma20 DOUBLE,ma50 DOUBLE,log_return DOUBLE,vwap_day DOUBLE,
 reference_price DOUBLE,reference_time TIMESTAMP,change_5m DOUBLE,metric_updated_at TIMESTAMP)
 USING iceberg PARTITIONED BY(days(minute_start))
 LOCATION 's3://gold/lakehouse-v2/minute_metrics'
 TBLPROPERTIES('format-version'='2');
CREATE TABLE IF NOT EXISTS lake.gold.daily_metrics (
 symbol STRING,day DATE,close DOUBLE,daily_return DOUBLE,daily_volatility DOUBLE,
 vwap DOUBLE,minutes BIGINT,is_complete BOOLEAN)
 USING iceberg PARTITIONED BY(day) LOCATION 's3://gold/lakehouse-v2/daily_metrics'
 TBLPROPERTIES('format-version'='2');
CREATE TABLE IF NOT EXISTS lake.gold.correlation_daily (
 symbol_a STRING,symbol_b STRING,day DATE,n BIGINT,sx DOUBLE,sy DOUBLE,
 sxx DOUBLE,syy DOUBLE,sxy DOUBLE)
 USING iceberg PARTITIONED BY(day) LOCATION 's3://gold/lakehouse-v2/correlation_daily'
 TBLPROPERTIES('format-version'='2');
CREATE TABLE IF NOT EXISTS lake.gold.correlations (
 symbol_a STRING,symbol_b STRING,correlation DOUBLE,observations BIGINT)
 USING iceberg LOCATION 's3://gold/lakehouse-v2/correlations'
 TBLPROPERTIES('format-version'='2');
