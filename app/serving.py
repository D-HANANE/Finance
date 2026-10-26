"""Transactional and replayable PostgreSQL publication; driver only (<=20/minute)."""
import os
from datetime import timezone
import psycopg2


FIELDS = ('symbol', 'minute_start', 'minute_end', 'open', 'high', 'low', 'close',
          'volume', 'notional', 'trade_count', 'last_event_time', 'last_trade_id')


def connect():
    connection = psycopg2.connect(os.environ['PG_DSN'], connect_timeout=10)
    with connection.cursor() as cursor:
        cursor.execute("SET TIME ZONE 'UTC'")
    return connection


def utc(value):
    # Spark collect() produces naive UTC timestamps with the configured session TZ.
    return value.replace(tzinfo=timezone.utc) if hasattr(value, 'tzinfo') and value.tzinfo is None else value


def publish_candles(rows, finalized):
    if not rows:
        return
    with connect() as connection, connection.cursor() as cursor:
        # Serialize live and final publishers; only finalized writes can replace a final row.
        cursor.execute('SELECT pg_advisory_xact_lock(487201)')
        for row in sorted(rows, key=lambda r: (r['minute_start'], r['symbol'])):
            values = [utc(row[name]) for name in FIELDS] + [finalized]
            cursor.execute(f'''INSERT INTO candles ({','.join(FIELDS)},is_final)
              VALUES ({','.join(['%s'] * len(values))})
              ON CONFLICT(symbol,minute_start) DO UPDATE SET
              {','.join(f'{name}=EXCLUDED.{name}' for name in FIELDS[2:])},
              is_final=EXCLUDED.is_final, updated_at=now()
              WHERE NOT candles.is_final OR EXCLUDED.is_final''', values)
            cursor.execute('''INSERT INTO latest_prices(symbol,price,event_time,trade_id)
               VALUES (%s,%s,%s,%s) ON CONFLICT(symbol) DO UPDATE SET
               price=EXCLUDED.price,event_time=EXCLUDED.event_time,
               trade_id=EXCLUDED.trade_id,updated_at=now()
               WHERE (EXCLUDED.event_time,EXCLUDED.trade_id) >
                     (latest_prices.event_time,latest_prices.trade_id)''',
                           (row['symbol'], row['close'], utc(row['last_event_time']), row['last_trade_id']))
        if not finalized:
            for symbol in sorted({row['symbol'] for row in rows}):
                # Latest candle close at/before T-5min, no older than T-6min.
                # Missing reference => no alert. Only current latest price is evaluated.
                cursor.execute('''INSERT INTO alerts
                 (symbol,bucket,event_time,price,reference_time,reference_price,change_fraction)
                 SELECT p.symbol, to_timestamp(floor(extract(epoch FROM p.event_time)/300)*300),
                    p.event_time,p.price,r.last_event_time,r.close,p.price/r.close-1
                 FROM latest_prices p JOIN LATERAL (
                   SELECT close,last_event_time FROM candles c WHERE c.symbol=p.symbol
                    AND c.last_event_time <= p.event_time-interval '5 minutes'
                    AND c.last_event_time >= p.event_time-interval '6 minutes'
                   ORDER BY last_event_time DESC LIMIT 1
                 ) r ON true WHERE p.symbol=%s AND abs(p.price/r.close-1)>0.05
                   AND NOT EXISTS (SELECT 1 FROM alerts a WHERE a.symbol=p.symbol
                      AND a.event_time > p.event_time-interval '5 minutes')
                 ON CONFLICT(symbol,bucket) DO NOTHING''', (symbol,))
