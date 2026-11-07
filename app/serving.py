"""PostgreSQL publication for the streaming chain only; no alert calculation."""
import os
from datetime import timezone
import psycopg2

FIELDS = ('symbol','minute_start','minute_end','open','high','low','close',
          'volume','notional','trade_count','last_event_time','last_trade_id')

def connect():
    connection = psycopg2.connect(os.environ['PG_DSN'],connect_timeout=10)
    with connection.cursor() as cursor:
        cursor.execute("SET TIME ZONE 'UTC'")
    return connection

def utc(value):
    return value.replace(tzinfo=timezone.utc) if hasattr(value,'tzinfo') and value.tzinfo is None else value

def publish_candles(rows,finalized):
    if not rows:
        return
    with connect() as connection,connection.cursor() as cursor:
        cursor.execute('SELECT pg_advisory_xact_lock(487201)')
        for row in sorted(rows,key=lambda r:(r['minute_start'],r['symbol'])):
            values=[utc(row[name]) for name in FIELDS]+[finalized]
            cursor.execute(f"""INSERT INTO candles ({','.join(FIELDS)},is_final)
              VALUES ({','.join(['%s']*len(values))})
              ON CONFLICT(symbol,minute_start) DO UPDATE SET
              {','.join(f'{name}=EXCLUDED.{name}' for name in FIELDS[2:])},
              is_final=EXCLUDED.is_final,updated_at=now()
              WHERE NOT candles.is_final OR EXCLUDED.is_final""",values)
            cursor.execute("""INSERT INTO latest_prices(symbol,price,event_time,trade_id)
              VALUES (%s,%s,%s,%s) ON CONFLICT(symbol) DO UPDATE SET
              price=EXCLUDED.price,event_time=EXCLUDED.event_time,
              trade_id=EXCLUDED.trade_id,updated_at=now()
              WHERE (EXCLUDED.event_time,EXCLUDED.trade_id)>
                    (latest_prices.event_time,latest_prices.trade_id)""",
              (row['symbol'],row['close'],utc(row['last_event_time']),row['last_trade_id']))
