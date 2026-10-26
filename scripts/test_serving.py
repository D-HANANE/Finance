"""Real PostgreSQL tests in a unique temporary schema, never the live tables."""
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
import psycopg2
sys.path.insert(0,'/app')
from serving import publish_candles

schema = f'test_serving_{time.time_ns()}'
dsn = os.environ['PG_DSN']
admin = psycopg2.connect(dsn)
admin.autocommit = True
with admin.cursor() as cursor:
    cursor.execute(f'CREATE SCHEMA {schema}')
os.environ['PGOPTIONS'] = f'-c search_path={schema}'
try:
    with psycopg2.connect(dsn) as connection,connection.cursor() as cursor:
        ddl = Path('/checks/../postgres/001-init.sql')
        if not ddl.exists():
            ddl = Path('/schema/001-init.sql')
        cursor.execute(ddl.read_text().split('\n',1)[1])
    base = datetime(2026,10,26,10,tzinfo=timezone.utc)
    def candle(symbol,minute,price):
        start = base+timedelta(minutes=minute)
        return dict(symbol=symbol,minute_start=start,minute_end=start+timedelta(minutes=1),
                    open=price,high=price,low=price,close=price,volume=10,notional=price*10,
                    trade_count=1,last_event_time=start+timedelta(seconds=59),last_trade_id=f'{symbol}-{minute}')
    rows = [candle(symbol,i,100 if i<5 else price)
            for symbol,price in [('AAPL',108),('MSFT',105),('NVDA',95),('GOOGL',92)]
            for i in range(6)]
    rows += [candle('TSLA',5,108)]
    publish_candles(rows,False)
    publish_candles(rows,False)
    with psycopg2.connect(dsn) as connection,connection.cursor() as cursor:
        cursor.execute('SELECT symbol FROM alerts ORDER BY symbol')
        assert cursor.fetchall()==[('AAPL',),('GOOGL',)],'Strict threshold or missing reference failed'
        cursor.execute('SELECT count(*),sum(volume) FROM candles')
        assert cursor.fetchone()==(25,250),'Replay changed candle counts or volumes'
    publish_candles(rows,True)
    attempted = dict(rows[0],volume=999)
    publish_candles([attempted],False)
    with psycopg2.connect(dsn) as connection,connection.cursor() as cursor:
        cursor.execute('SELECT volume,is_final FROM candles WHERE symbol=%s AND minute_start=%s',('AAPL',base))
        assert cursor.fetchone()==(10,True)
        cursor.execute('SELECT count(*) FROM alerts')
        assert cursor.fetchone()[0]==2
    print('POSTGRES_TEST_OK upserts replayable; +/-5% excluded; +/-8% alerted; absent reference ignored; final protected')
finally:
    os.environ.pop('PGOPTIONS',None)
    with admin.cursor() as cursor:
        cursor.execute(f'DROP SCHEMA {schema} CASCADE')
    admin.close()
