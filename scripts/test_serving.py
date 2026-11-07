"""Streaming publication is independent of Iceberg and does not evaluate alerts."""
import os
import sys
import time
from datetime import datetime,timedelta,timezone
from pathlib import Path
import psycopg2
sys.path.insert(0,'/app')
from serving import publish_candles
name='test_stream_'+str(time.time_ns())
admin=psycopg2.connect(os.environ['PG_DSN']); admin.autocommit=True
with admin.cursor() as cur: cur.execute('CREATE SCHEMA '+name)
os.environ['PGOPTIONS']='-c search_path='+name
try:
    with psycopg2.connect(os.environ['PG_DSN']) as pg,pg.cursor() as cur:
        cur.execute(Path('/schema/001-init.sql').read_text())
    start=datetime(2026,11,4,12,tzinfo=timezone.utc)
    rows=[dict(symbol='AAPL',minute_start=start,minute_end=start+timedelta(minutes=1),
      open=100,high=103,low=98,close=103,volume=10,notional=1007,trade_count=4,
      last_event_time=start+timedelta(seconds=59),last_trade_id='last')]
    publish_candles(rows,False); publish_candles(rows,False); publish_candles(rows,True)
    publish_candles([dict(rows[0],volume=999)],False)
    with psycopg2.connect(os.environ['PG_DSN']) as pg,pg.cursor() as cur:
        cur.execute('SELECT count(*),sum(volume),bool_and(is_final) FROM candles')
        assert cur.fetchone()==(1,10,True)
    print('POSTGRES_STREAM_OK replay stable, finalized protected, no alert dependency')
finally:
    os.environ.pop('PGOPTIONS',None)
    with admin.cursor() as cur: cur.execute('DROP SCHEMA '+name+' CASCADE')
    admin.close()
