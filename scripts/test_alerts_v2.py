"""PostgreSQL integration with an isolated schema and a simulated Gold HTTP response."""
import json
import os
import re
import sys
import time
from contextlib import contextmanager
from datetime import datetime,timedelta,timezone
from pathlib import Path
import psycopg2
sys.path.insert(0,'/app')
import alerting

schema='test_alerts_'+str(time.time_ns())
admin=psycopg2.connect(os.environ['PG_DSN'])
admin.autocommit=True
with admin.cursor() as cur: cur.execute('CREATE SCHEMA '+schema)
os.environ['PGOPTIONS']='-c search_path='+schema
base=datetime(2026,11,4,12,tzinfo=timezone.utc)
def row(minute,value):
    return dict(symbol='AAPL',minute_start=(base+timedelta(minutes=minute)).isoformat(),
      last_event_time=(base+timedelta(minutes=minute,seconds=59)).isoformat(),
      close=108,reference_price=100,reference_time=(base+timedelta(minutes=minute-5)).isoformat(),
      change_5m=value,metric_updated_at=(base+timedelta(hours=1)).isoformat())
payload={'snapshot':'fixture-gold','observations':[row(0,'0.08'),row(1,'0.09'),row(2,'0.01'),row(3,'-0.08')]}
@contextmanager
def fake_urlopen(*args,**kwargs):
    import io
    yield io.StringIO(json.dumps(payload))
try:
    sql=Path('/schema/002-lakehouse.sql').read_text()
    sql='CREATE TABLE latest_prices (symbol text PRIMARY KEY);\n'+sql
    sql=re.sub(r'DO \$\$.*?\$\$;','',sql,flags=re.S)
    with psycopg2.connect(os.environ['PG_DSN']) as pg,pg.cursor() as cur: cur.execute(sql)
    original=alerting.urlopen
    alerting.urlopen=fake_urlopen
    alerting.evaluate()
    with psycopg2.connect(os.environ['PG_DSN']) as pg,pg.cursor() as cur:
        cur.execute("SELECT status,count(*) FROM alerts GROUP BY status ORDER BY status")
        assert cur.fetchall()==[('active',1),('resolved',1)]
        cur.execute('SELECT count(*) FROM alert_history')
        history=cur.fetchone()[0]
        cur.execute("SELECT alert_id FROM alerts WHERE status='resolved'")
        resolved=cur.fetchone()[0]
    alerting.evaluate()
    with psycopg2.connect(os.environ['PG_DSN']) as pg,pg.cursor() as cur:
        cur.execute('SELECT count(*) FROM alert_history')
        assert cur.fetchone()[0]==history,'Replay duplicated history'
    assert alerting.close_resolved(resolved)
    assert not alerting.close_resolved(resolved)
    payload['observations'].append(row(4,'0.02'))
    payload['observations'][-1]['metric_updated_at']=(base+timedelta(hours=2)).isoformat()
    alerting.evaluate()
    with psycopg2.connect(os.environ['PG_DSN']) as pg,pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM alerts WHERE status='active'")
        assert cur.fetchone()[0]==0
        cur.execute('SELECT count(*) FROM alerts')
        assert cur.fetchone()[0]==2
        cur.execute("SELECT count(*) FROM alert_history WHERE action='manual_close'")
        assert cur.fetchone()[0]==1
    # A late correction updates the same observation key rather than appending a duplicate.
    payload['observations'][1]['change_5m']='0.11'
    payload['observations'][1]['metric_updated_at']=(base+timedelta(hours=3)).isoformat()
    alerting.evaluate()
    with psycopg2.connect(os.environ['PG_DSN']) as pg,pg.cursor() as cur:
        cur.execute('SELECT count(*) FROM alert_observations')
        assert cur.fetchone()[0]==5
    print('ALERTS_V2_OK creation, maintenance, resolution, manual closure, replay, late correction')
finally:
    os.environ.pop('PGOPTIONS',None)
    with admin.cursor() as cur: cur.execute('DROP SCHEMA '+schema+' CASCADE')
    admin.close()
