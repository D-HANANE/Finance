"""20-symbol Spark SQL -> real Iceberg snapshots -> PostgreSQL -> alert lifecycle fixture."""
import sys,os,json,math,statistics,tempfile,re,time,io
from pathlib import Path
from datetime import datetime,timedelta,timezone
from decimal import Decimal
from contextlib import contextmanager
sys.path.extend(['/opt/finance','/app',str(Path(__file__).resolve().parents[1]/'spark'),str(Path(__file__).resolve().parents[1]/'app')])
import psycopg2
from pyspark.sql import SparkSession,functions as F
from lakehouse import MINUTE_SQL,daily_sql
from market import SYMBOLS
from gold_serving import publish
import alerting

folder=tempfile.TemporaryDirectory()
spark=(SparkSession.builder.master('local[1]').appName('all-symbol-gold-contract')
 .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','2')
 .config('spark.sql.catalog.lake','org.apache.iceberg.spark.SparkCatalog')
 .config('spark.sql.catalog.lake.type','hadoop')
 .config('spark.sql.catalog.lake.warehouse',folder.name+'/warehouse').getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
schema='test_gold_'+str(time.time_ns())
admin=psycopg2.connect(os.environ['PG_DSN']);admin.autocommit=True
try:
    with admin.cursor() as cur: cur.execute('CREATE SCHEMA '+schema)
    os.environ['PGOPTIONS']='-c search_path='+schema
    migration=Path('/schema/002-lakehouse.sql').read_text()
    migration=re.sub(r'DO \$\$.*?\$\$;','',migration,flags=re.S)
    with psycopg2.connect(os.environ['PG_DSN']) as pg,pg.cursor() as cur:
        cur.execute('CREATE TABLE latest_prices(symbol text PRIMARY KEY)')
        cur.execute(migration)
    start=datetime(2026,11,3,23)
    definition='symbol string,minute_start timestamp,minute_end timestamp,open decimal(18,4),high decimal(18,4),low decimal(18,4),close decimal(18,4),volume long,notional decimal(38,4),trade_count long,last_event_time timestamp,last_trade_id string'
    rows=[];expected={}
    for index,symbol in enumerate(SYMBOLS):
        values=[]
        for i in range(120):
            price=(Decimal(100+i)*(1+Decimal(index)/100)*(Decimal('1.08') if i>=60 else 1)).quantize(Decimal('0.0001'))
            quantity=2+i%7+index;at=start+timedelta(minutes=i)
            values.append((at,float(price),quantity,float(price*quantity)))
            rows.append((symbol,at,at+timedelta(minutes=1),price,price,price,price,quantity,price*quantity,1,at+timedelta(seconds=59),str(i)))
        expected[symbol]=values
    spark.sql('CREATE NAMESPACE lake.silver');spark.sql('CREATE NAMESPACE lake.gold')
    candles=spark.createDataFrame(rows,definition)
    candles.writeTo('lake.silver.candles').using('iceberg').create()
    candles.createOrReplaceTempView('metric_input')
    spark.sql(MINUTE_SQL).writeTo('lake.gold.minute_metrics').using('iceberg').create()
    dates="DATE '2026-11-03',DATE '2026-11-04'"
    spark.sql(daily_sql(dates,dates)).writeTo('lake.gold.daily_metrics').using('iceberg').create()
    snapshots={}
    for table in ('minute_metrics','daily_metrics'):
        snapshots[table]=str(spark.sql('SELECT snapshot_id FROM lake.gold.'+table+'.snapshots ORDER BY committed_at DESC LIMIT 1').first()[0])
    with psycopg2.connect(os.environ['PG_DSN']) as pg,pg.cursor() as cur:
        counts=publish(spark,snapshots,cur)
    assert counts['gold_minute']==2400 and counts['gold_daily']==40,counts
    with psycopg2.connect(os.environ['PG_DSN']) as pg,pg.cursor() as cur:
        for symbol in SYMBOLS:
            values=expected[symbol]
            cur.execute('SELECT ma20,ma50,vwap_day FROM gold_minute WHERE symbol=%s ORDER BY minute_start DESC LIMIT 1',(symbol,))
            ma20,ma50,vwap=cur.fetchone()
            assert abs(ma20-statistics.mean(v[1] for v in values[-20:]))<1e-9,symbol
            assert abs(ma50-statistics.mean(v[1] for v in values[-50:]))<1e-9,symbol
            assert abs(vwap-sum(v[3] for v in values[60:])/sum(v[2] for v in values[60:]))<1e-9,symbol
            cur.execute("SELECT daily_return,daily_volatility,vwap FROM gold_daily WHERE symbol=%s AND day=DATE '2026-11-04'",(symbol,))
            ret,vol,dayvwap=cur.fetchone()
            returns=[math.log(values[i][1]/values[i-1][1]) for i in range(60,120)]
            assert abs(ret-(values[-1][1]/values[59][1]-1))<1e-9,symbol
            assert abs(vol-statistics.stdev(returns))<1e-9,symbol
            assert abs(vwap-dayvwap)<1e-9,symbol
            cur.execute('SELECT count(*) FILTER(WHERE ma20 IS NULL),count(*) FILTER(WHERE ma50 IS NULL) FROM gold_minute WHERE symbol=%s',(symbol,))
            assert cur.fetchone()==(19,49),symbol
        assert publish(spark,snapshots,cur)=={'already_published':True}
        cur.execute('SELECT count(*) FROM gold_minute');assert cur.fetchone()[0]==2400
    payload={'snapshot':snapshots['minute_metrics'],'observations':[r.asDict() for r in spark.table('lake.gold.minute_metrics').collect()]}
    @contextmanager
    def gold_response(*args,**kwargs):
        yield io.StringIO(json.dumps(payload,default=lambda v:str(v) if isinstance(v,Decimal) else v.replace(tzinfo=timezone.utc).isoformat()))
    alerting.urlopen=gold_response
    alerting.evaluate()
    with psycopg2.connect(os.environ['PG_DSN']) as pg,pg.cursor() as cur:
        cur.execute("SELECT symbol,count(*) FROM alerts WHERE origin='gold' GROUP BY symbol")
        alerts=dict(cur.fetchall())
        assert set(alerts)==set(SYMBOLS) and set(alerts.values())=={1},alerts
        cur.execute("SELECT count(*) FROM alerts WHERE status='resolved'");assert cur.fetchone()[0]==20
        cur.execute('SELECT count(*) FROM alert_history');history=cur.fetchone()[0]
    alerting.evaluate()
    with psycopg2.connect(os.environ['PG_DSN']) as pg,pg.cursor() as cur:
        cur.execute('SELECT count(*) FROM alert_history');assert cur.fetchone()[0]==history
    print('ALL_SYMBOLS_GOLD_OK symbols=20 minute_rows=2400 daily_rows=40; independent MA20 MA50 VWAP DailyReturn DailyVolatility references agree; 20 independent alert episodes; replay stable')
finally:
    os.environ.pop('PGOPTIONS',None)
    with admin.cursor() as cur: cur.execute('DROP SCHEMA IF EXISTS '+schema+' CASCADE')
    admin.close();spark.stop();folder.cleanup()
