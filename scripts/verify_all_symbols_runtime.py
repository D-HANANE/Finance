"""Compare every published PostgreSQL metric with its exact Iceberg Gold snapshot."""
import os,sys,json,math
from datetime import datetime,timezone
sys.path.insert(0,'/opt/finance')
import psycopg2
from catalog import session
from market import SYMBOLS
from gold_serving import MINUTE,DAILY,FORMULA_VERSION
spark=session('verify-all-symbol-serving');spark.sparkContext.setLogLevel('ERROR')
def normal(value):
    if isinstance(value,datetime) and value.tzinfo is None: return value.replace(tzinfo=timezone.utc)
    return value
try:
    with psycopg2.connect(os.environ['PG_DSN']) as pg:
        pg.set_session(isolation_level='REPEATABLE READ',readonly=True)
        with pg.cursor() as cur:
            cur.execute('SELECT snapshots,formula_version FROM gold_serving_publication WHERE id=1')
            snapshots,version=cur.fetchone();assert version==FORMULA_VERSION
            result={}
            for target,source,columns,keys in [('gold_minute','minute_metrics',MINUTE,2),('gold_daily','daily_metrics',DAILY,2)]:
                cur.execute('SELECT '+','.join(columns)+' FROM '+target+' WHERE source_snapshot IS NOT NULL')
                actual={tuple(normal(v) for v in r[:keys]):tuple(normal(v) for v in r) for r in cur.fetchall()}
                expected={tuple(normal(v) for v in tuple(r)[:keys]):tuple(normal(v) for v in tuple(r)) for r in
                  spark.read.option('snapshot-id',snapshots[source]).table('lake.gold.'+source).select(*columns).collect()}
                assert actual.keys()==expected.keys(),(target,len(actual),len(expected))
                for key,values in expected.items():
                    for field,a,b in zip(columns,actual[key],values):
                        if isinstance(b,float):
                            assert a is not None and math.isclose(a,b,rel_tol=1e-12,abs_tol=1e-12),(key,field,a,b)
                        else: assert a==b,(key,field,a,b)
                assert {key[0] for key in actual}==set(SYMBOLS)
                result[target]=len(actual)
            cur.execute('SELECT symbol,observations,eligible_observations FROM alert_symbol_coverage')
            coverage=cur.fetchall()
            assert {r[0] for r in coverage}==set(SYMBOLS)
            assert all(r[1]>0 and r[2]>0 for r in coverage)
            cur.execute("SELECT count(DISTINCT symbol) FROM gold_daily WHERE source_snapshot IS NOT NULL AND daily_return IS NOT NULL AND daily_volatility IS NOT NULL AND vwap IS NOT NULL")
            assert cur.fetchone()[0]==20
    print('ALL_SYMBOLS_RUNTIME_OK '+json.dumps(result)+'; 20 assets, every Spark/PostgreSQL metric agrees, alert coverage complete')
finally:
    spark.stop()
