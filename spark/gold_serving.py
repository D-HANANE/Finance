"""Publish computed Iceberg snapshots to the existing PostgreSQL Gold serving tables."""
from datetime import datetime,timezone
from psycopg2.extras import execute_values,Json
from pyspark.sql import functions as F

FORMULA_VERSION='gold-v3-vwap-precision'

MINUTE=('symbol','minute_start','close','last_event_time','ma20','ma50','log_return','vwap_day',
        'reference_price','reference_time','change_5m','metric_updated_at')
DAILY=('symbol','day','close','daily_return','daily_volatility','vwap','minutes','is_complete')
CORRELATIONS=('symbol_a','symbol_b','correlation','observations')

def upsert(cur,table,columns,keys,rows,snapshot):
    names=columns+('source_snapshot','published_at')
    update=','.join(c+'=EXCLUDED.'+c for c in names if c not in keys)
    sql=f"INSERT INTO {table}({','.join(names)}) VALUES %s ON CONFLICT({','.join(keys)}) DO UPDATE SET {update}"
    now=datetime.now(timezone.utc)
    batch=[];count=0
    for row in rows:
        values=[]
        for value in tuple(row):
            if isinstance(value,datetime) and value.tzinfo is None: value=value.replace(tzinfo=timezone.utc)
            values.append(value)
        batch.append(tuple(values)+(str(snapshot),now))
        if len(batch)==1000:
            execute_values(cur,sql,batch,page_size=1000);count+=len(batch);batch=[]
    if batch:
        execute_values(cur,sql,batch,page_size=1000);count+=len(batch)
    return count

def publish(spark,snapshots,cur):
    # This transaction also publishes lakehouse_publication in the caller.
    cur.execute('SELECT pg_advisory_xact_lock(487207)')
    cur.execute('SELECT snapshots,source_cursor,formula_version FROM gold_serving_publication WHERE id=1 FOR UPDATE')
    previous=cur.fetchone()
    if previous and previous[0]==snapshots and previous[2]==FORMULA_VERSION: return {'already_published':True}
    counts={};cursor=previous[1] if previous else datetime(1970,1,1,tzinfo=timezone.utc)
    for table,columns,keys in [('minute_metrics',MINUTE,('symbol','minute_start')),
                               ('daily_metrics',DAILY,('symbol','day')),
                               ('correlations',CORRELATIONS,('symbol_a','symbol_b'))]:
        snapshot=snapshots.get(table)
        if not snapshot: continue
        data=spark.read.option('snapshot-id',snapshot).table('lake.gold.'+table)
        if table=='minute_metrics' and previous and previous[2]==FORMULA_VERSION:
            data=data.filter(F.col('metric_updated_at')>=F.lit(cursor))
        destination={'minute_metrics':'gold_minute','daily_metrics':'gold_daily','correlations':'gold_correlations'}[table]
        selected=data.select(*columns)
        counts[destination]=upsert(cur,destination,columns,keys,selected.toLocalIterator(),snapshot)
        if table=='minute_metrics':
            latest=data.agg(F.max('metric_updated_at')).first()[0]
            if latest:
                latest=latest.replace(tzinfo=timezone.utc) if latest.tzinfo is None else latest
                cursor=max(cursor,latest)
    cur.execute("""INSERT INTO gold_serving_publication(id,snapshots,source_cursor,row_counts,formula_version)
      VALUES(1,%s,%s,%s,%s) ON CONFLICT(id) DO UPDATE SET snapshots=EXCLUDED.snapshots,
      source_cursor=EXCLUDED.source_cursor,row_counts=EXCLUDED.row_counts,formula_version=EXCLUDED.formula_version,published_at=now()""",
      (Json(snapshots),cursor,Json(counts),FORMULA_VERSION))
    return counts
