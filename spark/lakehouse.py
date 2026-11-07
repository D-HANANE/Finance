"""Batch chain: committed Bronze -> canonical trades -> minute Silver -> Spark SQL Gold."""
import json
import os
from datetime import datetime,timezone,timedelta
from pathlib import Path
import boto3
from botocore.config import Config
from psycopg2.extras import Json
from pyspark.sql import functions as F,Window
from serving import connect,utc
from market import SYMBOLS
from gold_serving import publish as publish_serving,FORMULA_VERSION

CANDLE_FIELDS=['symbol','minute_start','minute_end','open','high','low','close','volume','notional','trade_count','last_event_time','last_trade_id']

def s3_client():
    return boto3.client('s3',endpoint_url=os.environ['S3_ENDPOINT'],
      config=Config(s3={'addressing_style':'path'},retries={'max_attempts':3}))

def initialize(spark):
    with connect() as pg,pg.cursor() as cur:
        cur.execute(Path('/opt/finance/migration.sql').read_text())
    client=s3_client()
    for bucket in ('bronze','silver','gold'):
        try: client.head_bucket(Bucket=bucket)
        except client.exceptions.ClientError as error:
            if str(error.response['Error']['Code']) not in ('404','NoSuchBucket','NotFound'):
                raise
            client.create_bucket(Bucket=bucket)
    for statement in Path('/opt/finance/schema.sql').read_text().split(';'):
        if statement.strip(): spark.sql(statement)

def bronze_inventory():
    client=s3_client()
    with connect() as pg,pg.cursor() as cur:
        cur.execute('SELECT manifest_key,etag FROM lakehouse_files')
        completed=dict(cur.fetchall())
    pending=[]
    for page in client.get_paginator('list_objects_v2').paginate(Bucket='bronze',Prefix='manifests/trades/'):
        for item in page.get('Contents',[]):
            if completed.get(item['Key'])!=item['ETag']:
                pending.append((item['Key'],item['ETag']))
    pending.sort()
    return pending

def merge(spark,target,frame,keys,changed=None):
    # Materialize the batch boundary: input_file_name/current_timestamp and
    # self-table reads must not remain in Iceberg's correlated MERGE plan.
    stage=frame.localCheckpoint(eager=True)
    view='stage_'+target.split('.')[-1]
    try:
        stage.createOrReplaceTempView(view)
        condition=' AND '.join('t.'+k+'=s.'+k for k in keys)
        when='WHEN MATCHED'+(' AND '+changed if changed else '')+' THEN UPDATE SET *'
        spark.sql(f'MERGE INTO {target} t USING {view} s ON {condition} {when} WHEN NOT MATCHED THEN INSERT *')
    finally:
        spark.catalog.dropTempView(view)
        stage.unpersist()


def assert_candles(df):
    invalid=df.filter("""symbol IS NULL OR minute_start IS NULL OR open IS NULL OR close IS NULL
      OR high IS NULL OR low IS NULL OR volume IS NULL OR notional IS NULL
      OR low>open OR open>high OR low>close OR close>high OR volume<0 OR trade_count<=0""")
    if not invalid.limit(1).isEmpty(): raise ValueError('Silver OHLCV quality failed')
    if not df.groupBy('symbol','minute_start').count().filter('count>1').limit(1).isEmpty():
        raise ValueError('Duplicate Silver candle key')

def silver(spark):
    pending=bronze_inventory()[:int(os.getenv('BRONZE_FILES_PER_RUN','200'))]
    if not pending: return {'processed_files':0,'pending':0}
    client=s3_client()
    manifests=[]
    for key,etag in pending:
        manifest=json.loads(client.get_object(Bucket='bronze',Key=key)['Body'].read())
        manifests.append((key,etag,manifest))
    paths=['s3a://bronze/'+m['parquet_key'] for _,_,m in manifests]
    raw=spark.read.option('mergeSchema','true').parquet(*paths).withColumn('source_object',F.input_file_name())
    for name,kind in [('quantity','long'),('volume','long'),('_parse_error','string')]:
        if name not in raw.columns: raw=raw.withColumn(name,F.lit(None).cast(kind))
    df=raw.select(
      F.col('trade_id').cast('string'),F.col('symbol').cast('string'),
      F.col('price').cast('decimal(18,4)'),F.coalesce('quantity','volume').cast('long').alias('quantity'),
      F.col('event_time').cast('timestamp'),F.col('_topic').alias('source_topic'),
      F.col('_partition').cast('int').alias('source_partition'),
      F.col('_offset').cast('long').alias('source_offset'),'source_object','_parse_error')
    valid=F.coalesce(F.col('trade_id').isNotNull() & F.col('symbol').isin(list(SYMBOLS))
       & (F.col('price')>0) & (F.col('quantity')>0) & F.col('event_time').isNotNull()
       & F.col('_parse_error').isNull(),F.lit(False))
    rejected=df.filter(~valid).select('source_object','trade_id','symbol','event_time').withColumn('reason',F.lit('invalid_contract'))
    # Rejected output is replayable by object and trade identity, including NULL identifiers.
    if not rejected.isEmpty():
        rejected.dropDuplicates().createOrReplaceTempView('reject_stage')
        spark.sql("""MERGE INTO lake.silver.rejected t USING reject_stage s
          ON t.source_object=s.source_object AND t.trade_id <=> s.trade_id AND t.event_time <=> s.event_time
          WHEN NOT MATCHED THEN INSERT *""")
    clean=df.filter(valid).drop('_parse_error','source_object')
    clean=(clean.withColumn('_rn',F.row_number().over(Window.partitionBy('trade_id')
           .orderBy('source_topic','source_partition','source_offset'))).filter('_rn=1').drop('_rn'))
    clean.persist()
    try:
        # Contract: trade_id is immutable. Existing canonical identities win across batches.
        clean.createOrReplaceTempView('incoming_trades')
        spark.sql("""MERGE INTO lake.silver.trades t USING incoming_trades s ON t.trade_id=s.trade_id
          WHEN NOT MATCHED THEN INSERT *""")
        keys=clean.select('symbol',F.date_trunc('minute','event_time').alias('minute_start')).distinct()
        keys.createOrReplaceTempView('affected_minutes')
        # Read only days touched by this batch, then retain the affected minute keys.
        days=[r[0] for r in keys.select(F.to_date('minute_start')).distinct().collect()]
        if days:
            trades=spark.table('lake.silver.trades').filter(F.to_date('event_time').isin(days))
            impacted=trades.withColumn('minute_start',F.date_trunc('minute','event_time')).join(keys,['symbol','minute_start'])
            order=F.struct('event_time','trade_id')
            candles=(impacted.groupBy('symbol','minute_start').agg(
                F.min_by('price',order).alias('open'),F.max('price').alias('high'),
                F.min('price').alias('low'),F.max_by('price',order).alias('close'),
                F.sum('quantity').alias('volume'),F.sum(F.col('price')*F.col('quantity')).alias('notional'),
                F.count('*').alias('trade_count'),F.max('event_time').alias('last_event_time'),
                F.max_by('trade_id',order).alias('last_trade_id'))
                .withColumn('minute_end',F.col('minute_start')+F.expr('INTERVAL 1 MINUTE'))
                .select(*CANDLE_FIELDS))
            candles.persist()
            try:
                assert_candles(candles)
                merge(spark,'lake.silver.candles',candles,['symbol','minute_start'])
                changed=keys.collect()
            finally: candles.unpersist()
        else: changed=[]
        with connect() as pg,pg.cursor() as cur:
            for row in changed:
                cur.execute("""INSERT INTO lakehouse_dirty(symbol,minute_start) VALUES (%s,%s)
                  ON CONFLICT(symbol,minute_start) DO UPDATE SET changed_at=now()""",(row.symbol,utc(row.minute_start)))
            for key,etag,m in manifests:
                cur.execute("""INSERT INTO lakehouse_files(manifest_key,etag,parquet_key,row_count)
                 VALUES (%s,%s,%s,%s) ON CONFLICT(manifest_key) DO UPDATE SET
                 etag=EXCLUDED.etag,processed_at=now()""",(key,etag,m['parquet_key'],m['row_count']))
        return {'processed_files':len(pending),'affected_minutes':len(changed),'pending':len(bronze_inventory())}
    finally: clean.unpersist()

MINUTE_SQL="""
WITH ordered AS (
 SELECT *,lag(close) OVER w AS prev_close,lag(minute_start) OVER w AS prev_minute,
 count(*) OVER w20 AS n20,avg(close) OVER w20 AS a20,
 count(*) OVER w50 AS n50,avg(close) OVER w50 AS a50,
 cast(sum(notional) OVER d AS DOUBLE)/nullif(cast(sum(volume) OVER d AS DOUBLE),0) AS vwap_day
 FROM metric_input
 WINDOW w AS (PARTITION BY symbol ORDER BY minute_start),
 w20 AS (PARTITION BY symbol ORDER BY minute_start ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
 w50 AS (PARTITION BY symbol ORDER BY minute_start ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),
 d AS (PARTITION BY symbol,to_date(minute_start) ORDER BY minute_start
       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT c.symbol,c.minute_start,cast(c.close AS DOUBLE) close,c.last_event_time,
 CASE WHEN n20=20 THEN cast(a20 AS DOUBLE) END ma20,
 CASE WHEN n50=50 THEN cast(a50 AS DOUBLE) END ma50,
 CASE WHEN c.minute_start-prev_minute=INTERVAL 1 MINUTE THEN ln(c.close/prev_close) END log_return,
 cast(c.vwap_day AS DOUBLE) vwap_day,cast(r.close AS DOUBLE) reference_price,
 r.last_event_time AS reference_time,cast(c.close/r.close-1 AS DECIMAL(38,18)) AS change_5m,
 current_timestamp() AS metric_updated_at
FROM ordered c LEFT JOIN metric_input r
 ON c.symbol=r.symbol AND r.minute_start=c.minute_start-INTERVAL 5 MINUTES
"""

def daily_sql(relevant_dates,target_dates):
    return f"""
      WITH stats AS (
       SELECT c.symbol,to_date(c.minute_start) day,max_by(cast(c.close AS DOUBLE),c.minute_start) close,
        cast(sum(c.notional) AS DOUBLE)/nullif(cast(sum(c.volume) AS DOUBLE),0) vwap,count(*) minutes,
        CASE WHEN count(m.log_return)>=2 THEN stddev_samp(m.log_return) END daily_volatility
       FROM lake.silver.candles c JOIN lake.gold.minute_metrics m
        ON c.symbol=m.symbol AND c.minute_start=m.minute_start
       WHERE to_date(c.minute_start) IN ({relevant_dates})
       GROUP BY c.symbol,to_date(c.minute_start)
      )
      SELECT a.symbol,a.day,a.close,a.close/b.close-1 daily_return,
       a.daily_volatility,a.vwap,a.minutes,a.day<current_date() is_complete
      FROM stats a LEFT JOIN stats b ON a.symbol=b.symbol AND b.day=date_sub(a.day,1)
      WHERE a.day IN ({target_dates})
    """


def gold(spark):
    cutoff=datetime.now(timezone.utc)-timedelta(minutes=3)
    with connect() as pg,pg.cursor() as cur:
        cur.execute('SELECT symbol,min(minute_start),max(minute_start) FROM lakehouse_dirty WHERE minute_start<=%s GROUP BY symbol',(cutoff,))
        dirty=cur.fetchall()
        cur.execute('SELECT formula_version FROM gold_serving_publication WHERE id=1')
        version=cur.fetchone()
    if not version or version[0]!=FORMULA_VERSION:
        # One controlled historical repair when the calculation contract changes.
        dirty=[tuple(r) for r in spark.table('lake.silver.candles').groupBy('symbol').agg(
            F.min('minute_start'),F.max('minute_start')).collect()]
    if not dirty:
        with connect() as pg,pg.cursor() as cur:
            cur.execute('SELECT snapshots FROM lakehouse_publication WHERE id=1')
            publication=cur.fetchone()
            served=publish_serving(spark,publication[0],cur) if publication else {}
        return {'updated_minutes':0,'reason':'no eligible dirty minutes','postgres':served}
    start=datetime.now(timezone.utc)
    bounds=[(s,a.replace(hour=0,minute=0,second=0,microsecond=0,tzinfo=None),
             b.replace(hour=0,minute=0,second=0,microsecond=0,tzinfo=None)+timedelta(days=1))
            for s,a,b in dirty]
    bounds_df=spark.createDataFrame(bounds,'symbol string,lo timestamp,hi timestamp')
    source=(spark.table('lake.silver.candles')
             .filter(F.col('minute_end')<=F.current_timestamp()-F.expr('INTERVAL 2 MINUTES')))
    joined=source.join(bounds_df,'symbol')
    before=(joined.filter('minute_start<lo').withColumn('_rn',F.row_number().over(
             Window.partitionBy('symbol').orderBy(F.col('minute_start').desc()))).filter('_rn<=49').drop('_rn'))
    inside=joined.filter('minute_start>=lo AND minute_start<hi')
    after=(joined.filter('minute_start>=hi').withColumn('_rn',F.row_number().over(
            Window.partitionBy('symbol').orderBy('minute_start'))).filter('_rn<=49').drop('_rn'))
    metric_input=before.unionByName(inside).unionByName(after)
    metric_input.createOrReplaceTempView('metric_input')
    metrics=spark.sql(MINUTE_SQL).join(bounds_df.select('symbol','lo'),'symbol').filter('minute_start>=lo').drop('lo')
    metrics=metrics.select(*spark.table('lake.gold.minute_metrics').columns)
    metrics.persist()
    try:
        count=metrics.count()
        if count==0: return {'updated_minutes':0}
        fields=[c for c in metrics.columns if c not in ('symbol','minute_start','metric_updated_at')]
        changed='NOT ('+' AND '.join(f't.{c} <=> s.{c}' for c in fields)+')'
        merge(spark,'lake.gold.minute_metrics',metrics,['symbol','minute_start'],changed)
        touched=[r[0] for r in metrics.select(F.to_date('minute_start')).distinct().collect()]
    finally: metrics.unpersist()
    targets=sorted({day+timedelta(days=d) for day in touched for d in (-1,0,1)})
    relevant=sorted(set(targets)|{d-timedelta(days=1) for d in targets})
    target_dates=','.join("DATE '"+d.isoformat()+"'" for d in targets)
    relevant_dates=','.join("DATE '"+d.isoformat()+"'" for d in relevant)
    # Daily statistics read only relevant partitions, plus the previous day for return.
    daily=spark.sql(daily_sql(relevant_dates,target_dates))
    merge(spark,'lake.gold.daily_metrics',daily,['symbol','day'])
    date_list=','.join("DATE '"+d.isoformat()+"'" for d in touched)
    pairs=spark.sql(f"""
      SELECT a.symbol symbol_a,b.symbol symbol_b,to_date(a.minute_start) day,
       count(*) n,sum(a.log_return) sx,sum(b.log_return) sy,
       sum(a.log_return*a.log_return) sxx,sum(b.log_return*b.log_return) syy,
       sum(a.log_return*b.log_return) sxy
      FROM lake.gold.minute_metrics a JOIN lake.gold.minute_metrics b
       ON a.minute_start=b.minute_start AND a.symbol<b.symbol
      WHERE a.log_return IS NOT NULL AND b.log_return IS NOT NULL
       AND to_date(a.minute_start) IN ({date_list})
      GROUP BY a.symbol,b.symbol,to_date(a.minute_start)
    """)
    merge(spark,'lake.gold.correlation_daily',pairs,['symbol_a','symbol_b','day'])
    correlations=spark.sql("""
      WITH sums AS (SELECT symbol_a,symbol_b,sum(n) n,sum(sx) sx,sum(sy) sy,
       sum(sxx) sxx,sum(syy) syy,sum(sxy) sxy FROM lake.gold.correlation_daily GROUP BY symbol_a,symbol_b)
      SELECT symbol_a,symbol_b,CASE WHEN n>=3 AND n*sxx-sx*sx>0 AND n*syy-sy*sy>0
       THEN (n*sxy-sx*sy)/sqrt((n*sxx-sx*sx)*(n*syy-sy*sy)) END correlation,n observations FROM sums
    """)
    merge(spark,'lake.gold.correlations',correlations,['symbol_a','symbol_b'])
    if not spark.table('lake.gold.minute_metrics').filter('close IS NULL OR vwap_day<=0').limit(1).isEmpty():
        raise ValueError('Gold quality failed')
    snapshots={}
    for table in ('minute_metrics','daily_metrics','correlations'):
        latest=spark.sql(f'SELECT snapshot_id FROM lake.gold.{table}.snapshots ORDER BY committed_at DESC LIMIT 1').collect()
        snapshots[table]=str(latest[0][0]) if latest else None
    with connect() as pg,pg.cursor() as cur:
        served=publish_serving(spark,snapshots,cur)
        cur.execute('INSERT INTO lakehouse_publication(id,snapshots) VALUES (1,%s) ON CONFLICT(id) DO UPDATE SET snapshots=EXCLUDED.snapshots,published_at=now()',(Json(snapshots),))
        cur.execute('DELETE FROM lakehouse_dirty WHERE changed_at<=%s AND minute_start<=%s',(start,cutoff))
    return {'updated_minutes':count,'days':[d.isoformat() for d in touched],'postgres':served}

def observations(spark,since):
    stamp=datetime.fromisoformat(since.replace('Z','+00:00'))
    data=spark.table('lake.gold.minute_metrics')
    with connect() as pg,pg.cursor() as cur:
        cur.execute('SELECT snapshots FROM lakehouse_publication WHERE id=1')
        published=cur.fetchone()
    if not published or not published[0].get('minute_metrics'):
        return {'snapshot':None,'observations':[]}
    snapshot=[(published[0]['minute_metrics'],)]
    if snapshot:
        data=spark.read.option('snapshot-id',str(snapshot[0][0])).table('lake.gold.minute_metrics')
    rows=data.filter(F.col('metric_updated_at')>=F.lit(stamp)).orderBy('symbol','minute_start').collect()
    return {'snapshot':str(snapshot[0][0]) if snapshot else None,'observations':[r.asDict() for r in rows]}

def overview(spark,symbol):
    if symbol not in SYMBOLS: raise ValueError('Unknown symbol')
    with connect() as pg,pg.cursor() as cur:
        cur.execute('SELECT snapshots,published_at FROM lakehouse_publication WHERE id=1')
        publication=cur.fetchone()
    if not publication:
        return {'symbol':symbol,'minute':[],'daily':[],'correlations':[],'source':'Iceberg Gold','published_at':None}
    snapshots,published_at=publication
    def read(table):
        snapshot=snapshots.get(table.split('.')[-1])
        return spark.read.option('snapshot-id',snapshot).table(table) if snapshot else spark.table(table).limit(0)
    def records(table,condition,order,limit):
        return [r.asDict() for r in read(table).filter(condition).orderBy(F.desc(order)).limit(limit).collect()]
    return {
      'symbol':symbol,
      'minute':records('lake.gold.minute_metrics',F.col('symbol')==symbol,'minute_start',1440),
      'daily':records('lake.gold.daily_metrics',F.col('symbol')==symbol,'day',30),
      'correlations':[r.asDict() for r in read('lake.gold.correlations')
         .filter((F.col('symbol_a')==symbol)|(F.col('symbol_b')==symbol)).collect()],
      'source':'Iceberg Gold / Spark SQL','published_at':published_at
    }
