"""Separate durable queries avoid chaining two stateful operators in update mode."""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.streaming import StreamingQueryListener
from market import SYMBOLS
from serving import publish_candles

STATE = os.getenv('STATE_DIR', '/state') + '/v2'
WATERMARK = '2 minutes'


class Metrics(StreamingQueryListener):
    def onQueryStarted(self, event):
        pass

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        print(f'QUERY_TERMINATED id={event.id} exception={event.exception}', flush=True)

    def onQueryProgress(self, event):
        data = json.loads(event.progress.json)
        target = Path(STATE, 'metrics')
        target.mkdir(exist_ok=True)
        (target / f'{data["name"]}.json').write_text(json.dumps(data, indent=2))


def session():
    return (SparkSession.builder.appName('finance-streaming')
            .config('spark.sql.session.timeZone','UTC').getOrCreate())


def parse(kafka):
    schema = T.StructType([
        T.StructField('trade_id', T.StringType()), T.StructField('symbol', T.StringType()),
        T.StructField('price', T.DecimalType(18, 4)), T.StructField('quantity', T.LongType()),
        T.StructField('volume', T.LongType()), T.StructField('event_time', T.StringType()),
    ])
    df = (kafka.selectExpr('CAST(value AS STRING) raw', 'CAST(key AS STRING) kafka_key',
                           'topic', 'partition', 'offset', 'timestamp AS kafka_time')
          .withColumn('t', F.from_json('raw', schema)).select('*', 't.*').drop('t')
          .withColumn('quantity', F.coalesce('quantity', 'volume')).drop('volume')
          .withColumn('event_time_text', F.col('event_time'))
          .withColumn('event_time', F.to_timestamp('event_time')))
    valid = (F.col('trade_id').rlike('^[a-zA-Z0-9_-]{1,128}$')
             & F.col('symbol').isin(list(SYMBOLS)) & (F.col('kafka_key') == F.col('symbol'))
             & (F.col('price') > 0) & (F.col('quantity') > 0)
             & F.col('event_time').isNotNull()
             & F.col('event_time_text').rlike('(Z|[+-][0-9]{2}:[0-9]{2})$')
             & (F.col('event_time') <= F.col('kafka_time') + F.expr('INTERVAL 30 SECONDS')))
    return df.withColumn('valid', F.coalesce(valid, F.lit(False)))


def aggregate(valid):
    order = F.struct('event_time', 'trade_id')
    return (valid.withWatermark('event_time', WATERMARK)
            .groupBy('symbol', F.window('event_time', '1 minute'))
            .agg(F.min_by('price', order).alias('open'), F.max('price').alias('high'),
                 F.min('price').alias('low'), F.max_by('price', order).alias('close'),
                 F.sum('quantity').alias('volume'),
                 F.sum(F.col('price') * F.col('quantity')).alias('notional'),
                 F.count('*').alias('trade_count'),
                 F.max('event_time').alias('last_event_time'),
                 F.max_by('trade_id', order).alias('last_trade_id'))
            .select('symbol', F.col('window.start').alias('minute_start'),
                    F.col('window.end').alias('minute_end'), 'open', 'high', 'low', 'close',
                    'volume', 'notional', 'trade_count', 'last_event_time', 'last_trade_id'))



def main():
    Path(STATE).mkdir(parents=True,exist_ok=True)
    Path(STATE, 'heartbeat').unlink(missing_ok=True)
    spark = session()
    spark.sparkContext.setLogLevel('WARN')
    spark.streams.addListener(Metrics())
    raw = (spark.readStream.format('kafka')
           .option('kafka.bootstrap.servers', os.environ['KAFKA_BOOTSTRAP_SERVERS'])
           .option('subscribe', 'trades').option('startingOffsets', 'earliest')
           .option('maxOffsetsPerTrigger', os.getenv('SPARK_MAX_OFFSETS', '50000'))
           .option('failOnDataLoss', 'true').load())
    parsed = parse(raw)
    queries = []
    queries.append(parsed.filter('NOT valid').withColumn('reason', F.lit('invalid_contract'))
                   .writeStream.queryName('rejected').format('parquet').option('path', f'{STATE}/rejected')
                   .option('checkpointLocation', f'{STATE}/checkpoints/rejected')
                   .trigger(processingTime='5 seconds').start())
    clean = (parsed.filter('valid').select('trade_id', 'symbol', 'price', 'quantity', 'event_time')
             .withWatermark('event_time', WATERMARK).dropDuplicatesWithinWatermark(['trade_id']))
    queries.append(clean.writeStream.queryName('validated').format('parquet').option('path', f'{STATE}/validated')
                   .option('checkpointLocation', f'{STATE}/checkpoints/validated')
                   .trigger(processingTime='5 seconds').start())
    # Establish the schema even before the first committed parquet file exists.
    Path(STATE, 'validated').mkdir(exist_ok=True)
    validated = spark.readStream.schema(clean.schema).parquet(f'{STATE}/validated')

    def live(batch, batch_id):
        publish_candles([r.asDict() for r in batch.collect()], finalized=False)

    def finalized(batch, batch_id):
        publish_candles([r.asDict() for r in batch.collect()], finalized=True)

    queries.append(aggregate(validated).writeStream.queryName('live').outputMode('update').foreachBatch(live)
                   .option('checkpointLocation', f'{STATE}/checkpoints/live')
                   .trigger(processingTime='5 seconds').start())
    queries.append(aggregate(validated).writeStream.queryName('final-postgres').outputMode('append').foreachBatch(finalized)
                   .option('checkpointLocation', f'{STATE}/checkpoints/final-postgres')
                   .trigger(processingTime='10 seconds').start())
    class Health(BaseHTTPRequestHandler):
        def log_message(self,*args): pass
        def do_GET(self):
            active=all(q.isActive and q.exception() is None for q in queries)
            body=json.dumps({'active':active,'queries':[q.name for q in queries]}).encode()
            self.send_response(200 if active else 503)
            self.send_header('Content-Type','application/json')
            self.end_headers()
            self.wfile.write(body)
    server=ThreadingHTTPServer(('0.0.0.0',8091),Health)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    Path(STATE, 'started').touch()
    while True:
        for query in queries:
            if query.exception():
                raise query.exception()
            if not query.isActive:
                raise RuntimeError('Une requête streaming est arrêtée')
        Path(STATE, 'heartbeat').touch()
        time.sleep(5)


if __name__ == '__main__':
    main()
