"""Real Structured Streaming: known OHLCV, lateness, duplicate and checkpoint replay."""
import json
import sys
import tempfile
from pathlib import Path
from decimal import Decimal
from datetime import datetime
sys.path.insert(0, '/opt/finance')
repo = Path(__file__).resolve().parents[1]
sys.path.extend([str(repo/'spark'),str(repo/'app')])
from pyspark.sql import SparkSession, types as T
from streaming import aggregate, parse

spark = (SparkSession.builder.appName('finance-offline-tests').master('local[2]')
         .config('spark.sql.shuffle.partitions','2')
         .config('spark.sql.session.timeZone','UTC').getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
contract = dict(trade_id='contract-valid',symbol='AAPL',price=100,quantity=2,
                event_time='2026-10-26T12:00:00Z')
inputs = [contract, dict(contract,quantity=0),dict(contract,event_time='invalid'),
          dict(contract,symbol='UNKNOWN'),dict(contract,event_time='2026-10-26T13:00:00Z')]
legacy = dict(contract)
legacy['volume']=legacy.pop('quantity')
inputs.append(legacy)
raw_schema = 'value binary,key binary,topic string,partition int,offset long,timestamp timestamp'
raw = spark.createDataFrame([(json.dumps(e).encode(),e['symbol'].encode(),'trades',0,i,
                              datetime(2026,10,26,12)) for i,e in enumerate(inputs)],raw_schema)
assert parse(raw).filter('valid').count()==2,'Input validation or legacy quantity alias failed'
schema = T.StructType([
    T.StructField('trade_id',T.StringType()),T.StructField('symbol',T.StringType()),
    T.StructField('price',T.DecimalType(18,4)),T.StructField('quantity',T.LongType()),
    T.StructField('event_time',T.TimestampType())])

with tempfile.TemporaryDirectory() as folder:
    root = Path(folder)
    source,stage = root/'input',root/'stage'
    source.mkdir(); stage.mkdir()
    updates,finals = {},{}

    def collect_into(destination):
        def sink(batch,batch_id):
            for row in batch.collect():
                destination[row.symbol,row.minute_start] = row.asDict()
        return sink

    def start():
        stream = spark.readStream.schema(schema).json(str(source))
        q1 = (stream.withWatermark('event_time','2 minutes')
              .dropDuplicatesWithinWatermark(['trade_id']).writeStream.format('parquet')
              .option('path',str(stage)).option('checkpointLocation',str(root/'cp-dedup')).start())
        cleaned = spark.readStream.schema(schema).parquet(str(stage))
        q2 = (aggregate(cleaned).writeStream.outputMode('update').foreachBatch(collect_into(updates))
              .option('checkpointLocation',str(root/'cp-live')).start())
        q3 = (aggregate(cleaned).writeStream.outputMode('append').foreachBatch(collect_into(finals))
              .option('checkpointLocation',str(root/'cp-final')).start())
        return [q1,q2,q3]

    def event(identifier,minute,second,price,quantity):
        return dict(trade_id=identifier,symbol='AAPL',price=price,quantity=quantity,
                    event_time=f'2026-10-26T12:{minute:02d}:{second:02d}Z')

    counter = 0
    def push(events,queries):
        global counter
        (source/f'{counter}.json').write_text('\n'.join(json.dumps(e) for e in events))
        counter+=1
        for query in queries:
            query.processAllAvailable()

    queries = start()
    try:
        z = event('z',0,40,103,2)
        push([event('a',0,0,100,4),z],queries)
        push([event('b',0,0,101,3),event('c',0,20,98,1),z],queries)
        first = min(updates.values(),key=lambda r:r['minute_start'])
        assert [first[k] for k in ('open','high','low','close','volume','notional','trade_count')] == [100,103,98,103,10,Decimal('1007'),4],first
        push([event('advance',4,0,110,1)],queries)
        assert len(finals)==1,finals
        final = list(finals.values())[0]
        assert final['volume']==10 and final['notional']==1007
        push([event('too-late',0,30,1000,999)],queries)
        assert list(finals.values())[0]==final
        for query in queries:
            query.stop()
        queries = start()
        push([z,event('after-restart',5,0,111,1)],queries)
        assert list(finals.values())[0]==final
        print('SPARK_TEST_OK OHLCV=100/103/98/103 volume=10 notional=1007; late, duplicate, checkpoints verified')
    finally:
        for query in queries:
            query.stop()
spark.stop()
