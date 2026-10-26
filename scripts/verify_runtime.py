"""Read-only evidence collector. Run in producer with /checks bind mounted."""
import io
import json
import os
import sys
import time
import psycopg2
import pyarrow.parquet as pq
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient
sys.path.insert(0,'/app')
from storage import connect_s3

admin = AdminClient({'bootstrap.servers':os.environ['KAFKA_BOOTSTRAP_SERVERS']})
topic = admin.list_topics('trades',timeout=20).topics['trades']
assert len(topic.partitions)==6
consumer = Consumer({'bootstrap.servers':os.environ['KAFKA_BOOTSTRAP_SERVERS'],
                     'group.id':f'verify-{time.time_ns()}', 'auto.offset.reset':'latest',
                     'enable.auto.commit':False})
consumer.subscribe(['trades'])
symbols, partitions, count = set(),set(),0
assigned = False
started = time.monotonic()
while time.monotonic()-started<25:
    msg = consumer.poll(1)
    if msg is None:
        continue
    if msg.error():
        raise RuntimeError(msg.error())
    if not assigned:
        assigned = True
        started = time.monotonic()
    event = json.loads(msg.value())
    assert msg.key().decode()==event['symbol']
    symbols.add(event['symbol']); partitions.add(msg.partition()); count+=1
consumer.close()
assert len(symbols)==20, symbols
print(json.dumps({'topic_partitions':6,'symbols':len(symbols),'sample_partitions':sorted(partitions),
                  'received':count,'received_per_second':round(count/(time.monotonic()-started),2)}))
s3 = connect_s3()
objects = s3.list_objects_v2(Bucket='bronze',Prefix='manifests/trades/').get('Contents',[])
assert objects,'No completed Bronze batch'
manifest = json.loads(s3.get_object(Bucket='bronze',Key=objects[-1]['Key'])['Body'].read())
table = pq.read_table(io.BytesIO(s3.get_object(Bucket='bronze',Key=manifest['parquet_key'])['Body'].read()))
assert table.num_rows==manifest['row_count']
print('Bronze manifest and Parquet rows:',table.num_rows)
with psycopg2.connect(os.environ['PG_DSN']) as connection,connection.cursor() as cursor:
    for name in ('candles','latest_prices','alerts','gold_daily','pipeline_runs'):
        cursor.execute(f'SELECT count(*) FROM {name}')
        print(name,cursor.fetchone()[0])
    cursor.execute('SELECT count(*) FROM candles WHERE is_final')
    assert cursor.fetchone()[0]>0,'No finalized Silver candles published'
    cursor.execute('SELECT count(*) FROM candles WHERE low>open OR open>high OR low>close OR close>high OR volume<0')
    assert cursor.fetchone()[0]==0
print('Runtime sampling passed; Kafka delivery rate must ALSO be checked in producer logs.')
