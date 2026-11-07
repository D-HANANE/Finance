"""Read-only transaction trace; replay republishes exactly the captured event, only on explicit command."""
import argparse,base64,hashlib,json,os,time,uuid,sys
sys.path.insert(0,'/app')
from datetime import datetime,timezone

def emit(value):
    print(json.dumps(value,default=str,ensure_ascii=False,indent=2))

def load(path):
    with open(path,encoding='utf-8-sig') as source: trace=json.load(source)
    raw=base64.b64decode(trace['raw_base64'],validate=True)
    if hashlib.sha256(raw).hexdigest()!=trace['sha256']: raise ValueError('Payload checksum mismatch')
    event=json.loads(raw)
    if event['trade_id']!=trace['event']['trade_id'] or event['symbol']!=trace['key']:
        raise ValueError('Trade identity or Kafka key mismatch')
    return trace,raw

def consumer():
    from confluent_kafka import Consumer
    return Consumer({'bootstrap.servers':os.environ['KAFKA_BOOTSTRAP_SERVERS'],
      'group.id':'demo-readonly-'+uuid.uuid4().hex,'enable.auto.commit':False,
      'enable.auto.offset.store':False,'auto.offset.reset':'latest'})

def capture(symbol,timeout):
    from confluent_kafka import TopicPartition,OFFSET_END
    client=consumer();topic=os.getenv('KAFKA_TOPIC','trades')
    try:
        metadata=client.list_topics(topic,timeout=15).topics[topic]
        if metadata.error: raise RuntimeError(str(metadata.error))
        client.assign([TopicPartition(topic,p,OFFSET_END) for p in metadata.partitions])
        deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            message=client.poll(1)
            if message is None: continue
            if message.error(): raise RuntimeError(str(message.error()))
            raw=message.value();event=json.loads(raw)
            if event.get('symbol')!=symbol: continue
            key=message.key().decode()
            if key!=symbol: raise ValueError('Kafka key differs from symbol')
            return dict(captured_at=datetime.now(timezone.utc).isoformat(),topic=message.topic(),
              partition=message.partition(),offset=message.offset(),key=key,
              kafka_timestamp_ms=message.timestamp()[1],event=event,
              raw_base64=base64.b64encode(raw).decode(),sha256=hashlib.sha256(raw).hexdigest())
        raise TimeoutError('No new event for '+symbol+'; verify Producer and topic')
    finally: client.close()

def inspect(trace,max_files):
    import io
    import psycopg2
    import pyarrow.parquet as pq
    import pyarrow.compute as pc
    from storage import connect_s3
    from confluent_kafka import TopicPartition
    result={'trade_id':trace['event']['trade_id'],'symbol':trace['key'],'kafka':{},'bronze_matches':[]}
    client=consumer()
    try:
        partition=TopicPartition(trace['topic'],trace['partition'])
        low,high=client.get_watermark_offsets(partition,timeout=10)
        result['kafka']['retained_range']=[low,high]
        if low<=trace['offset']<high:
            client.assign([TopicPartition(trace['topic'],trace['partition'],trace['offset'])])
            message=client.poll(10)
            result['kafka']['same_payload']=(message is not None and not message.error()
               and message.offset()==trace['offset'] and hashlib.sha256(message.value()).hexdigest()==trace['sha256'])
        else: result['kafka']['note']='Offset no longer retained; this alone does not prove archive loss'
    finally: client.close()
    s3=connect_s3();bucket=os.getenv('BRONZE_BUCKET','bronze')
    files=[]
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket,Prefix='manifests/trades/'):
        files.extend(page.get('Contents',[]))
    files=sorted(files,key=lambda item:item['LastModified'],reverse=True)[:max_files]
    result['searched_recent_manifests']=len(files)
    for item in files:
        manifest=json.loads(s3.get_object(Bucket=bucket,Key=item['Key'])['Body'].read())
        parquet=s3.get_object(Bucket=bucket,Key=manifest['parquet_key'])['Body'].read()
        table=pq.read_table(io.BytesIO(parquet),columns=['trade_id','symbol','price','quantity','event_time','_topic','_partition','_offset'])
        matches=table.filter(pc.equal(table['trade_id'],trace['event']['trade_id'])).to_pylist()
        for row in matches:
            result['bronze_matches'].append(dict(manifest_key=item['Key'],json_key=manifest['json_key'],
               parquet_key=manifest['parquet_key'],manifest_row_count=manifest['row_count'],row=row))
    event=trace['event'];stamp=datetime.fromisoformat(event['event_time'].replace('Z','+00:00'))
    minute=stamp.replace(second=0,microsecond=0)
    with psycopg2.connect(os.environ['PG_DSN']) as pg,pg.cursor() as cur:
        for name,sql in [
          ('streaming_candle','SELECT symbol,minute_start,open,high,low,close,volume,notional,trade_count,is_final FROM candles WHERE symbol=%s AND minute_start=%s'),
          ('gold_minute','SELECT symbol,minute_start,close,ma20,ma50,vwap_day,change_5m,source_snapshot,published_at FROM gold_minute WHERE symbol=%s AND minute_start=%s')]:
            cur.execute(sql,(event['symbol'],minute));row=cur.fetchone()
            result[name]=dict(zip([c[0] for c in cur.description],row)) if row else None
    result['interpretation']='Pending outputs are null until their own processing completes. Bronze search is bounded, not exhaustive.'
    return result

def replay(trace,raw):
    from confluent_kafka import Producer
    producer=Producer({'bootstrap.servers':os.environ['KAFKA_BOOTSTRAP_SERVERS'],'enable.idempotence':True,'acks':'all'})
    delivered=[];errors=[]
    def callback(error,message):
        if error: errors.append(str(error))
        else: delivered.append(dict(topic=message.topic(),partition=message.partition(),offset=message.offset()))
    producer.produce(trace['topic'],key=trace['key'],value=raw,on_delivery=callback)
    if producer.flush(20) or errors or not delivered: raise RuntimeError('Replay not confirmed: '+str(errors))
    return dict(replayed_trade_id=trace['event']['trade_id'],same_payload_sha256=trace['sha256'],
      kafka_delivery=delivered[0],note='One deliberate duplicate Kafka record; no new trade identity or fabricated price')

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    modes=parser.add_subparsers(dest='mode',required=True)
    p=modes.add_parser('capture');p.add_argument('--symbol',default='MSFT');p.add_argument('--timeout',type=int,default=60)
    p=modes.add_parser('inspect');p.add_argument('path');p.add_argument('--max-files',type=int,default=30)
    p=modes.add_parser('replay');p.add_argument('path')
    args=parser.parse_args()
    if args.mode=='capture': emit(capture(args.symbol,args.timeout));return
    trace,raw=load(args.path)
    emit(inspect(trace,args.max_files) if args.mode=='inspect' else replay(trace,raw))
if __name__=='__main__': main()
