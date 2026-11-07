"""Finite Dagster assets; continuous services are managed by Compose, never held as infinite runs."""
import json
import os
from urllib.request import Request,urlopen
from dagster import (Definitions,asset,define_asset_job,AssetSelection,ScheduleDefinition,
                     DefaultScheduleStatus,RetryPolicy)
from confluent_kafka.admin import AdminClient
from serving import connect
from alerting import evaluate

def task(context,name):
    url=os.getenv('LAKEHOUSE_URL','http://lakehouse:8090')+'/tasks/'+name
    request=Request(url,data=json.dumps({'request_id':context.run_id}).encode(),
                    headers={'Content-Type':'application/json'},method='POST')
    with urlopen(request,timeout=1800) as response: result=json.load(response)
    context.log.info(json.dumps(result))
    return result

@asset(group_name='streaming',description='Source continue Python, gérée par Compose, observée via ses événements')
def producer():
    with connect() as pg,pg.cursor() as cur:
        cur.execute('SELECT count(*),max(event_time) FROM latest_prices')
        count,event=cur.fetchone()
    return {'observed_symbols':count,'latest_event':str(event)}

@asset(group_name='streaming',deps=[producer])
def kafka():
    topic=AdminClient({'bootstrap.servers':os.environ['KAFKA_BOOTSTRAP_SERVERS']}).list_topics('trades',timeout=15).topics['trades']
    if topic.error or len(topic.partitions)!=6: raise RuntimeError('Kafka trades must have six partitions')
    return {'partitions':6}

@asset(group_name='streaming',deps=[kafka])
def streaming_candles():
    with urlopen(os.getenv('STREAMING_URL','http://spark:8091')+'/health',timeout=15) as response:
        result=json.load(response)
    if not result['active']: raise RuntimeError('Streaming queries inactive')
    return result

@asset(group_name='streaming',deps=[streaming_candles])
def postgres_candles():
    with connect() as pg,pg.cursor() as cur:
        cur.execute('SELECT count(*),max(minute_start) FROM candles')
        count,latest=cur.fetchone()
    if not count: raise RuntimeError('No streaming candles')
    return {'rows':count,'latest_minute':str(latest)}

@asset(group_name='lakehouse',retry_policy=RetryPolicy(max_retries=2,delay=30))
def bronze(context):
    return task(context,'bronze')

@asset(group_name='lakehouse',deps=[bronze],retry_policy=RetryPolicy(max_retries=2,delay=30))
def silver(context):
    return task(context,'silver')

@asset(group_name='lakehouse',deps=[silver],retry_policy=RetryPolicy(max_retries=2,delay=30))
def gold(context):
    return task(context,'gold')

@asset(group_name='lakehouse',deps=[gold],retry_policy=RetryPolicy(max_retries=2,delay=30))
def alerts(context):
    result=evaluate()
    context.log.info(json.dumps(result))
    return result

streaming_job=define_asset_job('streaming_supervision',selection=AssetSelection.groups('streaming'))
lakehouse_job=define_asset_job('lakehouse_refresh',selection=AssetSelection.groups('lakehouse'))
alerts_job=define_asset_job('alerts_refresh',selection=AssetSelection.assets(alerts))
defs=Definitions(assets=[producer,kafka,streaming_candles,postgres_candles,bronze,silver,gold,alerts],
 jobs=[streaming_job,lakehouse_job,alerts_job],schedules=[
  ScheduleDefinition(job=lakehouse_job,cron_schedule='*/5 * * * *',execution_timezone='UTC',
    name='lakehouse_and_alerts_every_five_minutes',default_status=DefaultScheduleStatus.RUNNING),
  ScheduleDefinition(job=streaming_job,cron_schedule='*/5 * * * *',execution_timezone='UTC',
    name='independent_streaming_supervision',default_status=DefaultScheduleStatus.RUNNING),
  ScheduleDefinition(job=lakehouse_job,cron_schedule='7 0 * * *',execution_timezone='UTC',
    name='end_of_day',default_status=DefaultScheduleStatus.RUNNING)])
