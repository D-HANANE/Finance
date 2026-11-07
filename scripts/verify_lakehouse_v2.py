"""Read-only integration evidence from real Bronze-derived Silver and published Gold."""
import os
import sys
import json
from urllib.request import urlopen
import psycopg2
with psycopg2.connect(os.environ['PG_DSN']) as pg,pg.cursor() as cur:
    cur.execute('SELECT count(*) FROM lakehouse_files')
    archived=cur.fetchone()[0]
    assert archived>0,'No Bronze manifest processed'
    cur.execute('SELECT snapshots FROM lakehouse_publication WHERE id=1')
    publication=cur.fetchone()
    assert publication and publication[0].get('minute_metrics'),'No published Iceberg Gold snapshot'
    cur.execute('SELECT status,count(*) FROM alerts GROUP BY status')
    states=cur.fetchall()
with urlopen(os.getenv('LAKEHOUSE_URL','http://lakehouse:8090')+'/gold/overview?symbol=AAPL',timeout=120) as response:
    gold=json.load(response)
assert gold['minute'],'No Gold minutes'
assert gold['daily'],'No Gold daily rows'
print(json.dumps({'processed_manifests':archived,'gold_minute_snapshot':publication[0]['minute_metrics'],
                  'aapl_minute_rows':len(gold['minute']),'daily_rows':len(gold['daily']),
                  'alert_states':states,'source':gold['source']}))
print('LAKEHOUSE_V2_OK')
