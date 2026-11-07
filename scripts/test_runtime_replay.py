"""Real Gold API and PostgreSQL replay, without injecting financial observations."""
import os,json,sys
from urllib.request import Request,urlopen
sys.path.insert(0,'/app')
from alerting import evaluate
from serving import connect

first=evaluate()
with connect() as pg,pg.cursor() as cur:
    cur.execute('SELECT count(*) FROM alert_history')
    before=cur.fetchone()[0]
second=evaluate()
with connect() as pg,pg.cursor() as cur:
    cur.execute('SELECT count(*) FROM alert_history')
    assert cur.fetchone()[0]==before,'Replayed Gold observations duplicated alert history'
    cur.execute("SELECT symbol,alert_type,opened_at,count(*) FROM alerts GROUP BY symbol,alert_type,opened_at HAVING count(*)>1")
    assert not cur.fetchall(),'Duplicate alert episodes'
url=os.getenv('LAKEHOUSE_URL','http://lakehouse:8090')+'/tasks/bronze'
body=json.dumps({'request_id':'validation-v2-replay'}).encode()
def replay():
    with urlopen(Request(url,data=body,headers={'Content-Type':'application/json'}),timeout=120) as result:
        return json.load(result)
assert replay()==replay(),'Same request ID changed its committed result'
with connect() as pg,pg.cursor() as cur:
    cur.execute("SELECT count(*) FROM lakehouse_runs WHERE request_id='validation-v2-replay:bronze' AND status='passed'")
    assert cur.fetchone()[0]==1
print('RUNTIME_REPLAY_OK '+json.dumps({'first':first,'second':second,'history_rows':before}))
