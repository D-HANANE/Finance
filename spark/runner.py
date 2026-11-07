"""Internal bounded-task API. A single Spark session serves analytical jobs and Gold reads."""
import json
import os
import threading
import traceback
from datetime import datetime,date,timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from urllib.parse import urlparse,parse_qs
from psycopg2.extras import Json
from catalog import session
from serving import connect
import lakehouse

def encode(value):
    if isinstance(value,datetime):
        return value.replace(tzinfo=timezone.utc).isoformat() if value.tzinfo is None else value.isoformat()
    if isinstance(value,date): return value.isoformat()
    if isinstance(value,Decimal): return str(value)
    raise TypeError(type(value).__name__)

spark=session()
spark.sparkContext.setLogLevel('WARN')
lakehouse.initialize(spark)
mutex=threading.RLock()
read_mutex=threading.Lock()

class Handler(BaseHTTPRequestHandler):
    def log_message(self,*args): pass
    def send(self,status,data):
        body=json.dumps(data,default=encode).encode()
        self.send_response(status); self.send_header('Content-Type','application/json')
        self.send_header('Content-Length',str(len(body))); self.end_headers()
        try: self.wfile.write(body)
        except (BrokenPipeError,ConnectionResetError): pass
    def do_GET(self):
        route=urlparse(self.path); args=parse_qs(route.query)
        try:
            if route.path=='/health': return self.send(200,{'status':'ready','catalog':'jdbc'})
            # Gold readers use published immutable snapshots and need not wait for a batch.
            with read_mutex:
                if route.path=='/gold/overview':
                    result=lakehouse.overview(spark,args.get('symbol',['AAPL'])[0])
                elif route.path=='/gold/observations':
                    result=lakehouse.observations(spark,args.get('since',['1970-01-01T00:00:00+00:00'])[0])
                else: return self.send(404,{'error':'unknown route'})
            self.send(200,result)
        except Exception as error:
            traceback.print_exc(); self.send(500,{'error':str(error)})
    def do_POST(self):
        stage=urlparse(self.path).path[len('/tasks/'): ]
        if stage not in ('bronze','silver','gold'): return self.send(404,{'error':'unknown task'})
        body=json.loads(self.rfile.read(int(self.headers.get('Content-Length',0))) or '{}')
        request_id=str(body.get('request_id','manual-'+str(datetime.now().timestamp())))+':'+stage
        with mutex,connect() as guard,guard.cursor() as cur:
            cur.execute('SELECT pg_try_advisory_lock(487205)')
            if not cur.fetchone()[0]: return self.send(409,{'error':'another analytical writer is running'})
            try:
                cur.execute('SELECT status,result FROM lakehouse_runs WHERE request_id=%s',(request_id,))
                previous=cur.fetchone()
                if previous and previous[0]=='passed': return self.send(200,previous[1])
                cur.execute("""INSERT INTO lakehouse_runs(request_id,stage,status) VALUES (%s,%s,'running')
                 ON CONFLICT(request_id) DO UPDATE SET status='running',error=NULL""",(request_id,stage))
                guard.commit()
                try:
                    if stage=='bronze': result={'pending_files':len(lakehouse.bronze_inventory())}
                    else: result=getattr(lakehouse,stage)(spark)
                    cur.execute("UPDATE lakehouse_runs SET status='passed',completed_at=now(),result=%s WHERE request_id=%s",(Json(result),request_id))
                    guard.commit()
                    return self.send(200,result)
                except Exception as error:
                    guard.rollback()
                    cur.execute("UPDATE lakehouse_runs SET status='failed',completed_at=now(),error=%s WHERE request_id=%s",(str(error),request_id))
                    guard.commit()
                    traceback.print_exc()
                    return self.send(500,{'error':str(error),'stage':stage})
            finally:
                cur.execute('SELECT pg_advisory_unlock(487205)')
                guard.commit()

ThreadingHTTPServer(('0.0.0.0',8090),Handler).serve_forever()
