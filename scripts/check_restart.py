"""Host-side restart test, keeps volumes and compares immutable finalized rows."""
import json
import subprocess
import time


def sql(statement):
    result = subprocess.run(['docker','compose','exec','-T','postgres','psql','-U','finance',
                             '-d','finance','-t','-A','-c',statement],capture_output=True,text=True,check=True)
    return result.stdout.strip()


statement = '''SELECT coalesce(json_agg(c),'[]'::json) FROM (
 SELECT symbol,minute_start,open,high,low,close,volume,notional,trade_count,is_final
 FROM candles WHERE is_final ORDER BY minute_start,symbol LIMIT 20) c'''
before = json.loads(sql(statement))
assert len(before)==20,'Need at least 20 finalized rows before restart'
subprocess.run(['docker','compose','restart','consumer-bronze','spark'],check=True)
deadline=time.monotonic()+180
while time.monotonic()<deadline:
    status = subprocess.run(['docker','compose','exec','-T','spark','python3','-c',
        "import pathlib,time; p=pathlib.Path('/state/heartbeat'); assert time.time()-p.stat().st_mtime<15"],capture_output=True)
    if status.returncode==0:
        break
    time.sleep(3)
else:
    raise RuntimeError('Spark did not resume healthy checkpoint queries')
after = json.loads(sql(statement))
assert after==before,'Finalized candle values changed across restart'
assert sql('SELECT count(*) FROM (SELECT symbol,minute_start FROM candles GROUP BY 1,2 HAVING count(*)>1) d')=='0'
print('RESTART_TEST_OK 20 finalized rows unchanged, unique keys; Spark and Bronze restarted with existing volumes')
