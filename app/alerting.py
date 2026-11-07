"""Gold observations -> PostgreSQL alert episodes and immutable change history."""
import hashlib
import json
import os
import uuid
from datetime import datetime,timezone
from decimal import Decimal
from urllib.request import urlopen
from urllib.parse import urlencode
from psycopg2.extras import Json,RealDictCursor
from serving import connect

RULE='abs(close/reference_close-1)>0.05 sur 5 minutes'
THRESHOLD=Decimal('0.05')

def stamp(value):
    if isinstance(value,str): value=datetime.fromisoformat(value.replace('Z','+00:00'))
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value

def fingerprint(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,default=str).encode()).hexdigest()

def episodes(symbol,observations):
    result=[]; current=None
    for observation in sorted(observations,key=lambda r:stamp(r['observed_at'])):
        fraction=observation.get('change_fraction')
        if fraction is None: continue
        fraction=Decimal(str(fraction))
        at=stamp(observation['observed_at'])
        if abs(fraction)>THRESHOLD:
            if current is None:
                current=dict(alert_id=str(uuid.uuid5(uuid.NAMESPACE_URL,f'finance-price5m:{symbol}:{at.isoformat()}')),
                    symbol=symbol,alert_type='price_change_5m',description='Variation absolue supérieure à 5 % sur cinq minutes',
                    rule=RULE,threshold=THRESHOLD,opened_at=at,trigger_value=fraction,
                    peak_value=fraction,status='active',resolved_at=None)
                result.append(current)
            if abs(fraction)>abs(current['peak_value']): current['peak_value']=fraction
        elif current is None:
            continue
        else:
            current['status']='resolved'; current['resolved_at']=at
        current.update(observed_value=fraction,last_observed_at=at,price=observation['price'],
                       reference_price=observation.get('reference_price'),reference_time=observation.get('reference_time'))
        if current['status']=='resolved': current=None
    return result

FIELDS=('alert_id','symbol','alert_type','description','rule','observed_value','threshold',
        'opened_at','last_observed_at','resolved_at','status','price','reference_price',
        'reference_time','trigger_value','peak_value')

def record_history(cur,alert,action,signature):
    occurred=alert['opened_at'] if action=='created' else (alert.get('resolved_at') or alert.get('last_observed_at') or alert['opened_at'])
    cur.execute("""INSERT INTO alert_history(alert_id,status,action,occurred_at,details,fingerprint)
      VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
      (alert['alert_id'],alert['status'],action,occurred,Json(alert,dumps=lambda v:json.dumps(v,default=str)),signature))

def evaluate():
    with connect() as pg,pg.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute('SELECT pg_try_advisory_xact_lock(487206) AS acquired')
        if not cur.fetchone()['acquired']: raise RuntimeError('Alert evaluation already running')
        cur.execute("SELECT source_cursor FROM alert_state WHERE name='gold_minute' FOR UPDATE")
        since=cur.fetchone()['source_cursor']
        url=os.environ.get('LAKEHOUSE_URL','http://lakehouse:8090')+'/gold/observations?'+urlencode({'since':since.isoformat()})
        with urlopen(url,timeout=300) as response: payload=json.load(response)
        changed=set(); cursor=since
        for row in payload['observations']:
            updated=stamp(row['metric_updated_at']); cursor=max(cursor,updated)
            canonical=dict(symbol=row['symbol'],observed_at=stamp(row['minute_start']),
                price=Decimal(str(row['close'])),reference_price=row.get('reference_price'),
                reference_time=stamp(row['reference_time']) if row.get('reference_time') else None,
                change_fraction=Decimal(str(row['change_5m'])) if row.get('change_5m') is not None else None)
            digest=fingerprint(canonical)
            cur.execute("""INSERT INTO alert_observations
             (symbol,observed_at,price,reference_price,reference_time,change_fraction,source_updated_at,fingerprint)
             VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(symbol,observed_at) DO UPDATE SET
             price=EXCLUDED.price,reference_price=EXCLUDED.reference_price,
             reference_time=EXCLUDED.reference_time,change_fraction=EXCLUDED.change_fraction,
             source_updated_at=EXCLUDED.source_updated_at,fingerprint=EXCLUDED.fingerprint
             WHERE alert_observations.fingerprint<>EXCLUDED.fingerprint RETURNING symbol""",
             tuple(canonical[k] for k in ('symbol','observed_at','price','reference_price','reference_time','change_fraction'))+(updated,digest))
            if cur.fetchone(): changed.add(row['symbol'])
        for symbol in sorted(changed):
            cur.execute('SELECT * FROM alert_observations WHERE symbol=%s ORDER BY observed_at',(symbol,))
            computed=episodes(symbol,cur.fetchall())
            cur.execute("SELECT * FROM alerts WHERE symbol=%s AND origin='gold'",(symbol,))
            previous={str(row['alert_id']):dict(row) for row in cur.fetchall()}
            present=set()
            for alert in computed:
                identifier=alert['alert_id']; present.add(identifier); old=previous.get(identifier)
                # Manual closure is terminal for this episode; a new breach episode gets a new id.
                if old and old['status']=='closed':
                    alert['status']='closed'; alert['closed_at']=old['closed_at']
                signature=fingerprint(alert)
                if old and old['fingerprint']==signature: continue
                values=tuple(alert.get(k) for k in FIELDS)+(signature,alert.get('closed_at'))
                cur.execute(f"""INSERT INTO alerts({','.join(FIELDS)},fingerprint,closed_at)
                 VALUES ({','.join(['%s']*len(values))}) ON CONFLICT(alert_id) DO UPDATE SET
                 {','.join(k+'=EXCLUDED.'+k for k in FIELDS if k not in ('alert_id','symbol','opened_at'))},
                 fingerprint=EXCLUDED.fingerprint,closed_at=EXCLUDED.closed_at,updated_at=now()""",values)
                action='created' if not old else ('status_changed' if old['status']!=alert['status'] else 'updated')
                if not old and alert['status']=='resolved':
                    initial=dict(alert,status='active',resolved_at=None,last_observed_at=alert['opened_at'],observed_value=alert['trigger_value'])
                    record_history(cur,initial,'created',fingerprint({'created':identifier}))
                    record_history(cur,alert,'status_changed',signature)
                else:
                    record_history(cur,alert,action,signature)
            for identifier,old in previous.items():
                if identifier not in present and old['status']=='active':
                    old['status']='resolved'; old['resolved_at']=old['last_observed_at']
                    signature=fingerprint({'correction':identifier,'observations':cursor.isoformat()})
                    cur.execute("UPDATE alerts SET status='resolved',resolved_at=last_observed_at,updated_at=now(),fingerprint=%s WHERE alert_id=%s",(signature,identifier))
                    record_history(cur,old,'gold_correction',signature)
        cur.execute("""UPDATE alert_state SET source_cursor=%s,source_snapshot=%s,updated_at=now()
           WHERE name='gold_minute'""",(cursor,payload.get('snapshot')))
        return {'observations':len(payload['observations']),'changed_symbols':len(changed),'snapshot':payload.get('snapshot')}

def close_resolved(alert_id):
    with connect() as pg,pg.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute('SELECT pg_advisory_xact_lock(487206)')
        cur.execute("SELECT * FROM alerts WHERE alert_id=%s AND status='resolved' FOR UPDATE",(str(alert_id),))
        row=cur.fetchone()
        if row is None: return False
        alert=dict(row); alert['status']='closed'; alert['closed_at']=datetime.now(timezone.utc)
        signature=fingerprint({'manual_close':str(alert_id)})
        cur.execute("UPDATE alerts SET status='closed',closed_at=now(),updated_at=now(),fingerprint=%s WHERE alert_id=%s",(signature,str(alert_id)))
        record_history(cur,alert,'manual_close',signature)
        return True
