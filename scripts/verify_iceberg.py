import sys
sys.path.insert(0,'/opt/finance')
from streaming import session
from serving import connect

spark = session()
spark.sparkContext.setLogLevel('ERROR')
rows = spark.table('lake.finance.candles').collect()
assert rows,'Silver is empty'
keys = {(r.symbol,r.minute_start) for r in rows}
assert len(keys)==len(rows),'Duplicate Silver key'
with connect() as pg,pg.cursor() as cursor:
    for row in rows:
        cursor.execute('''SELECT open,high,low,close,volume,notional,is_final FROM candles
                          WHERE symbol=%s AND minute_start=%s AT TIME ZONE 'UTC' ''',
                       (row.symbol,row.minute_start))
        actual = cursor.fetchone()
        assert actual==tuple(row[k] for k in ('open','high','low','close','volume','notional'))+(True,),actual
snapshots = spark.sql('SELECT snapshot_id,committed_at FROM lake.finance.candles.snapshots ORDER BY committed_at DESC').collect()
snapshot = snapshots[0].snapshot_id
count = spark.read.option('snapshot-id',str(snapshot)).table('lake.finance.candles').count()
assert count==len(rows)
print(f'ICEBERG_TEST_OK rows={count} snapshot={snapshot} PostgreSQL agrees, unique keys, snapshot read works')
spark.stop()
