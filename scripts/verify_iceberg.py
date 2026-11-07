"""Read real JDBC Iceberg tables, check canonical OHLCV, uniqueness and snapshot replay."""
import sys,json
sys.path.insert(0,'/opt/finance')
from catalog import session
from pyspark.sql import functions as F
from lakehouse import assert_candles
spark=session('verify-persistent-lakehouse')
spark.sparkContext.setLogLevel('ERROR')
try:
    candles=spark.table('lake.silver.candles')
    assert candles.count()>0,'No Silver candles'
    assert_candles(candles)
    sample=candles.orderBy('minute_start','symbol').first()
    trades=(spark.table('lake.silver.trades').filter(
      (F.col('symbol')==sample.symbol)&(F.col('event_time')>=sample.minute_start)&(F.col('event_time')<sample.minute_end)))
    order=F.struct('event_time','trade_id')
    expected=trades.agg(F.min_by('price',order),F.max('price'),F.min('price'),
      F.max_by('price',order),F.sum('quantity'),F.sum(F.col('price')*F.col('quantity')),F.count('*')).first()
    assert tuple(expected)==tuple(sample[k] for k in ('open','high','low','close','volume','notional','trade_count'))
    counts={}
    for name in ('silver.trades','silver.candles','gold.minute_metrics','gold.daily_metrics','gold.correlations'):
        table='lake.'+name
        counts[name]=spark.table(table).count()
        assert counts[name]>0,table
        snapshot=spark.sql('SELECT snapshot_id FROM '+table+'.snapshots ORDER BY committed_at DESC LIMIT 1').first()[0]
        assert spark.read.option('snapshot-id',str(snapshot)).table(table).count()==counts[name]
    duplicates=spark.sql('SELECT symbol,minute_start,count(*) n FROM lake.gold.minute_metrics GROUP BY symbol,minute_start HAVING n>1')
    assert duplicates.isEmpty(),'Duplicate Gold minute key'
    print('ICEBERG_V2_OK '+json.dumps(counts))
finally:
    spark.stop()
