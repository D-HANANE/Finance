"""Verify the traced trade, its canonical Silver candle and the published Gold minute."""
import argparse,json,sys
from pathlib import Path
from decimal import Decimal
from datetime import datetime,timezone

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('path');args=parser.parse_args()
    sys.path.insert(0,'/opt/finance')
    from catalog import session
    from pyspark.sql import functions as F
    from serving import connect
    trace=json.loads(Path(args.path).read_text(encoding='utf-8-sig'));event=trace['event']
    at=datetime.fromisoformat(event['event_time'].replace('Z','+00:00')).astimezone(timezone.utc)
    minute=at.replace(second=0,microsecond=0,tzinfo=None)
    spark=session('demo-trace-trade');spark.sparkContext.setLogLevel('ERROR')
    try:
        canonical=spark.table('lake.silver.trades').filter(F.col('trade_id')==event['trade_id']).collect()
        if len(canonical)!=1: raise AssertionError('Expected exactly one canonical trade; run Silver first: '+str(len(canonical)))
        trade=canonical[0]
        assert trade.symbol==event['symbol'] and trade.price==Decimal(str(event['price']))
        assert trade.quantity==event.get('quantity',event.get('volume'))
        candles=spark.table('lake.silver.candles').filter((F.col('symbol')==event['symbol'])&(F.col('minute_start')==F.lit(minute))).collect()
        assert len(candles)==1,'Candle pending or duplicate key'
        candle=candles[0]
        source=spark.table('lake.silver.trades').filter((F.col('symbol')==event['symbol'])&
          (F.col('event_time')>=F.lit(minute))&(F.col('event_time')<F.lit(candle.minute_end)))
        order=F.struct('event_time','trade_id')
        expected=source.agg(F.min_by('price',order),F.max('price'),F.min('price'),F.max_by('price',order),
          F.sum('quantity'),F.sum(F.col('price')*F.col('quantity')),F.count('*')).first()
        fields=('open','high','low','close','volume','notional','trade_count')
        assert tuple(expected)==tuple(candle[k] for k in fields),'Silver candle disagrees with canonical trades'
        result=dict(trade_id=event['trade_id'],canonical_count=1,canonical=trade.asDict(),
          silver_candle=candle.asDict(),expected=dict(zip(fields,expected)),silver_verified=True)
        with connect() as pg,pg.cursor() as cur:
            cur.execute('SELECT snapshots FROM gold_serving_publication WHERE id=1');publication=cur.fetchone()
            if publication and publication[0].get('minute_metrics'):
                metrics=(spark.read.option('snapshot-id',publication[0]['minute_metrics']).table('lake.gold.minute_metrics')
                  .filter((F.col('symbol')==event['symbol'])&(F.col('minute_start')==F.lit(minute))).collect())
                result['gold_minute']=metrics[0].asDict() if metrics else None
        print(json.dumps(result,default=str,ensure_ascii=False,indent=2))
    finally: spark.stop()
if __name__=='__main__': main()
