"""Known-value Spark SQL Gold calculations; no Kafka, MinIO or external catalog required."""
import sys
from pathlib import Path
from datetime import datetime,timedelta
from decimal import Decimal
sys.path.extend(['/opt/finance',str(Path(__file__).resolve().parents[1]/'spark'),str(Path(__file__).resolve().parents[1]/'app')])
from pyspark.sql import SparkSession
from lakehouse import MINUTE_SQL
spark=SparkSession.builder.master('local[2]').appName('gold-sql-contract').config('spark.sql.session.timeZone','UTC').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
start=datetime(2026,11,4,12)
schema='symbol string,minute_start timestamp,minute_end timestamp,open decimal(18,4),high decimal(18,4),low decimal(18,4),close decimal(18,4),volume long,notional decimal(38,4),trade_count long,last_event_time timestamp,last_trade_id string'
rows=[]
for i in range(60):
    p=Decimal(100+i); stamp=start+timedelta(minutes=i)
    rows.append(('AAPL',stamp,stamp+timedelta(minutes=1),p,p,p,p,10,p*10,1,stamp+timedelta(seconds=59),str(i)))
spark.createDataFrame(rows,schema).createOrReplaceTempView('metric_input')
result=spark.sql(MINUTE_SQL).orderBy('minute_start').collect()
assert result[-1].ma20==149.5
assert result[-1].ma50==134.5
assert abs(result[-1].vwap_day-129.5)<1e-10
assert result[0].ma20 is None and result[48].ma50 is None
assert result[4].change_5m is None
assert result[5].change_5m==Decimal('0.05')
assert result[5].reference_price==100
assert len(result)==60
print('GOLD_SPARK_OK MA20=149.5 MA50=134.5 VWAP=129.5; exact 5min reference and insufficient history')
spark.stop()
