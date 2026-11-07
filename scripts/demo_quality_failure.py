"""Demonstrate a rejected OHLC candle in an isolated in-memory Spark DataFrame."""
import sys
from datetime import datetime
sys.path.insert(0,'/opt/finance')

def main():
    from pyspark.sql import SparkSession
    from lakehouse import assert_candles
    from pyspark.sql import functions as F
    spark=SparkSession.builder.master('local[1]').appName('demo-quality-rejection').getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    try:
        valid=spark.createDataFrame([('MSFT',datetime(2026,11,4,12),100.0,103.0,98.0,103.0,10,1007.0,4)],
            'symbol string,minute_start timestamp,open double,high double,low double,close double,volume long,notional double,trade_count long')
        assert_candles(valid)
        invalid=valid.withColumn('low',F.lit(105.0))
        try:
            assert_candles(invalid)
        except ValueError as error:
            print('EXPECTED_QUALITY_REJECTION low=105 > high=103 and open=100: '+str(error))
        else:
            raise AssertionError('Invalid candle was accepted')
        print('QUALITY_DEMO_OK valid accepted; invalid rejected; no production data written')
    finally: spark.stop()
if __name__=='__main__': main()
