"""Offline dbt integration with explicit simulated history and known values."""
import os
import subprocess
import shutil
import tempfile
import math
import statistics
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
import duckdb

root = Path(__file__).resolve().parents[1]
if not (root/'dbt').exists():
    root = Path('/app')  # packaged analytics image, script mounted under /checks
with tempfile.TemporaryDirectory() as folder:
    dbfile = str(Path(folder)/'test.duckdb')
    source = str(Path(folder)/'candles.parquet').replace('\\','/')
    db = duckdb.connect(dbfile)
    now = datetime.now(timezone.utc).replace(second=0,microsecond=0)
    db.execute('''CREATE TABLE fixture(symbol VARCHAR,minute_start TIMESTAMP,
     minute_end TIMESTAMP,open DECIMAL(18,4),high DECIMAL(18,4),low DECIMAL(18,4),
     close DECIMAL(18,4),volume BIGINT,notional DECIMAL(38,4),trade_count BIGINT)''')
    for i in range(60):
        start = now-timedelta(minutes=60-i)
        p = 100+i
        db.execute('INSERT INTO fixture VALUES (?,?,?,?,?,?,?,?,?,?)',
                   ['AAPL',start,start+timedelta(minutes=1),p,p,p,p,10,p*10,1])
    previous = (now-timedelta(minutes=60)).replace(hour=0,minute=0)-timedelta(minutes=1)
    db.execute('INSERT INTO fixture VALUES (?,?,?,?,?,?,?,?,?,?)',
               ['AAPL',previous,previous+timedelta(minutes=1),80,80,80,80,10,800,1])
    db.execute(f"COPY fixture TO '{source}' (FORMAT PARQUET)")
    db.execute('CREATE TABLE alerts_source(symbol VARCHAR,bucket TIMESTAMPTZ)')
    db.close()
    env = dict(os.environ, DUCKDB_PATH=dbfile, SILVER_PARQUET=source,
               DBT_PROFILES_DIR=str(root/'dbt'))
    subprocess.run(['dbt','build','--project-dir',str(root/'dbt')],env=env,check=True)
    shutil.copy2(root/'dbt/target/run_results.json',root/'dbt/target/run_results_success.json')
    with duckdb.connect(dbfile) as db:
        row = db.execute('SELECT close,ma20,ma50 FROM gold_minute ORDER BY minute_start DESC LIMIT 1').fetchone()
        assert row == (159,149.5,134.5), row
        assert db.execute('SELECT count(*) FROM gold_minute WHERE ma20 IS NULL').fetchone()[0] == 19
        assert db.execute('SELECT count(*) FROM gold_minute WHERE ma50 IS NULL').fetchone()[0] == 49
        # Independent Python reference also handles a run crossing UTC midnight.
        by_day,returns = {},defaultdict(list)
        prices = db.execute('SELECT minute_start,cast(close as double) FROM fixture ORDER BY minute_start').fetchall()
        for index,(stamp,price) in enumerate(prices):
            by_day[stamp.date()]=price
            if index and stamp-prices[index-1][0]==timedelta(minutes=1):
                returns[stamp.date()].append(math.log(price/prices[index-1][1]))
        for day,value,volatility in db.execute('SELECT day,daily_return,daily_volatility FROM gold_daily').fetchall():
            prior=by_day.get(day-timedelta(days=1))
            expected=by_day[day]/prior-1 if prior else None
            assert (value is None and expected is None) or abs(value-expected)<1e-9
            expected_vol=statistics.stdev(returns[day]) if len(returns[day])>=2 else None
            assert (volatility is None and expected_vol is None) or abs(volatility-expected_vol)<1e-12
        # Each daily VWAP must match the direct weighted reference, including runs crossing UTC midnight.
        for day,vwap in db.execute('SELECT day,vwap FROM gold_daily').fetchall():
            expected = db.execute('SELECT sum(notional)/sum(volume) FROM fixture WHERE cast(minute_start AS date)=?', [day]).fetchone()[0]
            assert abs(vwap-expected)<1e-9
    soda_bin = os.environ.get('SODA_BIN','/opt/soda/bin/soda')
    if Path(soda_bin).exists():
        config = Path(folder)/'soda.yml'
        config.write_text(f'data_source finance:\n  type: duckdb\n  path: {dbfile}\n  read_only: true\n')
        command = [soda_bin,'scan','-d','finance','-c',str(config),str(root/'soda/checks.yml')]
        subprocess.run(command,check=True)
    else:
        print('Soda absent de cet environnement : scan offline non exécuté.')
    # Exercise an actual failing quality gate in the temporary dataset.
    with duckdb.connect(dbfile) as db:
        db.execute('UPDATE fixture SET low=high+1 WHERE minute_start=(SELECT max(minute_start) FROM fixture)')
        db.execute(f"COPY fixture TO '{source}' (FORMAT PARQUET)")
    failed = subprocess.run(['dbt','test','--project-dir',str(root/'dbt'),'--select','candle_contract'],env=env)
    assert failed.returncode != 0,'Invalid OHLC must fail dbt'
    shutil.copy2(root/'dbt/target/run_results.json',root/'dbt/target/run_results_expected_failure.json')
    if Path(soda_bin).exists():
        assert subprocess.run(command).returncode!=0,'Invalid OHLC must fail Soda'
    print('GOLD_TEST_OK MA20/50, VWAP, return, volatility; insufficient history NULL; invalid OHLC rejected.')
