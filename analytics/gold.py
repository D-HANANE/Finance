"""Stable Silver snapshot -> dbt -> Soda -> atomic serving publication."""
import fcntl
import json
import os
import subprocess
from pathlib import Path
import duckdb
from serving import connect


def run():
    with open('/gold/job.lock', 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        pointer = Path('/exchange/silver.json')
        if not pointer.exists():
            raise RuntimeError('Silver absent : attendre les premières fenêtres finalisées (~4 minutes).')
        snapshot = json.loads(pointer.read_text())
        os.environ['SILVER_PARQUET'] = snapshot['path']
        database = os.environ.get('DUCKDB_PATH', '/gold/finance.duckdb')
        # Alert journal is copied at the same run; no DuckDB postgres extension download.
        with connect() as pg, pg.cursor() as cursor:
            cursor.execute('SELECT symbol,bucket,event_time,price,reference_time,reference_price,change_fraction,detected_at FROM alerts')
            rows = cursor.fetchall()
        with duckdb.connect(database) as db:
            db.execute('''CREATE TABLE IF NOT EXISTS alerts_source (
              symbol VARCHAR,bucket TIMESTAMPTZ,event_time TIMESTAMPTZ,price DOUBLE,
              reference_time TIMESTAMPTZ,reference_price DOUBLE,change_fraction DOUBLE,detected_at TIMESTAMPTZ)''')
            db.execute('DELETE FROM alerts_source')
            if rows:
                db.executemany('INSERT INTO alerts_source VALUES (?,?,?,?,?,?,?,?)', rows)
        subprocess.run(['dbt', 'build', '--project-dir', '/app/dbt'], check=True)
        subprocess.run(['dbt', 'docs', 'generate', '--project-dir', '/app/dbt'], check=True)
        subprocess.run(['/opt/soda/bin/soda', 'scan', '-d', 'finance', '-c',
                        '/app/soda/configuration.yml', '/app/soda/checks.yml'], check=True)
        with duckdb.connect(database, read_only=True) as db, connect() as pg, pg.cursor() as cursor:
            # All serving Gold tables change together, only after quality passes.
            for model in ('gold_daily', 'gold_minute', 'gold_correlations'):
                data = db.execute(f'SELECT * FROM {model}').fetchall()
                cursor.execute(f'DELETE FROM {model}')
                if data:
                    cursor.executemany(f'INSERT INTO {model} VALUES ({",".join(["%s"] * len(data[0]))})', data)
            cursor.execute("INSERT INTO pipeline_runs(snapshot_path,status) VALUES (%s,'passed')", (snapshot['path'],))
            # The EOD artifact uses real rows and the same validated run.
            db.execute("COPY (SELECT * FROM gold_daily ORDER BY day,symbol) TO '/gold/daily-report.csv' (HEADER, DELIMITER ',')")
        return f"Gold et qualité terminés : {snapshot['path']}"


if __name__ == '__main__':
    print(run())
