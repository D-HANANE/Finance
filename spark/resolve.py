"""Populate Spark's jars through Ivy during image build, with pinned coordinates."""
from pyspark.sql import SparkSession

PACKAGES = ','.join([
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5',
    'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.2',
    'org.apache.iceberg:iceberg-aws-bundle:1.7.2',
])
spark = (SparkSession.builder.master('local[1]').config('spark.jars.packages', PACKAGES)
         .config('spark.jars.ivySettings', '/opt/finance/ivysettings.xml')
         .config('spark.jars.ivy', '/tmp/ivy').getOrCreate())
spark.stop()
import shutil
from pathlib import Path
for jar in Path('/tmp/ivy/jars').glob('*.jar'):
    shutil.copy2(jar, '/opt/spark/jars/' + jar.name)
assert len(list(Path('/tmp/ivy/jars').glob('*.jar'))) >= 5, 'Ivy did not resolve the connector dependencies'
