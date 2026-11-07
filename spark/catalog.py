"""Iceberg JDBC catalog in existing PostgreSQL; data and metadata files remain in MinIO."""
import os
from urllib.parse import urlparse,unquote
from pyspark.sql import SparkSession

def session(name='finance-lakehouse'):
    pg=urlparse(os.environ['PG_DSN'])
    settings={
      'spark.sql.session.timeZone':'UTC',
      'spark.sql.extensions':'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
      'spark.sql.catalog.lake':'org.apache.iceberg.spark.SparkCatalog',
      'spark.sql.catalog.lake.catalog-impl':'org.apache.iceberg.jdbc.JdbcCatalog',
      'spark.sql.catalog.lake.uri':f'jdbc:postgresql://{pg.hostname}:{pg.port or 5432}/{pg.path.lstrip("/")}',
      'spark.sql.catalog.lake.jdbc.user':unquote(pg.username or 'finance'),
      'spark.sql.catalog.lake.jdbc.password':unquote(pg.password or ''),
      'spark.sql.catalog.lake.jdbc.currentSchema':'iceberg_catalog',
      'spark.sql.catalog.lake.jdbc.schema-version':'V1',
      'spark.sql.catalog.lake.warehouse':'s3://silver/lakehouse-v2',
      'spark.sql.catalog.lake.io-impl':'org.apache.iceberg.aws.s3.S3FileIO',
      'spark.sql.catalog.lake.s3.endpoint':os.environ['S3_ENDPOINT'],
      'spark.sql.catalog.lake.s3.path-style-access':'true',
      'spark.sql.catalog.lake.s3.access-key-id':os.environ['AWS_ACCESS_KEY_ID'],
      'spark.sql.catalog.lake.s3.secret-access-key':os.environ['AWS_SECRET_ACCESS_KEY'],
      'spark.sql.catalog.lake.client.region':'us-east-1',
      'spark.hadoop.fs.s3a.endpoint':os.environ['S3_ENDPOINT'],
      'spark.hadoop.fs.s3a.path.style.access':'true',
      'spark.hadoop.fs.s3a.connection.ssl.enabled':'false',
      'spark.hadoop.fs.s3a.access.key':os.environ['AWS_ACCESS_KEY_ID'],
      'spark.hadoop.fs.s3a.secret.key':os.environ['AWS_SECRET_ACCESS_KEY'],
      'spark.hadoop.fs.s3a.aws.credentials.provider':'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
      'spark.sql.shuffle.partitions':'4',
      'spark.sql.adaptive.enabled':'true'
    }
    builder=SparkSession.builder.appName(name)
    for key,value in settings.items():
        builder=builder.config(key,value)
    return builder.getOrCreate()
