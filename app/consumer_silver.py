"""Spark Structured Streaming consumer: Kafka trades -> Iceberg 1-minute OHLC candles.

FIN-03: event-time aggregation with a documented watermark.
FIN-04: Iceberg Silver table at grain (symbol, candle_start).
"""

import os
import signal
import threading

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


ICEBERG_CATALOG = "silver"
ICEBERG_NAMESPACE = "market"
ICEBERG_TABLE = f"{ICEBERG_CATALOG}.{ICEBERG_NAMESPACE}.ohlc_1m"

# The Kafka producer emits ISO-8601 timestamps with an explicit UTC offset.
TRADE_SCHEMA = StructType(
    [
        StructField("trade_id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("price", DecimalType(18, 4), True),
        StructField("quantity", IntegerType(), True),
        StructField("event_time", StringType(), True),
    ]
)


def build_spark() -> SparkSession:
    """Create Spark with pinned, Spark-3.5/Scala-2.12 Iceberg and S3 dependencies."""
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    iceberg_version = os.getenv("ICEBERG_VERSION", "1.12.0")
    # PySpark 3.5.x bundles Hadoop 3.3.4 in the standard distribution.
    # hadoop-aws is needed for the durable s3a:// Structured Streaming checkpoint.
    packages = ",".join(
        [
            f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{iceberg_version}",
            f"org.apache.iceberg:iceberg-aws-bundle:{iceberg_version}",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            # JDBC Catalog keeps Iceberg metadata in PostgreSQL, avoiding a
            # path-based HadoopCatalog on an object store without atomic rename.
            "org.postgresql:postgresql:42.7.14",
            # ADD THIS LINE FOR KAFKA INTEGRATION:
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        ]
    )

    return (
        SparkSession.builder.appName("consumer-silver-ohlc-1m")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", packages)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
	.config("spark.executor.memory", "2g")
	.config("spark.driver.memory", "1g")
	.config("spark.streaming.concurrentJobs", "2")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "jdbc")
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.uri",
            os.getenv(
                "ICEBERG_JDBC_URI",
                "jdbc:postgresql://postgres:5432/iceberg_catalog",
            ),
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.jdbc.user",
            os.getenv("ICEBERG_JDBC_USER", "iceberg"),
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.jdbc.password",
            os.getenv("ICEBERG_JDBC_PASSWORD", ""),
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse",
            "s3://silver/warehouse",
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        # Iceberg S3FileIO uses s3:// URIs; the Spark checkpoint uses s3a://.
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.endpoint", s3_endpoint)
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.path-style-access", "true"
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.access-key-id", access_key
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.secret-access-key", secret_key
        )
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.client.region", region)
        # Hadoop S3A settings are separate from Iceberg's S3FileIO settings.
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            str(s3_endpoint.lower().startswith("https://")).lower(),
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.shuffle.partitions",
            os.getenv("SPARK_SHUFFLE_PARTITIONS", "6"),
        )
        .config("spark.sql.streaming.metricsEnabled", "true")
        .getOrCreate()
    )


def create_silver_table(spark: SparkSession) -> None:
    """Create the Iceberg namespace/table once; never drop it on restart."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_NAMESPACE}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE} (
            symbol STRING,
            candle_start TIMESTAMP,
            candle_end TIMESTAMP,
            open DECIMAL(18, 4),
            high DECIMAL(18, 4),
            low DECIMAL(18, 4),
            close DECIMAL(18, 4),
            trade_count BIGINT,
            volume BIGINT
        )
        USING iceberg
        PARTITIONED BY (days(candle_start))
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'write.distribution-mode' = 'hash',
            'write.spark.fanout.enabled' = 'true',
            'write.target-file-size-bytes' = '134217728',
            'commit.retry.num-retries' = '4'
        )
        """
    )


class ProgressLogger(StreamingQueryListener):
    """Emit small, structured progress messages for container log monitoring."""

    def onQueryStarted(self, event):
        print(f"SILVER_QUERY_STARTED id={event.id}", flush=True)

    def onQueryIdle(self, event):
        # Spark 3.5 exposes this lifecycle callback when the source has no new data.
        return None

    def onQueryProgress(self, event):
        progress = event.progress
        event_time = progress.eventTime or {}
        watermark = event_time.get("watermark", "not-initialized")
        print(
            "SILVER_PROGRESS "
            f"batch_id={progress.batchId} "
            f"input_rows={progress.numInputRows} "
            f"input_rows_per_second={progress.inputRowsPerSecond:.2f} "
            f"processed_rows_per_second={progress.processedRowsPerSecond:.2f} "
            f"watermark={watermark}",
            flush=True,
        )

    def onQueryTerminated(self, event):
        error = getattr(event, "exception", None)
        print(
            f"SILVER_QUERY_TERMINATED id={event.id} error={error or 'none'}",
            flush=True,
        )


def main() -> None:
    topic = os.getenv("KAFKA_TOPIC", "trades")
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    watermark_delay = os.getenv("WATERMARK_DELAY", "2 minutes")
    trigger_interval = os.getenv("SILVER_TRIGGER_INTERVAL", "60 seconds")
    checkpoint = os.getenv(
        "SILVER_CHECKPOINT_LOCATION", "s3a://silver/checkpoints/ohlc_1m"
    )
    starting_offsets = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")
    max_offsets = os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "50000")

    if starting_offsets not in {"earliest", "latest"}:
        raise ValueError("KAFKA_STARTING_OFFSETS doit valoir earliest ou latest")
    if not max_offsets.isdigit() or int(max_offsets) <= 0:
        raise ValueError("KAFKA_MAX_OFFSETS_PER_TRIGGER doit être un entier positif")
    if not checkpoint:
        raise ValueError("SILVER_CHECKPOINT_LOCATION ne doit pas être vide")

    spark = build_spark()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    spark.streams.addListener(ProgressLogger())

    try:
        create_silver_table(spark)

        # Starting offsets only apply when the checkpoint has not been created.
        kafka = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", starting_offsets)
            .option("failOnDataLoss", "true")
            .option("maxOffsetsPerTrigger", int(max_offsets))
            .load()
        )

        decoded = kafka.select(
            F.from_json(
                F.col("value").cast("string"),
                TRADE_SCHEMA,
                # PERMISSIVE leaves malformed JSON fields null; the validation
                # below drops bad records instead of crashing the whole query.
                {"mode": "PERMISSIVE"},
            ).alias("trade")
        ).select("trade.*")

        # Producer event_time is ISO-8601 with UTC offset. Parse it in UTC and
        # reject records that do not contain the fields needed for a candle.
        trades = (
            decoded.withColumn(
                "event_ts", F.try_to_timestamp(F.col("event_time"))
            )
            .where(
                F.col("trade_id").isNotNull()
                & F.col("symbol").isNotNull()
                & (F.length(F.trim(F.col("symbol"))) > 0)
                & F.col("price").isNotNull()
                & (F.col("price") > 0)
                & F.col("quantity").isNotNull()
                & (F.col("quantity") > 0)
                & F.col("event_ts").isNotNull()
            )
            .select("trade_id", "symbol", "price", "quantity", "event_ts")
        )

        # Append mode emits only finalized event-time windows. With a 2-minute
        # watermark, candles are normally published after the watermark passes
        # their minute; events later than the delay may be discarded.
        windowed = trades.withWatermark("event_ts", watermark_delay).groupBy(
            F.window(F.col("event_ts"), "1 minute").alias("window"),
            F.col("symbol"),
        )

        candles = windowed.agg(
            # min_by/max_by provide the first/last price by event time. trade_id
            # is a deterministic tie-breaker for identical event_time values.
            F.min_by(
                F.col("price"), F.struct(F.col("event_ts"), F.col("trade_id"))
            ).alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by(
                F.col("price"), F.struct(F.col("event_ts"), F.col("trade_id"))
            ).alias("close"),
            F.count(F.lit(1)).cast("long").alias("trade_count"),
            F.sum(F.col("quantity")).cast("long").alias("volume"),
        ).select(
            F.col("symbol"),
            F.col("window.start").alias("candle_start"),
            F.col("window.end").alias("candle_end"),
            F.col("open"),
            F.col("high"),
            F.col("low"),
            F.col("close"),
            F.col("trade_count"),
            F.col("volume"),
        )

        print(
            "SILVER_DEMARRE "
            f"topic={topic} table={ICEBERG_TABLE} "
            f"watermark={watermark_delay} trigger={trigger_interval} "
            f"starting_offsets={starting_offsets} checkpoint={checkpoint}",
            flush=True,
        )

        query = (
            candles.writeStream.format("iceberg")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .option("fanout-enabled", "true")
            .trigger(processingTime=trigger_interval)
            .toTable(ICEBERG_TABLE)
        )

        stop_requested = threading.Event()

        def request_stop(*_args):
            stop_requested.set()

        signal.signal(signal.SIGINT, request_stop)
        signal.signal(signal.SIGTERM, request_stop)

        try:
            while query.isActive and not stop_requested.is_set():
                query.awaitTermination(5)
        finally:
            if query.isActive:
                print("SILVER_ARRET_DEMANDE", flush=True)
                query.stop()
            query.awaitTermination(30)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
