"""À intégrer dans le job Spark existant ; ne déploie pas le catalogue Iceberg.

Le connecteur spark-sql-kafka doit correspondre à la version Spark/Scala.
La session Spark doit utiliser UTC. Les sorties en mode update nécessitent
un upsert par (symbol, minute_start), et non un simple append vers Silver.
"""
from pyspark.sql import functions as F, types as T


def build_candles(spark, bootstrap="kafka:29092"):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    schema = T.StructType([
        T.StructField("trade_id", T.StringType()),
        T.StructField("symbol", T.StringType()),
        T.StructField("price", T.DecimalType(18, 4)),
        T.StructField("volume", T.LongType()),
        T.StructField("event_time", T.StringType()),
    ])
    kafka = (spark.readStream.format("kafka")
             .option("kafka.bootstrap.servers", bootstrap)
             .option("subscribe", "trades")
             .option("startingOffsets", "earliest")
             .load())
    trades = (kafka.select(F.from_json(F.col("value").cast("string"), schema).alias("t"))
              .select("t.*")
              .withColumn("event_time", F.to_timestamp("event_time")))
    valid = trades.filter(
        F.col("trade_id").isNotNull() & F.col("symbol").isNotNull()
        & F.col("event_time").isNotNull()
        & (F.col("price") > 0) & (F.col("volume") >= 0)
    )
    # Ordre métier par heure d'événement ; trade_id départage les égalités.
    order = F.struct(F.col("event_time"), F.col("trade_id"))
    candles = (valid.withWatermark("event_time", "2 minutes")
               .groupBy("symbol", F.window("event_time", "1 minute"))
               .agg(F.min_by("price", order).alias("open"),
                    F.max("price").alias("high"),
                    F.min("price").alias("low"),
                    F.max_by("price", order).alias("close"),
                    F.sum("volume").alias("volume"),
                    F.sum(F.col("price") * F.col("volume")).alias("notional"),
                    F.count("*").alias("trade_count"))
               .select("symbol", F.col("window.start").alias("minute_start"),
                       F.col("window.end").alias("minute_end"),
                       "open", "high", "low", "close", "volume", "notional", "trade_count"))
    return candles
