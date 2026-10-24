from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    try_to_timestamp,
    lit,
    window
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)


# ==========================================
# 1. Create Spark Session
# ==========================================

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("CommercePulseKafkaStreaming")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ==========================================
# 2. Define the Olist Order Schema
# ==========================================

order_data_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", StringType(), True),
    StructField("order_approved_at", StringType(), True),
    StructField("order_delivered_carrier_date", StringType(), True),
    StructField("order_delivered_customer_date", StringType(), True),
    StructField("order_estimated_delivery_date", StringType(), True),
])


# ==========================================
# 3. Define the Kafka Event Schema
# ==========================================

event_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("processing_time", StringType(), True),
    StructField("data", order_data_schema, True),
])


# ==========================================
# 4. Read Events from Kafka
# ==========================================

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "localhost:9092"
    )
    .option(
        "subscribe",
        "olist-orders"
    )
    .option(
        "startingOffsets",
        "earliest"
    )
    .load()
)


# ==========================================
# 5. Parse Kafka JSON Messages
# ==========================================

parsed_events = (
    kafka_stream
    .select(
        from_json(
            col("value").cast("string"),
            event_schema
        ).alias("event"),

        col("timestamp").alias("kafka_timestamp")
    )
    .select(
        "event.*",
        "kafka_timestamp"
    )
    .filter(
        col("event_type").isNotNull()
    )
)


# ==========================================
# 6. Convert event_time to Timestamp
# ==========================================

typed_events = parsed_events.withColumn(
    "event_time",
    try_to_timestamp(
        col("event_time"),
        lit("yyyy-MM-dd'T'HH:mm:ss")
    )
)


# ==========================================
# 7. Add Watermark
# ==========================================

watermarked_events = typed_events.withWatermark(
    "event_time",
    "10 minutes"
)


# ==========================================
# 8. Create 1-Hour Event-Time Window
# ==========================================

windowed_events = watermarked_events.withColumn(
    "time_window",
    window(
        col("event_time"),
        "1 hour"
    )
)


# ==========================================
# 9. Group Orders by Window
# ==========================================

hourly_orders = (
    windowed_events
    .groupBy("time_window")
    .count()
)


# ==========================================
# 10. Write Aggregated Results to Console
# ==========================================

query = (
    hourly_orders
    .writeStream
    .format("console")
    .outputMode("complete")
    .option("truncate", "false")
    .start()
)


# ==========================================
# 11. Keep Streaming Query Running
# ==========================================

query.awaitTermination()