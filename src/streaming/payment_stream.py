from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    try_to_timestamp,
    lit
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)


# --------------------------------------------------
# 1. Start Spark
# --------------------------------------------------

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("CommercePulsePaymentStream")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("Spark started successfully.")


# --------------------------------------------------
# 2. Define the payment data schema
# --------------------------------------------------

payment_data_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DoubleType(), True)
])


# --------------------------------------------------
# 3. Define the complete event schema
# --------------------------------------------------

event_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("processing_time", StringType(), True),
    StructField("data", payment_data_schema, True)
])


# --------------------------------------------------
# 4. Read payment events from Kafka
# --------------------------------------------------

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "localhost:9092"
    )
    .option(
        "subscribe",
        "olist-payments"
    )
    .option(
        "startingOffsets",
        "earliest"
    )
    .load()
)


# --------------------------------------------------
# 5. Convert Kafka value from bytes to string
# --------------------------------------------------

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
)


# --------------------------------------------------
# 6. Remove invalid events
# --------------------------------------------------

parsed_events = parsed_events.filter(
    col("event_type").isNotNull()
)


# --------------------------------------------------
# 7. Convert event_time to Spark timestamp
# --------------------------------------------------

typed_events = parsed_events.withColumn(
    "event_time",
    try_to_timestamp(
        col("event_time"),
        lit("yyyy-MM-dd'T'HH:mm:ss")
    )
)


# --------------------------------------------------
# 8. Select the fields we want to inspect
# --------------------------------------------------

payment_stream = typed_events.select(
    "event_type",
    "event_time",
    "processing_time",
    "data.order_id",
    "data.payment_type",
    "data.payment_installments",
    "data.payment_value",
    "kafka_timestamp"
)


# --------------------------------------------------
# 9. Display the payment stream
# --------------------------------------------------

query = (
    payment_stream
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)


# --------------------------------------------------
# 10. Keep the stream running
# --------------------------------------------------

query.awaitTermination()