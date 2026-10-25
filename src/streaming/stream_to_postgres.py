from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    try_to_timestamp,
    lit,
    coalesce,
    expr,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

from src.streaming.postgres_sink import write_batch


# ==================================================
# 1. Project and checkpoint configuration
# ==================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

CHECKPOINT_DIR = (
    PROJECT_ROOT
    / "checkpoints"
    / "order_payment_postgres_v1"
)

READY_FILE = (
    PROJECT_ROOT
    / "checkpoints"
    / "e2e_test_ready.txt"
)

CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
READY_FILE.parent.mkdir(parents=True, exist_ok=True)

# Remove a stale readiness signal from a previous run.
READY_FILE.unlink(missing_ok=True)


# ==================================================
# 2. Start Spark
# ==================================================

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("CommercePulseKafkaToPostgres")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "UTC")

print("Spark started successfully.")
print(f"Checkpoint directory: {CHECKPOINT_DIR}")


# ==================================================
# 3. Define event schemas
# ==================================================

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

order_event_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("processing_time", StringType(), True),
    StructField("data", order_data_schema, True),
])

payment_data_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DoubleType(), True),
])

payment_event_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("processing_time", StringType(), True),
    StructField("data", payment_data_schema, True),
])


# ==================================================
# 4. Timestamp parsing helper
# ==================================================

def parse_event_time(df):
    return df.withColumn(
        "event_time",
        coalesce(
            try_to_timestamp(
                col("event_time"),
                lit("yyyy-MM-dd'T'HH:mm:ss"),
            ),
            try_to_timestamp(
                col("event_time"),
                lit("yyyy-MM-dd HH:mm:ss"),
            ),
        ),
    )


# ==================================================
# 5. Read and parse orders from Kafka
# ==================================================

orders_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:19092")
    .option("subscribe", "olist-orders")
    .option("startingOffsets", "latest")
    .load()
)

orders = (
    orders_kafka
    .select(
        from_json(
            col("value").cast("string"),
            order_event_schema,
        ).alias("event")
    )
    .select("event.*")
    .filter(col("event_type") == "order")
    .select(
        col("event_type"),
        col("event_time"),
        col("processing_time"),
        col("data.order_id").alias("order_id"),
        col("data.customer_id").alias("customer_id"),
        col("data.order_status").alias("order_status"),
    )
)

orders = (
    parse_event_time(orders)
    .filter(
        col("order_id").isNotNull()
        & col("event_time").isNotNull()
    )
    .select(
        "order_id",
        "customer_id",
        "order_status",
        col("event_time").alias("order_event_time"),
    )
    .withWatermark("order_event_time", "10 minutes")
    .dropDuplicates(["order_id"])
)


# ==================================================
# 6. Read and parse payments from Kafka
# ==================================================

payments_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:19092")
    .option("subscribe", "olist-payments")
    .option("startingOffsets", "latest")
    .load()
)

payments = (
    payments_kafka
    .select(
        from_json(
            col("value").cast("string"),
            payment_event_schema,
        ).alias("event")
    )
    .select("event.*")
    .filter(col("event_type") == "payment")
    .select(
        col("event_time").alias("event_time"),
        col("data.order_id").alias("order_id"),
        col("data.payment_sequential").alias(
            "payment_sequential"
        ),
        col("data.payment_type").alias("payment_type"),
        col("data.payment_installments").alias(
            "payment_installments"
        ),
        col("data.payment_value").alias("payment_value"),
    )
)

payments = (
    parse_event_time(payments)
    .filter(
        col("order_id").isNotNull()
        & col("payment_sequential").isNotNull()
        & col("event_time").isNotNull()
    )
    .select(
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
        col("event_time").alias("payment_event_time"),
    )
    .withWatermark("payment_event_time", "10 minutes")
    .dropDuplicates(["order_id", "payment_sequential"])
)


# ==================================================
# 7. Join order and payment streams
# ==================================================

join_condition = (
    (orders.order_id == payments.order_id)
    & (payments.payment_event_time >= orders.order_event_time)
    & (
        payments.payment_event_time
        <= orders.order_event_time + expr("INTERVAL 10 MINUTES")
    )
)

joined_stream = orders.join(
    payments,
    join_condition,
    "inner",
)

enriched_orders = joined_stream.select(
    orders.order_id.alias("order_id"),
    orders.customer_id.alias("customer_id"),
    orders.order_status.alias("order_status"),
    orders.order_event_time.alias("order_event_time"),
    payments.payment_sequential.alias("payment_sequential"),
    payments.payment_type.alias("payment_type"),
    payments.payment_installments.alias("payment_installments"),
    payments.payment_value.alias("payment_value"),
    payments.payment_event_time.alias("payment_event_time"),
)


# ==================================================
# 8. Start the Kafka-to-PostgreSQL streaming query
# ==================================================

print("Starting Kafka-to-PostgreSQL streaming query.")
print("Kafka sources: olist-orders, olist-payments")
print("PostgreSQL target: order_payment_events")
print("Starting offsets: latest")
print("Output mode: append")

query = None

try:
    query = (
        enriched_orders
        .writeStream
        .outputMode("append")
        .foreachBatch(write_batch)
        .option(
            "checkpointLocation",
            str(CHECKPOINT_DIR),
        )
        .start()
    )

    if not query.isActive:
        raise RuntimeError(
            "The Kafka-to-PostgreSQL streaming query did not start."
        )

    # Signal that the streaming query has started.
    READY_FILE.write_text(
        "Spark streaming query is active.",
        encoding="utf-8",
    )

    print("Streaming query is active and ready for test events.")
    print(f"Readiness file: {READY_FILE}")

    query.awaitTermination()

except KeyboardInterrupt:
    print("Stopping CommercePulse streaming query...")

except Exception:
    READY_FILE.unlink(missing_ok=True)
    raise

finally:
    READY_FILE.unlink(missing_ok=True)

    if query is not None and query.isActive:
        query.stop()

    spark.stop()
