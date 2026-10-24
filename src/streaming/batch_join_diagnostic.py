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


# --------------------------------------------------
# Start a separate Spark batch diagnostic
# --------------------------------------------------

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("CommercePulseBatchJoinDiagnostic")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\nSpark batch diagnostic started.")


# --------------------------------------------------
#  Define the event schemas
# --------------------------------------------------

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


# --------------------------------------------------
#  Read the existing Kafka records in batch mode
# --------------------------------------------------

orders_kafka = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:19092")
    .option("subscribe", "olist-orders")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

payments_kafka = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:19092")
    .option("subscribe", "olist-payments")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

print("Order Kafka rows:", orders_kafka.count())
print("Payment Kafka rows:", payments_kafka.count())


# --------------------------------------------------
#  Parse and transform orders
# --------------------------------------------------

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
    .withColumn(
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
    .select(
        col("data.order_id").alias("order_id"),
        col("data.customer_id").alias("customer_id"),
        col("data.order_status").alias("order_status"),
        col("event_time").alias("order_event_time"),
    )
)

print("Parsed order rows:", orders.count())


# --------------------------------------------------
# Parse and transform payments
# --------------------------------------------------

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
    .withColumn(
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
    .select(
        col("data.order_id").alias("order_id"),
        col("data.payment_sequential").alias("payment_sequential"),
        col("data.payment_type").alias("payment_type"),
        col("data.payment_installments").alias("payment_installments"),
        col("data.payment_value").alias("payment_value"),
        col("event_time").alias("payment_event_time"),
    )
    .filter(col("payment_sequential").isNotNull())
)

print("Parsed payment rows:", payments.count())


# --------------------------------------------------
#  Check the target order before joining
# --------------------------------------------------

target_order_id = "e481f51cbdc54678b7cc49136f2d6af7"

print("\nTarget order records:")
orders.filter(
    col("order_id") == target_order_id
).show(truncate=False)

print("\nTarget payment records:")
payments.filter(
    col("order_id") == target_order_id
).show(truncate=False)


# --------------------------------------------------
#  Apply the existing join conditions
# --------------------------------------------------

join_condition = (
    (orders.order_id == payments.order_id)
    & (payments.payment_event_time >= orders.order_event_time)
    & (
        payments.payment_event_time
        <= orders.order_event_time + expr("INTERVAL 10 MINUTES")
    )
)

joined = orders.join(
    payments,
    join_condition,
    "inner",
).select(
    orders.order_id,
    orders.customer_id,
    orders.order_status,
    orders.order_event_time,
    payments.payment_sequential,
    payments.payment_type,
    payments.payment_installments,
    payments.payment_value,
    payments.payment_event_time,
)

print("\nTotal joined rows:", joined.count())

print("\nJoined records:")
joined.show(50, truncate=False)


# --------------------------------------------------
#  Shut down this diagnostic Spark session
# --------------------------------------------------

spark.stop()

print("\nSpark batch diagnostic finished.")

