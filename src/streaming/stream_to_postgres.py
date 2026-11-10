import json
from pathlib import Path

from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    from_json,
    try_to_timestamp,
    coalesce,
    lit,
    when,
    concat_ws,
    expr,
    udf
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType
)

from postgres_sink import (
    write_batch,
    write_exceptions,
    write_order_staging,
    write_payment_staging
)


# ==================================================
# 1. PROJECT CONFIGURATION
# ==================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

CHECKPOINT_DIR = (
    PROJECT_ROOT
    / "checkpoints"
    / "order_payment_postgres_v1"
)

EXCEPTION_CHECKPOINT_DIR = (
    PROJECT_ROOT
    / "checkpoints"
    / "order_payment_exceptions_v1"
)

ORDER_STAGING_CHECKPOINT = (
    PROJECT_ROOT
    / "checkpoints"
    / "order_staging_v1"
)

PAYMENT_STAGING_CHECKPOINT = (
    PROJECT_ROOT
    / "checkpoints"
    / "payment_staging_v1"
)

READY_FILE = (
    PROJECT_ROOT
    / "checkpoints"
    / "e2e_test_ready.txt"
)

for directory in [
    CHECKPOINT_DIR,
    EXCEPTION_CHECKPOINT_DIR,
    ORDER_STAGING_CHECKPOINT,
    PAYMENT_STAGING_CHECKPOINT
]:
    directory.mkdir(
        parents=True,
        exist_ok=True
    )

if READY_FILE.exists():
    READY_FILE.unlink()


# ==================================================
# 2. JSON VALIDATION
# ==================================================

def is_valid_json(raw_event):

    try:

        json.loads(raw_event)

        return True

    except (
        json.JSONDecodeError,
        TypeError
    ):

        return False


is_valid_json_udf = udf(
    is_valid_json,
    BooleanType()
)


# ==================================================
# 3. SPARK SESSION
# ==================================================

spark = (
    SparkSession.builder
    .appName("CommercePulseKafkaToPostgres")
    .master("local[2]")
    .config(
        "spark.driver.bindAddress",
        "127.0.0.1"
    )
    .config(
        "spark.driver.host",
        "127.0.0.1"
    )
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
    )
    .config(
        "spark.sql.session.timeZone",
        "UTC"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ==================================================
# 4. EVENT SCHEMAS
# ==================================================

order_data_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True)
])


payment_data_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DoubleType(), True)
])


order_event_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("processing_time", StringType(), True),
    StructField("data", order_data_schema, True),
    StructField("_corrupt_record", StringType(), True)
])


payment_event_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("processing_time", StringType(), True),
    StructField("data", payment_data_schema, True),
    StructField("_corrupt_record", StringType(), True)
])


# ==================================================
# 5. READ ORDERS FROM KAFKA
# ==================================================

raw_orders = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "localhost:19092"
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


# ==================================================
# 6. PARSE ORDER EVENTS
# ==================================================

parsed_orders = (
    raw_orders
    .select(
        col("topic").alias("source_topic"),
        col("partition").alias("source_partition"),
        col("offset").alias("source_offset"),
        col("value").cast("string").alias("raw_event"),

        from_json(
            col("value").cast("string"),
            order_event_schema,
            {
                "mode": "PERMISSIVE",
                "columnNameOfCorruptRecord": "_corrupt_record"
            }
        ).alias("event")
    )
    .select(
        "source_topic",
        "source_partition",
        "source_offset",
        "raw_event",

        (
            (~is_valid_json_udf(col("raw_event")))
            | col("event").isNull()
            | col("event._corrupt_record").isNotNull()
        ).alias("parse_failed"),

        "event.*"
    )
)


# ==================================================
# 7. ORDER TIMESTAMP PARSING
# ==================================================

parsed_orders = parsed_orders.withColumn(
    "parsed_order_event_time",
    coalesce(
        try_to_timestamp(col("event_time")),
        try_to_timestamp(
            col("event_time"),
            lit("yyyy-MM-dd HH:mm:ss")
        )
    )
)


# ==================================================
# 8. ORDER VALIDATION
# ==================================================

order_errors = concat_ws(
    "; ",

    when(
        col("parse_failed"),
        lit("Malformed JSON or incompatible event structure")
    ),

    when(
        (~col("parse_failed"))
        & (
            col("event_type").isNull()
            | (col("event_type") == "")
        ),
        lit("Missing event_type")
    ),

    when(
        (~col("parse_failed"))
        & (col("event_type") != "order"),
        lit("Incorrect event_type for orders topic")
    ),

    when(
        (~col("parse_failed"))
        & (
            col("data.order_id").isNull()
            | (col("data.order_id") == "")
        ),
        lit("Missing order_id")
    ),

    when(
        (~col("parse_failed"))
        & (
            col("data.customer_id").isNull()
            | (col("data.customer_id") == "")
        ),
        lit("Missing customer_id")
    ),

    when(
        (~col("parse_failed"))
        & (
            col("data.order_status").isNull()
            | (col("data.order_status") == "")
        ),
        lit("Missing order_status")
    ),

    when(
        (~col("parse_failed"))
        & col("parsed_order_event_time").isNull(),
        lit("Invalid or missing event_time")
    )
)

parsed_orders = parsed_orders.withColumn(
    "error_message",
    order_errors
)


# ==================================================
# 9. INVALID ORDER EVENTS
# ==================================================

invalid_orders = (
    parsed_orders
    .filter(col("error_message") != "")
    .select(
        "source_topic",
        "source_partition",
        "source_offset",
        "event_type",

        col("data.order_id").alias("order_id"),

        "raw_event",

        when(
            col("parse_failed"),
            lit("MALFORMED_JSON")
        ).otherwise(
            lit("INVALID_ORDER_EVENT")
        ).alias("error_category"),

        "error_message"
    )
)


# ==================================================
# 10. VALID ORDER EVENTS FOR STAGING
# ==================================================

valid_orders_for_staging = (
    parsed_orders
    .filter(col("error_message") == "")
    .select(
        "source_topic",
        "source_partition",
        "source_offset",
        "raw_event",

        col("data.order_id").alias("order_id"),
        col("data.customer_id").alias("customer_id"),
        col("data.order_status").alias("order_status"),

        col("parsed_order_event_time").alias(
            "event_time"
        )
    )
)


# ==================================================
# 11. ORIGINAL ORDER STREAM FOR JOIN
# ==================================================

orders = (
    valid_orders_for_staging
    .select(
        "order_id",
        "customer_id",
        "order_status",
        col("event_time").alias("order_event_time")
    )
    .withWatermark(
        "order_event_time",
        "10 minutes"
    )
    .dropDuplicates([
        "order_id"
    ])
)


# ==================================================
# 12. READ PAYMENTS FROM KAFKA
# ==================================================

raw_payments = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "localhost:19092"
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


# ==================================================
# 13. PARSE PAYMENT EVENTS
# ==================================================

parsed_payments = (
    raw_payments
    .select(
        col("topic").alias("source_topic"),
        col("partition").alias("source_partition"),
        col("offset").alias("source_offset"),
        col("value").cast("string").alias("raw_event"),

        from_json(
            col("value").cast("string"),
            payment_event_schema,
            {
                "mode": "PERMISSIVE",
                "columnNameOfCorruptRecord": "_corrupt_record"
            }
        ).alias("event")
    )
    .select(
        "source_topic",
        "source_partition",
        "source_offset",
        "raw_event",

        (
            (~is_valid_json_udf(col("raw_event")))
            | col("event").isNull()
            | col("event._corrupt_record").isNotNull()
        ).alias("parse_failed"),

        "event.*"
    )
)


# ==================================================
# 14. PAYMENT TIMESTAMP PARSING
# ==================================================

parsed_payments = parsed_payments.withColumn(
    "parsed_payment_event_time",
    coalesce(
        try_to_timestamp(col("event_time")),
        try_to_timestamp(
            col("event_time"),
            lit("yyyy-MM-dd HH:mm:ss")
        )
    )
)


# ==================================================
# 15. PAYMENT VALIDATION
# ==================================================

payment_errors = concat_ws(
    "; ",

    when(
        col("parse_failed"),
        lit("Malformed JSON or incompatible event structure")
    ),

    when(
        (~col("parse_failed"))
        & (
            col("event_type").isNull()
            | (col("event_type") == "")
        ),
        lit("Missing event_type")
    ),

    when(
        (~col("parse_failed"))
        & (col("event_type") != "payment"),
        lit("Incorrect event_type for payments topic")
    ),

    when(
        (~col("parse_failed"))
        & (
            col("data.order_id").isNull()
            | (col("data.order_id") == "")
        ),
        lit("Missing order_id")
    ),

    when(
        (~col("parse_failed"))
        & (
            col("data.payment_sequential").isNull()
            | (col("data.payment_sequential") < 1)
        ),
        lit("Invalid payment_sequential")
    ),

    when(
        (~col("parse_failed"))
        & (
            col("data.payment_type").isNull()
            | (col("data.payment_type") == "")
        ),
        lit("Missing payment_type")
    ),

    when(
        (~col("parse_failed"))
        & (
            col("data.payment_installments").isNull()
            | (col("data.payment_installments") < 0)
        ),
        lit("Invalid payment_installments")
    ),

    when(
        (~col("parse_failed"))
        & (
            col("data.payment_value").isNull()
            | (col("data.payment_value") < 0)
        ),
        lit("Invalid payment_value")
    ),

    when(
        (~col("parse_failed"))
        & col("parsed_payment_event_time").isNull(),
        lit("Invalid or missing event_time")
    )
)

parsed_payments = parsed_payments.withColumn(
    "error_message",
    payment_errors
)


# ==================================================
# 16. INVALID PAYMENT EVENTS
# ==================================================

invalid_payments = (
    parsed_payments
    .filter(col("error_message") != "")
    .select(
        "source_topic",
        "source_partition",
        "source_offset",
        "event_type",

        col("data.order_id").alias("order_id"),

        "raw_event",

        when(
            col("parse_failed"),
            lit("MALFORMED_JSON")
        ).otherwise(
            lit("INVALID_PAYMENT_EVENT")
        ).alias("error_category"),

        "error_message"
    )
)


# ==================================================
# 17. VALID PAYMENT EVENTS FOR STAGING
# ==================================================

valid_payments_for_staging = (
    parsed_payments
    .filter(col("error_message") == "")
    .select(
        "source_topic",
        "source_partition",
        "source_offset",
        "raw_event",

        col("data.order_id").alias("order_id"),

        col("data.payment_sequential").alias(
            "payment_sequential"
        ),

        col("data.payment_type").alias(
            "payment_type"
        ),

        col("data.payment_installments").alias(
            "payment_installments"
        ),

        col("data.payment_value").alias(
            "payment_value"
        ),

        col("parsed_payment_event_time").alias(
            "event_time"
        )
    )
)


# ==================================================
# 18. ORIGINAL PAYMENT STREAM FOR JOIN
# ==================================================

payments = (
    valid_payments_for_staging
    .select(
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
        col("event_time").alias("payment_event_time")
    )
    .withWatermark(
        "payment_event_time",
        "10 minutes"
    )
    .dropDuplicates([
        "order_id",
        "payment_sequential"
    ])
)


# ==================================================
# 19. COMBINE INVALID EVENTS
# ==================================================

invalid_events = invalid_orders.unionByName(
    invalid_payments
)


# ==================================================
# 20. EXCEPTION SINK
# ==================================================

def write_exception_batch(batch_df, batch_id):

    print(
        f"\nProcessing exception micro-batch: {batch_id}"
    )

    exception_records = []

    for row in batch_df.toLocalIterator():

        exception_records.append(
            row.asDict(recursive=True)
        )

    inserted = write_exceptions(
        exception_records
    )

    print(
        f"Exception batch {batch_id} completed. "
        f"New exceptions: {inserted}"
    )


# ==================================================
# 21. ORDER-PAYMENT JOIN
# ==================================================

joined_stream = (
    orders.alias("o")
    .join(
        payments.alias("p"),

        (
            col("o.order_id") == col("p.order_id")
        )
        & (
            col("p.payment_event_time")
            >= col("o.order_event_time")
        )
        & (
            col("p.payment_event_time")
            <= (
                col("o.order_event_time")
                + expr("INTERVAL 10 MINUTES")
            )
        ),

        "inner"
    )
    .select(
        col("o.order_id").alias("order_id"),
        col("o.customer_id").alias("customer_id"),
        col("o.order_status").alias("order_status"),

        col("o.order_event_time").alias(
            "order_event_time"
        ),

        col("p.payment_sequential").alias(
            "payment_sequential"
        ),

        col("p.payment_type").alias(
            "payment_type"
        ),

        col("p.payment_installments").alias(
            "payment_installments"
        ),

        col("p.payment_value").alias(
            "payment_value"
        ),

        col("p.payment_event_time").alias(
            "payment_event_time"
        )
    )
)


# ==================================================
# 22. START STREAMING QUERIES
# ==================================================

main_query = None
exception_query = None
order_staging_query = None
payment_staging_query = None

try:

    print(
        "\nStarting CommercePulse streaming pipeline..."
    )

    # ----------------------------------------------
    # MAIN ANALYTICAL QUERY
    # ----------------------------------------------

    main_query = (
        joined_stream.writeStream
        .outputMode("append")
        .foreachBatch(write_batch)
        .option(
            "checkpointLocation",
            str(CHECKPOINT_DIR)
        )
        .start()
    )

    print(
        "Main PostgreSQL streaming query started."
    )

    # ----------------------------------------------
    # ORDER STAGING QUERY
    # ----------------------------------------------

    order_staging_query = (
        valid_orders_for_staging.writeStream
        .outputMode("append")
        .foreachBatch(write_order_staging)
        .option(
            "checkpointLocation",
            str(ORDER_STAGING_CHECKPOINT)
        )
        .start()
    )

    print(
        "Order staging query started."
    )

    # ----------------------------------------------
    # PAYMENT STAGING QUERY
    # ----------------------------------------------

    payment_staging_query = (
        valid_payments_for_staging.writeStream
        .outputMode("append")
        .foreachBatch(write_payment_staging)
        .option(
            "checkpointLocation",
            str(PAYMENT_STAGING_CHECKPOINT)
        )
        .start()
    )

    print(
        "Payment staging query started."
    )

    # ----------------------------------------------
    # DATA QUALITY EXCEPTION QUERY
    # ----------------------------------------------

    exception_query = (
        invalid_events.writeStream
        .outputMode("append")
        .foreachBatch(write_exception_batch)
        .option(
            "checkpointLocation",
            str(EXCEPTION_CHECKPOINT_DIR)
        )
        .start()
    )

    print(
        "Data quality exception query started."
    )

    READY_FILE.write_text(
        "CommercePulse streaming queries started successfully.",
        encoding="utf-8"
    )

    print(
        "\nCommercePulse pipeline is running."
    )

    print(
        "Waiting for Kafka events...\n"
    )

    print("\n========== ACTIVE STREAMING QUERIES =========")
    for query in spark.streams.active:
        print(f"Query Name: {query.name}")
        print(f"Query ID: {query.id}")
        print(f"Is Active: {query.isActive}")
        print(f"Status: {query.status}")
        print("---------------------------------------------")

    spark.streams.awaitAnyTermination()


except KeyboardInterrupt:

    print(
        "\nShutdown requested by user."
    )


finally:

    print(
        "\nStopping CommercePulse streaming queries..."
    )

    for query in [
        main_query,
        order_staging_query,
        payment_staging_query,
        exception_query
    ]:

        if query is not None and query.isActive:
            query.stop()

    spark.stop()

    print(
        "CommercePulse Spark application stopped."
    )