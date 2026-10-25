
from datetime import datetime
from decimal import Decimal

import psycopg
from pyspark.sql import SparkSession

from src.streaming.postgres_sink import DB_CONFIG, write_batch


# --------------------------------------------------
# 1. Define a uniquely identifiable test record
# --------------------------------------------------

TEST_ORDER_ID = "commercepulse_sink_test_20261024"

test_record = {
    "order_id": TEST_ORDER_ID,
    "customer_id": "commercepulse_test_customer",
    "order_status": "test",
    "order_event_time": datetime(2026, 10, 24, 12, 0, 0),
    "payment_sequential": 1,
    "payment_type": "test",
    "payment_installments": 1,
    "payment_value": Decimal("1.00"),
    "payment_event_time": datetime(2026, 10, 24, 12, 0, 0),
}


def count_test_records(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM order_payment_events
            WHERE order_id = %s
              AND payment_sequential = %s
            """,
            (TEST_ORDER_ID, 1),
        )
        return cur.fetchone()[0]


# --------------------------------------------------
# 2. Start Spark and create a one-row DataFrame
# --------------------------------------------------

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("CommercePulsePostgresSinkTest")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    schema = """
        order_id STRING,
        customer_id STRING,
        order_status STRING,
        order_event_time TIMESTAMP,
        payment_sequential INT,
        payment_type STRING,
        payment_installments INT,
        payment_value DECIMAL(12, 2),
        payment_event_time TIMESTAMP
    """

    test_df = spark.createDataFrame([test_record], schema=schema)

    # Remove a stale record only if it belongs to this exact test ID.
    # This allows the test to be rerun safely.
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM order_payment_events
                WHERE order_id = %s
                  AND payment_sequential = %s
                """,
                (TEST_ORDER_ID, 1),
            )

    # --------------------------------------------------
    # 3. Test insertion
    # --------------------------------------------------

    print("\nTEST 1: Insert a new record")
    write_batch(test_df, batch_id=9001)

    with psycopg.connect(**DB_CONFIG) as conn:
        first_count = count_test_records(conn)

    print("Records after first insert:", first_count)
    assert first_count == 1, "Expected exactly one inserted test record."

    # --------------------------------------------------
    # 4. Test duplicate protection
    # --------------------------------------------------

    print("\nTEST 2: Submit the same record again")
    write_batch(test_df, batch_id=9002)

    with psycopg.connect(**DB_CONFIG) as conn:
        second_count = count_test_records(conn)

    print("Records after second insert:", second_count)
    assert second_count == 1, "Duplicate protection failed."

    print("\nPASS: Insert and duplicate protection verified.")

finally:
    # --------------------------------------------------
    # 5. Clean up only this test record
    # --------------------------------------------------

    try:
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM order_payment_events
                    WHERE order_id = %s
                      AND payment_sequential = %s
                    """,
                    (TEST_ORDER_ID, 1),
                )

        print("Test record cleanup completed.")

    finally:
        spark.stop()
