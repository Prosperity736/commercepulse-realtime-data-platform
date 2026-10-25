import os
import time
from pathlib import Path

import pandas as pd
import psycopg
from dotenv import load_dotenv


# --------------------------------------------------
# 1. Configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

load_dotenv(PROJECT_ROOT / ".env", override=True)

ORDERS_FILE = PROJECT_ROOT / "data" / "raw" / "olist_orders_dataset.csv"
PAYMENTS_FILE = PROJECT_ROOT / "data" / "raw" / "olist_order_payments_dataset.csv"

BATCH_SIZE = 2_000


# --------------------------------------------------
# 2. Database configuration
# --------------------------------------------------

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

required_settings = ("host", "dbname", "user", "password")
missing_settings = [
    key for key in required_settings if not DB_CONFIG.get(key)
]

if missing_settings:
    raise ValueError(
        f"Missing database configuration: {missing_settings}"
    )


# --------------------------------------------------
# 3. Load and validate source datasets
# --------------------------------------------------

def load_source_data():
    if not ORDERS_FILE.is_file():
        raise FileNotFoundError(f"Orders file not found: {ORDERS_FILE}")

    if not PAYMENTS_FILE.is_file():
        raise FileNotFoundError(f"Payments file not found: {PAYMENTS_FILE}")

    print("Loading historical orders and payments...")

    orders = pd.read_csv(ORDERS_FILE)
    payments = pd.read_csv(PAYMENTS_FILE)

    required_order_columns = {
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
    }

    required_payment_columns = {
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
    }

    missing_order_columns = required_order_columns - set(orders.columns)
    missing_payment_columns = required_payment_columns - set(payments.columns)

    if missing_order_columns:
        raise ValueError(
            f"Missing order columns: {sorted(missing_order_columns)}"
        )

    if missing_payment_columns:
        raise ValueError(
            f"Missing payment columns: {sorted(missing_payment_columns)}"
        )

    for name, frame in (("orders", orders), ("payments", payments)):
        if frame["order_id"].isna().any():
            raise ValueError(f"{name} contains missing order_id values.")

        if frame["order_id"].astype("string").str.strip().eq("").any():
            raise ValueError(f"{name} contains blank order_id values.")

    if orders["order_id"].duplicated().any():
        raise ValueError("Orders dataset contains duplicate order_id values.")

    if payments.duplicated(
        subset=["order_id", "payment_sequential"]
    ).any():
        raise ValueError(
            "Payments dataset contains duplicate "
            "(order_id, payment_sequential) keys."
        )

    orders["order_purchase_timestamp"] = pd.to_datetime(
        orders["order_purchase_timestamp"],
        errors="coerce",
    )

    if orders["order_purchase_timestamp"].isna().any():
        invalid_count = int(
            orders["order_purchase_timestamp"].isna().sum()
        )
        raise ValueError(
            f"{invalid_count} order records have invalid purchase timestamps."
        )

    payments["payment_sequential"] = pd.to_numeric(
        payments["payment_sequential"],
        errors="coerce",
    )

    payments["payment_installments"] = pd.to_numeric(
        payments["payment_installments"],
        errors="coerce",
    )

    payments["payment_value"] = pd.to_numeric(
        payments["payment_value"],
        errors="coerce",
    )

    if (
        payments["payment_sequential"].isna().any()
        or (payments["payment_sequential"] % 1 != 0).any()
        or (payments["payment_sequential"] <= 0).any()
    ):
        raise ValueError("Payments contain invalid payment_sequential values.")

    if (
        payments["payment_installments"].isna().any()
        or (payments["payment_installments"] % 1 != 0).any()
        or (payments["payment_installments"] < 0).any()
    ):
        raise ValueError("Payments contain invalid payment_installments values.")

    if (
        payments["payment_value"].isna().any()
        or (payments["payment_value"] < 0).any()
    ):
        raise ValueError("Payments contain missing, invalid, or negative payment values.")

    payments["payment_type"] = payments["payment_type"].astype("string")

    if (
        payments["payment_type"].isna().any()
        or payments["payment_type"].str.strip().eq("").any()
    ):
        raise ValueError("Payments contain missing or blank payment_type values.")

    print(f"Orders loaded:   {len(orders):,}")
    print(f"Payments loaded: {len(payments):,}")

    return orders, payments


# --------------------------------------------------
# 4. Join payments to orders
# --------------------------------------------------

def prepare_backfill(orders, payments):
    enriched = payments.merge(
        orders[
            [
                "order_id",
                "customer_id",
                "order_status",
                "order_purchase_timestamp",
            ]
        ],
        on="order_id",
        how="left",
        validate="many_to_one",
        indicator=True,
    )

    unmatched = enriched["_merge"].eq("left_only")
    unmatched_count = int(unmatched.sum())

    print(f"Payments without a matching order: {unmatched_count:,}")

    valid = enriched.loc[~unmatched].copy()
    valid["payment_event_time"] = valid["order_purchase_timestamp"]

    valid["payment_sequential"] = valid["payment_sequential"].astype("int64")
    valid["payment_installments"] = valid["payment_installments"].astype("int64")
    valid["payment_type"] = valid["payment_type"].astype(str)

    print(f"Joined payment records prepared: {len(valid):,}")

    return valid, unmatched_count


# --------------------------------------------------
# 5. Write records to PostgreSQL
# --------------------------------------------------

INSERT_SQL = """
    INSERT INTO order_payment_events (
        order_id,
        customer_id,
        order_status,
        order_event_time,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value,
        payment_event_time
    )
    VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s
    )
    ON CONFLICT (order_id, payment_sequential)
    DO NOTHING
    RETURNING event_id;
"""


def write_to_postgres(enriched):
    inserted_count = 0
    existing_count = 0
    start = time.perf_counter()

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            records = []

            def write_batch(batch):
                nonlocal inserted_count, existing_count

                for record in batch:
                    cur.execute(INSERT_SQL, record)

                    if cur.fetchone() is None:
                        existing_count += 1
                    else:
                        inserted_count += 1

            for row in enriched.itertuples(index=False):
                records.append(
                    (
                        row.order_id,
                        row.customer_id,
                        row.order_status,
                        row.order_purchase_timestamp.to_pydatetime(),
                        int(row.payment_sequential),
                        row.payment_type,
                        int(row.payment_installments),
                        float(row.payment_value),
                        row.payment_event_time.to_pydatetime(),
                    )
                )

                if len(records) >= BATCH_SIZE:
                    write_batch(records)
                    records.clear()

            if records:
                write_batch(records)

            cur.execute("SELECT COUNT(*) FROM order_payment_events;")
            total_in_table = cur.fetchone()[0]

    duration = time.perf_counter() - start

    return (
        inserted_count,
        existing_count,
        total_in_table,
        duration,
    )


# --------------------------------------------------
# 6. Execute the historical backfill
# --------------------------------------------------

def main():
    orders, payments = load_source_data()

    enriched, unmatched_count = prepare_backfill(orders, payments)

    print("\nStarting PostgreSQL historical backfill...")

    inserted, existing, total_in_table, duration = write_to_postgres(
        enriched
    )

    print("\nHISTORICAL BACKFILL SUMMARY")
    print("-" * 45)
    print(f"Source orders:                   {len(orders):,}")
    print(f"Source payment records:          {len(payments):,}")
    print(f"Matched payment records:         {len(enriched):,}")
    print(f"Payments without matching order: {unmatched_count:,}")
    print(f"New records inserted:            {inserted:,}")
    print(f"Existing records skipped:        {existing:,}")
    print(f"Total rows currently in table:   {total_in_table:,}")
    print(f"Execution time:                  {duration:.2f} seconds")
    print("-" * 45)
    print(
        "Historical payment_event_time uses order_purchase_timestamp "
        "because the source payment dataset has no separate payment timestamp."
    )


if __name__ == "__main__":
    main()
