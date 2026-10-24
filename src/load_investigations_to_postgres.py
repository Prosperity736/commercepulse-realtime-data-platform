import os
import sys
import time
from pathlib import Path
from datetime import datetime

import pandas as pd
import psycopg
from dotenv import load_dotenv


# --------------------------------------------------
#  Load project configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

ENV_FILE = PROJECT_ROOT / ".env"

load_dotenv(ENV_FILE, override=True)

print("Environment file exists:", ENV_FILE.is_file())
print("Database password loaded:", bool(os.getenv("DB_PASSWORD")))


# --------------------------------------------------
#  Define database configuration
# --------------------------------------------------

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}


# --------------------------------------------------
# Validate database configuration
# --------------------------------------------------

required_db_settings = [
    "host",
    "dbname",
    "user",
    "password",
]

missing_db_settings = [
    setting
    for setting in required_db_settings
    if not DB_CONFIG.get(setting)
]

if missing_db_settings:
    raise ValueError(
        "Missing required database settings: "
        f"{missing_db_settings}"
    )

print("Database configuration validation: PASSED")


# --------------------------------------------------
#  Locate the investigation register
# --------------------------------------------------

INPUT_FILE = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "investigation_register.csv"
)




# --------------------------------------------------
#  Validate and load the investigation register
# --------------------------------------------------

if not INPUT_FILE.is_file():
    raise FileNotFoundError(
        f"Investigation register not found: {INPUT_FILE}"
    )

investigations = pd.read_csv(INPUT_FILE)

print("\nINVESTIGATION REGISTER")
print(f"Records loaded: {len(investigations):,}")


# Required columns
REQUIRED_COLUMNS = {
    "order_id",
    "customer_id",
    "order_status",
    "issue_type",
    "investigation_status",
    "payment_record_count",
    "order_item_count",
    "review_count",
}


# Check whether required columns exist
missing_columns = REQUIRED_COLUMNS - set(investigations.columns)

if missing_columns:
    raise ValueError(
        "Investigation register is missing required columns: "
        f"{sorted(missing_columns)}"
    )

print("Required column validation: PASSED")


# --------------------------------------------------
# Validate order IDs
# --------------------------------------------------

missing_order_ids = (
    investigations["order_id"].isna()
    | investigations["order_id"].astype("string").str.strip().eq("")
)

if missing_order_ids.any():
    invalid_count = int(missing_order_ids.sum())

    raise ValueError(
        f"Investigation register contains {invalid_count} "
        "record(s) with missing or blank order_id values."
    )

print("Order ID validation: PASSED")


# --------------------------------------------------
# Validate investigation statuses
# --------------------------------------------------

VALID_INVESTIGATION_STATUSES = {
    "pending",
    "investigating",
    "resolved",
}

invalid_statuses = (
    investigations["investigation_status"].isna()
    | ~investigations["investigation_status"].isin(
        VALID_INVESTIGATION_STATUSES
    )
)

if invalid_statuses.any():
    invalid_values = (
        investigations.loc[
            invalid_statuses,
            "investigation_status"
        ]
        .unique()
        .tolist()
    )

    raise ValueError(
        "Invalid investigation status values found: "
        f"{invalid_values}. "
        f"Allowed values: {sorted(VALID_INVESTIGATION_STATUSES)}"
    )

print("Investigation status validation: PASSED")



# --------------------------------------------------
# Validate numeric evidence counts
# --------------------------------------------------

COUNT_COLUMNS = [
    "payment_record_count",
    "order_item_count",
    "review_count",
]

for column in COUNT_COLUMNS:

    numeric_values = pd.to_numeric(
        investigations[column],
        errors="coerce"
    )

    invalid_values = (
        numeric_values.isna()
        | (numeric_values < 0)
        | (numeric_values % 1 != 0)
    )

    if invalid_values.any():
        invalid_rows = investigations.index[invalid_values].tolist()

        raise ValueError(
            f"Invalid values found in '{column}' "
            f"at DataFrame row(s): {invalid_rows}. "
            "Counts must be non-negative integers."
        )

    investigations[column] = numeric_values.astype("int64")

print("Numeric evidence validation: PASSED")



# --------------------------------------------------
# Validate duplicate investigation keys
# --------------------------------------------------

KEY_COLUMNS = [
    "order_id",
    "issue_type",
]

duplicate_keys = investigations.duplicated(
    subset=KEY_COLUMNS,
    keep=False,
)

if duplicate_keys.any():
    duplicate_records = investigations.loc[
        duplicate_keys,
        KEY_COLUMNS,
    ]

    raise ValueError(
        "Duplicate investigation keys found in the CSV:\n"
        f"{duplicate_records.to_string(index=False)}"
    )

print("Duplicate investigation key validation: PASSED")


# --------------------------------------------------
# Validate issue types
# --------------------------------------------------

VALID_ISSUE_TYPES = {
    "unmatched_payment_record",
}

invalid_issue_types = (
    investigations["issue_type"].isna()
    | investigations["issue_type"].astype("string").str.strip().eq("")
    | ~investigations["issue_type"].isin(VALID_ISSUE_TYPES)
)

if invalid_issue_types.any():
    invalid_values = (
        investigations.loc[
            invalid_issue_types,
            "issue_type"
        ]
        .unique()
        .tolist()
    )

    raise ValueError(
        "Invalid issue_type values found: "
        f"{invalid_values}. "
        f"Allowed values: {sorted(VALID_ISSUE_TYPES)}"
    )

print("Issue type validation: PASSED")


# --------------------------------------------------
#  SQL: Insert new investigation cases
# --------------------------------------------------

insert_sql = """
    INSERT INTO investigation_cases (
        order_id,
        customer_id,
        order_status,
        issue_type,
        investigation_status,
        payment_record_count,
        order_item_count,
        review_count
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (order_id, issue_type)
    DO NOTHING
    RETURNING case_id;
"""


# --------------------------------------------------
# SQL: Update changed evidence
# --------------------------------------------------

update_sql = """
    UPDATE investigation_cases
    SET
        customer_id = %s,
        order_status = %s,
        payment_record_count = %s,
        order_item_count = %s,
        review_count = %s,
        updated_at = CURRENT_TIMESTAMP
    WHERE
        order_id = %s
        AND issue_type = %s
        AND (
            customer_id,
            order_status,
            payment_record_count,
            order_item_count,
            review_count
        ) IS DISTINCT FROM (
            %s, %s, %s, %s, %s
        )
    RETURNING case_id;
"""


# --------------------------------------------------
#  Load investigation records into PostgreSQL
# --------------------------------------------------

def load_investigations():

    # Start the execution timer.
    start_time = time.perf_counter()

     # Record when the loading process starts.
    print(
        f"\nLoad started at: "
        f"{datetime.now().astimezone().isoformat(timespec='seconds')}"
    )

    # Stop if there are no records to process.
    if investigations.empty:
        print("No investigation records to load.")
        return

    # Initialize operation counters.
    inserted_count = 0
    updated_count = 0
    unchanged_count = 0

    # Connect to PostgreSQL.
    with psycopg.connect(**DB_CONFIG) as connection:

        with connection.cursor() as cursor:

            # Process each investigation record.
            for row in investigations.itertuples(index=False):

                # Prepare the values for the INSERT statement.
                record = (
                    row.order_id,
                    row.customer_id,
                    row.order_status,
                    row.issue_type,
                    row.investigation_status,
                    int(row.payment_record_count),
                    int(row.order_item_count),
                    int(row.review_count),
                )

                # ------------------------------------------
                #  Attempt to insert a new case.
                # ------------------------------------------

                cursor.execute(insert_sql, record)

                result = cursor.fetchone()

                # If a case ID is returned, insertion succeeded.
                if result is not None:
                    inserted_count += 1
                    continue

                # ------------------------------------------
                #  Case already exists.
                # Compare and update its evidence.
                # ------------------------------------------

                update_record = (
                    row.customer_id,
                    row.order_status,
                    int(row.payment_record_count),
                    int(row.order_item_count),
                    int(row.review_count),
                    row.order_id,
                    row.issue_type,
                    row.customer_id,
                    row.order_status,
                    int(row.payment_record_count),
                    int(row.order_item_count),
                    int(row.review_count),
                )

                cursor.execute(update_sql, update_record)

                update_result = cursor.fetchone()

                # An updated case returns its case ID.
                if update_result is not None:
                    updated_count += 1

                # No returned row means the evidence matched.
                else:
                    unchanged_count += 1

            # ------------------------------------------
            #  Count all cases currently in PostgreSQL.
            # ------------------------------------------

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM investigation_cases;
                """
            )

            total_cases = cursor.fetchone()[0]

    # ------------------------------------------
    #  Print the loading summary.
    # ------------------------------------------
    end_time = time.perf_counter()

    duration = end_time - start_time

    print("\nLOADING SUMMARY")
    print("-" * 40)

    print(f"New cases inserted: {inserted_count}")
    print(f"Existing cases updated: {updated_count}")
    print(f"Unchanged cases: {unchanged_count}")

    print("-" * 40)

    print(f"Total cases currently in PostgreSQL: {total_cases:,}")
    print(f"Execution duration: {duration:.2f} seconds")

    # Record the completion timestamp.
    print(
    f"Load finished at: "
    f"{datetime.now().astimezone().isoformat(timespec='seconds')}"
    )
    print("Status: SUCCESS")

# --------------------------------------------------
# 9. Execute the loader
# --------------------------------------------------

if __name__ == "__main__":
    try:
        load_investigations()

    except psycopg.Error as error:
        print("\nDATABASE ERROR")
        print(f"Type: {type(error).__name__}")
        print(f"Details: {error}", file=sys.stderr)
        sys.exit(1)
