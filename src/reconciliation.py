import pandas as pd
from pathlib import Path


# --------------------------------------------------
# 1. Locate the project and source files
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data" / "raw"

ORDERS_FILE = DATA_DIR / "olist_orders_dataset.csv"

PAYMENTS_FILE = DATA_DIR / "olist_order_payments_dataset.csv"


# --------------------------------------------------
# 2. Load the source datasets
# --------------------------------------------------

orders = pd.read_csv(ORDERS_FILE)

payments = pd.read_csv(PAYMENTS_FILE)


# --------------------------------------------------
# 3. Display the dataset sizes
# --------------------------------------------------

print("ORDERS DATASET")
print(f"Number of orders: {len(orders):,}")

print("\nPAYMENTS DATASET")
print(f"Number of payment records: {len(payments):,}")


# --------------------------------------------------
# 4. Identify orders without payment records
# --------------------------------------------------

# Get the unique order IDs that appear in the payments dataset.
payment_order_ids = payments[["order_id"]].drop_duplicates()

# Find orders whose order_id does not appear in the payments dataset.
unmatched_orders = orders.merge(
    payment_order_ids,
    on="order_id",
    how="left",
    indicator=True
)

unmatched_orders = unmatched_orders[
    unmatched_orders["_merge"] == "left_only"
].drop(columns="_merge")


# --------------------------------------------------
# 5. Display reconciliation results
# --------------------------------------------------

print("\nRECONCILIATION RESULTS")

print(f"Total orders: {len(orders):,}")

print(
    f"Orders with at least one payment record: "
    f"{orders['order_id'].isin(payment_order_ids['order_id']).sum():,}"
)

print(
    f"Orders without a matching payment record: "
    f"{len(unmatched_orders):,}"
)


# --------------------------------------------------
# 6. Display sample unmatched orders
# --------------------------------------------------

print("\nSAMPLE ORDERS WITHOUT PAYMENT RECORDS")

print(
    unmatched_orders[
        ["order_id", "customer_id", "order_status"]
    ].head(10).to_string(index=False)
)



# --------------------------------------------------
# 7. Investigate the unmatched order
# --------------------------------------------------

print("\nUNMATCHED ORDER INVESTIGATION")

# Retrieve the unmatched order.
unmatched_order = unmatched_orders.iloc[0]

unmatched_order_id = unmatched_order["order_id"]

print(f"Order ID: {unmatched_order_id}")
print(f"Customer ID: {unmatched_order['customer_id']}")
print(f"Order Status: {unmatched_order['order_status']}")

# Check whether this order ID appears in the payment dataset.
matching_payments = payments[
    payments["order_id"] == unmatched_order_id
]

print(f"Matching payment records: {len(matching_payments)}")

# Check whether the order ID is missing.
print(
    "Order ID is null:",
    pd.isna(unmatched_order_id)
)

# Display the order's source record.
print("\nSOURCE ORDER RECORD:")

print(
    orders[
        orders["order_id"] == unmatched_order_id
    ].to_string(index=False)
)


# --------------------------------------------------
# 8. Investigate related source datasets
# --------------------------------------------------

print("\nRELATED DATASET INVESTIGATION")

# Load the order-items dataset.
items_file = DATA_DIR / "olist_order_items_dataset.csv"
order_items = pd.read_csv(items_file)

# Load the order-reviews dataset.
reviews_file = DATA_DIR / "olist_order_reviews_dataset.csv"
reviews = pd.read_csv(reviews_file)


# Check whether the unmatched order has order items.
matching_items = order_items[
    order_items["order_id"] == unmatched_order_id
]

print(f"Matching order-item records: {len(matching_items)}")


# Check whether the unmatched order has reviews.
matching_reviews = reviews[
    reviews["order_id"] == unmatched_order_id
]

print(f"Matching review records: {len(matching_reviews)}")


# Display order-item details if they exist.
if not matching_items.empty:
    print("\nORDER ITEM DETAILS")

    print(
        matching_items[
            [
                "order_id",
                "order_item_id",
                "product_id",
                "seller_id",
                "price"
            ]
        ].to_string(index=False)
    )


# Display review details if they exist.
if not matching_reviews.empty:
    print("\nORDER REVIEW DETAILS")

    print(
        matching_reviews[
            [
                "order_id",
                "review_id",
                "review_score"
            ]
        ].to_string(index=False)
    )


# --------------------------------------------------
# 9. Investigate payment data quality
# --------------------------------------------------

print("\nPAYMENT DATA QUALITY INVESTIGATION")

# Check for missing order IDs.
missing_payment_order_ids = payments["order_id"].isna().sum()

print(
    f"Payment records with missing order_id: "
    f"{missing_payment_order_ids:,}"
)


# Check whether the proposed payment identity fields exist.
required_payment_columns = [
    "order_id",
    "payment_sequential"
]

missing_payment_columns = [
    column
    for column in required_payment_columns
    if column not in payments.columns
]

if missing_payment_columns:
    print(
        "Cannot check payment identity duplicates. "
        f"Missing columns: {missing_payment_columns}"
    )

else:
    # Count records that share the proposed payment identity.
    duplicate_payment_mask = payments.duplicated(
        subset=[
            "order_id",
            "payment_sequential"
        ],
        keep=False
    )

    duplicate_payment_records = payments[
        duplicate_payment_mask
    ]

    print(
        "Records sharing an (order_id, payment_sequential) key: "
        f"{len(duplicate_payment_records):,}"
    )

    if not duplicate_payment_records.empty:
        print("\nSAMPLE PAYMENT IDENTITY DUPLICATES")

        print(
            duplicate_payment_records[
                [
                    "order_id",
                    "payment_sequential",
                    "payment_type",
                    "payment_value"
                ]
            ]
            .head(10)
            .to_string(index=False)
        )


# --------------------------------------------------
# 10. Check payment-to-order referential integrity
# --------------------------------------------------

print("\nPAYMENT-TO-ORDER REFERENTIAL INTEGRITY")

# Collect unique order IDs from the orders dataset.
known_order_ids = orders[["order_id"]].drop_duplicates()

# Find payment records whose order_id does not exist
# in the orders dataset.
orphan_payments = payments.merge(
    known_order_ids,
    on="order_id",
    how="left",
    indicator=True
)

orphan_payments = orphan_payments[
    orphan_payments["_merge"] == "left_only"
].drop(columns="_merge")

print(
    "Payment records referencing nonexistent orders: "
    f"{len(orphan_payments):,}"
)

# Display examples if any orphan payments are found.
if not orphan_payments.empty:
    print("\nSAMPLE ORPHAN PAYMENT RECORDS")

    print(
        orphan_payments[
            [
                "order_id",
                "payment_sequential",
                "payment_type",
                "payment_value"
            ]
        ]
        .head(10)
        .to_string(index=False)
    )
else:
    print("No orphan payment records were found.")


# --------------------------------------------------
# 11. Create an investigation register
# --------------------------------------------------

print("\nCREATING INVESTIGATION REGISTER")

# Count order-item records for each order.
order_item_counts = (
    order_items
    .groupby("order_id")
    .size()
    .rename("order_item_count")
)

# Count review records for each order.
review_counts = (
    reviews
    .groupby("order_id")
    .size()
    .rename("review_count")
)

# Start with the orders that have no payment records.
investigation_register = unmatched_orders[
    [
        "order_id",
        "customer_id",
        "order_status"
    ]
].copy()

# Add the issue classification.
investigation_register["issue_type"] = (
    "unmatched_payment_record"
)

# The case remains pending until investigated.
investigation_register["investigation_status"] = (
    "pending"
)

# Add the payment-record count.
investigation_register["payment_record_count"] = (
    investigation_register["order_id"]
    .map(payments["order_id"].value_counts())
    .fillna(0)
    .astype(int)
)

# Add the order-item count.
investigation_register["order_item_count"] = (
    investigation_register["order_id"]
    .map(order_item_counts)
    .fillna(0)
    .astype(int)
)

# Add the review count.
investigation_register["review_count"] = (
    investigation_register["order_id"]
    .map(review_counts)
    .fillna(0)
    .astype(int)
)

# Display the register.
print(
    investigation_register.to_string(index=False)
)

print(
    "\nInvestigation cases created: "
    f"{len(investigation_register):,}"
)


# --------------------------------------------------
# 12. Persist the investigation register
# --------------------------------------------------

print("\nSAVING INVESTIGATION REGISTER")

# Create a directory for reconciliation outputs.
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True
)

# Define the output file.
OUTPUT_FILE = (
    OUTPUT_DIR / "investigation_register.csv"
)

# Save the investigation register.
investigation_register.to_csv(
    OUTPUT_FILE,
    index=False
)

print(f"Investigation register saved to: {OUTPUT_FILE}")

print(
    f"Number of investigation cases saved: "
    f"{len(investigation_register):,}"
)