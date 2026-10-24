# CommercePulse — Real-Time E-Commerce Intelligence Platform

CommercePulse is a data engineering project that processes e-commerce order and payment events using Apache Kafka, Apache Spark Structured Streaming, and PostgreSQL. The project uses the Brazilian Olist e-commerce dataset to explore event-driven data processing, order-payment matching, data quality, and exception recovery.

## Project Objectives

* Ingest order and payment events through Apache Kafka.
* Process and join streams using Spark Structured Streaming.
* Store enriched order-payment records in PostgreSQL.
* Identify and investigate data quality issues.
* Preserve failed events for investigation and recovery.
* Prepare validated data for business intelligence reporting.

## Technology Stack

* **Python:** Data ingestion and pipeline utilities.
* **Apache Kafka:** Event ingestion and messaging.
* **Apache Spark Structured Streaming:** Stream processing and event-time joins.
* **PostgreSQL:** Persistent storage, investigation records, and recovery tracking.
* **Power BI:** Planned business intelligence and reporting layer.
* **Git:** Source control and change tracking.

## Project Structure

```text
commercepulse-realtime-kafka/
├── data/
│   ├── raw/                       # Local Olist source datasets
│   └── processed/                 # Generated investigation data
├── checkpoints/                   # Local Spark streaming checkpoints
├── src/
│   ├── producer/
│   │   ├── kafka_producer.py      # Order event producer
│   │   └── payment_producer.py    # Payment event producer
│   ├── streaming/
│   │   └── stream_join.py         # Spark order-payment stream join
│   ├── utils/
│   │   └── event_recovery.py      # Failed-event preservation utility
│   ├── reconciliation.py          # Data quality investigations
│   └── load_investigations_to_postgres.py
├── create_streaming_table.py
├── create_unprocessed_events_table.py
├── test_event_recovery.py
├── requirements.txt
├── .env                           # Local configuration; not committed
└── README.md
```

## PostgreSQL Tables

### `investigation_cases`

Stores data quality investigation cases identified during reconciliation.

### `order_payment_events`

Designed to store enriched order-payment records produced by matching orders and payments. The table includes a uniqueness constraint on `(order_id, payment_sequential)`.

### `unprocessed_events`

Stores events requiring investigation or recovery, including their source coordinates, original payload, failure reason, and resolution status.

## Event Processing

The current Spark streaming implementation reads from the `olist-orders` and `olist-payments` Kafka topics, parses event envelopes, deduplicates events, and joins records by `order_id` and event-time conditions.

The stream currently writes joined results to the console. PostgreSQL persistence from the streaming query remains to be implemented and verified.

The event recovery utility stores failed events in PostgreSQL and publishes them to the `commercepulse-dead-letter` Kafka topic. Full automatic detection and recovery integration remains future work.

## Configuration

Store local database and Kafka configuration in the root `.env` file. Do not commit credentials or other secrets.

Required PostgreSQL environment variables:

```text
DB_HOST
DB_PORT
DB_NAME
DB_USER
DB_PASSWORD
```

The default Kafka bootstrap server used by the current application is:

```text
localhost:19092
```

## Current Development Status

Implemented components include:

* Kafka order and payment producers.
* Spark Structured Streaming order-payment join.
* PostgreSQL investigation and streaming table definitions.
* Investigation register generation and loading.
* Failed-event preservation utility.
* Initial Git repository and project baseline.

Remaining work includes:

* A reliable historical backfill strategy.
* PostgreSQL persistence for Spark join results.
* End-to-end reconciliation of processed records.
* Complete invalid, unmatched, and late-event recovery.
* Power BI reporting.

## Development Principles

* Preserve existing working components.
* Keep database credentials out of source control.
* Make database writes idempotent where appropriate.
* Validate data against the source datasets.
* Verify results before describing a pipeline stage as complete.
