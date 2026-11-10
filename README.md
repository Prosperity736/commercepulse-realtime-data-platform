# CommercePulse – Real-Time E-Commerce Intelligence Platform

CommercePulse is a real-time data engineering platform built to ingest, process, validate, reconcile and analyse e-commerce order and payment events.

The project uses the Brazilian Olist e-commerce dataset to demonstrate event-driven data processing, stream ingestion, event-time joins, PostgreSQL persistence, data quality management and automated failed-event recovery.

The platform is built around Apache Kafka, Apache Spark Structured Streaming, Python and PostgreSQL.

## Project Objectives

- Ingest historical e-commerce order and payment records into Kafka.
- Process order and payment events using Spark Structured Streaming.
- Perform event-time-based order-payment matching.
- Persist validated events and analytical records in PostgreSQL.
- Detect malformed, invalid and unmatched events.
- Preserve failed events for investigation and recovery.
- Maintain publishing and recovery records for traceability.
- Reconcile processed records against the original source datasets.
- Prepare reliable analytical data for business intelligence reporting.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Event producers, validation, recovery and pipeline utilities |
| Apache Kafka 4.2.2 | Event ingestion and message distribution |
| Apache Spark 4.2.0 | Structured Streaming and event-time processing |
| PostgreSQL 18 | Staging, analytical storage, publishing ledger and investigations |
| Pandas | Dataset preparation and reconciliation |
| SQL | Data validation, investigation and analytical queries |
| Power BI | Planned business intelligence layer |
| Docker | Container infrastructure for future orchestration |
| Apache Airflow | Planned workflow orchestration |
| Git | Version control |

## Architecture

The current processing architecture follows this sequence:

1. **Source Data:** Historical Olist order and payment datasets.
2. **Event Production:** Python producers validate records and publish event envelopes to Kafka.
3. **Event Ingestion:** Kafka distributes order and payment events through separate topics.
4. **Stream Processing:** Spark Structured Streaming consumes, parses and validates incoming events.
5. **Data Quality:** Invalid events are routed to a PostgreSQL exception table.
6. **Staging:** Valid order and payment events are persisted in separate PostgreSQL staging tables.
7. **Stream Joining:** Order and payment events are matched using order identifiers and event-time conditions.
8. **Analytical Storage:** Joined records are written to PostgreSQL.
9. **Recovery:** Failed publishing events are preserved and processed through a recovery worker.
10. **Reconciliation:** Processed records are compared with source data, and unresolved cases are documented.
11. **Business Intelligence:** Validated data will support future Power BI reporting.

## Event Processing and Streaming

The platform currently consumes events from two Kafka topics:

- `olist-orders`
- `olist-payments`

Spark Structured Streaming performs the following operations:

- JSON parsing and schema validation.
- Event-type validation.
- Order and payment field validation.
- Timestamp parsing.
- Malformed-event detection.
- Event deduplication.
- Kafka source metadata preservation.
- Event-time-based order-payment matching.
- PostgreSQL batch persistence.

The order-payment join uses a 10-minute event-time window and watermarks.

Four independent streaming queries handle:

- Analytical order-payment records.
- Validated order staging.
- Validated payment staging.
- Data-quality exceptions.

Each query maintains its own Spark checkpoint directory to preserve streaming progress and support recovery.

## Reliable Event Publishing and Recovery

CommercePulse includes a PostgreSQL-backed publishing ledger and a failed-event recovery mechanism.

The publishing workflow tracks events through their publishing lifecycle:

- Pending.
- Published.
- Failed.

Events are marked as published after Kafka acknowledges successful delivery.

Failed events are preserved in PostgreSQL for subsequent recovery.

The recovery worker uses database-backed claims, lease ownership, heartbeat handling, retry scheduling and deterministic event identity checks.

The recovery process preserves the original event payload rather than reconstructing a different event during replay.

The delivery model is **at-least-once**, with downstream idempotency used to protect against duplicate processing. Kafka publishing and PostgreSQL ledger updates are not one atomic transaction.

## PostgreSQL Data Management

PostgreSQL serves as the persistent storage and investigation layer.

Key tables include:

| Table | Purpose |
|---|---|
| `order_event_staging` | Validated order events |
| `payment_event_staging` | Validated payment events |
| `order_payment_events` | Joined order-payment analytical records |
| `data_quality_exceptions` | Malformed and invalid event records |
| `producer_failed_events` | Events requiring publishing recovery |
| `kafka_publish_ledger` | Publishing status and event tracking |
| `investigation_cases` | Unmatched and unresolved business records |
| `unprocessed_events` | Events requiring further investigation |

Database writes use appropriate uniqueness constraints and conflict-handling logic to support idempotent downstream processing.

## Data Reconciliation and Validation

The project uses the original Olist datasets as the reference for reconciliation.

### Historical source dataset

| Metric | Records |
|---|---:|
| Total orders | 99,441 |
| Unique order IDs | 99,441 |
| Total payment records | 103,886 |
| Unique orders with payments | 99,440 |

### Kafka publishing validation

| Event type | Published events |
|---|---:|
| Orders | 99,441 |
| Payments | 103,886 |
| **Total** | **203,327** |

An initial batch of 21 order publishing failures was successfully recovered.

Recovery validation reported:

- Claimed: 21
- Resolved: 21
- Retried: 0
- Rejected: 0
- Errors: 0

### PostgreSQL validation

| Dataset | Records | Unique business records |
|---|---:|---:|
| Order staging | 99,442 | 99,441 orders |
| Payment staging | 103,886 | 99,440 orders |
| Analytical table | 103,886 | 99,440 orders |

The additional order staging record represents a duplicate delivery, consistent with at-least-once processing. The downstream analytical table retains the intended unique order-payment combinations.

### Unmatched order investigation

Source reconciliation identified exactly one order without a corresponding payment record:

`bfbd0f9bdef84302105ad712db648a6c`

The record was added to the investigation register for review rather than being assigned a fabricated payment.

This preserves the distinction between a genuine source-data issue and a processing failure.

## Checkpoint Management and Recovery

Spark Structured Streaming maintains separate checkpoints for its four active queries:

- `order_payment_postgres_v1`
- `order_staging_v1`
- `payment_staging_v1`
- `order_payment_exceptions_v1`

Kafka source offsets and Spark committed offsets were checked and found to match the available log-end positions across the relevant partitions.

Checkpoint directories and Kafka data are maintained locally and excluded from Git version control.

**Important:** Existing checkpoints must be preserved when restarting the streaming application. Deleting or resetting them can cause previously processed Kafka records to be consumed again.

## Configuration and Security

Local configuration is stored in environment files and is not committed to source control.

The root `.env` file contains PostgreSQL connection settings and other local configuration.

Required PostgreSQL variables include:

```text
DB_HOST
DB_PORT
DB_NAME
DB_USER
DB_PASSWORD
```

The Kafka bootstrap server currently used by the application is:

```text
localhost:19092
```

Credentials, local datasets, Spark checkpoints, logs and temporary development artifacts are excluded from Git.

## Current Development Status

### Completed

- Kafka order and payment event producers.
- Historical event publication.
- PostgreSQL-backed publishing ledger.
- Failed-event preservation and recovery worker.
- Event validation and deterministic event identity.
- Spark Structured Streaming ingestion.
- Order and payment event parsing.
- Data-quality exception handling.
- Order and payment staging.
- Event-time-based order-payment joining.
- PostgreSQL analytical persistence.
- Investigation register.
- Source-to-database reconciliation.
- Kafka and Spark offset validation.

### Planned

- Apache Airflow orchestration.
- Scheduled reconciliation and reporting workflows.
- Expanded operational monitoring.
- Power BI business intelligence dashboards.
- Further late-arriving event and recovery scenarios.

Airflow is intended to orchestrate batch workflows, validation, reconciliation and reporting. It will complement the existing continuous Kafka and Spark streaming services rather than replace them.

## Development Principles

- Preserve existing working pipeline components.
- Protect Kafka data and Spark checkpoints.
- Keep credentials and sensitive configuration out of source control.
- Use deterministic event identities where appropriate.
- Design downstream database writes to be idempotent.
- Preserve original event payloads during recovery.
- Maintain source metadata for traceability.
- Reconcile pipeline outputs against original datasets.
- Distinguish genuine source-data exceptions from processing failures.
- Verify results before declaring a milestone complete.

---

**Project:** CommercePulse – Real-Time E-Commerce Intelligence Platform

**Current milestone:** Reliable Event Ingestion, Recovery & Data Reconciliation.