# CommercePulse: Reliable Event Ingestion, Recovery and Data Reconciliation

## 1. Milestone Overview

This milestone establishes the reliable event ingestion, streaming, persistence, recovery and reconciliation capabilities of CommercePulse.

CommercePulse processes Brazilian Olist e-commerce order and payment data using Python, Apache Kafka, Apache Spark Structured Streaming and PostgreSQL.

The objective is to build an event-driven data pipeline that can ingest historical records, process events continuously, validate incoming data, preserve failed events, recover publishing failures and reconcile processed records against their original source datasets.

### Milestone scope

- Historical order and payment event publishing.
- Kafka publishing ledger.
- Failed-event preservation and recovery.
- Spark Structured Streaming ingestion.
- Event schema and data-quality validation.
- PostgreSQL staging and analytical persistence.
- Event-time-based order-payment matching.
- Source-to-database reconciliation.
- Investigation of unmatched business records.
- Kafka offset and Spark checkpoint validation.

---

## 2. Technology Stack

| Technology | Implementation |
|---|---|
| Python | Event producers, validation, recovery and reconciliation |
| Apache Kafka 4.2.2 | Event ingestion and distribution |
| Apache Spark 4.2.0 | Structured Streaming and event-time processing |
| PostgreSQL 18 | Staging, analytical storage and recovery tracking |
| Pandas | Dataset processing and reconciliation |
| SQL | Data validation and investigation |
| Git | Source control |

---

## 3. Architecture and Data Flow

The pipeline follows this processing sequence:

1. Historical Olist order and payment datasets provide the source records.
2. Python producers validate records and construct event envelopes.
3. Events are published to separate Kafka topics.
4. A PostgreSQL publishing ledger tracks publishing status.
5. Spark Structured Streaming consumes the Kafka topics.
6. Incoming events are parsed and validated.
7. Invalid events are routed to the PostgreSQL data-quality exception table.
8. Valid order and payment events are persisted in separate staging tables.
9. Spark performs event-time-based order-payment matching.
10. Joined records are persisted in the PostgreSQL analytical table.
11. Failed publishing events are preserved and processed by a recovery worker.
12. Reconciliation compares the source datasets with the resulting database records.
13. Unmatched records are registered for investigation.

### Kafka topics

- `olist-orders`
- `olist-payments`

### Spark streaming queries

The streaming application maintains four independent queries:

| Query | Responsibility | Checkpoint |
|---|---|---|
| Main analytical query | Joined order-payment persistence | `order_payment_postgres_v1` |
| Order staging | Validated order persistence | `order_staging_v1` |
| Payment staging | Validated payment persistence | `payment_staging_v1` |
| Exception processing | Invalid-event persistence | `order_payment_exceptions_v1` |

Each query maintains its own checkpoint directory.

---

## 4. Historical Event Publishing

The historical producers publish order and payment records to Kafka.

### Source datasets

| Dataset | Total records |
|---|---:|
| Olist orders | 99,441 |
| Olist payments | 103,886 |

The producers use shared event schemas and deterministic event identities.

The publishing ledger tracks event status and prevents unnecessary republication of events already recorded as published.

An event is marked as published after Kafka acknowledges successful delivery.

### Publishing results

| Event type | Published events |
|---|---:|
| Orders | 99,441 |
| Payments | 103,886 |
| **Total** | **203,327** |

---

## 5. Failed Event Preservation and Recovery

CommercePulse includes a PostgreSQL-backed mechanism for preserving events that fail during publishing.

The recovery architecture consists of:

- `producer_recovery.py`
- `producer_recovery_worker.py`
- `publishing_ledger.py`

The recovery worker uses database-backed event claims, lease ownership, heartbeat handling and retry scheduling.

It preserves the original event payload and checks deterministic event identity before replay.

### Recovery validation

The initial order publishing attempt encountered 21 failures.

The recovery worker subsequently processed these records.

| Recovery metric | Result |
|---|---:|
| Claimed | 21 |
| Resolved | 21 |
| Retried | 0 |
| Rejected | 0 |
| Errors | 0 |

All 21 previously failed order events were successfully recovered.

### Delivery semantics

The pipeline uses at-least-once delivery.

Kafka publishing and PostgreSQL ledger updates are not one atomic transaction. Therefore, duplicate delivery remains possible in certain failure scenarios.

Downstream idempotency and database uniqueness constraints are used to protect analytical records from duplicate processing.

---

## 6. Spark Structured Streaming

The main streaming application is:

`src/streaming/stream_to_postgres.py`

It consumes events from `olist-orders` and `olist-payments`.

### Event processing

The application performs:

- JSON parsing.
- Event schema validation.
- Event-type validation.
- Order and payment field validation.
- Timestamp parsing.
- Malformed-event detection.
- Kafka source metadata preservation.
- Event deduplication.
- Event-time-based order-payment matching.
- PostgreSQL persistence.

### Data-quality handling

Invalid events are separated from valid records.

The application distinguishes malformed JSON from structurally readable but invalid business events.

For traceability, exception records retain:

- Source topic.
- Source partition.
- Source offset.
- Event type.
- Order identifier where available.
- Original raw event.
- Error category.
- Error message.

### Event-time matching

The order-payment join uses:

- Matching `order_id`.
- Payment event time greater than or equal to order event time.
- Payment event time no more than 10 minutes after order event time.

The join uses an inner join.

A 10-minute watermark is applied to both streams.

This design constrains the matching window and streaming state. Late-arriving events outside the supported event-time window may not participate in the join.

---

## 7. PostgreSQL Persistence

PostgreSQL provides the persistent storage layer for validated events, analytical records, exceptions and recovery tracking.

Important tables include:

| Table | Responsibility |
|---|---|
| `order_event_staging` | Validated order events |
| `payment_event_staging` | Validated payment events |
| `order_payment_events` | Joined analytical records |
| `data_quality_exceptions` | Invalid and malformed events |
| `producer_failed_events` | Failed publishing events |
| `kafka_publish_ledger` | Publishing status and event tracking |
| `investigation_cases` | Unmatched and unresolved business records |
| `unprocessed_events` | Events requiring further investigation |

The analytical table uses a uniqueness constraint on:

`(order_id, payment_sequential)`

The database sink uses conflict-handling logic to support idempotent analytical persistence.

---

## 8. Source-to-Database Reconciliation

The original Olist datasets were used as the reference for validating the pipeline.

### Original source metrics

| Metric | Count |
|---|---:|
| Total orders | 99,441 |
| Unique order IDs | 99,441 |
| Total payment records | 103,886 |
| Unique orders with payments | 99,440 |

### PostgreSQL staging and analytical results

| Dataset | Records | Unique orders |
|---|---:|---:|
| Order staging | 99,442 | 99,441 |
| Payment staging | 103,886 | 99,440 |
| Analytical table | 103,886 | 99,440 |

The additional order staging record represents a duplicate delivery of an order event.

This is consistent with at-least-once event processing.

The analytical table retains the expected payment records without creating additional analytical payment rows from the duplicate order delivery.

---

## 9. Unmatched Order Investigation

Source reconciliation identified one genuine unmatched order.

**Order ID:**

`bfbd0f9bdef84302105ad712db648a6c`

The original source contains 99,441 unique orders but only 99,440 unique orders with payment records.

The unmatched order was added to the investigation register.

Its investigation record includes:

- Order status: delivered.
- Issue type: `unmatched_payment_record`.
- Investigation status: pending.
- Payment record count: 0.
- Order item count: 3.
- Review count: 1.

The record remains unresolved because the source dataset contains no corresponding payment record.

No artificial payment event was created to force reconciliation.

This distinction is important: a genuine source-data exception should not be incorrectly classified as a pipeline failure.

---

## 10. Kafka Offset and Spark Checkpoint Validation

The four active Spark checkpoint directories were preserved.

The committed Spark source offsets were compared with the Kafka log-end offsets.

The comparison showed that the committed offsets matched the available Kafka log-end positions across the relevant partitions for all four active streaming queries.

This provides evidence that the streaming queries had committed progress through the available Kafka records at the time of validation.

Checkpoint directories:

- `order_payment_postgres_v1`
- `order_staging_v1`
- `payment_staging_v1`
- `order_payment_exceptions_v1`

Older checkpoint directories were also preserved.

### Checkpoint preservation

Existing checkpoint directories must not be deleted, reset or reformatted during normal project development.

They contain the streaming progress information required for recovery.

---

## 11. Known Data-Quality and Audit Records

The database also contains deliberately generated test records used to validate exception handling.

At the time of validation:

- Five pending payment staging records represented deliberate test events.
- Eleven data-quality exception records represented deliberate malformed or invalid test events.

These records were preserved as test and audit evidence rather than being blindly deleted or marked as successfully processed.

---

## 12. Reliability and Design Considerations

The current implementation incorporates several reliability mechanisms:

1. Deterministic event identity.
2. Kafka acknowledgements before publishing-ledger success updates.
3. PostgreSQL-backed failed-event preservation.
4. Recovery worker leases and heartbeat handling.
5. Original event payload preservation during recovery.
6. Separate Spark checkpoints for independent queries.
7. Kafka source metadata preservation.
8. PostgreSQL uniqueness constraints.
9. Idempotent analytical writes.
10. Source-to-database reconciliation.

The pipeline uses at-least-once delivery rather than claiming end-to-end exactly-once processing.

The separation between Kafka publishing and PostgreSQL state updates means that recovery and downstream idempotency remain important.

---

## 13. Current Milestone Status

### Completed and validated

- Historical order event publishing.
- Historical payment event publishing.
- Kafka publishing ledger.
- Recovery of 21 failed order events.
- Shared event schemas and validation.
- Spark Structured Streaming ingestion.
- Order and payment event parsing.
- Invalid-event classification.
- PostgreSQL order staging.
- PostgreSQL payment staging.
- Event-time-based analytical joining.
- PostgreSQL analytical persistence.
- Data-quality exception persistence.
- Reconciliation against original source datasets.
- Investigation of the unmatched source order.
- Kafka offset and Spark checkpoint validation.

### Planned development

- Apache Airflow orchestration.
- Scheduled reconciliation workflows.
- Automated validation reporting.
- Expanded operational monitoring.
- Power BI analytical dashboards.

Airflow is intended to orchestrate batch workflows, reconciliation, validation and reporting. It will complement the existing continuous Kafka and Spark streaming services.

---

## 14. Project Development Principles

- Preserve existing working pipeline components.
- Protect Kafka data and Spark checkpoint directories.
- Keep database credentials out of source control.
- Preserve original event identity during recovery.
- Design downstream database writes to be idempotent.
- Retain source metadata for traceability.
- Investigate genuine source-data exceptions rather than manufacturing missing records.
- Reconcile processed records against original datasets.
- Verify results before declaring a milestone complete.

---

**Project:** CommercePulse – Real-Time E-Commerce Intelligence Platform

**Milestone:** Reliable Event Ingestion, Recovery and Data Reconciliation

**Next development phase:** Workflow orchestration with Apache Airflow.
