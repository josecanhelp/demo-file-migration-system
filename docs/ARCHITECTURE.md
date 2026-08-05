# Architecture

## Component overview

```mermaid
flowchart LR
    subgraph Source
        MySQL[(MySQL sourcedb.files)]
        Debezium[Debezium Kafka Connect]
    end

    subgraph Kafka
        CDCTopic[[cdc.sourcedb.files]]
        BackfillTopic[[files.backfill]]
        DlqTopic[[files.dlq]]
    end

    Seeder[seeder] -->|inserts rows| MySQL
    MySQL -->|binlog| Debezium
    Debezium -->|change envelopes, blob column excluded| CDCTopic

    Coordinator[migrator-coordinator] -->|reads id ranges| MySQL
    Coordinator -->|publishes id chunks| BackfillTopic

    CDCTopic --> Worker
    BackfillTopic --> Worker

    subgraph Worker[migrator-worker, N replicas]
        Governor[Governor: rate limiter, circuit breaker, retry]
    end

    Worker -->|fetches blob by id| MySQL
    Worker --> Governor
    Governor -->|OCR batch call| Vendor[vendor-mock]
    Worker -->|permanently failed ids| DlqTopic
    Worker -->|object bytes| MinIO[(MinIO documents bucket)]
    Worker -->|ledger + document rows + events| Postgres[(Postgres targetdb)]

    ControlPlane[control-plane] -->|reads state, inserts new files| MySQL
    ControlPlane -->|reads state, stats, events| Postgres
    ControlPlane -->|admin mode, health| Vendor
    ControlPlane -->|reconcile proxy| Worker
    ControlPlane -->|SSE + REST| Dashboard[browser dashboard]
```

## Request flow: a file added from the dashboard

```mermaid
sequenceDiagram
    participant Browser
    participant ControlPlane as control-plane
    participant MySQL
    participant Debezium
    participant Kafka as cdc.sourcedb.files
    participant Worker as migrator-worker
    participant Vendor as vendor-mock
    participant Postgres
    participant MinIO

    Browser->>ControlPlane: POST /api/files
    ControlPlane->>MySQL: INSERT INTO files (...)
    MySQL-->>ControlPlane: sourceId
    ControlPlane-->>Browser: 201 { sourceId }
    Browser->>ControlPlane: GET /api/trace/:sourceId (polling)

    MySQL->>Debezium: binlog row-created event
    Debezium->>Kafka: change envelope (op=c, blob excluded)
    Kafka->>Worker: consume envelope
    Worker->>Postgres: seedPending, record CDC_CAPTURED
    Worker->>Postgres: claim() -> IN_FLIGHT, record CLAIMED
    Worker->>MySQL: fetch blob content by id
    Worker->>Vendor: POST /v1/ocr/batch (through Governor)
    Vendor-->>Worker: OCR text, confidence, page count
    Worker->>Postgres: save OCR payload, record OCR_DONE
    Worker->>MinIO: PUT object at files/{sourceId}
    Worker->>Postgres: upsert document row, markDone, record STORED
    Worker->>Kafka: acknowledge envelope

    Postgres-->>ControlPlane: migration_event rows (polled)
    ControlPlane-->>Browser: SSE pipeline + stats events
```

## Postgres tables

| Table | Holds |
| --- | --- |
| `document` | The migrated record, one row per source id: filename, content type, object key, byte size, `checksum_sha256`, OCR text, confidence, page count, vendor job id, `migrated_at`. |
| `migration_state` | The ledger: lane, status, `attempts`, `consecutive_failures`, `source_version`, `source_created_at`, checksum, cached `ocr_payload`, last error. |
| `migration_event` | Append-only stage trace: source id, stage, lane, JSON detail, timestamp. |
| `backfill_checkpoint` | One row per planned id range, keyed by `(range_start, range_end)`, with a status and `claimed_at`. Claims use `FOR UPDATE SKIP LOCKED`. |

`migration_state.status` is one of `PENDING`, `IN_FLIGHT`, `OCR_DONE`, `DONE`, `FAILED_RETRYABLE`,
`FAILED_PERMANENT`. The happy path runs `PENDING` to `IN_FLIGHT` to `OCR_DONE` to `DONE`.

## Known limits

- **Single Kafka broker, no replication.** Every topic is created with `replicas: 1` (`KafkaTopicConfig`), and the compose file runs one Kafka container in KRaft mode. A broker failure loses whatever has not been consumed. A real deployment needs a multi-broker cluster with a replication factor above one.