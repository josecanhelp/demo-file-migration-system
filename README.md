# file-migration-system

Moves files out of an aging MySQL-backed document store and into a new home: object storage
for the bytes, Postgres for metadata, OCR text pulled from a third-party vendor API along the
way. It runs two lanes at once. A backfill lane walks the existing table end to end so nothing
already sitting there waits on new activity. A CDC lane tails the MySQL binlog so a row written,
updated, or deleted after the migration starts lands in the target system within seconds, not
whenever the backfill happens to reach it. Both lanes share one vendor rate budget, one circuit
breaker, and one retry policy, so a flaky OCR vendor slows the migration down instead of
corrupting it.

A dashboard shows the pipeline moving in real time: files entering, getting claimed, coming back
from OCR, landing in the target store, and (on demand) a vendor outage tripping the breaker and
the pipeline holding steady until it clears.

## Quickstart

```bash
cp .env.example .env
docker compose up
```

Wait for the containers to report healthy (`docker compose ps`), then open
**http://localhost:8080**. On a cold start the seeder loads 500 synthetic files into MySQL, the
backfill coordinator plans and drains them, and by the time the dashboard finishes its first
few stats ticks the pipeline should already show all 500 as stored.

## Dashboard walkthrough

The dashboard is one page, served by the control plane at port 8080, arranged top to bottom as
Controls, Pipeline, Freshness, and Recently stored. Four things worth doing:

**Add files and watch them travel.** The Controls panel at the top has four buttons: **Add 1
file**, **Add 10 files**, **Add 100 files**, and **Add 1,000 files**. Each inserts that many rows
directly into `sourcedb.files`, the same table any real application write would land in, so the
new rows are picked up by Debezium off the binlog like anything else; a bulk click still issues
one multi-row `INSERT`, not one request per file. Below, the Pipeline panel lays out six columns
left to right, always in that order and never wrapping: Original files (source), Picked up
(captured), Waiting in line (queued), In progress (claimed), Reading the text (text extraction,
OCR), and Stored, plus a Set aside row for files that hit repeated errors. A plain sentence under
each heading explains what that step means. On a narrow window the column strip scrolls
sideways instead of stacking into extra rows, so the left-to-right order always reads the same
way.

Every chip is paced to sit in each column for a moment before advancing, so even a file that
finishes in well under a second still visibly steps through the whole pipeline; a note next to
the legend says so. That pacing is purely visual: every count on screen still comes straight from
`/api/stats` on the same tick it always did, so a slow-moving chip never makes a count lag behind
reality.

**Watch the columns generally.** Original files is the row count in MySQL. Picked up is a
tally tracked in your browser of capture events seen since the page loaded, from either lane
(the live lane noticing a row through the binlog, or the backfill lane noticing one by scanning
the source table), not a status bucket, since capture is a transient point between the source
table and a claim rather than a status any row holds. Waiting in line, In progress, Reading the
text, and Stored come straight from `migration_state` counts by status, so they never drift from
the ledger. The Set aside row lists files stuck at `FAILED_PERMANENT`; the explanation under it
is accurate, since a later update to that same row (on the live lane) revives it and the chip
moves back into the in-flight columns. The row itself stays hidden while that count is zero and
only appears once a file genuinely lands there.

**Drive the vendor into failure and watch the breaker hold the line.** The Controls panel has a
vendor mode selector with two plain-language options, Vendor: working and Vendor: offline, which
send the vendor mock's real `healthy` and `down` modes underneath (the vendor mock also still
answers to `slow`, `rate_limited`, and `erroring`, kept for the integration tests that exercise
them, just not offered on the dashboard). Switch it to Vendor: offline and add a few files.
Within a handful of failed calls the breaker pill in the top bar flips to OPEN, and every Kafka
consumer container pauses rather than continuing to fail and retry against a vendor that has
already told you it's unavailable. The files already in flight sit where they are; nothing is
lost, nothing is retried into a wall. Switch the mode back to Vendor: working; the breaker moves
to HALF_OPEN, lets a few trial calls through, and once those succeed it closes and every paused
consumer resumes on its own. The files queued up during the outage drain normally once it does.
Since only `healthy` and `down` are reachable from the dashboard, a vendor problem is always
classified transient there and never counts toward a file's retry cap, so the Set aside row
should stay empty in ordinary dashboard use; a file the vendor rejects outright as unreadable is
a genuine `FAILED_PERMANENT` regardless of vendor mode and still lands there.

**Check freshness and browse what landed.** The Freshness panel (service level agreement, SLA)
shows how long the oldest unfinished live-lane file has been waiting, against an alert and a
breach marker. Below it, Recently stored shows the most recently completed files, newest first,
each card showing the filename, where it landed (its object key), the extracted text, the OCR
confidence, and its actual measured end-to-end duration, taken from that file's own event history
rather than the pipeline's paced animation. A link to the MinIO console is available from the
panel header to look at a stored object directly.

**Run a reconciliation.** Click **Run reconciliation** (or `POST /api/reconcile`, see below). A
`clean: true` result means the source table, the ledger, and the target document table agree not
just on row counts but on the actual id sets and content: every source row has a matching document
and ledger row and no extra ones exist on either side, every stored checksum and OCR text matches
what the source blob actually produces, and no row is currently `FAILED_PERMANENT`.

## Configuration

Every setting below is a `.env` variable (see `.env.example`) with the default shown. All of it
is plumbed through `docker-compose.yml`.

| Variable | Default | Controls |
| --- | --- | --- |
| `SEED_FILE_COUNT` | `500` | Target row count the one-shot seeder tops `sourcedb.files` up to. Already-present rows are left alone, so re-running the seeder against a table with 500 rows and a target of 500 does nothing. |
| `SEED_FILE_SIZE_BYTES` | `2048` | Size in bytes of each synthetic file the seeder generates. |
| `SEED_BATCH_SIZE` | `500` | Rows per multi-row `INSERT` while seeding. |
| `SEED_PROGRESS_LOG_INTERVAL` | `5000` | How often (in rows) the seeder logs progress. |
| `SEED_CONNECT_RETRIES` | `30` | How many times the seeder retries its first MySQL connection, a second apart, before giving up. A passing healthcheck does not guarantee the server accepts a connection the instant it reports healthy. |
| `VENDOR_RATE_LIMIT_RPS` | `50` | The vendor mock's combined request budget per second, and the ceiling the migrator's rate limiter enforces across both lanes. |
| `VENDOR_BATCH_SIZE` | `25` | Files per OCR call, on both sides: the vendor mock rejects a batch larger than this, and it is the chunk size the backfill coordinator uses when calling it. The CDC lane calls the vendor with one id per envelope and never chunks, since each envelope already represents a single row change. |
| `VENDOR_LATENCY_MS` | `150` | Simulated per-call latency in the vendor's healthy mode; its slow mode multiplies this by 20. |
| `VENDOR_FAILURE_MODE` | `healthy` | The vendor mock's boot-time chaos mode: `healthy`, `slow`, `rate_limited`, `erroring`, or `down`. Changeable at runtime via `POST /admin/mode` without a restart; the dashboard's own vendor mode selector only offers `healthy` (labeled Vendor: working) and `down` (labeled Vendor: offline), the two the integration tests do not already cover on their own. |
| `VENDOR_BASE_URL` | `http://vendor-mock:8088` | Where the migrator and the control plane reach the vendor mock. |
| `VENDOR_CONNECT_TIMEOUT_MS` | `2000` | Connect timeout for a vendor call. |
| `VENDOR_READ_TIMEOUT_MS` | `10000` | Read timeout for a vendor call; `slow` mode's 20x latency multiplier is tuned to stay under this so a slow vendor is observably slow rather than simply timing out. |
| `CDC_RESERVED_RATE_PCT` | `20` | Percentage of `VENDOR_RATE_LIMIT_RPS` reserved exclusively for the CDC lane. The backfill lane is capped at the remaining share, and a shared permit pool caps the two lanes combined, so a saturated backfill can never starve CDC and the two lanes together never exceed the vendor's real budget. |
| `BACKFILL_RANGE_SIZE` | `1000` | Size of each id range the backfill coordinator plans across the source table. |
| `BACKFILL_RANGE_LEASE_SECONDS` | `300` | How long a coordinator instance holds a claimed range before another pass can reclaim it as abandoned. |
| `BACKFILL_PLAN_INTERVAL_SECONDS` | `30` | How often the coordinator checks the source table for new ranges to plan and re-checks for claimable ones. |
| `BACKFILL_NACK_BACKOFF_SECONDS` | `30` | Backoff before a failed backfill message is redelivered. |
| `CDC_NACK_BACKOFF_SECONDS` | `10` | Backoff before a failed CDC envelope is redelivered. |
| `CDC_TOPIC_PARTITIONS` | `3` | Partition count for the CDC Kafka topic. |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Broker address every Kafka client in the stack connects to. |
| `BACKFILL_TOPIC_PARTITIONS` | `3` | Partition count for the backfill Kafka topic. |
| `WORKER_CONCURRENCY` | `8` | Kafka listener concurrency per lane inside `migrator-worker`, and an input to the JDBC pool size (see `DB_POOL_SIZE`). |
| `MICROBATCH_MAX_WAIT_MS` | `500` | Reserved for tuning how long a consumer batches records before processing; not currently wired to any consumer, which claims each record as it arrives rather than batching by wait time. |
| `SLA_TARGET_SECONDS` | `3600` | The Freshness panel's breach marker: how long a live-lane file may sit unresolved before the migration is missing its service level agreement (SLA) target. |
| `SLA_ALERT_SECONDS` | `1800` | The Freshness panel's earlier alert marker, meant to fire before `SLA_TARGET_SECONDS` is reached. |
| `BREAKER_FAILURE_RATE_THRESHOLD` | `50` | Failure rate percentage that trips the vendor circuit breaker open. |
| `BREAKER_OPEN_DURATION_SECONDS` | `10` | How long the breaker stays open before moving to half-open and letting trial calls through. |
| `MAX_RETRY_ATTEMPTS` | `5` | Two things at once: the Governor's per-call retry cap for a single vendor invocation, and the consecutive-failures cap past which a file is condemned to `FAILED_PERMANENT` (see the architecture doc for why that counter is not the same as lifetime attempts). |
| `GOVERNOR_RETRY_BASE_BACKOFF_MS` | `200` | Base delay for the Governor's exponential backoff between retries, before jitter. |
| `DLQ_TOPIC_PARTITIONS` | `3` | Partition count for the dead-letter Kafka topic. |
| `RECONCILE_BATCH_SIZE` | `500` | Page size the reconciler uses while walking the source, ledger, and document tables. |
| `DB_POOL_SIZE` | `0` | JDBC pool size per datasource (source and target each get their own pool at this size). `0` means auto-size to `WORKER_CONCURRENCY + 4`, enough for every worker thread to hold a connection with headroom for housekeeping queries. |
| `CLAIM_LEASE_SECONDS` | `60` | How long a claimed file stays owned before another attempt may treat it as abandoned and reclaim it. |
| `CLAIM_RENEW_INTERVAL_SECONDS` | `10` | How often an in-progress claim is renewed, keeping the lease well short of the time an actual crash takes to detect. |
| `CONTROL_PLANE_PORT` | `8080` | Port the control plane (API, SSE stream, dashboard) listens on. |
| `MIGRATOR_BASE_URL` | `http://migrator-worker:8082` | Where the control plane reaches a migrator worker to proxy the reconcile action. With more than one worker replica this resolves round-robin across all of them; reconcile is stateless against the shared databases, so it doesn't matter which one answers. |
| `MINIO_CONSOLE_URL` | `http://localhost:9001` | The MinIO console address the dashboard's Recently stored panel links to. Served to the browser from `GET /api/config`, so it stays correct even when the console is reachable at an address other than the default. |
| `EVENT_POLL_INTERVAL_MS` | `500` | How often the control plane polls `migration_event` for new rows to push over SSE. |
| `EVENT_POLL_LIMIT` | `500` | Max rows fetched per poll. |
| `SSE_MAX_EVENTS_PER_TICK` | `200` | Max events forwarded to browsers per tick; anything past this is dropped for that tick and counted in the "dropped by the server" label rather than queued up. |
| `STATS_STREAM_INTERVAL_MS` | `1000` | How often the control plane recomputes and broadcasts the stats snapshot over SSE, and skips the query entirely when no browser is connected. |

## Throughput arithmetic

The vendor is the bottleneck. Every file needs one OCR call, and the vendor's own rate limit
(`VENDOR_RATE_LIMIT_RPS`) times its batch size (`VENDOR_BATCH_SIZE`) is what sets the ceiling on
how many files per second can actually clear the pipeline, not worker count, not Kafka partition
count, not anything else in this stack. At 15 million files:

| Effective throughput | Time to migrate 15,000,000 files |
| --- | --- |
| 10 files/s | ~17 days |
| 100 files/s | ~42 hours |
| 1,000 files/s | ~4.2 hours |

Same architecture, three wildly different projects, depending entirely on what the vendor
contract and rate limit actually allow. That is exactly why every number in the table above
(vendor rate limit, batch size, worker concurrency, lane reservation, retry and breaker tuning)
is a configuration value read from the environment and not a constant baked into the code: the
system that migrates 500 seeded files in a demo and the system that migrates 15 million files
against a production vendor contract are the same jar, run with different numbers.

## Scaling workers

```bash
docker compose up --scale migrator-worker=4
```

Runs four instances of the worker profile side by side, each consuming the same Kafka consumer
groups (`backfill` and `cdc`), so partitions rebalance across all four and each instance claims
and processes a different slice of the backlog concurrently. It demonstrates that horizontal
scaling is exactly that: point more workers at the same topics and the same rate-limited vendor
budget, and the shared budget (`VENDOR_RATE_LIMIT_RPS`) still caps total throughput across all of
them, since adding workers increases parallelism in claiming and OCR-processing files, not the
vendor's willingness to accept more calls per second. The coordinator and every other service
stay at one instance; only `migrator-worker` is meant to be run more than one of.

`migrator-worker` publishes its HTTP port (`8082` in the container) to an ephemeral host port
rather than a fixed one, since a fixed host port mapping can only ever be bound by one of the
replicas: every other service reaches it by its internal Docker DNS name instead, which resolves
round-robin across however many replicas are running, so nothing inside the compose network cares
which host port got picked. `docker compose port migrator-worker 8082` looks up whichever port a
given replica actually landed on, useful if you want to reach one specific instance directly.

## Verifying a migration

```bash
curl -X POST http://localhost:8080/api/reconcile
```

Or click **Run reconciliation** on the dashboard. The result reports row counts across the
source, ledger, and document tables, plus explicit lists: checksum mismatches, OCR text
mismatches, missing or unreadable objects, missing or orphaned document rows, missing or orphaned
ledger rows, and current permanent failures. `clean: true` means every one of those lists is
empty and all three row counts agree, which is a stronger claim than the counts merely matching:
a deleted source row and an unrelated orphan row can make two counts agree by coincidence while
one real file is missing and another is stale, and the id-set checks are what catch that.

## Running the tests

Unit tests for the migrator (no infrastructure required):

```bash
cd services/migrator && mvn test
```

Integration tests for the migrator (the stack must already be up via `docker compose up`, since
these tests hit the real MySQL, Postgres, MinIO, Kafka, vendor mock, and the real Debezium
connector):

```bash
docker compose stop migrator-worker migrator-coordinator
cd services/migrator && mvn verify
```

Stop `migrator-worker` and `migrator-coordinator` first, every time, even though `docker compose up`
starts them. Every integration test that needs a worker or a reconciler wires one up directly
inside its own test JVM rather than depending on the containerized one, and the containerized
worker and coordinator go right on consuming the real backfill and cdc topics in the background
regardless. Since every id an integration test seeds is a real row in MySQL, the real Debezium
connector captures it into the real `cdc.sourcedb.files` topic no matter which suite's reserved id
range it falls in, and a live worker or coordinator claims and processes it on its own retry cap,
independently of the test that seeded it. That can condemn a test's own id to `FAILED_PERMANENT`
before the test's own consumer ever claims it, which fails the test for a reason that has nothing
to do with what it is actually checking. The suite refuses to guess around this: each integration
test class that writes to the source database checks, before it does anything else, whether a
containerized worker or coordinator is running, and fails immediately with a one-line message
naming the `docker compose stop` command above if it finds one, rather than letting the failure
surface later as an unexplained retry-cap error.

`docker compose up` still starts `migrator-worker` and `migrator-coordinator` unchanged, since
demoing the project depends on them actually migrating the seeded corpus; only running the
integration suite against that same stack requires stopping them first.

Reconcile tests build their own `ReconcileService` directly against the same JDBC and object store
connections the rest of that test class uses, rather than going through the control plane's HTTP
proxy to a running `migrator-worker`, so they need nothing beyond the databases and object store
already listed above.

Tests for the control plane:

```bash
cd services/control-plane && yarn test
```
