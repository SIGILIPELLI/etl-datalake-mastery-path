---
description: "Data Sources & Ingestion Patterns — Before you can write a line of transform logic, you need data flowing in. This lesson covers the shapes data sources…"
---

# 02 · Data Sources & Ingestion Patterns

Before you can write a line of transform logic, you need data flowing in.
This lesson covers the shapes data sources come in and the two fundamental
ingestion patterns — **batch** and **streaming** — that every pipeline
chooses between (or, often, mixes).

!!! note "What actually ran"
    Code below uses the Python standard library (`csv`, `time`, `json`) —
    no external services or network calls.

## The three source shapes

Most sources you'll ingest from fall into one of these categories:

- **Files** — CSVs dropped in a folder, JSON exports, logs, Parquet files
  someone else's pipeline produced.
- **APIs** — REST/GraphQL endpoints returning JSON, usually paginated and
  sometimes rate-limited.
- **Databases** — an operational Postgres/MySQL table you need a copy of for
  analytics, without hammering the production system.

Each shape has a different "am I done reading?" signal: a file ends at EOF,
an API tells you via pagination metadata (or you paginate until an empty
page), a database query returns a fixed result set — or, for ongoing
ingestion, you track a watermark (see Level 2's incremental loads lesson).

## Batch: ingest on a schedule, in chunks

Batch ingestion reads a bounded chunk of data — "everything since last
run," or "today's file" — on a schedule (hourly, nightly). It's the default
choice for the majority of analytics pipelines because it's simple, easy to
retry, and easy to reason about.

```python
import csv, io

# Simulates "today's file" landing in a folder, one batch at a time
daily_files = {
    "2026-08-27.csv": "order_id,amount\n1,50.00\n2,30.00\n",
    "2026-08-28.csv": "order_id,amount\n3,20.00\n4,45.50\n",
}

def process_batch(filename, content):
    rows = list(csv.DictReader(io.StringIO(content)))
    total = sum(float(r["amount"]) for r in rows)
    print(f"Batch {filename}: {len(rows)} rows, ${total:.2f} total")
    return rows

all_rows = []
for filename, content in daily_files.items():
    all_rows.extend(process_batch(filename, content))

print(f"Ingested {len(all_rows)} rows across {len(daily_files)} batches")
```

```text
Batch 2026-08-27.csv: 2 rows, $80.00 total
Batch 2026-08-28.csv: 2 rows, $65.50 total
Ingested 4 rows across 2 batches
```

Each batch is a complete, self-contained unit of work — if batch
`2026-08-28.csv` fails halfway through, you re-run *that batch*, not the
whole history. This is why batch pipelines are usually organized by a
natural boundary (a day, an hour, a file) rather than one giant continuous
stream of rows.

## Streaming: ingest as events arrive

Streaming ingestion processes each record (or small micro-batch of records)
as soon as it's available, rather than waiting for a scheduled window. It
trades simplicity for lower latency — useful when "we found out about this
order 6 hours late" is a real business problem (fraud detection, live
dashboards, alerting).

```python
import time

# Simulates events arriving one at a time, as they would from a message
# queue (Kafka, Kinesis, Pub/Sub) — here just a Python generator standing
# in for that queue.
def event_stream():
    events = [
        {"order_id": 1, "amount": 50.00, "ts": "10:00:01"},
        {"order_id": 2, "amount": 30.00, "ts": "10:00:04"},
        {"order_id": 3, "amount": 20.00, "ts": "10:00:09"},
    ]
    for e in events:
        yield e

running_total = 0.0
for event in event_stream():
    running_total += event["amount"]
    print(f"[{event['ts']}] processed order {event['order_id']}, "
          f"running total ${running_total:.2f}")
```

```text
[10:00:01] processed order 1, running total $50.00
[10:00:04] processed order 2, running total $80.00
[10:00:09] processed order 3, running total $100.00
```

In a real streaming system, `event_stream()` would be a consumer reading
from Kafka/Kinesis/Pub-Sub, and processing would keep running indefinitely —
there's no "end of file." That has a real consequence: streaming pipelines
need a different mental model for correctness (what happens if you crash
between processing an event and acknowledging it?) which Level 3's streaming
lessons cover in depth.

## Batch vs. streaming: the real tradeoff

| | Batch | Streaming |
|---|---|---|
| Latency | Minutes to a day | Seconds or less |
| Complexity | Lower — bounded, retryable units | Higher — unbounded, needs checkpointing |
| Cost model | Runs briefly, on a schedule | Runs continuously, needs always-on infra |
| Failure recovery | Re-run the failed batch | Resume from last checkpoint/offset |
| Typical tools | Cron, Airflow, plain scripts | Kafka, Kinesis, Flink, Spark Structured Streaming |

A large majority of real pipelines are batch, because most business
questions ("how did we do yesterday?") don't need sub-minute latency, and
batch is dramatically simpler to build, test, and debug. Reach for streaming
only when the *business* requires low latency — not because it sounds more
sophisticated.

## Traps

- **Streaming for its own sake.** If nightly batch answers the business
  question, streaming adds operational complexity (always-on infrastructure,
  checkpointing, harder debugging) for no benefit.
- **Batch windows that don't match a natural boundary.** Splitting a batch
  arbitrarily (every 10,000 rows, regardless of source structure) makes
  failures harder to reason about than splitting by day/file/source unit.
- **Ignoring what "done" means for a source.** For an API, forgetting to
  check for an empty next-page token means you either loop forever or stop
  one page early — always confirm the source's actual end-of-data signal.

## Cheat sheet

| Source | Typical ingestion pattern | "Am I done?" signal |
|---|---|---|
| Flat files | Batch | End of file / all files in folder processed |
| REST API | Batch (with pagination) | Empty page / no `next` token |
| Operational database | Batch (scheduled query) or CDC (Level 2) | Query completes / no new watermark rows |
| Message queue (Kafka, etc.) | Streaming | Never — runs continuously |

## How It Actually Works

Underneath every ingestion pattern is the same mechanical question: **how does the source
system decide what to hand you, and how does that decision get made durable enough to
survive a crash on either side?**

- **Full extraction** issues a query (or file listing) that scans the *entire* source dataset
  every run. Mechanically the source database walks its storage (a full table scan or an
  index scan across all rows) and streams a result set back over a cursor; the ingestion
  client pages through that cursor in fixed-size batches (e.g. `fetchmany(10000)`) to avoid
  holding the whole result in memory. There is no bookkeeping between runs — correctness
  comes from re-deriving state from zero every time, which is simple but means cost scales
  with total source size, not with what changed.
- **Incremental/watermark extraction** instead tracks a monotonically increasing column
  (an `updated_at` timestamp or auto-increment `id`) as a **watermark** persisted by the
  ingestion tool itself, outside the source system. Each run issues `WHERE updated_at >
  :last_watermark`, which the source's query planner can satisfy with an index range scan
  instead of a full scan — the mechanical win is turning an O(n) scan into an O(delta) scan.
  The correctness risk is entirely in *when* the watermark is advanced: if you advance it
  before confirming the batch landed downstream, a mid-batch failure silently drops rows
  written between the old and new watermark on retry.
- **Event/webhook-driven ingestion** flips the pull model to push: the source system itself
  fires an HTTP callback or writes to a message broker (Kafka, SNS/SQS) the instant a change
  commits. The mechanical guarantee you actually get depends on the broker's delivery
  semantics — Kafka's at-least-once delivery with consumer offset commits means your
  ingestion code will occasionally see the same event twice after a consumer restart, so
  idempotent upserts (keyed by an event ID) are not optional, they are how correctness is
  achieved despite duplicate delivery, not despite the pattern.
- **File-drop ingestion** relies on the filesystem or object store's own change notification
  (S3 `ObjectCreated` events, or a directory listener polling `mtime`) to trigger a load; the
  mechanical hazard is partial writes — a listener firing on a file that a producer is still
  uploading will read truncated bytes, which is why production pipelines write to a temp key
  and atomically rename/move only after the upload completes.

## 🔀 Related lessons on other tracks

- [Tableau — 02 · Connecting to Data Sources](https://sigilipelli.github.io/tableau-mastery-path/level-1/02-connecting-data-sources/)
- [Terraform — 05 · Resources & Data Sources](https://sigilipelli.github.io/terraform-mastery-path/level-1/05-resources-data-sources/)

## Exercise

Extend the batch example so that it also tracks, per batch, the **minimum
and maximum** `amount` seen — print a one-line summary per batch showing
count, total, min, and max. Then write a short paragraph (3-4 sentences)
arguing whether the daily-order-totals pipeline in this lesson should stay
batch or move to streaming, using the tradeoff table above to justify your
answer.
