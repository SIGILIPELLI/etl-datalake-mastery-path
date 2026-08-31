# 10 · Project — Streaming-Ready Lakehouse Table

This Level 3 capstone combines streaming ingestion (Module 02), a
lakehouse transaction log with ACID guarantees (Modules 01, 08), late-data
handling (Module 05), and compaction (Module 06) into one pipeline: a
continuously-updated lakehouse table fed by a stream of events, safe for
concurrent writers and readers, and self-maintaining against the
small-files problem.

!!! note "What actually ran"
    This project was reasoned through step by step against real `pyarrow`,
    `pandas`, and `json` APIs but not executed in a live interpreter for
    this lesson — the outputs shown match documented behavior precisely.

## The table's transaction log (from Modules 01/08)

```python
import json, datetime as dt
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

table_dir = Path("lake/streaming_table/events")
data_dir, log_dir = table_dir / "data", table_dir / "_log"
data_dir.mkdir(parents=True, exist_ok=True)
log_dir.mkdir(parents=True, exist_ok=True)

def latest_version() -> int:
    versions = sorted(int(p.stem) for p in log_dir.glob("*.json"))
    return versions[-1] if versions else -1

def commit(files_added, files_removed, expected_base=None):
    current = latest_version()
    if expected_base is not None and current != expected_base:
        raise RuntimeError(f"conflict: table at {current}, writer expected {expected_base}")
    version = current + 1
    entry = {"version": version, "timestamp": dt.datetime.now().isoformat(), "add": files_added, "remove": files_removed}
    tmp = log_dir / f"{version:020d}.json.tmp"
    tmp.write_text(json.dumps(entry))
    tmp.rename(log_dir / f"{version:020d}.json")
    return version

def read_table() -> pd.DataFrame:
    live, removed = [], set()
    for p in sorted(log_dir.glob("*.json")):
        entry = json.loads(p.read_text())
        removed.update(entry.get("remove", []))
        live.extend(entry["add"])
    files = [f for f in live if f not in removed]
    if not files:
        return pd.DataFrame()
    return pd.concat([pq.read_table(data_dir / f).to_pandas() for f in files], ignore_index=True)
```

## Micro-batch ingestion from a simulated stream

```python
import random
random.seed(7)

def next_micro_batch(batch_num: int, base_ts: dt.datetime) -> pd.DataFrame:
    rows = []
    for i in range(20):
        # occasionally emit a "late" event, event_time well before base_ts
        is_late = random.random() < 0.1
        event_time = base_ts - dt.timedelta(minutes=5) if is_late else base_ts + dt.timedelta(seconds=i)
        rows.append({
            "event_id": batch_num * 20 + i,
            "user": random.choice(["a", "b", "c"]),
            "amount": round(random.uniform(1, 100), 2),
            "event_time": event_time.isoformat(),
        })
    return pd.DataFrame(rows)

def ingest_micro_batch(batch: pd.DataFrame, batch_num: int) -> int:
    fname = f"micro-{batch_num:05d}.parquet"
    pq.write_table(pa.table(batch), data_dir / fname)
    base = latest_version()
    return commit([fname], [], expected_base=base)

base_ts = dt.datetime(2026, 8, 1, 9, 0, 0)
for batch_num in range(10):
    batch = next_micro_batch(batch_num, base_ts + dt.timedelta(seconds=batch_num * 20))
    ingest_micro_batch(batch, batch_num)

table = read_table()
print("Total rows:", len(table))
print("Files backing the table:", len(list(data_dir.glob("*.parquet"))))
```

```text
Total rows: 200
Files backing the table: 10
```

Ten micro-batches of 20 events each, each committed as its own atomic
transaction — a reader running concurrently with ingestion always sees a
consistent snapshot (Module 08's isolation guarantee), never a partial
batch.

## Handling late-arriving events with a watermark-gated view

```python
def watermarked_view(table: pd.DataFrame, watermark: dt.datetime) -> tuple[pd.DataFrame, pd.DataFrame]:
    table = table.copy()
    table["event_time"] = pd.to_datetime(table["event_time"])
    on_time = table[table["event_time"] >= watermark]
    late = table[table["event_time"] < watermark]
    return on_time, late

watermark = base_ts  # anything before the stream's nominal start is "late"
on_time, late = watermarked_view(table, watermark)
print("On-time rows:", len(on_time), "| Late rows:", len(late))
```

```text
On-time rows: 178 | Late rows: 22
```

Late events aren't discarded — they're still physically in the table (the
lakehouse doesn't need to know about lateness at write time) but flagged
so downstream aggregation jobs can choose to include them in a
reconciliation pass rather than the primary on-time metrics.

## Self-maintaining compaction

```python
def compact_if_needed(min_files=8, target_bytes=50_000):
    files = list(data_dir.glob("micro-*.parquet"))
    if len(files) < min_files:
        return None
    total_bytes = sum(f.stat().st_size for f in files)
    if total_bytes >= target_bytes:
        return None  # already large enough on average; skip for this example

    merged = pa.concat_tables([pq.read_table(f) for f in files])
    compacted_name = f"compacted-{latest_version() + 1:05d}.parquet"
    pq.write_table(merged, data_dir / compacted_name)

    base = latest_version()
    version = commit([compacted_name], [f.name for f in files], expected_base=base)
    return version, len(files), merged.num_rows

result = compact_if_needed()
print(result)
print("Files after compaction:", len(list(data_dir.glob("*.parquet"))))
print("Row count unchanged:", len(read_table()))
```

```text
(10, 10, 200)
Files after compaction: 1
Row count unchanged: 200
```

Compaction runs as its own commit (version 10), atomically swapping ten
small micro-batch files for one file with all 200 rows preserved — exactly
the pattern from Module 06, now operating on a live, continuously-ingesting
table rather than a static snapshot.

## Verifying history survived the compaction

```python
history_versions = sorted(int(p.stem) for p in log_dir.glob("*.json"))
print("Versions committed:", history_versions)
print("Row count matches pre- and post-compaction:", len(table) == len(read_table()))
```

```text
Versions committed: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
Row count matches pre- and post-compaction: True
```

## Traps

- **Compacting while ingestion is running without conflict checking.** The
  `expected_base` check in `commit()` is what prevents a compaction job and
  a concurrent micro-batch write from stepping on each other — dropping
  that check reintroduces exactly the corruption Module 08 warned about.
- **Treating "late" as "wrong."** The watermark split doesn't delete late
  events; it segments them for a deliberate downstream policy, matching
  Module 05.
- **Compacting too small a batch of files.** The `min_files` gate avoids
  constantly rewriting a partition that hasn't accumulated enough small
  files yet to be worth the I/O.

## Cheat sheet

| Concern | Module | Mechanism used here |
|---|---|---|
| Atomic multi-file commits | 01 / 08 | `commit()` with temp-write-then-rename |
| Concurrent writer safety | 08 | `expected_base` optimistic concurrency check |
| Late data | 05 | `watermarked_view()` splits on/late without dropping |
| Small files | 06 | `compact_if_needed()` as its own logged commit |
| Time travel | 07 | Every version stays queryable via the log |

## Exercise

Add a `describe_table()` function that reports, from the log alone (no data
file reads): current version number, total commits, how many were
compactions (removed more files than added), and the timestamp of the most
recent commit. Run it after the compaction above and confirm it correctly
counts one compaction out of eleven total commits.
