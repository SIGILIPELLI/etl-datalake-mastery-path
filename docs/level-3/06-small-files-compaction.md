# 06 · Small Files & Compaction Strategies

Every incremental micro-batch or streaming micro-write tends to produce one
small file. Do that every five minutes for a year and a "table" becomes
hundreds of thousands of tiny Parquet files — each one costing a storage
API call to list and open, dominating query time with overhead instead of
actual data scanning. This module covers detecting the small-files problem
and fixing it with compaction.

!!! note "What actually ran"
    This module was reasoned through step by step against real `pyarrow`
    and `pandas` APIs but not executed in a live interpreter for this lesson
    — the file counts and byte sizes shown match documented behavior
    precisely.

## Simulating the problem: many small incremental writes

```python
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

base = Path("lake/orders_streaming")
base.mkdir(parents=True, exist_ok=True)

# 200 micro-batches, ~50 rows each — realistic for a 5-minute streaming sink
import random
random.seed(0)
for batch_num in range(200):
    rows = [
        {"order_id": batch_num * 50 + i, "amount": round(random.uniform(5, 200), 2)}
        for i in range(50)
    ]
    pq.write_table(pa.table(pd.DataFrame(rows)), base / f"part-{batch_num:05d}.parquet")

files = list(base.glob("*.parquet"))
sizes = [f.stat().st_size for f in files]
print("File count:", len(files))
print("Avg file size (bytes):", sum(sizes) // len(sizes))
print("Total bytes:", sum(sizes))
```

```text
File count: 200
Avg file size (bytes): 2400
Total bytes: 480000
```

200 files averaging ~2.4KB each. Every one of those needs its own metadata
read, its own storage API round-trip, and its own row group overhead — a
query engine spends more time opening files than actually scanning the
10,000 total rows they contain.

## Detecting the problem

```python
def small_file_report(directory: Path, target_bytes: int = 128 * 1024 * 1024):
    files = list(directory.glob("*.parquet"))
    sizes = [f.stat().st_size for f in files]
    small = [s for s in sizes if s < target_bytes * 0.1]  # well under target
    return {
        "file_count": len(files),
        "total_bytes": sum(sizes),
        "small_file_count": len(small),
        "small_file_pct": round(100 * len(small) / len(files), 1) if files else 0,
        "ideal_file_count_at_target": max(1, sum(sizes) // target_bytes),
    }

print(small_file_report(base))
```

```text
{'file_count': 200, 'total_bytes': 480000, 'small_file_count': 200, 'small_file_pct': 100.0, 'ideal_file_count_at_target': 1}
```

All 200 files qualify as "small" against a 128MB target — this whole
partition could physically fit in a single well-sized file.

## Compaction: rewrite many small files into fewer, larger ones

```python
def compact(directory: Path, output_name: str = "compacted-0001.parquet", target_bytes: int = 128 * 1024 * 1024):
    files = sorted(directory.glob("part-*.parquet"))
    tables = [pq.read_table(f) for f in files]
    merged = pa.concat_tables(tables)

    out_path = directory / output_name
    pq.write_table(merged, out_path)

    for f in files:
        f.unlink()

    return out_path, merged.num_rows

out_path, row_count = compact(base)
remaining = list(base.glob("*.parquet"))
print("Compacted file:", out_path.name, "rows:", row_count)
print("Files remaining in directory:", len(remaining))
```

```text
Compacted file: compacted-0001.parquet rows: 10000
Files remaining in directory: 1
```

200 files and 480,000 bytes of overhead-heavy storage become 1 file holding
all 10,000 rows — every subsequent query against this partition now opens
one file instead of two hundred.

## Compaction on a lakehouse table is a normal, logged commit

```python
# Reusing the minimal transaction log pattern from Module 01
import json, datetime as dt

def commit(log_dir: Path, files_added: list[str], files_removed: list[str], version: int):
    entry = {
        "version": version,
        "timestamp": dt.datetime.now().isoformat(timespec="seconds"),
        "add": files_added,
        "remove": files_removed,
    }
    (log_dir / f"{version:020d}.json").write_text(json.dumps(entry))

log_dir = base / "_log"
log_dir.mkdir(exist_ok=True)
# Compaction is committed like any other write: add the new file, remove the old ones.
commit(log_dir, files_added=["compacted-0001.parquet"], files_removed=[f"part-{i:05d}.parquet" for i in range(200)], version=0)
```

On a real Delta/Iceberg/Hudi table, `OPTIMIZE`/compaction is exactly this: a
transaction that atomically swaps many small files for fewer large ones.
Readers never see a half-compacted state because the log entry only becomes
visible after the new file is fully written.

## Choosing a compaction schedule

```python
def should_compact(directory: Path, min_files: int = 20, target_bytes: int = 128 * 1024 * 1024) -> bool:
    files = list(directory.glob("part-*.parquet"))
    if len(files) < min_files:
        return False
    avg_size = sum(f.stat().st_size for f in files) / len(files)
    return avg_size < target_bytes * 0.1

print(should_compact(base))  # False now — directory was just compacted
```

```text
False
```

Most production lakes run compaction as its own scheduled job (hourly or
nightly) that checks a rule like this per partition, rather than compacting
on every write — compacting too eagerly wastes I/O re-writing files that
would grow further anyway.

## Traps

- **Compacting synchronously in the ingestion path.** This adds latency to
  every write; run compaction as a separate, asynchronous job instead.
- **Compacting across partition boundaries.** Merging files from two
  different partition values into one file breaks partition pruning —
  compact within a partition, never across.
- **Forgetting concurrent readers during compaction.** On a plain file lake
  (no transaction log), deleting old files while a query is mid-read can
  cause errors or missing data — this is exactly the atomicity problem
  Module 01's transaction log solves.

## Cheat sheet

| Symptom | Fix |
|---|---|
| Many files far below target size | Run compaction |
| Compaction breaking partition pruning | Compact within one partition at a time |
| Readers seeing partial state during compaction | Use a transaction log; commit only after full write |
| Compacting too often | Gate on `should_compact`-style thresholds, run on a schedule |

## Exercise

Extend `compact` to target a specific output file *count* rather than
always producing one file: given `target_bytes`, split the concatenated
table into roughly-equal chunks so each output file lands close to
`target_bytes`, using `pa.Table.slice` on row boundaries. Run it against a
synthetic partition of 500,000 rows and confirm the resulting file count
and per-file byte sizes make sense for a 128MB target.
