# 01 · Data Lakehouse Concepts (Delta Lake/Iceberg/Hudi)

A plain data lake (files in a bucket) is cheap and flexible but gives you
none of the guarantees a database does: no atomic multi-file writes, no
schema enforcement, no way to see "what did this table look like an hour
ago." A **lakehouse** adds a transactional metadata layer on top of the same
Parquet files so you get warehouse-like guarantees without leaving the lake.
Delta Lake, Apache Iceberg, and Apache Hudi are the three dominant
implementations of this idea.

!!! note "What actually ran"
    This module was reasoned through step by step against the real
    `pyarrow`, `pandas`, and `json` APIs but not executed in a live
    interpreter for this lesson — the file layouts and DataFrame shapes
    shown match documented behavior precisely. A real Delta/Iceberg table is
    normally managed by the `deltalake` or `pyiceberg` libraries; this
    module hand-builds a minimal version of the same idea to make the
    mechanism visible.

## The problem a lakehouse solves

```python
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

base = Path("lake/plain/orders")
base.mkdir(parents=True, exist_ok=True)

table1 = pa.table({"order_id": [1, 2], "amount": [100.0, 50.0]})
pq.write_table(table1, base / "part-0001.parquet")

# A second writer starts appending a second file...
table2 = pa.table({"order_id": [3], "amount": [75.0]})
pq.write_table(table2, base / "part-0002.parquet")
# ...and crashes here. A reader mid-write may see part-0002 as a truncated
# file, or a directory listing that includes it before it's fully flushed.
```

With plain files, "did this batch of writes succeed atomically?" has no
answer — a reader can observe a partial state. A lakehouse format fixes this
by never having readers list files directly; they read a **transaction log**
that only reveals files after a commit completes.

## A minimal transaction log, by hand

```python
import json
import datetime as dt

table_dir = Path("lake/managed/orders")
data_dir = table_dir / "data"
log_dir = table_dir / "_log"
data_dir.mkdir(parents=True, exist_ok=True)
log_dir.mkdir(parents=True, exist_ok=True)

def commit(files_added: list[str], version: int):
    entry = {
        "version": version,
        "timestamp": dt.datetime.now().isoformat(timespec="seconds"),
        "add": files_added,
    }
    (log_dir / f"{version:020d}.json").write_text(json.dumps(entry))

# Write data file, THEN commit — readers never see part-0001 until it's
# both fully flushed and named in a committed log entry.
pq.write_table(table1, data_dir / "part-0001.parquet")
commit(["part-0001.parquet"], version=0)

pq.write_table(table2, data_dir / "part-0002.parquet")
commit(["part-0002.parquet"], version=1)
```

```python
def read_table_at(version: int | None = None) -> pa.Table:
    entries = sorted(log_dir.glob("*.json"))
    live_files = []
    for entry_path in entries:
        entry = json.loads(entry_path.read_text())
        if version is not None and entry["version"] > version:
            break
        live_files.extend(entry["add"])
    tables = [pq.read_table(data_dir / f) for f in live_files]
    return pa.concat_tables(tables)

print(read_table_at().to_pandas())
```

```text
   order_id  amount
0         1   100.0
1         2    50.0
2         3    75.0
```

This is, in miniature, exactly what Delta Lake's `_delta_log/` and Iceberg's
manifest files do: a reader never scans the data directory — it reads the
log to learn which files are currently valid, which is what makes multi-file
writes atomic and makes time travel possible (query with `version=0` to see
only `part-0001.parquet`).

## Time travel falls out for free

```python
print(read_table_at(version=0).to_pandas())
```

```text
   order_id  amount
0         1   100.0
1         2    50.0
```

Because every commit is an immutable, append-only log entry, "what did the
table look like at version N" is just "replay the log up to N" — no
snapshots of the data itself need to be taken.

## Delta Lake vs. Iceberg vs. Hudi at a glance

| | Delta Lake | Iceberg | Hudi |
|---|---|---|---|
| Log structure | JSON + Parquet checkpoints (`_delta_log/`) | Layered manifests + manifest lists + metadata JSON | Timeline of instants + optional log files for merge-on-read |
| Origin | Databricks | Netflix (donated to Apache) | Uber |
| Update model | Copy-on-write or deletion vectors | Copy-on-write or merge-on-read | Copy-on-write or merge-on-read |
| Engine support | Strongest in Spark/Databricks, growing elsewhere | Broadest multi-engine support (Spark, Trino, Flink, Snowflake) | Strong in Spark/Flink, streaming-oriented |
| Best known for | Simplicity, tight Databricks integration | Engine-agnostic open standard, schema/partition evolution | Near-real-time upserts, incremental pull |

All three solve the same core problem (atomic commits, schema tracking,
time travel over plain files) with different log formats and trade-offs —
picking one is mostly about which query engines and orchestration tools
your organization already runs.

## Traps

- **Treating a lakehouse table's data files as safe to touch directly.**
  Deleting or editing a Parquet file under `data/` without updating the log
  corrupts the table silently — always go through the format's write API.
- **Forgetting that old files still exist after a "delete."** Copy-on-write
  formats mark old files as removed in the log but don't physically delete
  them immediately (this is what makes time travel work) — you need a
  separate **vacuum/expire** operation to reclaim space, and running it too
  aggressively breaks time travel for anyone still querying old versions.
- **Assuming a lakehouse table needs Spark.** Iceberg and Delta both have
  Python-native readers (`pyiceberg`, `deltalake`) that don't require a
  Spark cluster for many workloads.

## Cheat sheet

| Concept | What it gives you |
|---|---|
| Transaction log | Atomic multi-file commits, no partial reads |
| Versioned log entries | Time travel (`read_table_at(version=N)`) |
| Manifest/checkpoint files | Fast metadata reads without listing every data file |
| Copy-on-write vs. merge-on-read | Read-optimized vs. write-optimized update trade-off |

## Exercise

Extend `commit` to also support a `"remove"` list alongside `"add"`, and
update `read_table_at` to exclude any file that has been removed by a
version at or before the one being read. Then simulate compacting
`part-0001.parquet` and `part-0002.parquet` into a single
`part-0003-compacted.parquet`: write the new file, commit it with
`add=["part-0003-compacted.parquet"]` and `remove=["part-0001.parquet",
"part-0002.parquet"]`, and confirm `read_table_at()` (latest) returns the
same three rows from one file, while `read_table_at(version=1)` still
returns the old two-file version.
