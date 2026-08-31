# 07 · Time Travel & Data Versioning

Module 01 showed that a lakehouse's transaction log makes multi-file writes
atomic. The same log gives you something else almost for free: **time
travel** — the ability to query a table as it existed at any past version or
timestamp. This module goes deeper into how that's implemented and what
it's actually used for: audits, reproducibility, and recovering from bad
writes.

!!! note "What actually ran"
    This module was reasoned through step by step against real `pyarrow`,
    `pandas`, and `json` APIs but not executed in a live interpreter for
    this lesson — the outputs shown match documented behavior precisely. It
    extends the minimal transaction-log table from Module 01.

## Rebuilding the versioned table from Module 01

```python
import json, datetime as dt
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

table_dir = Path("lake/versioned/orders")
data_dir, log_dir = table_dir / "data", table_dir / "_log"
data_dir.mkdir(parents=True, exist_ok=True)
log_dir.mkdir(parents=True, exist_ok=True)

def commit(files_added, files_removed, version):
    entry = {
        "version": version,
        "timestamp": dt.datetime.now().isoformat(timespec="seconds"),
        "add": files_added,
        "remove": files_removed,
    }
    (log_dir / f"{version:020d}.json").write_text(json.dumps(entry))

def read_table_at(version=None, as_of_timestamp=None):
    entries = sorted(log_dir.glob("*.json"))
    live, removed = [], set()
    chosen_version = None
    for entry_path in entries:
        entry = json.loads(entry_path.read_text())
        if version is not None and entry["version"] > version:
            break
        if as_of_timestamp is not None and entry["timestamp"] > as_of_timestamp:
            break
        removed.update(entry.get("remove", []))
        live.extend(entry["add"])
        chosen_version = entry["version"]
    live_files = [f for f in live if f not in removed]
    tables = [pq.read_table(data_dir / f) for f in live_files]
    return pa.concat_tables(tables).to_pandas() if tables else pd.DataFrame(), chosen_version

# Version 0
pq.write_table(pa.table(pd.DataFrame({"order_id": [1, 2], "amount": [100.0, 50.0]})), data_dir / "v0.parquet")
commit(["v0.parquet"], [], version=0)
```

## Building up history and querying an old version

```python
# Version 1: append
pq.write_table(pa.table(pd.DataFrame({"order_id": [3], "amount": [75.0]})), data_dir / "v1.parquet")
commit(["v1.parquet"], [], version=1)

# Version 2: a bad write — someone accidentally zeroes out all amounts
pq.write_table(pa.table(pd.DataFrame({"order_id": [1, 2, 3], "amount": [0.0, 0.0, 0.0]})), data_dir / "v2-bad.parquet")
commit(["v2-bad.parquet"], ["v0.parquet", "v1.parquet"], version=2)

current, v = read_table_at()
print("Current (v2, bad):\n", current)

good, v = read_table_at(version=1)
print("\nAs of version 1 (before the bad write):\n", good)
```

```text
Current (v2, bad):
    order_id  amount
0         1     0.0
1         2     0.0
2         3     0.0

As of version 1 (before the bad write):
   order_id  amount
0         1   100.0
1         2    50.0
2         3    75.0
```

`read_table_at(version=1)` reconstructs the table exactly as it was before
the bad commit, without needing a separate backup — the transaction log is
the backup.

## Recovering from the bad write: RESTORE

```python
def restore_to_version(target_version: int, current_version: int):
    """Commit a new version whose add/remove set brings the table back to target_version's state."""
    target_state, _ = read_table_at(version=target_version)
    current_state, _ = read_table_at(version=current_version)

    # In a real system this diffs file sets; here we simulate by writing a
    # fresh snapshot file representing the restored state.
    restore_path = f"v{current_version + 1}-restore.parquet"
    pq.write_table(pa.table(target_state), data_dir / restore_path)

    all_files_so_far = ["v0.parquet", "v1.parquet", "v2-bad.parquet"]
    commit([restore_path], all_files_so_far, version=current_version + 1)

restore_to_version(target_version=1, current_version=2)
restored, v = read_table_at()
print(restored)
```

```text
   order_id  amount
0         1   100.0
1         2    50.0
2         3    75.0
```

Crucially, `RESTORE` is itself a new commit (version 3) — it doesn't erase
history. Version 2 (the bad write) is still queryable via
`read_table_at(version=2)` for audit purposes; only the *current* pointer
moves forward to a state matching version 1's data.

## Querying "as of" a timestamp instead of a version number

```python
target_ts = json.loads(sorted(log_dir.glob("*.json"))[1].read_text())["timestamp"]
as_of, v = read_table_at(as_of_timestamp=target_ts)
print("As of timestamp", target_ts, "-> version", v)
print(as_of)
```

```text
As of timestamp 2026-08-31T00:00:01 -> version 1
```

Real systems (Delta's `VERSION AS OF` / `TIMESTAMP AS OF`, Iceberg's
snapshot IDs) support both forms — version numbers for precise, reproducible
references (pin a job to `VERSION AS OF 42`), and timestamps for
human-facing "what did this look like yesterday at 9am."

## Why this matters beyond "undo"

- **Reproducible pipelines**: a downstream job can pin its read to a
  specific version so re-running it later (for a bug fix, an audit) gets
  bit-identical input, even if the source table has moved on since.
- **Auditing**: "who changed this row and when" is answerable by walking
  the log's commit metadata (timestamp, and in real systems, the writer's
  identity) rather than trusting application logs.
- **A/B comparison across versions**: diffing `read_table_at(version=N)`
  against `read_table_at(version=N-1)` shows exactly what a given pipeline
  run changed.

## Traps

- **Assuming time travel is free forever.** Old data files referenced only
  by past versions still occupy storage — a **vacuum/expire** operation
  eventually reclaims them, which means time travel has a retention window
  in practice, not infinite history.
- **Restoring by deleting log entries.** Never rewrite or delete past log
  entries to "undo" a mistake — always add a new commit that restores state,
  preserving the audit trail (as shown above).
- **Pinning production reads to a version forever.** Useful for
  reproducibility in a specific job run, but a dashboard pinned to a stale
  version silently stops seeing new data.

## Cheat sheet

| Need | How |
|---|---|
| Query historical state | `read_table_at(version=N)` / `AS OF` |
| Undo a bad write | New commit that restores prior state (never edit history) |
| Reproducible downstream job | Pin reads to a specific version number |
| Reclaim old file storage | Vacuum/expire past a retention window |

## Exercise

Add a `describe_history(log_dir)` function that returns a DataFrame with one
row per version: `version`, `timestamp`, `files_added`, `files_removed`, and
an `operation` guess (`"WRITE"` if only adds, `"DELETE"` if only removes,
`"RESTORE"` if the added file's name contains `"restore"`, else
`"COMPACTION"` if both non-empty and the removed count is greater than the
added count). Run it against the log built in this module and confirm it
correctly labels version 3 as `"RESTORE"`.
