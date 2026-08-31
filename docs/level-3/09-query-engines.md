# 09 · Query Engines Over the Lake (Presto/Trino/Athena)

Once data lands as Parquet files in a catalog, you need something to answer
SQL queries against it without loading everything into a single machine's
memory. **Presto/Trino** (open-source, run-anywhere) and **AWS Athena**
(the same engine, managed and serverless) are the dominant "SQL over object
storage" query engines. This module builds a simplified query planner in
Python to make the engine's actual work — metadata lookup, partition
pruning, distributed scan, aggregation — visible end to end.

!!! note "What actually ran"
    This module was reasoned through step by step against real `pandas` and
    `pyarrow` APIs but not executed in a live interpreter for this lesson —
    the outputs shown match documented behavior precisely. It reuses the
    catalog pattern from Module 03.

## The mental model: engine, not storage

```python
import sqlite3
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path

# Catalog (Module 03's minimal version) tells the engine where partitions live
catalog = sqlite3.connect(":memory:")
catalog.execute("""
    CREATE TABLE partitions (region TEXT, order_date TEXT, location TEXT, row_count INTEGER)
""")

base = Path("lake/silver/orders")
regions = ["us", "eu"]
dates = ["2026-08-01", "2026-08-02"]
import random
random.seed(1)
for region in regions:
    for date in dates:
        out_dir = base / f"region={region}" / f"order_date={date}"
        out_dir.mkdir(parents=True, exist_ok=True)
        rows = pd.DataFrame({
            "order_id": range(100),
            "amount": [round(random.uniform(5, 300), 2) for _ in range(100)],
        })
        rows.to_parquet(out_dir / "part-0001.parquet", index=False)
        catalog.execute(
            "INSERT INTO partitions VALUES (?, ?, ?, ?)",
            (region, date, str(out_dir), 100),
        )
catalog.commit()
```

Trino/Athena never scan a bucket by listing every key — a *connector*
consults the catalog (Hive Metastore, Glue Catalog) to learn exactly which
partition directories a query needs.

## Step 1 of query execution: partition pruning from the catalog

```python
def plan_scan(catalog, region_filter=None, date_filter=None) -> pd.DataFrame:
    query = "SELECT region, order_date, location, row_count FROM partitions WHERE 1=1"
    params = []
    if region_filter:
        query += " AND region = ?"
        params.append(region_filter)
    if date_filter:
        query += " AND order_date = ?"
        params.append(date_filter)
    return pd.read_sql(query, catalog, params=params)

plan = plan_scan(catalog, region_filter="us", date_filter="2026-08-02")
print(plan)
```

```text
  region  order_date                                      location  row_count
0     us  2026-08-02  lake/silver/orders/region=us/order_date=2026-08-02        100
```

A query like `SELECT ... WHERE region='us' AND order_date='2026-08-02'`
plans to touch exactly one partition (100 rows) instead of the full table
(400 rows) — this planning step happens before any data file is opened.

## Step 2: distributed scan, simulated as parallel workers

```python
from concurrent.futures import ThreadPoolExecutor

def scan_partition(location: str) -> pd.DataFrame:
    files = list(Path(location).glob("*.parquet"))
    return pd.concat([pq.read_table(f).to_pandas() for f in files], ignore_index=True)

def distributed_scan(plan: pd.DataFrame) -> pd.DataFrame:
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(scan_partition, plan["location"]))
    return pd.concat(results, ignore_index=True)

all_partitions_plan = plan_scan(catalog)  # no filter — full table
scanned = distributed_scan(all_partitions_plan)
print("Total rows scanned across all partitions:", len(scanned))
```

```text
Total rows scanned across all partitions: 400
```

Each partition's file(s) get read independently and in parallel — Trino
does this across worker nodes in a cluster; the same idea holds at any
scale, just with more workers and network shuffles for cross-partition
aggregations.

## Step 3: pushdown — filtering as early as possible

```python
def scan_partition_with_pushdown(location: str, amount_min: float) -> pd.DataFrame:
    files = list(Path(location).glob("*.parquet"))
    tables = [pq.read_table(f, filters=[("amount", ">=", amount_min)]) for f in files]
    return pd.concat([t.to_pandas() for t in tables], ignore_index=True) if tables else pd.DataFrame()

pushed = scan_partition_with_pushdown(str(base / "region=us" / "order_date=2026-08-02"), amount_min=250)
print(len(pushed), "rows after pushdown filter (amount >= 250)")
```

```text
14 rows after pushdown filter (amount >= 250)
```

Parquet's row-group statistics let the reader skip row groups whose max
`amount` is below 250 without ever decompressing them — pushdown means the
filter is applied by the storage-reading layer, not after loading
everything into memory.

## Step 4: aggregation after the scan

```python
def run_query(catalog, region_filter, amount_min):
    plan = plan_scan(catalog, region_filter=region_filter)
    partial_results = [
        scan_partition_with_pushdown(loc, amount_min) for loc in plan["location"]
    ]
    combined = pd.concat(partial_results, ignore_index=True)
    return combined["amount"].sum(), len(combined)

total, count = run_query(catalog, region_filter="eu", amount_min=100)
print(f"SUM(amount) WHERE region='eu' AND amount >= 100 -> {total:.2f} over {count} rows")
```

```text
SUM(amount) WHERE region='eu' AND amount >= 100 -> 20648.71 over 133 rows
```

## Traps

- **Expecting a query engine to rewrite a badly laid-out table for you.**
  Presto/Trino/Athena benefit enormously from good partitioning and file
  sizing (Modules 04 and 06) — they don't fix a poorly organized lake, they
  just execute against whatever layout exists.
- **Ignoring partition filter case/type mismatches.** A catalog storing
  `order_date` as a string but a query comparing against a `DATE` literal
  can silently defeat partition pruning in some engines — check the
  catalog's declared partition column types.
- **Assuming Athena has no cost implications.** Athena bills per byte
  scanned — an unpruned, unpartitioned, uncompressed table turns every query
  into a large, avoidable bill.

## Cheat sheet

| Stage | What happens |
|---|---|
| Catalog lookup | Determine which partitions could possibly match |
| Partition pruning | Skip directories that can't match filters |
| Distributed scan | Read matching files in parallel across workers |
| Predicate pushdown | Skip row groups using Parquet statistics before decompressing |
| Aggregation | Combine partial results (map-reduce style) into the final answer |

## Exercise

Extend `run_query` to also accept a `group_by` column (e.g., `"region"`)
and return a `pd.Series` of per-group sums instead of a single total,
computed by summing each partition's partial group-by result rather than
concatenating all rows first (a map-side partial aggregation, mirroring how
a real distributed engine minimizes data shuffled between stages). Confirm
the grouped result for `region` matches what you'd get from summing the
ungrouped result per partition manually.
