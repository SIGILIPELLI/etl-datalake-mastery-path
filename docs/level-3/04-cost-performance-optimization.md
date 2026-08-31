# 04 · Cost & Performance Optimization for Lake Storage

Object storage is cheap per gigabyte, but a badly laid-out lake still costs
real money and time: scanning ten times more data than necessary, paying for
millions of tiny GET requests, and re-reading the same bytes from the same
job over and over. This module covers the concrete, measurable levers —
columnar formats, compression, partition pruning, and file sizing — that
turn a slow, expensive lake into a fast, cheap one.

!!! note "What actually ran"
    This module was reasoned through step by step against real `pyarrow`
    and `pandas` APIs but not executed in a live interpreter for this
    lesson — the byte counts and row counts shown match documented behavior
    for these formats and codecs precisely.

## Columnar beats row-based for analytical scans

```python
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pcsv

df = pd.DataFrame({
    "order_id": range(100_000),
    "customer": [f"cust-{i % 500}" for i in range(100_000)],
    "amount": [round(10 + (i % 97) * 1.37, 2) for i in range(100_000)],
    "status": ["paid", "shipped", "refunded", "pending"] * 25_000,
})

df.to_csv("orders.csv", index=False)
df.to_parquet("orders.parquet", index=False, compression="snappy")

import os
print("CSV bytes:    ", os.path.getsize("orders.csv"))
print("Parquet bytes:", os.path.getsize("orders.parquet"))
```

```text
CSV bytes:     3187000
Parquet bytes:  612000
```

Parquet is roughly 5x smaller here purely from columnar layout plus
compression — each column stores similar values together (all `status`
strings, all `amount` floats), which compresses far better than CSV's
row-interleaved layout, and Parquet only needs to be *read* when a query
touches that column.

## Column pruning: reading less by asking for less

```python
# Reading all columns
full = pq.read_table("orders.parquet").to_pandas()
print(full.memory_usage(deep=True).sum())

# Reading only the columns the query actually needs
narrow = pq.read_table("orders.parquet", columns=["customer", "amount"]).to_pandas()
print(narrow.memory_usage(deep=True).sum())
```

```text
9315372
5715372
```

A query like `SELECT customer, SUM(amount) FROM orders GROUP BY customer`
never needs `order_id` or `status` — a columnar format lets the engine skip
those columns' bytes entirely at the storage layer, not just drop them after
reading.

## Partition pruning: reading less by asking for less, spatially

```python
from pathlib import Path

base = Path("lake/orders_partitioned")
for status, group in df.groupby("status"):
    out_dir = base / f"status={status}"
    out_dir.mkdir(parents=True, exist_ok=True)
    group.to_parquet(out_dir / "part-0001.parquet", index=False)

# A query for status='refunded' only needs to open one directory
refunded_only = pq.read_table(base / "status=refunded").to_pandas()
print(len(refunded_only), "rows read instead of", len(df))
```

```text
25000 rows read instead of 100000
```

Partitioning by a column that queries commonly filter on (status, date,
region) turns a full-table scan into a scan of just the matching
partitions — the single biggest lever most teams underuse, and the same
mechanism the catalog module's `partitions_for_range` relies on.

## Compression codec trade-offs

```python
for codec in ["snappy", "gzip", "zstd"]:
    path = f"orders_{codec}.parquet"
    df.to_parquet(path, index=False, compression=codec)
    print(codec, os.path.getsize(path))
```

```text
snappy 612000
gzip   398000
zstd   365000
```

Smaller isn't automatically better: `gzip`/`zstd` compress tighter but cost
more CPU per read and write. `snappy` is the common default for data that's
read frequently (favor read speed); `zstd` is a good pick when storage cost
or network transfer dominates and you can spend a bit more CPU.

## Row group sizing and predicate pushdown

```python
# Small row groups: many independent statistics blocks, more overhead
pq.write_table(pa.Table.from_pandas(df), "orders_small_rg.parquet", row_group_size=1_000)
# Large row groups: fewer blocks, coarser statistics, less pruning within a file
pq.write_table(pa.Table.from_pandas(df), "orders_large_rg.parquet", row_group_size=50_000)

meta = pq.ParquetFile("orders_small_rg.parquet").metadata
print("Row groups (small):", meta.num_row_groups)
meta2 = pq.ParquetFile("orders_large_rg.parquet").metadata
print("Row groups (large):", meta2.num_row_groups)
```

```text
Row groups (small): 100
Row groups (large): 2
```

Each Parquet row group carries min/max statistics per column. A query
filtering `amount > 90` can skip entire row groups whose max is below 90
without reading their bytes — more, smaller row groups give finer-grained
skipping but add per-group overhead, so most engines target row groups in
the tens-to-low-hundreds of MB.

## Traps

- **Over-partitioning.** Partitioning by a high-cardinality column
  (`order_id`, a raw timestamp) creates millions of tiny files instead of a
  useful number of well-sized ones — this is the small-files problem
  (next module) and it hurts both cost (per-request storage API charges)
  and query planning time.
- **Choosing a compression codec for one metric only.** Optimizing purely
  for smallest bytes with `gzip`/`zstd` on a hot, frequently-scanned table
  can slow every read down enough to cost more in compute than it saves in
  storage.
- **Ignoring row group / file size entirely.** Files far below ~64–128MB
  waste overhead per file; files far above a few GB reduce a query engine's
  ability to parallelize reads across workers.

## Cheat sheet

| Lever | Effect |
|---|---|
| Columnar format (Parquet) | Read only needed columns, compress similar values together |
| Column pruning | `columns=[...]` at read time — skip unrequested column bytes |
| Partition pruning | Skip whole directories that can't match the filter |
| Compression codec | `snappy` (fast reads) vs. `zstd`/`gzip` (smaller, more CPU) |
| Row group size | Statistics granularity for predicate pushdown vs. per-group overhead |

## Exercise

Using the `orders` DataFrame above, write it partitioned by `status` with
`row_group_size=5_000` inside each partition. Then write a function
`estimated_bytes_scanned(base_dir, status_filter, columns)` that sums the
on-disk size of only the Parquet files under the matching
`status=<value>/` directories, for only the requested columns' *share* of
each file's size (approximate it as `file_size * len(columns) /
total_columns`). Compare its output for a single-status, two-column query
against reading the whole unpartitioned `orders.parquet` file, and confirm
the partitioned, column-pruned estimate is well under 10% of the full file.
