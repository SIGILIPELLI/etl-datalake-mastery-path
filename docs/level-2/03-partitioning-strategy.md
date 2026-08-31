# 03 · Partitioning Strategy for a Data Lake

A data lake without a partitioning strategy is a folder of files that every
query has to scan in full. Partitioning splits data into directories by a
column's value so queries can skip irrelevant files entirely — the single
highest-leverage lake performance decision you'll make.

!!! note "What actually ran"
    Reasoned through step by step against the real `pandas` and `pyarrow`
    APIs, not executed in a live interpreter — directory layouts and row
    counts match documented `pyarrow.dataset` partitioning behavior.

## Writing partitioned Parquet with pyarrow

```python
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

orders = pd.DataFrame([
    {"order_id": 1, "amount": 120.50, "country": "US", "order_date": "2026-08-01"},
    {"order_id": 2, "amount": 89.00,  "country": "US", "order_date": "2026-08-01"},
    {"order_id": 3, "amount": 45.25,  "country": "IN", "order_date": "2026-08-01"},
    {"order_id": 4, "amount": 60.00,  "country": "US", "order_date": "2026-08-02"},
    {"order_id": 5, "amount": 30.00,  "country": "IN", "order_date": "2026-08-02"},
])

table = pa.Table.from_pandas(orders)
pq.write_to_dataset(
    table,
    root_path="lake/silver/orders",
    partition_cols=["country", "order_date"],
)
```

```text
lake/silver/orders/
  country=US/order_date=2026-08-01/<file>.parquet   (2 rows)
  country=US/order_date=2026-08-02/<file>.parquet   (1 row)
  country=IN/order_date=2026-08-01/<file>.parquet   (1 row)
  country=IN/order_date=2026-08-02/<file>.parquet   (1 row)
```

`partition_cols` writes each unique combination of `country` and
`order_date` into its own directory (Hive-style `key=value` naming), and
drops those columns from the physical file body — they're reconstructed
from the folder path at read time.

## Partition pruning: why this matters

```python
import pyarrow.dataset as ds

dataset = ds.dataset("lake/silver/orders", format="parquet", partitioning="hive")

# A query filtered on the partition column only reads matching directories
us_only = dataset.to_table(filter=(ds.field("country") == "US")).to_pandas()
print(us_only)
```

```text
   order_id  amount order_date
0         1  120.50 2026-08-01
1         2   89.00 2026-08-01
2         4   60.00 2026-08-02
```

The engine reads only `country=US/*` directories — it never opens the
`country=IN` files at all. This is **partition pruning**, and it's the
entire performance argument for partitioning: on a table with a year of
daily-partitioned data, a query for "last 7 days" scans 7 directories
instead of 365.

## Choosing a partition key: cardinality matters

```python
# A column with too many unique values makes "too many small files"
customer_ids = [f"cust_{i}" for i in range(500)]
cardinality_check = pd.Series(customer_ids).nunique()
print(f"Unique customers: {cardinality_check}  (partitioning on this = 500 tiny directories)")

order_dates = pd.date_range("2026-01-01", "2026-12-31", freq="D")
print(f"Unique dates in a year: {len(order_dates)}  (a reasonable partition count)")
```

```text
Unique customers: 500  (partitioning on this = 500 tiny directories)
Unique dates in a year: 365  (a reasonable partition count)
```

A good partition key has **low-to-moderate cardinality** (tens to low
thousands of distinct values, not millions) and is **commonly filtered on**
in real queries. `customer_id` fails both tests for most workloads: too many
values, and most queries don't filter by a single customer. `order_date` or
`country` typically pass both.

## Multi-level partitioning and the "too many partitions" trap

```python
# Partitioning by country AND order_date AND status creates a combinatorial
# explosion if each level has real cardinality
countries, dates, statuses = 20, 365, 5
total_partition_dirs = countries * dates * statuses
print(f"Hypothetical partition directories: {total_partition_dirs}")
```

```text
Hypothetical partition directories: 36500
```

36,500 directories for one year of one table means most of them hold only a
handful of rows each — the classic **small files problem** (covered in
depth in Level 3). The fix is almost always to drop the lowest-value
partition level (here, `status` — better handled as a regular filterable
column inside each file) and keep partitioning to one or two high-value
keys, most often a date field.

## Reading with a partition filter vs. a full scan

```python
import time

# Full scan (no filter) reads every partition
full = dataset.to_table().to_pandas()
print(f"Full scan rows: {len(full)}")

# Pruned scan only touches the matching partition directory
pruned = dataset.to_table(
    filter=(ds.field("country") == "IN") & (ds.field("order_date") == "2026-08-01")
).to_pandas()
print(pruned)
```

```text
Full scan rows: 5
   order_id  amount order_date
0         3   45.25 2026-08-01
```

On this toy dataset the difference is invisible, but at scale (millions of
rows across thousands of partitions) the pruned query's I/O is proportional
to the matching partitions only — this is why "what will people filter on?"
should be the first question you ask before choosing a partition scheme,
not an afterthought.

## Traps

- **Partitioning on high-cardinality columns** (`customer_id`, `order_id`,
  free-text fields) — produces too many tiny files and directories.
- **Partitioning on a column nobody filters by.** If queries never filter on
  `country`, partitioning by it buys you nothing and adds directory-listing
  overhead.
- **Over-partitioning with three or more levels.** Each extra level
  multiplies the directory count; two levels (commonly a date plus one
  business key) is usually the practical ceiling.
- **Choosing partitions and never revisiting them.** Query patterns change;
  a partition scheme that made sense at launch may need repartitioning a
  year later (an expensive rewrite — plan for it rather than being
  surprised by it).

## Cheat sheet

| Task | Code |
|---|---|
| Write partitioned Parquet | `pq.write_to_dataset(table, root_path=..., partition_cols=[...])` |
| Read with pruning | `ds.dataset(path, partitioning="hive").to_table(filter=...)` |
| Check column cardinality | `series.nunique()` |
| Rule of thumb | Partition on what's low-cardinality *and* commonly filtered |

## Exercise

Given a table with columns `region` (5 values), `order_date` (365 values/
year), and `payment_method` (4 values), design a partitioning scheme that
keeps directory count under 2,000 for one year of data while still
supporting fast queries for "orders in a region over a date range." Show
your arithmetic for why your chosen combination stays under the limit.
