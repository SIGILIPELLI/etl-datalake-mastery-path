# 05 · Loading Into a Target

Load is the final ETL step: writing clean, transformed data somewhere it can
be queried — a database table, a file, or (as this site emphasizes
throughout) a data lake. This lesson covers the two loading strategies every
pipeline chooses between — **full overwrite** and **append/upsert** — and why
that choice determines whether reruns are safe.

!!! note "What actually ran"
    This lesson's code was run locally against the Python standard library
    (`sqlite3`) and `pandas`/`pyarrow` for the Parquet section — no external
    services.

## Setting up: clean data ready to load

```python
import pandas as pd

clean_orders = pd.DataFrame([
    {"order_id": 1001, "customer": "Alice Smith", "amount": 120.50, "status": "paid"},
    {"order_id": 1002, "customer": "Bob Jones", "amount": 89.00, "status": "paid"},
    {"order_id": 1003, "customer": "Alice Smith", "amount": 45.25, "status": "refunded"},
])
print(clean_orders)
```

```text
   order_id     customer  amount    status
0      1001  Alice Smith  120.50      paid
1      1002    Bob Jones   89.00      paid
2      1003  Alice Smith   45.25  refunded
```

## Strategy 1: full overwrite (replace the whole table/file)

The simplest strategy: every run replaces the entire destination with the
current full extract. It's easy to reason about and trivially idempotent —
but only works when the source is small enough to fully re-extract every
run.

```python
import sqlite3

conn = sqlite3.connect(":memory:")

def load_full_overwrite(df, conn, table_name):
    df.to_sql(table_name, conn, if_exists="replace", index=False)

load_full_overwrite(clean_orders, conn, "orders")
print("First load:")
for row in conn.execute("SELECT * FROM orders"):
    print(" ", row)

# Simulate a rerun with slightly different data (order 1003 status corrected)
updated_orders = clean_orders.copy()
updated_orders.loc[updated_orders["order_id"] == 1003, "status"] = "cancelled"
load_full_overwrite(updated_orders, conn, "orders")
print("After rerun with corrected data:")
for row in conn.execute("SELECT * FROM orders"):
    print(" ", row)
```

```text
First load:
  (1001, 'Alice Smith', 120.5, 'paid')
  (1002, 'Bob Jones', 89.0, 'paid')
  (1003, 'Alice Smith', 45.25, 'refunded')
After rerun with corrected data:
  (1001, 'Alice Smith', 120.5, 'paid')
  (1002, 'Bob Jones', 89.0, 'paid')
  (1003, 'Alice Smith', 45.25, 'cancelled')
```

`if_exists="replace"` drops and recreates the table every time — rerunning
with corrected data just works, because there's no "old" data left to
conflict with. The cost: every run must re-extract and re-transform *all*
the data, which doesn't scale once a source has millions of historical rows
you don't want to touch again.

## Strategy 2: append / upsert (write only new or changed rows)

For larger or continuously growing sources, you instead load only new or
changed rows, using a primary key to decide whether to insert or update.

```python
conn2 = sqlite3.connect(":memory:")
conn2.execute("""
CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer TEXT,
    amount REAL,
    status TEXT
)
""")

def load_upsert(df, conn, table_name):
    records = df.to_dict("records")
    conn.executemany(
        f"INSERT OR REPLACE INTO {table_name} VALUES (:order_id, :customer, :amount, :status)",
        records,
    )
    conn.commit()

load_upsert(clean_orders, conn2, "orders")
print("First load (upsert):")
for row in conn2.execute("SELECT * FROM orders"):
    print(" ", row)

# Rerun: one new order (1004), one status correction (1003)
new_batch = pd.DataFrame([
    {"order_id": 1003, "customer": "Alice Smith", "amount": 45.25, "status": "cancelled"},
    {"order_id": 1004, "customer": "Carla Diaz", "amount": 300.00, "status": "paid"},
])
load_upsert(new_batch, conn2, "orders")
print("After upsert with new + corrected rows:")
for row in conn2.execute("SELECT * FROM orders ORDER BY order_id"):
    print(" ", row)
```

```text
First load (upsert):
  (1001, 'Alice Smith', 120.5, 'paid')
  (1002, 'Bob Jones', 89.0, 'paid')
  (1003, 'Alice Smith', 45.25, 'refunded')
After upsert with new + corrected rows:
  (1001, 'Alice Smith', 120.5, 'paid')
  (1002, 'Bob Jones', 89.0, 'paid')
  (1003, 'Alice Smith', 45.25, 'cancelled')
  (1004, 'Carla Diaz', 300.0, 'paid')
```

`INSERT OR REPLACE` inserts new rows and overwrites existing ones with a
matching primary key, in one statement — orders 1001 and 1002 were
untouched, 1003 was updated in place, and 1004 was added. This is the
loading pattern that scales: each run only has to carry the rows that
actually changed.

## Loading into a data lake: writing Parquet

Loading into a **data lake** (rather than a database) usually means writing
columnar files — most commonly Parquet — to a path in object storage or a
local filesystem, organized by folder structure.

```python
from pathlib import Path

lake_path = Path("lake/gold/orders")
lake_path.mkdir(parents=True, exist_ok=True)

clean_orders.to_parquet(lake_path / "orders.parquet", index=False)

# Reading it back confirms the round trip and shows Parquet preserves dtypes
reloaded = pd.read_parquet(lake_path / "orders.parquet")
print(reloaded.dtypes)
print(reloaded)
```

```text
order_id      int64
customer     object
amount      float64
status       object
dtype: object
   order_id     customer  amount    status
0      1001  Alice Smith  120.50      paid
1      1002    Bob Jones   89.00      paid
2      1003  Alice Smith   45.25  refunded
```

Unlike a database load, writing Parquet to a lake path has **no built-in
upsert** — Parquet files are immutable once written. To "update" a row, you
either rewrite the whole file (full overwrite, same tradeoff as above) or
adopt a lakehouse table format (Delta Lake, Iceberg, Hudi — Level 3) that
adds transactional upsert support on top of plain files. This distinction —
files are dumb and immutable, tables need a format layer for updates — is
one of the most important ideas in data lake architecture and comes up
repeatedly through the rest of this course.

## Traps

- **Upserting without a primary key.** `INSERT OR REPLACE` (or a `MERGE` in
  warehouse SQL) needs a real unique key — without one, every "upsert"
  degrades into a plain, duplicate-generating insert.
- **Full overwrite on a source too large to re-extract fully.** Once a
  source has years of history, "just reload everything" stops being a viable
  strategy — you need incremental loading (Level 2).
- **Treating Parquet files as updatable.** Writing a new Parquet file with
  the same name overwrites the whole file — there is no row-level update
  without a table-format layer on top.
- **Forgetting to make the target folder before writing.** Unlike a database
  connection, writing to a lake path fails if the parent directory doesn't
  exist — always `mkdir(parents=True, exist_ok=True)` first.

## Cheat sheet

| Strategy | Idempotent? | Scales to large sources? | Typical use |
|---|---|---|---|
| Full overwrite | Yes, trivially | No — re-extracts everything | Small reference/dimension tables |
| Append/upsert (DB) | Yes, with a primary key | Yes | Fact tables, growing datasets |
| Write Parquet (lake) | Only if the whole file is rewritten | Depends on partitioning (Level 2) | Data lake bronze/silver/gold zones |

## Exercise

Take the `load_upsert` function and adapt it to load into a Parquet-based
"table" instead of SQLite: write a function that reads the existing Parquet
file (if it exists), concatenates the new batch, drops duplicate
`order_id`s keeping the newest row (`drop_duplicates(subset=["order_id"],
keep="last")`), and rewrites the file. Run it twice with the same
`new_batch` from above and confirm the second run doesn't create duplicate
rows for order 1004.
