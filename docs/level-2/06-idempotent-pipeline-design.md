# 06 · Idempotent Pipeline Design

An **idempotent** pipeline produces the same result no matter how many
times you run it for the same input. This sounds academic until a DAG
retries a task that already partially succeeded, or you backfill a month of
data and discover every row got duplicated three times over.

!!! note "What actually ran"
    Reasoned through step by step against the real `sqlite3` and `pandas`
    APIs, not executed in a live interpreter — row counts and query outputs
    match documented behavior precisely.

## The non-idempotent trap: naive append

```python
import sqlite3
import pandas as pd

conn = sqlite3.connect(":memory:")
conn.execute("CREATE TABLE daily_sales (order_date TEXT, total REAL)")

def naive_load(conn, order_date: str, total: float):
    conn.execute("INSERT INTO daily_sales VALUES (?, ?)", (order_date, total))
    conn.commit()

# Simulate: the pipeline runs successfully, then someone re-triggers it
# for the same date (a retry, a manual re-run, a backfill overlap)
naive_load(conn, "2026-08-01", 1500.00)
naive_load(conn, "2026-08-01", 1500.00)  # re-run for the same date

print(pd.read_sql("SELECT * FROM daily_sales", conn))
print("SUM:", pd.read_sql("SELECT SUM(total) AS t FROM daily_sales", conn)["t"][0])
```

```text
  order_date   total
0 2026-08-01  1500.0
1 2026-08-01  1500.0
SUM: 3000.0
```

A plain `INSERT` re-run doubles the day's total. Nothing crashed, no error
was raised — the pipeline just quietly produced wrong numbers, which is the
most dangerous kind of failure because nothing alerts you to it.

## Pattern 1: delete-then-insert (overwrite by partition)

```python
def idempotent_load_overwrite(conn, order_date: str, total: float):
    conn.execute("DELETE FROM daily_sales WHERE order_date = ?", (order_date,))
    conn.execute("INSERT INTO daily_sales VALUES (?, ?)", (order_date, total))
    conn.commit()

conn.execute("DELETE FROM daily_sales")  # reset for this example
conn.commit()

idempotent_load_overwrite(conn, "2026-08-01", 1500.00)
idempotent_load_overwrite(conn, "2026-08-01", 1500.00)  # re-run
idempotent_load_overwrite(conn, "2026-08-02", 900.00)

print(pd.read_sql("SELECT * FROM daily_sales ORDER BY order_date", conn))
```

```text
  order_date   total
0 2026-08-01  1500.0
1 2026-08-02   900.0
```

Running the exact same call twice for `2026-08-01` leaves the table in the
identical state either way — the definition of idempotent. This is also
exactly why lake tables are partitioned by date (Module 3): "delete this
partition, rewrite it" is a cheap, atomic-enough operation at the file/
directory level.

## Pattern 2: upsert on a natural key

```python
conn.execute("""
    CREATE TABLE orders (
        order_id INTEGER PRIMARY KEY,
        amount REAL,
        status TEXT
    )
""")

def idempotent_upsert(conn, order_id: int, amount: float, status: str):
    conn.execute("""
        INSERT INTO orders (order_id, amount, status) VALUES (?, ?, ?)
        ON CONFLICT(order_id) DO UPDATE SET amount = excluded.amount, status = excluded.status
    """, (order_id, amount, status))
    conn.commit()

idempotent_upsert(conn, 1, 120.50, "paid")
idempotent_upsert(conn, 1, 120.50, "paid")   # exact re-run
idempotent_upsert(conn, 1, 120.50, "refunded")  # a real status change

print(pd.read_sql("SELECT * FROM orders", conn))
```

```text
   order_id  amount    status
0         1   120.50  refunded
```

`ON CONFLICT ... DO UPDATE` (SQLite/Postgres upsert syntax) is idempotent
by construction: re-running with the same values leaves the row unchanged,
and running with new values for the same key correctly reflects the latest
state rather than appending a duplicate.

## Pattern 3: deterministic, content-addressed writes

```python
import hashlib

def deterministic_filename(order_date: str, source: str) -> str:
    # Same inputs always produce the same output path — re-running the
    # extract for the same date+source overwrites the same file instead of
    # creating file_2, file_3, ...
    key = f"{order_date}:{source}"
    digest = hashlib.sha256(key.encode()).hexdigest()[:12]
    return f"bronze/orders/{order_date}/{source}_{digest}.parquet"

print(deterministic_filename("2026-08-01", "api"))
print(deterministic_filename("2026-08-01", "api"))  # identical, every time
```

```text
bronze/orders/2026-08-01/api_1f3c9a7b2e04.parquet
bronze/orders/2026-08-01/api_1f3c9a7b2e04.parquet
```

Contrast this with a common anti-pattern: naming files by wall-clock write
time (`orders_20260830_143201.parquet`). That guarantees every re-run
writes a *new* file rather than overwriting the previous attempt, silently
accumulating duplicate data in the lake even if every individual write
"succeeds."

## Idempotency at the DAG level: partition-scoped tasks

```python
def process_partition(order_date: str):
    """A task idempotent with respect to its logical date."""
    # 1. Delete any existing output for this exact partition
    # 2. Recompute deterministically from source data for this date only
    # 3. Write to the same, deterministic partition path
    output_path = f"silver/orders/order_date={order_date}/"
    return output_path

# Running this 5 times for the same date is safe: same output_path,
# same delete-then-rewrite semantics each time.
for _ in range(3):
    print(process_partition("2026-08-01"))
```

```text
silver/orders/order_date=2026-08-01/
silver/orders/order_date=2026-08-01/
silver/orders/order_date=2026-08-01/
```

This is what makes Airflow backfills (Module 5) safe: each logical-date run
only ever touches its own partition, so re-running Aug 1st ten times never
affects Aug 2nd's data, and running Aug 1st twice produces the same result
both times.

## Traps

- **Plain `INSERT` without a uniqueness constraint or delete-first step.**
  The single most common source of silent duplication in ETL pipelines.
- **Timestamp-based file/task naming.** Makes every run unique by
  construction, which is the opposite of idempotent.
- **Idempotent load, non-idempotent side effects.** Sending a notification
  email or incrementing an external counter inside a task that might retry
  makes *that* action non-idempotent even if the data load itself is fine.
- **Assuming "idempotent" means "safe to run concurrently."** Idempotency
  is about repeated *sequential* runs producing the same result — running
  two instances of the same partition-overwrite task at the same time can
  still race and corrupt output. Use locking or a scheduler's built-in
  concurrency controls for that.

## Cheat sheet

| Pattern | When to use |
|---|---|
| Delete-then-insert (by partition) | Batch loads scoped to a date/partition |
| Upsert on natural key | Row-level incremental loads (Module 1) |
| Deterministic file naming | Any file-based bronze/silver write |
| Partition-scoped task logic | Any pipeline that will ever need backfilling |

## Exercise

Take the `naive_load` function from the top of this lesson and rewrite it
as `idempotent_load_merge` using pandas instead of SQL: it should accept
the full `daily_sales` history as a DataFrame plus one new `(order_date,
total)` pair, and return an updated DataFrame where re-running with the
same date+total leaves the DataFrame unchanged. Test it by calling it three
times in a row with identical arguments.
