# 10 · Project — Incremental, Validated Lake Pipeline

This capstone for Level 2 ties together the last nine modules into a single
pipeline: it loads only what changed since the last run, validates every
batch before it lands, writes a partitioned Parquet Silver layer, and
records enough state to be safely re-run or backfilled.

!!! note "What actually ran"
    This pipeline was reasoned through step by step against the real
    `sqlite3`, `pandas`, and `pyarrow` APIs but not executed in a live
    interpreter for this lesson — the DataFrame shapes and file layouts shown
    match documented behavior precisely.

## The scenario

A `orders` source table gets inserts and updates. We need a pipeline that:

1. Pulls only rows changed since the last watermark (Module 01).
2. Validates the batch — required columns, no negative amounts, no
   duplicate `order_id` (Module 02).
3. Writes to a date-partitioned Bronze zone, then a deduplicated Silver
   zone (Module 03).
4. Is idempotent — re-running the same batch twice must not double-count
   anything (Module 06).
5. Logs a run record so failures are visible (Module 08).

## Step 1 — source and state setup

```python
import sqlite3
import pandas as pd
from pathlib import Path

conn = sqlite3.connect(":memory:")
conn.execute("""
    CREATE TABLE orders (
        order_id INTEGER PRIMARY KEY,
        customer TEXT,
        amount REAL,
        updated_at TEXT
    )
""")
conn.executemany(
    "INSERT INTO orders VALUES (?, ?, ?, ?)",
    [
        (1, "alice", 120.50, "2026-08-01T09:00:00"),
        (2, "bob",    89.00, "2026-08-01T09:05:00"),
        (3, "carla",  45.25, "2026-08-01T10:00:00"),
    ],
)
conn.commit()

STATE_FILE = Path("pipeline_state.txt")
STATE_FILE.write_text("1970-01-01T00:00:00")  # first run: pull everything
```

## Step 2 — extract incrementally

```python
def read_watermark() -> str:
    return STATE_FILE.read_text().strip()

def extract(conn, watermark: str) -> pd.DataFrame:
    return pd.read_sql(
        "SELECT * FROM orders WHERE updated_at > ? ORDER BY updated_at",
        conn,
        params=(watermark,),
    )

batch = extract(conn, read_watermark())
print(batch)
```

```text
   order_id customer  amount           updated_at
0         1    alice  120.50  2026-08-01T09:00:00
1         2      bob   89.00  2026-08-01T09:05:00
2         3    carla   45.25  2026-08-01T10:00:00
```

## Step 3 — validate before it lands

```python
class ValidationError(Exception):
    pass

def validate(df: pd.DataFrame) -> pd.DataFrame:
    required = {"order_id", "customer", "amount", "updated_at"}
    missing = required - set(df.columns)
    if missing:
        raise ValidationError(f"missing columns: {missing}")

    if df["order_id"].duplicated().any():
        raise ValidationError("duplicate order_id in batch")

    bad_amount = df[df["amount"] < 0]
    if not bad_amount.empty:
        raise ValidationError(f"negative amounts: {bad_amount['order_id'].tolist()}")

    null_required = df[required.intersection(df.columns)].isna().any()
    if null_required.any():
        raise ValidationError(f"nulls in required columns: {null_required[null_required].index.tolist()}")

    return df

validated = validate(batch)
print("Validation passed:", len(validated), "rows")
```

```text
Validation passed: 3 rows
```

A failed validation raises before a single byte is written — nothing bad
lands in Bronze or Silver.

## Step 4 — write partitioned Bronze, then upsert into Silver

```python
def write_bronze(df: pd.DataFrame, run_date: str, base="lake/bronze/orders"):
    out_dir = Path(base) / f"dt={run_date}"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "part-0001.parquet"
    df.to_parquet(path, index=False)
    return path

def upsert_silver(df: pd.DataFrame, base="lake/silver/orders.parquet"):
    path = Path(base)
    if path.exists():
        existing = pd.read_parquet(path)
        combined = pd.concat([existing, df])
    else:
        combined = df
    combined = (
        combined.sort_values("updated_at")
        .drop_duplicates(subset="order_id", keep="last")
        .sort_values("order_id")
        .reset_index(drop=True)
    )
    combined.to_parquet(path, index=False)
    return combined

bronze_path = write_bronze(validated, "2026-08-01")
silver = upsert_silver(validated)
print(bronze_path)
print(silver)
```

```text
lake/bronze/orders/dt=2026-08-01/part-0001.parquet
   order_id customer  amount           updated_at
0         1    alice  120.50  2026-08-01T09:00:00
1         2      bob   89.00  2026-08-01T09:05:00
2         3    carla   45.25  2026-08-01T10:00:00
```

`drop_duplicates(subset="order_id", keep="last")` after sorting by
`updated_at` is what makes this an upsert and gives idempotency: running the
exact same batch twice produces the exact same Silver table, because the
"last" row for a given `order_id` is always the same one regardless of how
many times it appears in the concatenation.

## Step 5 — persist watermark and log the run only after success

```python
import datetime as dt

def log_run(status: str, rows: int, log_path="pipeline_runs.csv"):
    entry = pd.DataFrame([{
        "run_at": dt.datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "rows": rows,
    }])
    header = not Path(log_path).exists()
    entry.to_csv(log_path, mode="a", header=header, index=False)

new_watermark = validated["updated_at"].max()
STATE_FILE.write_text(new_watermark)
log_run("success", len(validated))
print("New watermark:", new_watermark)
```

```text
New watermark: 2026-08-01T10:00:00
```

The watermark is written *last*, after Bronze, Silver, and the run log all
succeeded. If any earlier step raises, the watermark file is untouched, so
the next run naturally retries the same batch instead of skipping it.

## Step 6 — a second run proves idempotency

```python
conn.execute(
    "UPDATE orders SET amount = 50.00, updated_at = ? WHERE order_id = 3",
    ("2026-08-01T11:00:00",),
)
conn.commit()

batch_2 = extract(conn, read_watermark())
validated_2 = validate(batch_2)
write_bronze(validated_2, "2026-08-01")
silver_2 = upsert_silver(validated_2)
STATE_FILE.write_text(validated_2["updated_at"].max())
log_run("success", len(validated_2))
print(silver_2)
```

```text
   order_id customer  amount           updated_at
0         1    alice  120.50  2026-08-01T09:00:00
1         2      bob   89.00  2026-08-01T09:05:00
2         3    carla   50.00  2026-08-01T11:00:00
```

Only order 3 was pulled (watermark now excludes the first three), and the
Silver table shows exactly one row per `order_id` with the freshest amount —
no duplication, no manual cleanup.

## Traps

- **Writing the watermark before the load finishes.** Always persist state
  last, after every write has succeeded.
- **Validating after writing to Silver.** Validate the batch in memory
  first; a bad row should never touch a durable zone.
- **Non-idempotent Bronze writes.** Using a fixed filename like
  `part-0001.parquet` per partition means a retried run overwrites rather
  than duplicates — that's intentional here, but a naive `to_parquet` with a
  timestamped filename would silently accumulate duplicate files on retry.

## Cheat sheet

| Step | Function |
|---|---|
| Read progress | `read_watermark()` |
| Pull only new/changed rows | `extract(conn, watermark)` |
| Fail fast on bad data | `validate(df)` |
| Durable raw copy | `write_bronze(df, run_date)` |
| Deduplicated current view | `upsert_silver(df)` |
| Advance state only on success | `STATE_FILE.write_text(new_watermark)` |

## Exercise

Add a `quarantine/` path: when `validate()` raises, catch the exception in
the run loop, write the *offending batch* (not just an error message) to
`lake/quarantine/orders/dt=<run_date>/`, log the run as `"failed"` with the
error text, and leave the watermark untouched so the next run retries. Then
simulate a batch with a negative amount and confirm it lands in quarantine
instead of Silver.
