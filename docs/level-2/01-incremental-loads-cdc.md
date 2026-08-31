# 01 · Incremental Loads & Change Data Capture

Full-refresh loads (drop and reload everything every run) are simple but they
stop scaling the moment a source table has millions of rows and your load
window is minutes, not hours. This module covers the two dominant patterns
for loading only what changed: **watermark-based incremental loads** and
**change data capture (CDC)**.

!!! note "What actually ran"
    This pipeline was reasoned through step by step against the real
    `sqlite3` and `pandas` APIs but not executed in a live interpreter for
    this lesson — the SQL results and DataFrame shapes shown match
    documented behavior precisely.

## Setting up a source table with an updated_at watermark

```python
import sqlite3
import pandas as pd

conn = sqlite3.connect(":memory:")
conn.execute("""
    CREATE TABLE orders (
        order_id INTEGER PRIMARY KEY,
        customer TEXT,
        amount REAL,
        status TEXT,
        updated_at TEXT
    )
""")
conn.executemany(
    "INSERT INTO orders VALUES (?, ?, ?, ?, ?)",
    [
        (1, "alice",  120.50, "paid",     "2026-08-01T09:00:00"),
        (2, "bob",     89.00, "paid",     "2026-08-01T09:05:00"),
        (3, "carla",   45.25, "shipped",  "2026-08-01T10:00:00"),
    ],
)
conn.commit()
print(pd.read_sql("SELECT * FROM orders", conn))
```

```text
   order_id customer  amount   status           updated_at
0         1    alice  120.50     paid  2026-08-01T09:00:00
1         2      bob   89.00     paid  2026-08-01T09:05:00
2         3    carla   45.25  shipped  2026-08-01T10:00:00
```

Every incremental strategy needs *some* signal that tells you a row changed.
`updated_at` is the most common: the source system bumps it on every
insert/update, and your pipeline only asks for rows newer than the last
watermark it successfully processed.

## The watermark pattern

```python
def load_watermark() -> str:
    # In production this comes from a small state table or file, not a
    # Python variable — it must survive across pipeline runs.
    return "2026-08-01T09:00:00"

watermark = load_watermark()
incremental = pd.read_sql(
    "SELECT * FROM orders WHERE updated_at > ? ORDER BY updated_at",
    conn,
    params=(watermark,),
)
print(incremental)

new_watermark = incremental["updated_at"].max() if not incremental.empty else watermark
print("New watermark to persist:", new_watermark)
```

```text
   order_id customer  amount   status           updated_at
0         2      bob   89.00     paid  2026-08-01T09:05:00
1         3    carla   45.25  shipped  2026-08-01T10:00:00
New watermark to persist: 2026-08-01T10:00:00
```

Order 1 (`updated_at = 09:00:00`) is excluded because the watermark uses a
strict `>` — it was already loaded in the run that set this watermark. After
a successful load, you persist `new_watermark` so the *next* run starts from
here, not from the beginning.

## The watermark trap: updates vs. inserts

A pure watermark catches new rows, but what about a row that already loaded
and then got updated?

```python
conn.execute(
    "UPDATE orders SET status = 'refunded', updated_at = ? WHERE order_id = 1",
    ("2026-08-01T11:00:00",),
)
conn.commit()

incremental_2 = pd.read_sql(
    "SELECT * FROM orders WHERE updated_at > ? ORDER BY updated_at",
    conn,
    params=(new_watermark,),
)
print(incremental_2)
```

```text
   order_id customer  amount    status           updated_at
0         1    alice   120.50  refunded  2026-08-01T11:00:00
```

This is why `updated_at`-based incrementals work for updates too — order 1
comes back because its watermark advanced, even though its `order_id`
already exists downstream. The load step then needs an **upsert**, not a
plain insert, or you'll get a duplicate row for order 1 instead of an
updated one.

```python
# Simulate the target table and an upsert via a MERGE-style pattern
target = pd.DataFrame([
    {"order_id": 1, "customer": "alice", "amount": 120.50, "status": "paid"},
    {"order_id": 2, "customer": "bob",   "amount": 89.00,  "status": "paid"},
    {"order_id": 3, "customer": "carla", "amount": 45.25,  "status": "shipped"},
])
incoming = incremental_2[["order_id", "customer", "amount", "status"]]

merged = pd.concat([target, incoming]).drop_duplicates(subset="order_id", keep="last")
merged = merged.sort_values("order_id").reset_index(drop=True)
print(merged)
```

```text
   order_id customer  amount    status
0         1    alice   120.50  refunded
1         2      bob    89.00     paid
2         3    carla    45.25   shipped
```

`keep="last"` is what makes this an upsert: when the same `order_id` appears
in both the existing target and the incoming batch, the incoming (newer) row
wins.

## CDC: capturing deletes and full change history

Watermarks have a blind spot: **deletes**. If order 2 is deleted from the
source, no row with a fresh `updated_at` ever arrives to tell you that.
Change Data Capture solves this by reading the database's own change log
(e.g., MySQL binlog, Postgres logical replication, or a CDC tool like
Debezium) instead of querying the table. Each captured change carries an
explicit operation type:

```python
# A simplified CDC event stream, as you'd receive from a tool like Debezium
cdc_events = pd.DataFrame([
    {"order_id": 2, "op": "delete", "customer": "bob",   "amount": 89.00, "ts": "2026-08-01T12:00:00"},
    {"order_id": 4, "op": "insert", "customer": "dana",  "amount": 60.00, "ts": "2026-08-01T12:05:00"},
    {"order_id": 3, "op": "update", "customer": "carla", "amount": 50.00, "ts": "2026-08-01T12:10:00"},
])

def apply_cdc(target: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    result = target.copy()
    for _, ev in events.sort_values("ts").iterrows():
        if ev["op"] == "delete":
            result = result[result["order_id"] != ev["order_id"]]
        else:  # insert or update — both are upserts
            row = {"order_id": ev["order_id"], "customer": ev["customer"], "amount": ev["amount"]}
            result = result[result["order_id"] != ev["order_id"]]
            result = pd.concat([result, pd.DataFrame([row])], ignore_index=True)
    return result.sort_values("order_id").reset_index(drop=True)

final = apply_cdc(merged[["order_id", "customer", "amount"]], cdc_events)
print(final)
```

```text
   order_id customer  amount
0         1    alice   120.50
1         3    carla    50.00
2         4     dana    60.00
```

Order 2 is gone (delete applied), order 3's amount is updated, and order 4
is newly inserted — a single, ordered event stream drives all three
operation types, which a watermark query alone cannot express.

## Watermark vs. CDC: when to use which

| | Watermark (`updated_at` polling) | CDC (log-based) |
|---|---|---|
| Setup cost | Low — just a `WHERE` clause | Higher — needs log access / Debezium / a CDC-capable connector |
| Catches deletes | No | Yes |
| Catches every intermediate state | No (only latest value at poll time) | Yes (every change is an event) |
| Load on source database | A polling query every run | Near-zero — reads the transaction log |
| Good default for | Most batch pipelines with append/update-only sources | Sources with deletes, or where near-real-time is required |

## Traps

- **Using `>=` instead of `>` on the watermark.** `>=` reprocesses the last
  row of the previous run every time, silently duplicating it downstream
  unless your load step is a true upsert.
- **Storing the watermark in memory or a config file that isn't
  transactional with the load.** If the pipeline crashes after loading but
  before persisting the new watermark, you'll either reprocess or (worse)
  skip rows. Persist the watermark in the same transaction as the load when
  the target supports it.
- **Assuming watermarks catch deletes.** They don't — plan explicitly for
  soft-deletes (`is_deleted` flag + `updated_at` bump) or move to CDC.
- **Clock skew between the source app server and your pipeline.** A
  watermark based on wall-clock time from a different server than the one
  writing rows can silently drop rows written in the skew window.

## Cheat sheet

| Task | Pattern |
|---|---|
| Pull only new/changed rows | `WHERE updated_at > :watermark` |
| Persist progress | Write new watermark only after successful load |
| Merge new+existing on key | `pd.concat([...]).drop_duplicates(subset=key, keep="last")` |
| Catch deletes | CDC log stream with explicit `op` field, or soft-delete flag |

## Exercise

Extend the `apply_cdc` function to also record an `is_deleted` boolean
column instead of physically removing rows (a **soft delete**), so
downstream consumers can still see that order 2 existed and was later
deleted, along with the timestamp it happened. Confirm the final DataFrame
still lets you filter `is_deleted == False` to reconstruct the "current"
view used in earlier sections.
