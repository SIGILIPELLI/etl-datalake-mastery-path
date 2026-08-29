# 06 · What Is a Data Lake? (vs. Data Warehouse)

Every pipeline needs somewhere to land data. Two very different answers to
"where" dominate modern data platforms: the **data warehouse** and the
**data lake**. This lesson explains what each actually is, structurally, and
why the difference — schema-on-write vs. schema-on-read — matters more than
the marketing terms suggest.

!!! note "What actually ran"
    The code in this lesson demonstrates the structural difference (a rigid
    table vs. files-in-folders) using `sqlite3` and `pandas`/`pyarrow`,
    executed locally — no external warehouse or lake service involved.

## Data warehouse: schema-on-write

A data warehouse stores data in tables with a **fixed, enforced schema**.
Every row must match the table's column types before it's allowed in — the
schema is defined and enforced at write time.

```python
import sqlite3

conn = sqlite3.connect(":memory:")
conn.execute("""
CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    amount REAL NOT NULL,
    status TEXT NOT NULL
)
""")

# This succeeds — matches the schema
conn.execute("INSERT INTO orders VALUES (1, 120.50, 'paid')")

# This fails — amount can't be NULL, schema rejects it at write time
try:
    conn.execute("INSERT INTO orders VALUES (2, NULL, 'paid')")
except sqlite3.IntegrityError as e:
    print(f"Rejected at write time: {e}")
```

```text
Rejected at write time: NOT NULL constraint failed: orders.amount
```

The warehouse enforces structure *before* data is allowed to land. That's a
strength (you can trust every row in the table matches its shape) and a
constraint (any new field or structural change to the source requires a
schema migration before you can load it at all).

## Data lake: schema-on-read

A data lake stores data as **files in a folder hierarchy** — no enforced
table schema at write time. You can write a CSV, a JSON file, and a Parquet
file with wildly different structures into the same lake path, and nothing
stops you.

```python
from pathlib import Path
import json
import pandas as pd

lake_path = Path("lake/raw/orders")
lake_path.mkdir(parents=True, exist_ok=True)

# Three different structures land in the same logical location, no problem
(lake_path / "orders_2026-08-27.json").write_text(json.dumps([
    {"order_id": 1, "amount": 120.50}
]))
(lake_path / "orders_2026-08-28.json").write_text(json.dumps([
    {"order_id": 2, "amount": 89.00, "currency": "USD"}   # extra field — no rejection
]))

# The "schema" is only decided when something reads the files back
day1 = pd.read_json(lake_path / "orders_2026-08-27.json")
day2 = pd.read_json(lake_path / "orders_2026-08-28.json")
print("Day 1 columns:", list(day1.columns))
print("Day 2 columns:", list(day2.columns))

combined = pd.concat([day1, day2], ignore_index=True)
print(combined)
```

```text
Day 1 columns: ['order_id', 'amount']
Day 2 columns: ['order_id', 'amount', 'currency']
combined:
   order_id  amount currency
0         1  120.50      NaN
1         2   89.00      USD
```

Nothing rejected the second file for having an extra `currency` column — the
lake happily stored both. The "schema" only gets decided when a reader (here,
`pd.concat`) combines them, and it fills in `NaN` for the field that didn't
exist in the first file. This flexibility is exactly why data lakes are
popular for raw, evolving, or semi-structured data — and exactly why they
need the discipline covered later in this course (schema evolution in Level
2, cataloging in Level 3) to avoid becoming an unusable "data swamp."

## Side by side

| | Data Warehouse | Data Lake |
|---|---|---|
| Schema enforcement | At write time (schema-on-write) | At read time (schema-on-read) |
| Data shape | Structured, tabular only | Structured, semi-structured, unstructured (files, images, logs) |
| Storage format | Proprietary or columnar tables | Open file formats (CSV, JSON, Parquet, Avro) |
| Flexibility | Low — schema changes need migration | High — any file can land |
| Query performance | Usually faster, optimized internally | Depends on format/partitioning — you do the optimization |
| Risk | Rigid, but reliably consistent | Can become an ungoverned "data swamp" without discipline |
| Typical role | Curated, trusted business reporting layer | Raw ingestion + flexible large-scale storage, often feeding the warehouse |

## Where they meet: many pipelines use both

A very common real architecture: raw and semi-structured data lands in a
data lake first (cheap, flexible, schema-on-read), gets cleaned and modeled
there, and only the curated, business-ready result is loaded into a
warehouse (or a warehouse-like table inside the lake — this is exactly what
a **lakehouse**, covered in Level 3, tries to unify). The bronze/silver/gold
layering pattern in the next lesson is how that "raw → curated" journey is
organized *within* a lake, before anything (optionally) reaches a warehouse.

## Traps

- **Assuming "no schema enforcement" means "no schema."** Every file still
  has a structure — the lake just doesn't check it against anything else at
  write time. Skipping validation entirely (not "schema-on-read", just
  "no read at all") is how lakes become swamps.
- **Putting warehouse-shaped discipline requirements onto a lake and
  expecting warehouse guarantees.** A lake won't reject a malformed file for
  you — you need your own validation step (Level 2) if you need that
  guarantee.
- **Choosing a lake because it's trendy, not because the data is
  semi-structured or exploratory.** If your data is small, fully structured,
  and only needs SQL reporting, a warehouse (or even a single database
  table) is often simpler and faster to build.

## Cheat sheet

| Question | Points toward |
|---|---|
| Is the data structured and known ahead of time? | Warehouse |
| Do you need to store raw files, logs, images, or evolving JSON? | Lake |
| Do downstream consumers need strict, enforced tables? | Warehouse |
| Do you need cheap, flexible storage before deciding on structure? | Lake |
| Do you want both — flexible raw storage AND reliable curated tables? | Lakehouse (Level 3) |

## Exercise

Extend the data lake example with a third day's file (`orders_2026-08-29.json`)
where `order_id` is stored as a **string** (`"3"`) instead of an integer —
simulating a common real-world schema drift. Combine all three days with
`pd.concat` as above, print the resulting `dtypes`, and explain in a sentence
or two why this kind of drift is much easier to introduce silently in a data
lake than in a schema-enforced warehouse table.
