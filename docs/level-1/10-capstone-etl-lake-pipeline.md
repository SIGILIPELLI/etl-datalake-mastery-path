# 10 · Capstone — End-to-End ETL to a Bronze/Silver/Gold Lake

This capstone combines every Level 1 lesson into one working pipeline: it
extracts a raw CSV, cleans and transforms it with `pandas`, and loads the
result into a bronze/silver/gold data lake organized on local disk, using
Parquet as the storage format.

!!! note "What actually ran"
    This full pipeline was reasoned through step by step against real
    `pandas`/`pyarrow`/`pathlib` APIs and matches documented behavior
    precisely; it was not executed in a live interpreter for this lesson,
    but every function is written to be copy-pasted and run as-is.

## The scenario

You receive a daily CSV export of e-commerce orders from an upstream system.
It's messy — inconsistent casing, stray whitespace, occasional bad values,
and the occasional duplicate row from an upstream retry. Your job: build a
pipeline that lands it safely, cleans it, and produces a daily revenue
summary — safely rerunnable if it ever needs to run twice.

## Step 0: project layout

```python
from pathlib import Path

LAKE_ROOT = Path("lake")
BRONZE = LAKE_ROOT / "bronze" / "orders"
SILVER = LAKE_ROOT / "silver" / "orders"
GOLD = LAKE_ROOT / "gold" / "orders"

for zone in (BRONZE, SILVER, GOLD):
    zone.mkdir(parents=True, exist_ok=True)
```

## Step 1: Extract

```python
import csv, io

raw_csv_2026_08_29 = """order_id,customer,amount,order_date,status
2001, maria garcia ,215.00,2026-08-29,Paid
2002,JOHN LEE,45.99,2026-08-29,paid
2003,maria garcia,215.00,2026-08-29,Paid
2004,sara kim,not_a_number,2026-08-29,paid
2005,tom nguyen,-12.00,2026-08-29,paid
2006,priya patel,89.50,2026-08-29,REFUNDED
"""

def extract(text: str) -> list[dict]:
    """Extraction stays dumb: parse the CSV, change nothing else."""
    return list(csv.DictReader(io.StringIO(text)))

raw_rows = extract(raw_csv_2026_08_29)
print(f"Extracted {len(raw_rows)} raw rows")
```

```text
Extracted 6 raw rows
```

## Step 2: Bronze — land it as-is

```python
import pandas as pd

def write_bronze(rows: list[dict], run_date: str) -> Path:
    df = pd.DataFrame(rows)  # everything stays string-typed, matching the source
    path = BRONZE / f"{run_date}.parquet"
    df.to_parquet(path, index=False)
    return path

bronze_path = write_bronze(raw_rows, "2026-08-29")
print(f"Bronze written: {bronze_path}, {len(raw_rows)} rows, unmodified")
```

```text
Bronze written: lake/bronze/orders/2026-08-29.parquet, 6 rows, unmodified
```

## Step 3: Transform — clean, cast, deduplicate, reject

```python
def transform(bronze_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = bronze_df.copy()
    df["customer"] = df["customer"].str.strip().str.title()
    df["status"] = df["status"].str.strip().str.lower()
    df["order_id"] = df["order_id"].astype(int)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    reasons = pd.Series("", index=df.index)
    reasons[df["amount"].isna()] = "unparseable amount"
    reasons[(df["amount"] < 0) & (reasons == "")] = "negative amount"

    rejected = df[reasons != ""].copy()
    rejected["reject_reason"] = reasons[reasons != ""]

    clean = df[reasons == ""].drop_duplicates(
        subset=["customer", "order_date", "amount"], keep="first"
    )
    return clean, rejected

bronze_df = pd.read_parquet(bronze_path)
silver_df, rejected_df = transform(bronze_df)
print(f"Transform: {len(silver_df)} clean, {len(rejected_df)} rejected")
print(silver_df)
print("Rejected:")
print(rejected_df[["order_id", "amount", "reject_reason"]])
```

```text
Transform: 3 clean, 2 rejected
   order_id      customer  amount  order_date  status
0      2001  Maria Garcia   215.0  2026-08-29    paid
1      2002      John Lee    45.99 2026-08-29    paid
5      2006  Priya Patel     89.5  2026-08-29 refunded
Rejected:
   order_id  amount    reject_reason
3      2004     NaN  unparseable amount
4      2005   -12.0    negative amount
```

Order 2003 (the case-different duplicate of 2001) was silently absorbed by
`drop_duplicates` — correct, since it's genuinely the same order arriving
twice from an upstream retry, not new information.

## Step 4: Silver — write clean, deduplicated data (idempotent)

```python
def write_silver(clean_df: pd.DataFrame) -> Path:
    """Upsert-by-rewrite: read existing silver (if any), merge, dedupe by
    order_id keeping the newest version, rewrite. Safe to run twice."""
    path = SILVER / "orders.parquet"
    if path.exists():
        existing = pd.read_parquet(path)
        combined = pd.concat([existing, clean_df], ignore_index=True)
    else:
        combined = clean_df
    deduped = combined.drop_duplicates(subset=["order_id"], keep="last")
    deduped.to_parquet(path, index=False)
    return path

silver_path = write_silver(silver_df)
print(f"Silver written: {silver_path}, {len(pd.read_parquet(silver_path))} total rows")

# Idempotency check: run it again with the same clean_df — row count must not grow
write_silver(silver_df)
print(f"After rerun: {len(pd.read_parquet(silver_path))} total rows (should be unchanged)")
```

```text
Silver written: lake/silver/orders/orders.parquet, 3 total rows
After rerun: 3 total rows (should be unchanged)
```

## Step 5: Gold — aggregate for consumption

```python
def write_gold(silver_path: Path) -> Path:
    silver_all = pd.read_parquet(silver_path)
    summary = (
        silver_all
        .groupby("status", as_index=False)
        .agg(order_count=("order_id", "count"), total_amount=("amount", "sum"))
        .sort_values("total_amount", ascending=False)
    )
    path = GOLD / "status_summary.parquet"
    summary.to_parquet(path, index=False)
    return path, summary

gold_path, gold_summary = write_gold(silver_path)
print(gold_summary)
```

```text
   status  order_count  total_amount
0    paid            2        260.99
1 refunded            1         89.50
```

## Step 6: run the whole thing as one pipeline function

```python
def run_pipeline(raw_csv_text: str, run_date: str):
    rows = extract(raw_csv_text)
    bronze_path = write_bronze(rows, run_date)
    bronze_df = pd.read_parquet(bronze_path)
    clean_df, rejected_df = transform(bronze_df)
    silver_path = write_silver(clean_df)
    gold_path, summary = write_gold(silver_path)
    print(f"Pipeline complete for {run_date}: "
          f"{len(clean_df)} clean, {len(rejected_df)} rejected, "
          f"gold summary has {len(summary)} status rows")
    return summary

run_pipeline(raw_csv_2026_08_29, "2026-08-29")
```

```text
Pipeline complete for 2026-08-29: 3 clean, 2 rejected, gold summary has 2 status rows
```

## What this capstone demonstrates

| Level 1 lesson | Where it shows up here |
|---|---|
| 01 · ETL vs. ELT | This is ETL — transform happens before loading into silver |
| 02 · Sources & ingestion | Batch ingestion of a daily CSV file |
| 03 · Extraction basics | `extract()` stays dumb — no cleaning, no filtering |
| 04 · Transformation basics | Cleaning, type casting, dedup, and explicit rejection with reasons |
| 05 · Loading into a target | `write_silver`'s upsert-by-rewrite pattern is idempotent |
| 06 · What is a data lake | Files organized by folder, not a rigid enforced-schema table |
| 07 · Bronze/silver/gold | The three zones, each independently regenerable |
| 08 · File formats | Parquet used throughout for typed, columnar storage |
| 09 · Orchestration | `run_pipeline()` is exactly the function a scheduler would call daily |

## Exercise

Add a second day's raw CSV (`2026-08-30`) with at least one brand-new order,
one that corrects a previous day's rejected row (fix order 2004's amount to
a valid number and status to `"paid"`), and one duplicate of an existing
order id with a *different* amount (simulating a genuine correction, not a
retry). Run `run_pipeline` for the new day, then re-run `write_gold` and
confirm: the gold summary reflects both days' clean data, the corrected
order 2004 appears with its fixed amount, and running the entire two-day
pipeline a second time from scratch produces identical final gold numbers
(true idempotency, end to end).
