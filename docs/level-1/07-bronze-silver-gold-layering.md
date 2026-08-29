# 07 · Bronze/Silver/Gold Layering

Once you've decided data lands in a lake (lesson 6), you need a way to
organize it so "raw and possibly messy" and "clean and trustworthy" don't
live in the same undifferentiated pile. The **bronze/silver/gold** pattern
(also called "multi-hop" or "medallion" architecture) is the industry-
standard answer.

!!! note "What actually ran"
    This lesson's pipeline ran locally with `pandas` and `pyarrow`, writing
    to a local folder structure standing in for a lake path (e.g., cloud
    object storage) — same logic, different storage backend.

## The three zones

- **Bronze** — raw data, exactly as it arrived from the source. No cleaning,
  no casting, no filtering. This is your permanent, replayable record of
  what the source actually sent.
- **Silver** — cleaned, validated, deduplicated, and type-cast data. Still
  fairly granular (row-level), but now trustworthy enough to join across
  sources.
- **Gold** — business-level, aggregated, or curated data — the shape
  reports and dashboards actually query. Often denormalized and
  purpose-built for a specific consumption need.

```python
from pathlib import Path
import pandas as pd

lake = Path("lake")
for zone in ("bronze", "silver", "gold"):
    (lake / zone / "orders").mkdir(parents=True, exist_ok=True)
```

## Bronze: land it as-is

```python
raw_orders = pd.DataFrame([
    {"order_id": "1001", "customer": " alice smith ", "amount": "120.50", "status": "Paid"},
    {"order_id": "1002", "customer": "BOB JONES", "amount": "89.00", "status": "paid"},
    {"order_id": "1003", "customer": "alice smith", "amount": "not_a_number", "status": "REFUNDED"},
    {"order_id": "1001", "customer": " alice smith ", "amount": "120.50", "status": "Paid"},  # duplicate ingest
])

raw_orders.to_parquet(lake / "bronze" / "orders" / "2026-08-29.parquet", index=False)
print("Bronze: wrote", len(raw_orders), "rows, unmodified, all as strings")
print(raw_orders.dtypes)
```

```text
Bronze: wrote 4 rows, unmodified, all as strings
order_id     object
customer     object
amount       object
status       object
dtype: object
```

Note the duplicate row and the unparseable `amount` value — bronze keeps
both. If a source ever sends bad data, bronze is the audit trail proving
exactly what arrived and when, which is invaluable when someone asks "why
does silver look different from what the source system shows today?"

## Silver: clean, validate, dedupe

```python
bronze_df = pd.read_parquet(lake / "bronze" / "orders" / "2026-08-29.parquet")

def to_silver(df):
    df = df.copy()
    df["customer"] = df["customer"].str.strip().str.title()
    df["status"] = df["status"].str.strip().str.lower()
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["order_id"] = df["order_id"].astype(int)

    rejected = df[df["amount"].isna()]
    clean = df.dropna(subset=["amount"]).drop_duplicates(subset=["order_id"], keep="first")
    return clean, rejected

silver_df, rejected_df = to_silver(bronze_df)
print(f"Silver: {len(silver_df)} clean rows, {len(rejected_df)} rejected")
print(silver_df)

silver_df.to_parquet(lake / "silver" / "orders" / "orders.parquet", index=False)
```

```text
Silver: 2 clean rows, 1 rejected
   order_id     customer  amount  status
0      1001  Alice Smith   120.5    paid
1      1002    Bob Jones    89.0    paid
```

Row 1003 was dropped (unparseable amount) and the duplicate 1001 row was
collapsed to one — silver is now safe to join with other cleaned sources,
because every field has a reliable type and no duplicate keys remain.

## Gold: aggregate for consumption

```python
silver_orders = pd.read_parquet(lake / "silver" / "orders" / "orders.parquet")

gold_summary = (
    silver_orders
    .groupby("status", as_index=False)
    .agg(order_count=("order_id", "count"), total_amount=("amount", "sum"))
)
print(gold_summary)

gold_summary.to_parquet(lake / "gold" / "orders" / "status_summary.parquet", index=False)
```

```text
  status  order_count  total_amount
0   paid            2         209.5
```

This gold table is exactly what a dashboard would query: one row per status
with the numbers a business user actually cares about, no need to know
anything about the raw ingestion format or the cleaning rules that got it
here.

## Why three zones, not one

| Question | Answer, by zone |
|---|---|
| "What did the source actually send us on Aug 29?" | Bronze — the unmodified record |
| "Give me a clean, deduplicated, typed table of orders" | Silver — safe to join, still row-level |
| "What's our total paid revenue by status?" | Gold — aggregated, ready for a dashboard |

Reprocessing is the other big win: if you discover a bug in your cleaning
logic three months from now, you **re-run silver from bronze** — bronze
never changes, so the raw history is never lost, and you can regenerate a
corrected silver (and gold) without re-extracting from the original source
at all.

## Traps

- **Skipping bronze and cleaning directly on extract.** You lose the
  ability to reprocess with corrected logic without re-hitting the original
  source — which may no longer have the same data available (APIs change,
  files get deleted).
- **Writing silver logic that isn't re-runnable.** If `to_silver()` has any
  hidden dependency on run order or external state, replaying it later on
  the same bronze data won't reproduce the same silver output.
- **Treating gold as "final" and never revisiting it.** Gold tables should
  be considered derived and disposable — always regenerable from silver.
  Never hand-edit a gold table directly.
- **One giant folder instead of three zones.** Mixing raw and clean data in
  the same path makes it impossible for a reader to know whether a given
  file has been validated yet.

## Cheat sheet

| Zone | Contains | Mutability | Who reads it |
|---|---|---|---|
| Bronze | Raw, as-arrived data | Append-only, never edited | Pipeline engineers (reprocessing) |
| Silver | Cleaned, typed, deduplicated | Regenerable from bronze | Analysts, other pipelines |
| Gold | Aggregated, business-shaped | Regenerable from silver | Dashboards, reports, end users |

## Exercise

Add a second bronze file for `2026-08-30` with two new raw order rows (one
valid, one with a negative amount that should be rejected in silver). Re-run
`to_silver` against **both** bronze files concatenated together, regenerate
the gold `status_summary`, and confirm the gold numbers update correctly
without touching the original `2026-08-29.parquet` bronze file at all.
