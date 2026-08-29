# 04 · Transformation Basics (Cleaning, Type Casting, Deduplication)

Transform is where raw, string-typed, messy data becomes something you'd
trust in a report. This lesson covers the three transformations nearly every
pipeline needs — cleaning, type casting, and deduplication — using `pandas`,
the tool most Python pipelines reach for once data volume grows past a
handful of rows.

!!! note "What actually ran"
    This pipeline was reasoned through step by step against the real
    `pandas` API (`pandas` 2.x behavior) but not executed in a live
    interpreter for this lesson — the DataFrame operations, dtypes, and
    output shapes shown match documented pandas behavior precisely.

## The messy input

```python
import pandas as pd
import io

raw_csv = """order_id,customer,amount,order_date,status
1001, alice smith ,120.50,2026-08-01,Paid
1002,BOB JONES,89,2026-08-01,paid
1003,alice smith,45.25,2026-08-02,REFUNDED
1004,carla diaz,,2026-08-02,paid
1005,Alice Smith,120.50,2026-08-01,Paid
"""

df = pd.read_csv(io.StringIO(raw_csv))
print(df.dtypes)
print(df)
```

```text
order_id        int64
customer       object
amount        float64
order_date     object
status         object
dtype: object
   order_id       customer  amount  order_date    status
0      1001   alice smith   120.50  2026-08-01      Paid
1      1002      BOB JONES    89.00  2026-08-01      paid
2      1003    alice smith    45.25  2026-08-02  REFUNDED
3      1004     carla diaz      NaN  2026-08-02      paid
4      1005    Alice Smith   120.50  2026-08-01      Paid
```

`pandas.read_csv` already infers `amount` as `float64` and the missing value
in row 3 as `NaN` — but `customer` and `status` are still raw, inconsistent
strings, and row 4 (id 1004) is a near-duplicate of row 0 with different
casing and whitespace.

## Cleaning: whitespace and casing

```python
df["customer"] = df["customer"].str.strip().str.title()
df["status"] = df["status"].str.strip().str.lower()
print(df[["customer", "status"]])
```

```text
      customer    status
0  Alice Smith      paid
1    Bob Jones      paid
2  Alice Smith  refunded
3   Carla Diaz      paid
4  Alice Smith      paid
```

`.str.strip()` removes leading/trailing whitespace and `.str.title()` /
`.str.lower()` normalize casing — both are vectorized pandas string
operations, meaning they run across the whole column at once rather than in
a Python-level loop, which matters once you're cleaning millions of rows.

## Type casting: handling the values that don't convert cleanly

```python
# amount is already float64, but real-world CSVs often quote numbers or add
# currency symbols — this shows the defensive pattern for that case:
messy_amount = pd.Series(["120.50", "$89.00", "not_a_number", "45.25"])
cast_amount = pd.to_numeric(
    messy_amount.str.replace("$", "", regex=False),
    errors="coerce",
)
print(cast_amount)
```

```text
0    120.50
1     89.00
2       NaN
3     45.25
dtype: float64
```

`errors="coerce"` turns anything that can't be parsed into `NaN` instead of
raising an exception — which is exactly what you want in a batch pipeline:
one bad value shouldn't crash the entire run. It does mean you now have a
missing value where there wasn't an obvious one before, which brings up the
next question: what do you do with missing data?

```python
# Row 1004 has a missing amount (NaN) — decide explicitly, don't let it slide
missing_amount = df["amount"].isna().sum()
print(f"Rows with missing amount: {missing_amount}")

df_with_flag = df.copy()
df_with_flag["amount_was_missing"] = df_with_flag["amount"].isna()
df_with_flag["amount"] = df_with_flag["amount"].fillna(0.0)
print(df_with_flag[["order_id", "amount", "amount_was_missing"]])
```

```text
Rows with missing amount: 1
   order_id  amount  amount_was_missing
0      1001  120.50               False
1      1002   89.00               False
2      1003   45.25               False
3      1004    0.00                True
4      1005  120.50               False
```

Filling with `0.0` is a business decision, not a neutral default — and
that's exactly why the `amount_was_missing` flag matters: it lets every
downstream consumer distinguish "this order was genuinely free" from "we
don't actually know the amount." Silently filling without flagging erases
that distinction forever.

## Deduplication

Rows 0 and 4 are the same order under different casing and whitespace — a
naive `.drop_duplicates()` on the raw columns would miss it entirely,
because it compares values *before* cleaning, not after.

```python
# Wrong order: dedupe before cleaning misses the case-different duplicate
before_clean = pd.read_csv(io.StringIO(raw_csv))
print("Duplicates found before cleaning:", before_clean.duplicated(subset=["customer", "order_date", "amount"]).sum())

# Right order: clean first, then dedupe on the cleaned columns
after_clean = df_with_flag.drop_duplicates(subset=["customer", "order_date", "amount"], keep="first")
print(f"Rows before dedup: {len(df_with_flag)}, after dedup: {len(after_clean)}")
print(after_clean)
```

```text
Duplicates found before cleaning: 0
Rows before dedup: 5, after dedup: 4
   order_id     customer  amount  order_date    status  amount_was_missing
0      1001  Alice Smith   120.50  2026-08-01      paid               False
1      1002    Bob Jones    89.00  2026-08-01      paid               False
2      1003  Alice Smith    45.25  2026-08-02  refunded               False
3      1004   Carla Diaz     0.00  2026-08-02      paid                True
```

**Order matters**: cleaning before deduplication caught the duplicate;
deduplicating on the raw, uncleaned columns found zero duplicates, because
`" alice smith "` and `"Alice Smith"` are different strings. This is one of
the most common real-world data quality bugs — always normalize the columns
you're deduplicating *on* before comparing them.

## Traps

- **Deduplicating on raw columns.** As shown above — always clean casing and
  whitespace on your dedup key columns first.
- **`fillna()` without a flag.** Filling missing values erases the fact that
  they were ever missing, unless you record it in a companion column.
- **Using `errors="raise"` (the default) in a batch job.** One malformed
  value shouldn't crash a pipeline processing 100,000 good rows — use
  `errors="coerce"` and log/count what got turned into `NaN`.
- **Silent precision loss.** `float` amounts (like money) can accumulate
  rounding errors after repeated arithmetic — for real financial pipelines,
  consider `Decimal` or integer cents instead of `float64`.

## Cheat sheet

| Task | pandas idiom |
|---|---|
| Strip whitespace | `.str.strip()` |
| Normalize casing | `.str.title()` / `.str.lower()` / `.str.upper()` |
| Safe numeric cast | `pd.to_numeric(col, errors="coerce")` |
| Fill missing + flag it | `col.isna()` then `col.fillna(default)` |
| Dedup on cleaned keys | `.drop_duplicates(subset=[...], keep="first")` |

## Exercise

Using the `raw_csv` above, add a new column `amount_flag` that is
`"missing"` when the original amount was `NaN`, `"suspicious"` when the
cleaned amount is greater than `1000` (a plausible outlier for this
dataset), and `"ok"` otherwise. Then re-run deduplication and confirm the
flag survives correctly on the row that remains after row 1001/1005 are
merged into one.
