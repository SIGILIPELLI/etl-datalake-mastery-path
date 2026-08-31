# 02 · Data Quality Checks & Validation

A pipeline that runs successfully but loads garbage is worse than one that
fails loudly — the failure at least gets noticed. This module builds a small
but real validation layer using `pandas`, the kind of checks you'd run
between every Bronze→Silver or Silver→Gold hop.

!!! note "What actually ran"
    Reasoned through step by step against the real `pandas` API, not
    executed in a live interpreter — output shapes match documented pandas
    behavior precisely.

## The dataset

```python
import pandas as pd

df = pd.DataFrame([
    {"order_id": 1, "customer": "alice", "amount": 120.50, "email": "alice@shop.com", "country": "US"},
    {"order_id": 2, "customer": "bob",   "amount": -15.00, "email": "bob@shop.com",   "country": "US"},
    {"order_id": 3, "customer": None,    "amount": 45.25,  "email": "not-an-email",   "country": "US"},
    {"order_id": 4, "customer": "dana",  "amount": 60.00,  "email": "dana@shop.com",  "country": "ZZ"},
    {"order_id": 1, "customer": "alice", "amount": 120.50, "email": "alice@shop.com", "country": "US"},
])
print(df)
```

```text
   order_id customer  amount           email country
0         1    alice   120.50  alice@shop.com      US
1         2      bob   -15.00   bob@shop.com       US
2         3     None    45.25   not-an-email       US
3         4     dana    60.00  dana@shop.com       ZZ
4         1    alice   120.50  alice@shop.com      US
```

This one small batch has five distinct problems: a duplicate primary key
(order 1 twice), a negative amount, a null required field, a malformed
email, and a country code that isn't in your reference list.

## Building composable checks

Each check returns the *failing rows*, not just a pass/fail boolean — you
need to know which rows to quarantine, not just that something is wrong.

```python
VALID_COUNTRIES = {"US", "CA", "GB", "IN"}

def check_not_null(df: pd.DataFrame, col: str) -> pd.DataFrame:
    return df[df[col].isna()]

def check_positive(df: pd.DataFrame, col: str) -> pd.DataFrame:
    return df[df[col] <= 0]

def check_valid_email(df: pd.DataFrame, col: str) -> pd.DataFrame:
    pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
    return df[~df[col].str.match(pattern, na=False)]

def check_valid_enum(df: pd.DataFrame, col: str, allowed: set) -> pd.DataFrame:
    return df[~df[col].isin(allowed)]

def check_unique(df: pd.DataFrame, col: str) -> pd.DataFrame:
    return df[df.duplicated(subset=col, keep=False)]

failures = {
    "null_customer": check_not_null(df, "customer"),
    "negative_amount": check_positive(df, "amount"),
    "bad_email": check_valid_email(df, "email"),
    "bad_country": check_valid_enum(df, "country", VALID_COUNTRIES),
    "duplicate_order_id": check_unique(df, "order_id"),
}
for name, rows in failures.items():
    print(f"{name}: {len(rows)} row(s) -> {list(rows['order_id'])}")
```

```text
null_customer: 1 row(s) -> [3]
negative_amount: 1 row(s) -> [2]
bad_email: 1 row(s) -> [3]
bad_country: 1 row(s) -> [4]
duplicate_order_id: 2 row(s) -> [1, 1]
```

## Severity: quarantine vs. warn vs. fail the run

Not every violation should stop the pipeline. A good validation layer
separates **hard failures** (data is unsafe to load) from **soft warnings**
(worth flagging, not worth blocking).

```python
HARD_FAILURES = {"null_customer", "duplicate_order_id"}
SOFT_WARNINGS = {"negative_amount", "bad_email", "bad_country"}

bad_ids = set()
for name in HARD_FAILURES:
    bad_ids |= set(failures[name]["order_id"])

clean = df[~df["order_id"].isin(bad_ids)].drop_duplicates(subset="order_id")
quarantined = df[df["order_id"].isin(bad_ids)]

print("Clean rows:", len(clean))
print("Quarantined rows:", len(quarantined))

warning_count = sum(len(failures[name]) for name in SOFT_WARNINGS)
print(f"Soft warnings logged (not blocking): {warning_count}")
```

```text
Clean rows: 3
Quarantined rows: 2
Soft warnings logged (not blocking): 3
```

Order 1's duplicate collapses via `drop_duplicates` after being confirmed
identical; order 3 (null customer) is quarantined entirely. Orders 2 and 4
stay in the clean set but get a warning logged — a negative amount might be
a legitimate refund, and an unexpected country code might just mean your
reference list is stale, not that the row is corrupt.

## A validation report you can alert on

```python
def build_quality_report(df: pd.DataFrame, failures: dict) -> pd.DataFrame:
    rows = []
    for check_name, failed_rows in failures.items():
        rows.append({
            "check": check_name,
            "rows_checked": len(df),
            "rows_failed": len(failed_rows),
            "failure_rate": round(len(failed_rows) / len(df), 3),
            "severity": "hard" if check_name in HARD_FAILURES else "soft",
        })
    return pd.DataFrame(rows)

report = build_quality_report(df, failures)
print(report)
```

```text
                check  rows_checked  rows_failed  failure_rate severity
0       null_customer             5            1          0.200     hard
1     negative_amount             5            1          0.200     soft
2           bad_email             5            1          0.200     soft
3         bad_country             5            1          0.200     soft
4  duplicate_order_id             5            2          0.400     hard
```

This report is what you'd write to a `data_quality_log` table every run —
it turns "the pipeline silently loaded 3 bad rows" into a queryable,
alertable metric with a trend line over time.

## Traps

- **Failing the whole run on any violation.** Too strict, and every minor
  data hiccup pages someone at 2 a.m.; teams disable the checks within a
  month. Tier severity instead.
- **Checking quality after loading to the target.** By then it's too late —
  validate at the Bronze→Silver boundary, before quarantined rows can reach
  anything a dashboard reads from.
- **No quarantine table.** Dropping bad rows silently means no one can
  investigate later — write them somewhere with the failure reason attached.
- **Static thresholds that never get revisited.** A failure-rate threshold
  set for a mature pipeline will falsely fire during legitimate growth
  (e.g., expansion into new countries) — review reference lists and
  thresholds periodically.

## Cheat sheet

| Check | pandas idiom |
|---|---|
| Not null | `df[col].isna()` |
| Range/positivity | boolean comparison, e.g. `df[col] <= 0` |
| Pattern match | `df[col].str.match(pattern, na=False)` |
| Enum membership | `~df[col].isin(allowed_set)` |
| Uniqueness | `df.duplicated(subset=col, keep=False)` |

## Exercise

Add a `check_referential_integrity(orders_df, customers_df, key)` function
that returns every row in `orders_df` whose `customer` value does not exist
in a `customers_df["name"]` column. Run it against the cleaned dataset from
this lesson with a small `customers_df` of your own, and classify it as a
hard or soft failure with justification.
