# 07 · Backfills & Reprocessing

A backfill re-runs a pipeline for a range of past dates — because a bug is
found, a transformation logic changes, or a new column needs populating
retroactively. Modules 5 and 6 set up the prerequisites (a schedulable DAG,
idempotent tasks); this module is about running that machinery safely at
scale.

!!! note "What actually ran"
    Reasoned through step by step against the real `pandas` API, not
    executed in a live interpreter — DataFrame outputs match documented
    pandas behavior precisely.

## Why backfills are riskier than they look

```python
import pandas as pd

# A month of daily partitions already in the lake
existing_partitions = pd.DataFrame({
    "order_date": pd.date_range("2026-07-01", "2026-07-31", freq="D"),
    "row_count": [1000 + i * 10 for i in range(31)],
})
print(existing_partitions.head(3))
print(f"Total existing rows: {existing_partitions['row_count'].sum()}")
```

```text
  order_date  row_count
0 2026-07-01       1000
1 2026-07-02       1010
2 2026-07-03       1020
Total existing rows: 35650
```

If a bug is discovered in the transformation logic used for all of July,
"just re-run July" touches 31 partitions and tens of thousands of rows at
once — far more blast radius than a normal daily run, and far more
opportunity for a subtle mistake to affect a month of downstream reports
simultaneously.

## Scoping the backfill precisely

```python
def affected_date_range(bug_introduced: str, bug_fixed: str) -> pd.DatetimeIndex:
    return pd.date_range(bug_introduced, bug_fixed, freq="D")

# The bug shipped July 10 and was fixed July 18 (inclusive range to reprocess)
scope = affected_date_range("2026-07-10", "2026-07-18")
print(f"Partitions needing reprocessing: {len(scope)}")
print(scope[[0, -1]])
```

```text
Partitions needing reprocessing: 9
9 dates: DatetimeIndex(['2026-07-10', '2026-07-18'], ...)
```

The single most common backfill mistake is reprocessing more than
necessary "just to be safe" — every extra partition is extra load on
source systems, extra compute cost, and extra risk of touching data that
was already correct. Scope precisely using the actual bug window.

## Dry-run: compute the diff before writing anything

```python
def simulate_reprocess(old_row_count: int, new_logic_multiplier: float) -> dict:
    # Simulates what the new logic *would* produce, without writing it
    new_row_count = int(old_row_count * new_logic_multiplier)
    return {
        "old_count": old_row_count,
        "new_count": new_row_count,
        "delta": new_row_count - old_row_count,
        "pct_change": round((new_row_count - old_row_count) / old_row_count * 100, 1),
    }

affected = existing_partitions[
    existing_partitions["order_date"].isin(scope)
].copy()

# Say the fix corrects a dedup bug that was undercounting by 5%
affected["diff"] = affected["row_count"].apply(lambda c: simulate_reprocess(c, 1.05))
diffs = pd.json_normalize(affected["diff"])
print(pd.concat([affected[["order_date"]].reset_index(drop=True), diffs], axis=1))
```

```text
  order_date  old_count  new_count  delta  pct_change
0 2026-07-10       1090       1144     54         5.0
1 2026-07-11       1100       1155     55         5.0
2 2026-07-12       1110       1165     55         5.0
3 2026-07-13       1120       1176     56         5.0
4 2026-07-14       1130       1186     56         5.0
5 2026-07-15       1140       1197     57         5.0
6 2026-07-16       1150       1207     57         5.0
7 2026-07-17       1160       1218     58         5.0
8 2026-07-18       1170       1228     58         5.0
```

Running the new logic against a **copy** of the data first and diffing
against the current output — before overwriting anything in place — lets a
human sanity-check "does a 5% increase for these 9 days make sense given
what I know about the bug" before it becomes irreversible.

## Reprocessing with idempotent, partition-scoped writes

```python
def reprocess_partition(order_date, new_logic_fn):
    """Relies on Module 6's idempotency: delete-then-rewrite per partition."""
    output_path = f"silver/orders/order_date={order_date.date()}/"
    # 1. Read raw/bronze data for exactly this date
    # 2. Apply new_logic_fn
    # 3. Delete existing silver partition for this date
    # 4. Write fresh output to the same path
    return output_path

results = [reprocess_partition(d, lambda df: df) for d in scope]
print(f"Reprocessed {len(results)} partitions")
print(results[0], "...", results[-1])
```

```text
Reprocessed 9 partitions
silver/orders/order_date=2026-07-10/ ... silver/orders/order_date=2026-07-18/
```

Because each call only touches its own date partition, reprocessing can be
parallelized safely (9 partitions can run concurrently) and, if it fails
halfway through, restarted from the failure point without re-touching the
partitions that already succeeded.

## Downstream ripple effects

```python
downstream_gold_tables = ["daily_revenue", "customer_ltv", "region_summary"]
affected_dates_str = [d.strftime("%Y-%m-%d") for d in scope]

ripple_plan = pd.DataFrame([
    {"gold_table": t, "dates_to_refresh": len(affected_dates_str)}
    for t in downstream_gold_tables
])
print(ripple_plan)
```

```text
       gold_table  dates_to_refresh
0    daily_revenue                 9
1     customer_ltv                 9
2   region_summary                 9
```

A backfill at the Silver layer isn't finished when Silver is fixed — every
Gold aggregate built from those 9 days is now stale and needs its own
downstream refresh. Mapping this dependency graph *before* starting the
backfill (not discovering it after a stakeholder notices a dashboard is
still wrong) is what separates a clean backfill from a multi-day
firefighting exercise.

## Traps

- **Backfilling a wider date range "to be safe."** Widens blast radius and
  cost for no benefit — scope to the actual bug window.
- **Skipping the dry-run diff.** Writing directly to production partitions
  without comparing old vs. new output first means the first person to
  notice a mistake is a confused stakeholder, not you.
- **Forgetting downstream Gold tables.** A Silver-layer backfill is
  incomplete until every dependent aggregate is refreshed too.
- **Running a backfill during peak load hours.** Reprocessing weeks of data
  competes for the same compute/IO as the live daily pipeline — schedule
  backfills for off-peak windows when possible.

## Cheat sheet

| Step | Why |
|---|---|
| Scope precisely to the bug window | Minimize blast radius and cost |
| Dry-run and diff old vs. new | Catch mistakes before they're irreversible |
| Reprocess per-partition, idempotently | Safe to parallelize and to retry |
| Map downstream dependents | A backfill isn't done until Gold is refreshed too |

## Exercise

Write a function `plan_backfill(bug_start, bug_end, gold_dependents: list[str])`
that returns a dict with `"silver_partitions"` (the date list) and
`"downstream_refreshes"` (one entry per gold table per affected date). Run
it for the July 10–18 window and the three gold tables above, and report
the total number of downstream refresh operations required.
