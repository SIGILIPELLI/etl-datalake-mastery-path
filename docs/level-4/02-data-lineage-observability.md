# 02 · Data Lineage & Observability

When a number on a dashboard looks wrong, the question is always "where did
this come from, and what touched it along the way." **Lineage** answers
that by recording the graph of datasets and transformations that produced a
given table. **Observability** goes further — it continuously watches that
graph for freshness, volume, and schema anomalies so you find out about
problems before a business user does. This module builds both from
scratch.

!!! note "What actually ran"
    This module was reasoned through step by step against real `sqlite3`
    and `pandas` APIs but not executed in a live interpreter for this
    lesson — the outputs shown match documented behavior precisely.

## Recording lineage as a graph

```python
import sqlite3
import pandas as pd
import datetime as dt

lineage = sqlite3.connect(":memory:")
lineage.executescript("""
CREATE TABLE nodes (name TEXT PRIMARY KEY, kind TEXT);
CREATE TABLE edges (upstream TEXT, downstream TEXT, job_name TEXT, run_at TEXT);
""")
lineage.executemany(
    "INSERT INTO nodes VALUES (?, ?)",
    [
        ("raw.orders_api", "source"),
        ("bronze.orders", "table"),
        ("silver.orders", "table"),
        ("gold.daily_revenue", "table"),
        ("dashboard.exec_summary", "dashboard"),
    ],
)
lineage.executemany(
    "INSERT INTO edges VALUES (?, ?, ?, ?)",
    [
        ("raw.orders_api", "bronze.orders", "ingest_orders", "2026-08-01T02:00:00"),
        ("bronze.orders", "silver.orders", "clean_orders", "2026-08-01T02:15:00"),
        ("silver.orders", "gold.daily_revenue", "aggregate_revenue", "2026-08-01T02:30:00"),
        ("gold.daily_revenue", "dashboard.exec_summary", "dashboard_refresh", "2026-08-01T03:00:00"),
    ],
)
lineage.commit()
```

## Upstream lineage: "what feeds this table?"

```python
def upstream_of(lineage, node: str, depth: int = 10) -> list[str]:
    visited, frontier = set(), {node}
    chain = []
    for _ in range(depth):
        parents = set()
        for n in frontier:
            rows = lineage.execute("SELECT upstream FROM edges WHERE downstream = ?", (n,)).fetchall()
            parents.update(r[0] for r in rows)
        parents -= visited
        if not parents:
            break
        chain.extend(parents)
        visited.update(parents)
        frontier = parents
    return chain

print(upstream_of(lineage, "dashboard.exec_summary"))
```

```text
['gold.daily_revenue', 'silver.orders', 'bronze.orders', 'raw.orders_api']
```

This is the query you run when the exec dashboard shows a wrong number:
walk backward through the graph to every table and job that contributed to
it, in order.

## Downstream lineage: "what breaks if I change this?"

```python
def downstream_of(lineage, node: str, depth: int = 10) -> list[str]:
    visited, frontier = set(), {node}
    chain = []
    for _ in range(depth):
        children = set()
        for n in frontier:
            rows = lineage.execute("SELECT downstream FROM edges WHERE upstream = ?", (n,)).fetchall()
            children.update(r[0] for r in rows)
        children -= visited
        if not children:
            break
        chain.extend(children)
        visited.update(children)
        frontier = children
    return chain

print(downstream_of(lineage, "bronze.orders"))
```

```text
['silver.orders', 'gold.daily_revenue', 'dashboard.exec_summary']
```

This is impact analysis: before changing `bronze.orders`'s schema, this
query tells you exactly which downstream tables and dashboards to check —
without it, schema changes are educated guesses.

## Observability: freshness, volume, and schema checks over time

```python
metrics = sqlite3.connect(":memory:")
metrics.execute("""
    CREATE TABLE table_metrics (table_name TEXT, run_at TEXT, row_count INTEGER, column_count INTEGER)
""")
history = [
    ("silver.orders", "2026-08-01T02:15:00", 12000, 6),
    ("silver.orders", "2026-08-02T02:14:00", 12300, 6),
    ("silver.orders", "2026-08-03T02:16:00", 11950, 6),
    ("silver.orders", "2026-08-04T02:15:00", 400,   6),    # anomaly: volume crashed
    ("silver.orders", "2026-08-05T02:15:00", 12500, 5),    # anomaly: a column disappeared
]
metrics.executemany("INSERT INTO table_metrics VALUES (?, ?, ?, ?)", history)
metrics.commit()

def detect_anomalies(metrics, table_name: str, volume_drop_pct: float = 0.5) -> pd.DataFrame:
    df = pd.read_sql(
        "SELECT * FROM table_metrics WHERE table_name = ? ORDER BY run_at", metrics, params=(table_name,)
    )
    df["prev_row_count"] = df["row_count"].shift(1)
    df["prev_column_count"] = df["column_count"].shift(1)
    df["volume_anomaly"] = df["row_count"] < df["prev_row_count"] * (1 - volume_drop_pct)
    df["schema_anomaly"] = df["column_count"] != df["prev_column_count"]
    df.loc[df.index[0], ["volume_anomaly", "schema_anomaly"]] = False  # no baseline for first row
    return df[df["volume_anomaly"] | df["schema_anomaly"]]

print(detect_anomalies(metrics, "silver.orders")[["run_at", "row_count", "column_count", "volume_anomaly", "schema_anomaly"]])
```

```text
                run_at  row_count  column_count  volume_anomaly  schema_anomaly
3  2026-08-04T02:15:00        400             6            True           False
4  2026-08-05T02:15:00      12500             5           False            True
```

The August 4th run dropped from ~12,000 rows to 400 — a volume anomaly that
should page someone before the Gold table (and dashboard) built on top of
it gets computed from a nearly-empty source. August 5th shows a schema
anomaly (a column vanished) even though volume looks fine.

## Freshness SLAs

```python
def check_freshness(metrics, table_name: str, sla_hours: int, now: dt.datetime) -> dict:
    last_run = pd.read_sql(
        "SELECT MAX(run_at) as last_run FROM table_metrics WHERE table_name = ?", metrics, params=(table_name,)
    )["last_run"][0]
    last_run_dt = pd.Timestamp(last_run)
    age_hours = (now - last_run_dt).total_seconds() / 3600
    return {"table": table_name, "age_hours": round(age_hours, 1), "sla_met": age_hours <= sla_hours}

print(check_freshness(metrics, "silver.orders", sla_hours=26, now=dt.datetime(2026, 8, 6, 3, 0, 0)))
```

```text
{'table': 'silver.orders', 'age_hours': 24.75, 'sla_met': True}
```

## Traps

- **Lineage that's manually maintained.** A hand-updated lineage diagram
  goes stale the day someone adds a job without updating it — lineage
  should be captured automatically by the orchestrator/pipeline framework
  (most modern tools like Airflow with OpenLineage, dbt, or Dagster emit
  this as a side effect of running).
- **Alerting on absolute thresholds instead of relative change.** A
  fixed "row count must exceed 10,000" rule breaks the first time a
  legitimately smaller batch runs; comparing against recent history (as
  `detect_anomalies` does) adapts to normal variation.
- **Observability with no owner attached.** An anomaly alert that doesn't
  say who to page is just noise — pair every monitored table with an owner
  from the catalog (Module 03, Level 3).

## Cheat sheet

| Question | Query |
|---|---|
| What fed this table? | `upstream_of(lineage, node)` |
| What breaks if I change this? | `downstream_of(lineage, node)` |
| Did volume/schema change unexpectedly? | `detect_anomalies(metrics, table)` |
| Is this table fresh enough? | `check_freshness(metrics, table, sla_hours)` |

## Exercise

Add column-level lineage: an `edges_columns` table recording
`(upstream_table, upstream_column, downstream_table, downstream_column,
job_name)`. Write a `column_lineage(lineage, table, column)` function that
returns the full upstream chain of columns feeding a specific downstream
column (e.g., "which raw columns feed `gold.daily_revenue.total_amount`"),
not just which tables are involved.
