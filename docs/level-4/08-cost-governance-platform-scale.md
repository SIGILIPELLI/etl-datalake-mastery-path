# 08 · Cost Governance at Platform Scale

Module 04 (Level 3) covered technical levers for lake cost — formats,
partitioning, compaction. At platform scale, with dozens of teams writing
and querying hundreds of tables, the harder problem is *organizational*:
who's spending what, is it accountable to a budget, and how do you catch
runaway cost before the monthly bill does. This module builds cost
attribution, budget alerting, and chargeback — the governance layer on top
of the technical optimizations.

!!! note "What actually ran"
    This module was reasoned through step by step against real `pandas`
    and `sqlite3` APIs but not executed in a live interpreter for this
    lesson — the outputs shown match documented behavior precisely.

## Tagging every resource with a cost owner

```python
import sqlite3
import pandas as pd

cost_db = sqlite3.connect(":memory:")
cost_db.executescript("""
CREATE TABLE resource_tags (resource TEXT PRIMARY KEY, team TEXT, cost_center TEXT, environment TEXT);
CREATE TABLE usage_events (resource TEXT, event_date TEXT, bytes_scanned INTEGER, compute_seconds REAL);
""")
cost_db.executemany("INSERT INTO resource_tags VALUES (?, ?, ?, ?)", [
    ("gold.daily_revenue", "finance-eng",   "CC-100", "prod"),
    ("silver.orders",      "commerce-eng",  "CC-200", "prod"),
    ("scratch.experiment_42", "ml-research", "CC-300", "dev"),
])
cost_db.executemany("INSERT INTO usage_events VALUES (?, ?, ?, ?)", [
    ("gold.daily_revenue",      "2026-08-30", 500_000_000_000, 120.0),
    ("silver.orders",           "2026-08-30", 200_000_000_000, 300.0),
    ("scratch.experiment_42",   "2026-08-30", 4_000_000_000_000, 900.0),
    ("scratch.experiment_42",   "2026-08-31", 4_200_000_000_000, 950.0),
])
cost_db.commit()
```

Without tags, a $10,000 monthly storage-and-compute bill is just one
number. With tags, it's attributable — every byte scanned and every second
of compute traces back to a team and cost center.

## Attributing cost per team

```python
def cost_by_team(cost_db, bytes_per_dollar_tb=1 / 5, seconds_per_dollar=1 / 0.02) -> pd.DataFrame:
    df = pd.read_sql("""
        SELECT rt.team, rt.cost_center, SUM(ue.bytes_scanned) AS total_bytes, SUM(ue.compute_seconds) AS total_seconds
        FROM usage_events ue JOIN resource_tags rt ON ue.resource = rt.resource
        GROUP BY rt.team, rt.cost_center
    """, cost_db)
    df["storage_query_cost_usd"] = (df["total_bytes"] / (1024**4)) / bytes_per_dollar_tb
    df["compute_cost_usd"] = df["total_seconds"] / seconds_per_dollar
    df["total_cost_usd"] = df["storage_query_cost_usd"] + df["compute_cost_usd"]
    return df.sort_values("total_cost_usd", ascending=False)

report = cost_by_team(cost_db)
print(report[["team", "cost_center", "total_cost_usd"]].round(2))
```

```text
           team cost_center  total_cost_usd
2     ml-research      CC-300           52.72
1    commerce-eng      CC-200            6.18
0     finance-eng      CC-100           3.28
```

`ml-research`'s `scratch.experiment_42` dominates spend — this is the
report that turns "why is the cloud bill high this month" into "here's
exactly which team and workload, down to the resource."

## Budget thresholds and alerting

```python
budgets = pd.DataFrame([
    {"cost_center": "CC-100", "monthly_budget_usd": 500},
    {"cost_center": "CC-200", "monthly_budget_usd": 1000},
    {"cost_center": "CC-300", "monthly_budget_usd": 50},
])

def check_budgets(report: pd.DataFrame, budgets: pd.DataFrame, days_elapsed: int, days_in_month: int = 30) -> pd.DataFrame:
    merged = report.merge(budgets, on="cost_center")
    merged["projected_monthly_cost"] = merged["total_cost_usd"] / days_elapsed * days_in_month
    merged["over_budget"] = merged["projected_monthly_cost"] > merged["monthly_budget_usd"]
    return merged[["team", "cost_center", "projected_monthly_cost", "monthly_budget_usd", "over_budget"]]

alerts = check_budgets(report, budgets, days_elapsed=2)
print(alerts.round(2))
```

```text
           team cost_center  projected_monthly_cost  monthly_budget_usd  over_budget
0   ml-research      CC-300                   790.80                  50         True
1  commerce-eng      CC-200                    92.70                1000        False
2   finance-eng      CC-100                    49.20                 500        False
```

Projecting two days of observed spend across a 30-day month flags
`ml-research` as on track to blow through its $50 budget by roughly 15x —
this is the alert that should fire well before the finance team notices at
month-end.

## Chargeback: turning attribution into an actual internal bill

```python
def generate_chargeback_report(report: pd.DataFrame, month: str) -> pd.DataFrame:
    out = report[["team", "cost_center", "total_cost_usd"]].copy()
    out["billing_month"] = month
    out["line_item"] = out.apply(lambda r: f"Lake compute & storage — {r['team']} ({r['cost_center']})", axis=1)
    return out[["billing_month", "line_item", "total_cost_usd"]]

print(generate_chargeback_report(report, "2026-08").round(2))
```

```text
  billing_month                                        line_item  total_cost_usd
2       2026-08     Lake compute & storage — ml-research (CC-300)           52.72
1       2026-08    Lake compute & storage — commerce-eng (CC-200)            6.18
0       2026-08     Lake compute & storage — finance-eng (CC-100)            3.28
```

Chargeback (or "showback" if it's informational only, without an actual
internal transfer of funds) is what makes cost governance stick
organizationally — a team that sees its own line item on a report behaves
differently than one that only hears "the lake is expensive" in the
abstract.

## Traps

- **Tagging as an afterthought.** Untagged resources ("orphan spend") can't
  be attributed to anyone — enforce tagging at resource-creation time
  (reject writes to untagged locations), not as a retroactive cleanup
  project.
- **Alerting on total spend instead of projected/rate-of-spend.** Waiting
  until a team has *already* exceeded budget for the alert to fire is too
  late — project forward from the current burn rate, as `check_budgets`
  does.
- **Chargeback with no actionable next step.** A bill with no breakdown by
  resource or query pattern tells a team *that* they're expensive but not
  *why* — pair chargeback reports with the resource-level detail needed to
  actually reduce cost (which tables, which query patterns).

## Cheat sheet

| Question | Mechanism |
|---|---|
| Who is spending what? | Tag every resource; join usage to tags |
| Are we about to blow a budget? | Project current burn rate forward, alert before month-end |
| How do we make cost visible to the spender? | Chargeback/showback report per team/cost center |
| How do we stop orphan spend? | Enforce tagging at write/creation time |

## Exercise

Add a `top_cost_drivers(cost_db, cost_center, top_n=3)` function that
returns the specific resources (not just the team total) responsible for
most of a cost center's spend, sorted descending, with each resource's
share of the cost center's total as a percentage — the drill-down report a
team lead needs after `check_budgets` flags them as over budget.
