# 04 · Lake vs. Warehouse vs. Lakehouse: Choosing for a Use Case

By Level 4 you've built pieces of all three architectures — plain object
storage (Level 1), a transactional lakehouse table (Level 3), and touched
warehouse-style guarantees (schema enforcement, ACID). This module is about
the actual decision: given a real workload, which architecture fits, and
why "just use a lakehouse for everything" is itself a trade-off, not a free
win.

!!! note "What actually ran"
    This module was reasoned through step by step against real `pandas`
    and `pyarrow` characteristics but not executed in a live interpreter for
    this lesson — the comparisons shown reflect documented, well-known
    behavior of each architecture precisely.

## Modeling the three architectures' cost/capability trade-offs

```python
import pandas as pd

architectures = pd.DataFrame([
    {"arch": "Data Lake",       "schema_enforcement": "none (schema-on-read)", "acid": "no (per-file only)",     "cost_per_tb": "$",   "query_latency": "high (full scans common)", "best_for": "raw/unstructured retention, ML training data, cheap archival"},
    {"arch": "Data Warehouse",  "schema_enforcement": "strict (schema-on-write)", "acid": "yes, mature",          "cost_per_tb": "$$$", "query_latency": "low (optimized engine)",    "best_for": "BI dashboards, financial reporting, structured analytics"},
    {"arch": "Lakehouse",       "schema_enforcement": "enforced, evolvable",     "acid": "yes (via transaction log)", "cost_per_tb": "$$",  "query_latency": "medium-low",                "best_for": "unify BI + ML on one copy of data, avoid dual-write pipelines"},
])
pd.set_option("display.max_colwidth", 40)
print(architectures.to_string(index=False))
```

```text
       arch     schema_enforcement                 acid cost_per_tb                query_latency                                    best_for
  Data Lake  none (schema-on-read)   no (per-file only)           $ high (full scans common)     raw/unstructured retention, ML training data, cheap archival
Data Warehouse strict (schema-on-write)         yes, mature         $$$    low (optimized engine)          BI dashboards, financial reporting, structured analytics
  Lakehouse  enforced, evolvable  yes (via transaction log)          $$               medium-low  unify BI + ML on one copy of data, avoid dual-write pipelines
```

## A decision function grounded in workload characteristics

```python
def recommend_architecture(
    needs_sub_second_bi: bool,
    has_ml_training_workloads: bool,
    data_is_mostly_unstructured: bool,
    needs_acid_updates: bool,
    team_already_runs_spark_or_trino: bool,
) -> str:
    if data_is_mostly_unstructured and not needs_acid_updates:
        return "Data Lake — cheap storage for raw/unstructured data with no update guarantees needed"
    if needs_sub_second_bi and not has_ml_training_workloads:
        return "Data Warehouse — dedicated engine optimization matters more than unifying with ML workloads"
    if has_ml_training_workloads and needs_acid_updates:
        return "Lakehouse — one copy of governed, ACID data serves both BI and ML without duplicating pipelines"
    if needs_acid_updates and team_already_runs_spark_or_trino:
        return "Lakehouse — you already have the query engines; add the transactional layer on existing storage"
    return "Data Warehouse — default to the simpler, most mature option absent a specific lake/lakehouse driver"

print(recommend_architecture(
    needs_sub_second_bi=False,
    has_ml_training_workloads=True,
    data_is_mostly_unstructured=False,
    needs_acid_updates=True,
    team_already_runs_spark_or_trino=True,
))
```

```text
Lakehouse — one copy of governed, ACID data serves both BI and ML without duplicating pipelines
```

## Worked example: three teams, three different right answers

```python
scenarios = [
    {
        "team": "Fraud ML team",
        "needs_sub_second_bi": False, "has_ml_training_workloads": True,
        "data_is_mostly_unstructured": True, "needs_acid_updates": False,
        "team_already_runs_spark_or_trino": True,
    },
    {
        "team": "Finance reporting",
        "needs_sub_second_bi": True, "has_ml_training_workloads": False,
        "data_is_mostly_unstructured": False, "needs_acid_updates": True,
        "team_already_runs_spark_or_trino": False,
    },
    {
        "team": "Platform data team (serves both)",
        "needs_sub_second_bi": False, "has_ml_training_workloads": True,
        "data_is_mostly_unstructured": False, "needs_acid_updates": True,
        "team_already_runs_spark_or_trino": True,
    },
]

for s in scenarios:
    team = s.pop("team")
    print(f"{team}: {recommend_architecture(**s)}")
```

```text
Fraud ML team: Data Lake — cheap storage for raw/unstructured data with no update guarantees needed
Finance reporting: Data Warehouse — dedicated engine optimization matters more than unifying with ML workloads
Platform data team (serves both): Lakehouse — one copy of governed, ACID data serves both BI and ML without duplicating pipelines
```

Three legitimate architectures for three different teams inside the same
company — the mistake isn't picking "the wrong one" in the abstract, it's
picking one architecture company-wide and forcing every workload to fit it.

## The real cost of "just use a lakehouse for everything"

```python
def lakehouse_overkill_check(workload: dict) -> str:
    if not workload["needs_acid_updates"] and not workload["has_ml_training_workloads"]:
        return "Lakehouse adds transaction-log overhead and operational complexity with no benefit here — plain lake or warehouse is simpler"
    return "Lakehouse trade-offs (extra metadata, compaction jobs, learning curve) are justified by the ACID/unification need"

print(lakehouse_overkill_check({"needs_acid_updates": False, "has_ml_training_workloads": False}))
```

```text
Lakehouse adds transaction-log overhead and operational complexity with no benefit here — plain lake or warehouse is simpler
```

A lakehouse still needs compaction jobs (Level 3, Module 06), a catalog
(Level 3, Module 03), and a team that understands transaction-log semantics
(Level 3, Modules 01/07/08) — none of that is free, and a workload that
never needs ACID updates or ML access to the same data gets none of the
benefit while paying all of the operational cost.

## Traps

- **Choosing based on vendor marketing rather than workload shape.** "Lake"
  and "Lakehouse" are heavily marketed terms; the actual question is always
  update patterns, latency requirements, and who else needs the same data.
- **Migrating a stable, working warehouse workload to a lakehouse "because
  it's newer."** If BI latency and update patterns are already met, this
  trades a mature, well-understood system for migration risk with no
  concrete win.
- **Running three separate copies of the same data across lake, warehouse,
  and lakehouse "just in case."** This is exactly the dual-write pipeline
  problem lakehouses exist to solve — pick one governed source of truth per
  dataset.

## Cheat sheet

| Signal | Leans toward |
|---|---|
| Mostly unstructured, cheap archival, ML raw data | Data Lake |
| Sub-second BI, mature tooling, no ML/unification need | Data Warehouse |
| Same data needs both BI and ML, or needs ACID + existing lake tooling | Lakehouse |
| No update/ACID need at all | Whichever is simplest to operate — usually not a lakehouse |

## Exercise

Extend `recommend_architecture` with a `data_volume_tb` and
`team_size` parameter, and add a rule: below some threshold (e.g., under 1TB
and a team of 1-2 people), recommend against a lakehouse or warehouse
regardless of other signals, favoring "a managed cloud data warehouse's
free/small tier or just Parquet-on-object-storage with pandas/DuckDB" —
capturing the real-world point that architecture choice also depends on
whether the team has the headcount to operate the platform they're
choosing.
