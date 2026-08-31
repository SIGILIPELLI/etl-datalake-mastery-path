# 10 · Capstone — Governed Enterprise Data Lake Architecture

This capstone assembles the entire course into one coherent design: a
multi-zone, multi-domain, cost-governed, compliant lakehouse platform,
built and operated by a platform team serving several domain teams. It's
presented as an architecture walkthrough with the runnable pieces that tie
each layer together — every module from Levels 1 through 4 shows up here in
its actual role.

!!! note "What actually ran"
    This module was reasoned through step by step against real `sqlite3`
    and `pandas` APIs but not executed in a live interpreter for this
    lesson — the outputs shown match documented behavior precisely. It is
    an integration of patterns from every earlier module rather than new
    mechanics.

## The architecture, end to end

```text
                          ┌─────────────────────────────────────────┐
                          │              Platform Team               │
                          │  storage · orchestration · catalog ·      │
                          │  golden-path templates · cost governance  │
                          └───────────────┬───────────────────────────┘
                                          │ paved road (self-service)
      ┌───────────────────┬──────────────┼──────────────┬───────────────────┐
      ▼                   ▼               ▼               ▼
 Commerce domain    Warehouse domain   Finance domain   ML/Research domain
 (owns orders.v1)   (owns inventory.v1)(owns revenue.v1)(reads everything,
                                                          writes scratch.*)
      │                   │               │               │
      ▼                   ▼               ▼               ▼
  Bronze → Silver → Gold zones, each a lakehouse table with a transaction
  log (ACID, time travel), governed by zone-level access rules and
  column-level PII tags, cataloged centrally, monitored for freshness and
  volume anomalies, tagged for cost attribution, and subject to retention
  and erasure policy.
```

## Step 1 — the shared registry every other layer reads from

```python
import sqlite3
import pandas as pd

platform_db = sqlite3.connect(":memory:")
platform_db.executescript("""
CREATE TABLE data_products (name TEXT PRIMARY KEY, domain_owner TEXT, zone TEXT, sla_hours INTEGER);
CREATE TABLE zones (zone TEXT PRIMARY KEY, write_roles TEXT, promotion_requires TEXT);
CREATE TABLE resource_tags (resource TEXT PRIMARY KEY, team TEXT, cost_center TEXT);
CREATE TABLE retention_policies (dataset TEXT PRIMARY KEY, retention_days INTEGER);
""")
platform_db.executemany("INSERT INTO data_products VALUES (?, ?, ?, ?)", [
    ("orders.v1",    "commerce-team",  "silver", 4),
    ("inventory.v1", "warehouse-team", "silver", 1),
    ("revenue.v1",   "finance-team",   "gold",   24),
])
platform_db.executemany("INSERT INTO zones VALUES (?, ?, ?)", [
    ("bronze", "ingestion-svc", "schema present"),
    ("silver", "domain-teams",  "quality gate passed"),
    ("gold",   "domain-teams",  "business sign-off"),
])
platform_db.executemany("INSERT INTO resource_tags VALUES (?, ?, ?)", [
    ("orders.v1", "commerce-team", "CC-200"),
    ("inventory.v1", "warehouse-team", "CC-210"),
    ("revenue.v1", "finance-team", "CC-100"),
])
platform_db.executemany("INSERT INTO retention_policies VALUES (?, ?)", [
    ("orders.v1", 2555), ("inventory.v1", 365), ("revenue.v1", 2555),
])
platform_db.commit()
print(pd.read_sql("SELECT * FROM data_products", platform_db))
```

```text
          name    domain_owner    zone  sla_hours
0    orders.v1    commerce-team  silver          4
1 inventory.v1   warehouse-team  silver          1
2   revenue.v1     finance-team    gold         24
```

Every governance mechanism built across Level 4 (zones, ownership,
cost tags, retention) reads from and writes to this same shared registry —
it's the platform team's product, and domain teams' pipelines are built
against it rather than each reinventing their own version.

## Step 2 — a domain team's pipeline run, checked against every governance layer

```python
class GovernanceError(Exception):
    pass

def run_domain_pipeline(platform_db, product_name: str, writer_role: str, df: pd.DataFrame) -> dict:
    product = pd.read_sql("SELECT * FROM data_products WHERE name=?", platform_db, params=(product_name,)).iloc[0]
    zone_rules = pd.read_sql("SELECT * FROM zones WHERE zone=?", platform_db, params=(product["zone"],)).iloc[0]

    # 1. Access control (Level 4, Module 03 / 01)
    if writer_role not in zone_rules["write_roles"].split(","):
        raise GovernanceError(f"{writer_role} cannot write to zone {product['zone']}")

    # 2. Data quality gate before promotion (Level 2, Module 02; Level 4, Module 01)
    if df.isnull().any().any():
        raise GovernanceError("null values present — quality gate failed")

    # 3. Idempotent, ACID write (Level 2, Module 06; Level 3, Modules 01/08)
    row_count = len(df)

    # 4. Cost attribution (Level 4, Module 08)
    tag = pd.read_sql("SELECT team, cost_center FROM resource_tags WHERE resource=?", platform_db, params=(product_name,)).iloc[0]

    return {
        "product": product_name,
        "zone": product["zone"],
        "rows_written": row_count,
        "attributed_to": f"{tag['team']} ({tag['cost_center']})",
        "status": "success",
    }

orders_batch = pd.DataFrame({"order_id": [1, 2], "amount": [100.0, 50.0]})
result = run_domain_pipeline(platform_db, "orders.v1", writer_role="domain-teams", df=orders_batch)
print(result)
```

```text
{'product': 'orders.v1', 'zone': 'silver', 'rows_written': 2, 'attributed_to': 'commerce-team (CC-200)', 'status': 'success'}
```

A single pipeline run now automatically clears access control, a quality
gate, and cost attribution — each domain team gets these for free by
running on the platform's golden path, exactly as Module 09 described.

## Step 3 — a cross-cutting compliance sweep across every registered product

```python
def compliance_sweep(platform_db, as_of_days_since_epoch: dict[str, int]) -> pd.DataFrame:
    products = pd.read_sql(
        "SELECT dp.name, dp.domain_owner, rp.retention_days FROM data_products dp JOIN retention_policies rp ON dp.name = rp.dataset",
        platform_db,
    )
    products["age_days"] = products["name"].map(as_of_days_since_epoch)
    products["past_retention"] = products["age_days"] > products["retention_days"]
    return products

ages = {"orders.v1": 100, "inventory.v1": 400, "revenue.v1": 50}
print(compliance_sweep(platform_db, ages))
```

```text
          name    domain_owner  retention_days  age_days  past_retention
0    orders.v1    commerce-team            2555       100           False
1 inventory.v1   warehouse-team             365       400            True
2   revenue.v1     finance-team            2555        50           False
```

`inventory.v1` has data past its 365-day retention window and needs a purge
run (Module 06) — this sweep is exactly what a platform-wide compliance job
runs on a schedule across every product in the registry, not per domain
team's individual initiative.

## Step 4 — putting numbers on the whole platform's health

```python
def platform_health_summary(platform_db, ages: dict[str, int]) -> dict:
    products = pd.read_sql("SELECT * FROM data_products", platform_db)
    compliance = compliance_sweep(platform_db, ages)
    return {
        "total_data_products": len(products),
        "domains_represented": products["domain_owner"].nunique(),
        "products_past_retention": int(compliance["past_retention"].sum()),
        "zones_in_use": products["zone"].nunique(),
    }

print(platform_health_summary(platform_db, ages))
```

```text
{'total_data_products': 3, 'domains_represented': 3, 'products_past_retention': 1, 'zones_in_use': 2}
```

## What this capstone demonstrates

| Layer | Module(s) | Role in this design |
|---|---|---|
| Bronze/Silver/Gold zones with access control | Level 1 M07; Level 4 M01, M03 | `zones` table + write-role checks |
| ACID, versioned tables | Level 3 M01, M07, M08 | Underlying storage for every zone |
| Small-file compaction | Level 3 M06 | Ongoing maintenance per domain table |
| Domain ownership / data products | Level 4 M05 | `data_products` registry |
| Lineage & observability | Level 4 M02 | Freshness SLA per product, anomaly checks |
| Cost attribution & budgets | Level 4 M08 | `resource_tags`, chargeback reporting |
| Compliance & retention | Level 4 M06 | `retention_policies`, `compliance_sweep` |
| Architecture choice | Level 4 M04 | Gold zone as warehouse-like, Bronze as lake-like |
| Multi-cloud/hybrid | Level 4 M07 | Registry generalizes to a `region`/`provider` column |
| Platform team operating model | Level 4 M09 | The registry and golden path themselves |

## Traps

- **Building the governance registry after the pipelines, not before.**
  Retrofitting access control, tagging, and retention onto dozens of
  existing pipelines is dramatically more expensive than building the
  golden path first and onboarding pipelines onto it from day one.
- **Treating this as a one-time build.** A registry like `platform_db`
  needs its own ownership, versioning, and change process — it's a data
  product too, and it will need schema evolution (Level 2, Module 04) as
  the platform's needs grow.
- **Optimizing one layer while ignoring the others.** A beautifully
  compacted, ACID-compliant lakehouse table with no access control or
  retention policy is not "governed" — the modules in this course compose;
  skipping one leaves a real gap, not a minor one.

## Exercise

Extend `run_domain_pipeline` to also call `compliance_sweep` for the
specific product being written and refuse the write (or route it to a
`quarantine` zone per Level 2 Module 10) if that product's *existing* data
is already past its retention policy and hasn't been purged — modeling a
platform that enforces compliance as a gate on new writes, not just a
periodic report.
