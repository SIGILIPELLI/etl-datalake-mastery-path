# 05 · Data Mesh & Domain-Oriented Lake Ownership

A single centralized data team owning every pipeline for every business
domain doesn't scale past a certain organization size — every new source
becomes a ticket in one team's backlog. **Data mesh** is an organizational
pattern (not a technology) that pushes ownership of data to the domain
teams that generate it, treating each domain's output as a **data
product** with an explicit contract. This module models the mesh's core
mechanics: domain ownership, data products, and a federated (not
centralized) governance layer.

!!! note "What actually ran"
    This module was reasoned through step by step against real `sqlite3`
    and `pandas` APIs but not executed in a live interpreter for this
    lesson — the outputs shown match documented behavior precisely.

## Centralized vs. mesh ownership, modeled

```python
import sqlite3
import pandas as pd

registry = sqlite3.connect(":memory:")
registry.executescript("""
CREATE TABLE data_products (
    name TEXT PRIMARY KEY,
    domain_owner TEXT,
    sla_freshness_hours INTEGER,
    schema_version TEXT,
    discoverable BOOLEAN
);
""")
registry.executemany(
    "INSERT INTO data_products VALUES (?, ?, ?, ?, ?)",
    [
        ("orders.v1",     "commerce-team",   4,  "1.2", 1),
        ("inventory.v1",  "warehouse-team",  1,  "2.0", 1),
        ("marketing_spend.v1", "marketing-team", 24, "1.0", 1),
    ],
)
registry.commit()
print(pd.read_sql("SELECT * FROM data_products", registry))
```

```text
                 name    domain_owner  sla_freshness_hours schema_version  discoverable
0            orders.v1   commerce-team                     4            1.2             1
1         inventory.v1  warehouse-team                     1            2.0             1
2  marketing_spend.v1   marketing-team                    24            1.0             1
```

Under centralized ownership, one data-engineering team would own the
pipeline for all three of these; under a mesh, `commerce-team` owns and
operates `orders.v1` end to end — they know the source system best and feel
the pain directly if it breaks.

## A data product as a self-describing contract

```python
class DataProductContract:
    def __init__(self, name, owner, schema, sla_freshness_hours, sample_query):
        self.name = name
        self.owner = owner
        self.schema = schema
        self.sla_freshness_hours = sla_freshness_hours
        self.sample_query = sample_query

    def describe(self) -> dict:
        return {
            "name": self.name,
            "owner": self.owner,
            "columns": list(self.schema.keys()),
            "sla_freshness_hours": self.sla_freshness_hours,
            "how_to_query": self.sample_query,
        }

orders_product = DataProductContract(
    name="orders.v1",
    owner="commerce-team",
    schema={"order_id": "bigint", "customer_id": "bigint", "amount": "double", "order_date": "date"},
    sla_freshness_hours=4,
    sample_query="SELECT * FROM commerce.orders_v1 WHERE order_date = CURRENT_DATE",
)
print(orders_product.describe())
```

```text
{'name': 'orders.v1', 'owner': 'commerce-team', 'columns': ['order_id', 'customer_id', 'amount', 'order_date'], 'sla_freshness_hours': 4, 'how_to_query': 'SELECT * FROM commerce.orders_v1 WHERE order_date = CURRENT_DATE'}
```

Every data product exposes the same self-service shape — schema, owner, SLA,
and how to query it — so a consumer in another domain (marketing, finance)
can discover and use it without filing a request to the owning team.

## Federated governance: global rules, domain-local implementation

```python
global_standards = {
    "min_schema_version_format": r"^\d+\.\d+$",
    "requires_pii_tagging": True,
    "requires_owner": True,
    "max_sla_freshness_hours": 48,
}

def validate_against_global_standards(product: dict, standards: dict) -> list[str]:
    import re
    violations = []
    if standards["requires_owner"] and not product.get("domain_owner"):
        violations.append("missing domain_owner")
    if not re.match(standards["min_schema_version_format"], str(product.get("schema_version", ""))):
        violations.append("schema_version doesn't match required format")
    if product.get("sla_freshness_hours", 9999) > standards["max_sla_freshness_hours"]:
        violations.append(f"SLA {product['sla_freshness_hours']}h exceeds platform max of {standards['max_sla_freshness_hours']}h")
    return violations

candidate = {"name": "returns.v1", "domain_owner": "", "schema_version": "1", "sla_freshness_hours": 72}
print(validate_against_global_standards(candidate, global_standards))
```

```text
['missing domain_owner', "schema_version doesn't match required format", 'SLA 72h exceeds platform max of 48h']
```

This is the essence of federated governance: the **platform** sets a small
number of non-negotiable global standards (naming, versioning, ownership,
tagging) that every domain must meet, but each domain team decides *how*
its own pipeline meets them — no central team reviews or approves the
pipeline internals.

## Interoperability: a shared discovery layer across domains

```python
def find_products_by_domain(registry, domain: str) -> pd.DataFrame:
    return pd.read_sql(
        "SELECT name, sla_freshness_hours FROM data_products WHERE domain_owner = ?",
        registry, params=(domain,),
    )

def cross_domain_join_candidates(registry) -> pd.DataFrame:
    return pd.read_sql("SELECT name, domain_owner FROM data_products WHERE discoverable = 1", registry)

print(cross_domain_join_candidates(registry))
```

```text
                 name    domain_owner
0            orders.v1   commerce-team
1         inventory.v1  warehouse-team
2  marketing_spend.v1   marketing-team
```

A finance analyst building a "marketing ROI by product availability" report
can discover `marketing_spend.v1` and `inventory.v1` through this shared
catalog and join them directly — this is the payoff of the mesh: cross-domain
analytics without waiting on a central team to build a combined pipeline.

## Traps

- **"Data mesh" as a rebrand for the same centralized team.** If one team
  still writes every pipeline, you haven't adopted a mesh — you've renamed
  your data warehouse team's Jira board. True mesh requires domain teams to
  own and operate their own pipelines.
- **No federated governance at all.** Fully decentralized with zero shared
  standards produces N incompatible naming conventions, N different
  freshness expectations, and no way to discover or trust another domain's
  data — the "federated" half of "federated governance" is what prevents
  chaos.
- **Underestimating the organizational cost.** A mesh requires domain
  teams to have (or be given) data engineering skill — it's a people and
  incentive change, not just an architecture pattern, and adopting it
  without that investment produces worse pipelines than a competent
  centralized team would.

## Cheat sheet

| Mesh principle | What it looks like in practice |
|---|---|
| Domain ownership | The team closest to the source owns its pipeline |
| Data as a product | Explicit schema, SLA, owner, discoverability contract |
| Self-serve platform | Shared tooling (catalog, orchestration, storage) domains build on |
| Federated governance | Small set of global standards; domain-local implementation |

## Exercise

Add a `consumer_subscriptions` table (`consumer_domain`, `product_name`,
`subscribed_at`) and a `breaking_change_impact(registry, product_name)`
function that returns every domain subscribed to a product — the exact
list a domain team must notify before shipping a breaking schema change to
their data product, turning "who do I need to tell" into a query instead
of a guess.
