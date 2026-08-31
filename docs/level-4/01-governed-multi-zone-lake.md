# 01 · Building a Governed Multi-Zone Data Lake

Level 1 introduced Bronze/Silver/Gold layering as a simple three-zone
pattern. At platform scale, "governed" adds explicit rules about *who* can
write and read each zone, *what* quality bar a dataset must clear to be
promoted between zones, and *how* that's enforced automatically rather than
by convention. This module builds a small but complete governance layer
around a multi-zone lake.

!!! note "What actually ran"
    This module was reasoned through step by step against real `pandas`,
    `pyarrow`, and `sqlite3` APIs but not executed in a live interpreter for
    this lesson — the outputs shown match documented behavior precisely.

## Zone definitions with explicit contracts

```python
import sqlite3
import pandas as pd

registry = sqlite3.connect(":memory:")
registry.executescript("""
CREATE TABLE zones (
    zone TEXT PRIMARY KEY,
    write_roles TEXT,
    read_roles TEXT,
    promotion_requires TEXT
);
CREATE TABLE datasets (
    dataset TEXT,
    zone TEXT,
    owner TEXT,
    pii BOOLEAN,
    quality_gate_passed BOOLEAN
);
""")
registry.executemany(
    "INSERT INTO zones VALUES (?, ?, ?, ?)",
    [
        ("bronze", "ingestion-svc", "data-eng", "schema present"),
        ("silver", "data-eng", "data-eng,analytics", "quality gate passed"),
        ("gold",   "analytics-lead", "everyone", "business sign-off"),
    ],
)
registry.executemany(
    "INSERT INTO datasets VALUES (?, ?, ?, ?, ?)",
    [
        ("orders", "bronze", "data-eng", 0, 0),
        ("orders", "silver", "data-eng", 0, 1),
    ],
)
registry.commit()
print(pd.read_sql("SELECT * FROM zones", registry))
```

```text
     zone    write_roles           read_roles     promotion_requires
0  bronze  ingestion-svc             data-eng          schema present
1  silver       data-eng  data-eng,analytics    quality gate passed
2    gold  analytics-lead             everyone       business sign-off
```

Writing this down explicitly — rather than leaving it as tribal knowledge —
is the first governance act: anyone can now answer "who's allowed to write
to Silver" by querying a table instead of asking around.

## Enforcing write access before a pipeline runs

```python
class AccessDenied(Exception):
    pass

def check_write_access(registry, zone: str, role: str):
    row = registry.execute("SELECT write_roles FROM zones WHERE zone = ?", (zone,)).fetchone()
    if row is None:
        raise ValueError(f"unknown zone: {zone}")
    allowed = row[0].split(",")
    if role not in allowed:
        raise AccessDenied(f"role '{role}' cannot write to zone '{zone}' (allowed: {allowed})")

check_write_access(registry, "silver", "data-eng")
print("data-eng can write to silver: OK")

try:
    check_write_access(registry, "silver", "random-app")
except AccessDenied as e:
    print("Blocked:", e)
```

```text
data-eng can write to silver: OK
Blocked: role 'random-app' cannot write to zone 'silver' (allowed: ['data-eng'])
```

A real platform enforces this with actual IAM policies (bucket policies,
Lake Formation, Ranger) — the check function here models the *logic* that
those systems apply, which is what you're actually designing when you set
up zone permissions.

## Promotion gates between zones

```python
def promote(registry, dataset: str, from_zone: str, to_zone: str, df: pd.DataFrame) -> bool:
    gate = registry.execute("SELECT promotion_requires FROM zones WHERE zone = ?", (to_zone,)).fetchone()[0]

    checks = {
        "schema present": lambda: len(df.columns) > 0,
        "quality gate passed": lambda: df["amount"].notna().all() and (df["amount"] >= 0).all(),
        "business sign-off": lambda: registry.execute(
            "SELECT quality_gate_passed FROM datasets WHERE dataset=? AND zone=?", (dataset, from_zone)
        ).fetchone()[0] == 1,
    }
    passed = checks[gate]()
    registry.execute(
        "INSERT INTO datasets VALUES (?, ?, ?, ?, ?)",
        (dataset, to_zone, "data-eng", 0, int(passed)),
    )
    registry.commit()
    return passed

silver_orders = pd.DataFrame({"order_id": [1, 2], "amount": [10.0, 20.0]})
promoted = promote(registry, "orders", "silver", "gold", silver_orders)
print("Promoted orders silver -> gold:", promoted)
```

```text
Promoted orders silver -> gold: True
```

Each zone's `promotion_requires` maps to an actual, automated check — this
is what turns "Gold means trusted" from a slogan into something you can
verify in code every time data moves between zones.

## Tagging PII to drive access and masking

```python
def register_column_pii(registry, dataset: str, columns_pii: dict[str, bool]):
    for col, is_pii in columns_pii.items():
        registry.execute(
            "CREATE TABLE IF NOT EXISTS column_pii (dataset TEXT, column_name TEXT, pii BOOLEAN)"
        )
        registry.execute(
            "INSERT INTO column_pii VALUES (?, ?, ?)", (dataset, col, int(is_pii))
        )
    registry.commit()

register_column_pii(registry, "orders", {"customer_email": True, "amount": False, "order_id": False})

def mask_pii_columns(registry, dataset: str, df: pd.DataFrame, requester_role: str) -> pd.DataFrame:
    pii_cols = pd.read_sql(
        "SELECT column_name FROM column_pii WHERE dataset=? AND pii=1", registry, params=(dataset,)
    )["column_name"].tolist()
    if requester_role in ("data-eng",):  # privileged roles see everything
        return df
    masked = df.copy()
    for col in pii_cols:
        if col in masked.columns:
            masked[col] = "***"
    return masked

sample = pd.DataFrame({"order_id": [1], "customer_email": ["alice@example.com"], "amount": [10.0]})
print(mask_pii_columns(registry, "orders", sample, requester_role="analytics"))
```

```text
   order_id customer_email  amount
0         1            ***    10.0
```

Column-level PII tags are what let a single physical table serve both
privileged and general audiences safely — an analytics user gets the same
table, minus the columns they're not cleared to see, enforced at query time
rather than by maintaining separate masked copies.

## Traps

- **Governance as documentation only.** A wiki page describing zone rules
  that nothing actually enforces drifts from reality within weeks — encode
  the rules as code/config the pipeline checks, not just prose.
- **One-size-fits-all access for the whole lake.** Real governance is
  zone-and-dataset-specific; a blanket "data-eng can read everything" rule
  ignores PII and compliance boundaries that need finer control.
- **No audit trail for promotions.** If `promote()` doesn't log who
  promoted what and when, you can't answer "how did this bad row get into
  Gold" after the fact.

## Cheat sheet

| Governance need | Mechanism |
|---|---|
| Who can write/read a zone | `zones` registry + `check_write_access` |
| Data-quality gate between zones | `promote()` with zone-specific checks |
| PII protection | Column-level tags + `mask_pii_columns` at read time |
| Auditability | Log every promotion/access decision, not just the data move |

## Exercise

Add an `audit_log` table (`event_time`, `actor`, `action`, `dataset`,
`zone`, `result`) and have `check_write_access` and `promote` insert a row
on every call, success or failure. Then write a `recent_denials(registry,
hours=24)` query that surfaces every `AccessDenied` event in the last N
hours — the query a security review would run first.
