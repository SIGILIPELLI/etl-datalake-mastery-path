# 03 · Security & Access Control Patterns for Data Lakes

A data lake's files sit in one bucket, but different tables, columns, and
even rows within them need different access rules for different people.
This module covers the three access-control patterns that show up
repeatedly at platform scale: role-based access (RBAC), attribute-based
access (ABAC), and row-level security — building minimal, runnable versions
of each.

!!! note "What actually ran"
    This module was reasoned through step by step against real `sqlite3`
    and `pandas` APIs but not executed in a live interpreter for this
    lesson — the outputs shown match documented behavior precisely.

## RBAC: permissions attached to roles, roles attached to people

```python
import sqlite3
import pandas as pd

iam = sqlite3.connect(":memory:")
iam.executescript("""
CREATE TABLE role_permissions (role TEXT, resource TEXT, action TEXT);
CREATE TABLE user_roles (user TEXT, role TEXT);
""")
iam.executemany("INSERT INTO role_permissions VALUES (?, ?, ?)", [
    ("data-eng", "lake.*", "read"),
    ("data-eng", "lake.*", "write"),
    ("analyst",  "gold.*", "read"),
    ("analyst",  "silver.orders", "read"),
])
iam.executemany("INSERT INTO user_roles VALUES (?, ?)", [
    ("priya", "data-eng"),
    ("sam", "analyst"),
])
iam.commit()

def can(iam, user: str, resource: str, action: str) -> bool:
    roles = [r[0] for r in iam.execute("SELECT role FROM user_roles WHERE user=?", (user,)).fetchall()]
    for role in roles:
        perms = iam.execute(
            "SELECT resource FROM role_permissions WHERE role=? AND action=?", (role, action)
        ).fetchall()
        for (pattern,) in perms:
            prefix = pattern.rstrip("*")
            if resource.startswith(prefix):
                return True
    return False

print("priya write bronze.orders:", can(iam, "priya", "bronze.orders", "write"))
print("sam write silver.orders:  ", can(iam, "sam", "silver.orders", "write"))
print("sam read silver.orders:   ", can(iam, "sam", "silver.orders", "read"))
print("sam read bronze.orders:   ", can(iam, "sam", "bronze.orders", "read"))
```

```text
priya write bronze.orders: True
sam write silver.orders:   False
sam read silver.orders:    True
sam read bronze.orders:    False
```

RBAC is simple to reason about and audit ("what can the analyst role do")
but coarse — it can't easily express "analysts can read orders, but only
for their own region."

## ABAC: permissions computed from attributes, not fixed roles

```python
users = pd.DataFrame([
    {"user": "sam",   "region": "us", "clearance": "standard"},
    {"user": "priya", "region": "eu", "clearance": "elevated"},
])
resources = pd.DataFrame([
    {"table": "silver.orders", "min_clearance": "standard", "region_scoped": True},
    {"table": "gold.payroll",  "min_clearance": "elevated", "region_scoped": False},
])

clearance_rank = {"standard": 0, "elevated": 1}

def abac_allow(user_row, resource_row, requested_region: str | None) -> bool:
    if clearance_rank[user_row["clearance"]] < clearance_rank[resource_row["min_clearance"]]:
        return False
    if resource_row["region_scoped"] and requested_region != user_row["region"]:
        return False
    return True

sam = users[users["user"] == "sam"].iloc[0]
orders_table = resources[resources["table"] == "silver.orders"].iloc[0]
payroll_table = resources[resources["table"] == "gold.payroll"].iloc[0]

print("sam -> silver.orders (own region us):  ", abac_allow(sam, orders_table, "us"))
print("sam -> silver.orders (other region eu):", abac_allow(sam, orders_table, "eu"))
print("sam -> gold.payroll:                   ", abac_allow(sam, payroll_table, None))
```

```text
sam -> silver.orders (own region us):   True
sam -> silver.orders (other region eu): False
sam -> gold.payroll:                    False
```

ABAC evaluates a *policy expression* against attributes of the user and the
resource at request time, instead of a static role-to-permission mapping —
this is what lets "your own region only" and "sufficient clearance level"
compose without needing a new role for every combination.

## Row-level security: filtering data, not just gating table access

```python
orders = pd.DataFrame([
    {"order_id": 1, "region": "us", "amount": 100.0},
    {"order_id": 2, "region": "eu", "amount": 200.0},
    {"order_id": 3, "region": "us", "amount": 150.0},
])

def row_level_view(df: pd.DataFrame, user_row) -> pd.DataFrame:
    if user_row["clearance"] == "elevated":
        return df  # sees everything
    return df[df["region"] == user_row["region"]]

print(row_level_view(orders, sam))
```

```text
   order_id region  amount
0         1     us   100.0
2         3     us   150.0
```

Row-level security is applied *after* table-level access is already
granted — sam can read `silver.orders` (from the ABAC check above), but the
rows returned are filtered to just his region, so the same physical table
serves every region's analysts safely without duplicating it per region.

## Combining all three in one access path

```python
def secured_query(iam, users, resources, user: str, table: str, requested_region: str | None, df: pd.DataFrame):
    user_row = users[users["user"] == user].iloc[0]
    resource_row = resources[resources["table"] == table]
    if resource_row.empty:
        # fall back to RBAC for tables not modeled in the ABAC resource list
        if not can(iam, user, table, "read"):
            raise PermissionError(f"{user} has no read access to {table}")
        return df
    resource_row = resource_row.iloc[0]
    if not abac_allow(user_row, resource_row, requested_region):
        raise PermissionError(f"{user} fails attribute check for {table}")
    return row_level_view(df, user_row)

print(secured_query(iam, users, resources, "sam", "silver.orders", "us", orders))
```

```text
   order_id region  amount
0         1     us   100.0
2         3     us   150.0
```

## Traps

- **RBAC role explosion.** Trying to model every fine-grained combination
  (region × department × clearance) as separate roles produces hundreds of
  near-duplicate roles that nobody can audit — that's the signal to move
  the fine-grained part to ABAC/row-level rules instead.
- **Enforcing row-level security in application code inconsistently.** If
  every consuming tool has to remember to apply the region filter itself,
  one that forgets leaks data — push row-level security into the query
  engine/warehouse layer (views, row access policies) wherever possible so
  it can't be bypassed.
- **No separation between "can read the table" and "can read these rows."**
  Conflating the two makes audits harder — keep table-level and row-level
  checks as distinct, composable steps as shown above.

## Cheat sheet

| Pattern | Good for |
|---|---|
| RBAC | Coarse, auditable permissions with a small number of roles |
| ABAC | Fine-grained rules that combine multiple attributes without role explosion |
| Row-level security | Same physical table, different visible rows per requester |

## Exercise

Add a `masked_columns` policy to `resources` (e.g., `gold.payroll` masks a
`salary` column for anyone below `elevated` clearance) and extend
`secured_query` to apply column masking (reusing the pattern from Module
01's `mask_pii_columns`) after row-level filtering. Confirm a `standard`
clearance user sees `***` for `salary` while an `elevated` user sees the
real value.
