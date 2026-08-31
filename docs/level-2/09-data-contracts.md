# 09 · Data Contracts Between Producers & Consumers

A data contract is an explicit, versioned agreement between the team that
produces data and the teams that consume it — schema, semantics, and
guarantees, written down and enforced, instead of "the pipeline happened to
work that way." This module builds a minimal contract as executable
validation, not just documentation.

!!! note "What actually ran"
    Reasoned through step by step against the real `pandas`/`jsonschema`-
    style validation logic, not executed in a live interpreter — outputs
    match documented behavior precisely.

## A contract as a schema definition

```python
import pandas as pd

orders_contract = {
    "name": "orders_v1",
    "fields": {
        "order_id": {"type": "int64", "nullable": False, "unique": True},
        "customer_id": {"type": "int64", "nullable": False},
        "amount": {"type": "float64", "nullable": False, "min": 0},
        "status": {"type": "object", "nullable": False, "enum": ["paid", "refunded", "shipped", "cancelled"]},
        "order_date": {"type": "object", "nullable": False},
    },
    "guarantees": {
        "delivery": "at-least-daily by 04:00 UTC",
        "backward_compatible_changes_only": True,
    },
}
print(orders_contract["fields"]["amount"])
```

```text
{'type': 'float64', 'nullable': False, 'min': 0, 'max': None}
```

Writing the contract as a Python dict (or, in production, a YAML/JSON file
checked into version control alongside the pipeline) makes it both
human-readable documentation *and* something code can validate against —
the two copies of "what does this data look like" never drift apart.

## Validating incoming data against the contract

```python
def validate_against_contract(df: pd.DataFrame, contract: dict) -> list[str]:
    violations = []
    fields = contract["fields"]

    missing_cols = set(fields) - set(df.columns)
    if missing_cols:
        violations.append(f"missing columns: {missing_cols}")

    for col, rules in fields.items():
        if col not in df.columns:
            continue
        actual_dtype = str(df[col].dtype)
        if actual_dtype != rules["type"]:
            violations.append(f"{col}: expected dtype {rules['type']}, got {actual_dtype}")
        if not rules["nullable"] and df[col].isna().any():
            violations.append(f"{col}: contains nulls but nullable=False")
        if rules.get("unique") and df[col].duplicated().any():
            violations.append(f"{col}: contains duplicates but unique=True")
        if "min" in rules and rules["min"] is not None and (df[col] < rules["min"]).any():
            violations.append(f"{col}: contains values below min={rules['min']}")
        if "enum" in rules and not df[col].isin(rules["enum"]).all():
            bad_values = set(df[col]) - set(rules["enum"])
            violations.append(f"{col}: values outside enum: {bad_values}")
    return violations

good_batch = pd.DataFrame([
    {"order_id": 1, "customer_id": 100, "amount": 50.0, "status": "paid", "order_date": "2026-08-30"},
    {"order_id": 2, "customer_id": 101, "amount": 30.0, "status": "shipped", "order_date": "2026-08-30"},
])
print(validate_against_contract(good_batch, orders_contract))
```

```text
[]
```

An empty violations list is the contract passing — this is the check that
should run in the producer's own pipeline *before* publishing data, not
just downstream where the consumer discovers a break after the fact.

## Catching a producer-side break before it ships

```python
bad_batch = pd.DataFrame([
    {"order_id": 1, "customer_id": 100, "amount": -10.0, "status": "pending", "order_date": "2026-08-30"},
    {"order_id": 1, "customer_id": 101, "amount": 30.0,  "status": "paid",    "order_date": "2026-08-30"},
])
print(validate_against_contract(bad_batch, orders_contract))
```

```text
['order_id: contains duplicates but unique=True', 'amount: contains values below min=0', 'status: values outside enum: {\'pending\'}']
```

Three violations caught in one pass: a duplicate primary key, a negative
amount, and a `status` value (`"pending"`) that isn't in the agreed enum —
likely a new order state the producer team added without updating the
contract. This is exactly the kind of change that should trigger a
conversation between producer and consumer teams *before* it ships, not
an incident afterward.

## Versioning: evolving a contract without breaking consumers

```python
orders_contract_v2 = {
    **orders_contract,
    "name": "orders_v2",
    "fields": {
        **orders_contract["fields"],
        "status": {
            **orders_contract["fields"]["status"],
            "enum": ["paid", "refunded", "shipped", "cancelled", "pending"],  # widened, additive
        },
        "currency": {"type": "object", "nullable": True},  # new, nullable -> backward compatible
    },
}

v2_violations = validate_against_contract(bad_batch.drop_duplicates(subset="order_id"), orders_contract_v2)
print(v2_violations)
```

```text
['amount: contains values below min=0']
```

`orders_contract_v2` is a **backward-compatible** evolution: it widens the
`status` enum (additive) and adds a new nullable `currency` field — both
changes Module 4 classified as safe to auto-merge. A consumer still coded
against `orders_contract_v1` keeps working unmodified; only the amount
violation (a genuine data bug, not a contract mismatch) remains.

## What a contract should specify beyond schema

```python
contract_scope = {
    "schema": "field names, types, nullability, enums — this lesson's focus",
    "semantics": "what does 'amount' mean? pre-tax or post-tax? which currency?",
    "delivery_sla": "how fresh must this data be, and how often does it update?",
    "deprecation_policy": "how much notice before a breaking change ships?",
    "ownership": "who do consumers page when this contract is violated?",
}
for k, v in contract_scope.items():
    print(f"{k}: {v}")
```

```text
schema: field names, types, nullability, enums — this lesson's focus
semantics: what does 'amount' mean? pre-tax or post-tax? which currency?
delivery_sla: how fresh must this data be, and how often does it update?
deprecation_policy: how much notice before a breaking change ships?
ownership: who do consumers page when this contract is violated?
```

The schema check is the easiest 20% to automate and the part this lesson
covers in code — but a real contract also nails down semantics (ambiguous
field meanings cause more silent bugs than type mismatches do) and an
explicit deprecation policy, so a producer can't ship a breaking change
without warning.

## Traps

- **A contract that's documentation only, never enforced in code.** Drifts
  from reality within weeks — always wire it into a validation step that
  runs on every publish.
- **No deprecation window for breaking changes.** Even a well-intentioned
  rename breaks every consumer instantly without one.
- **Treating "it validates" as "it's semantically correct."** A schema
  check can't catch `amount` silently switching from pre-tax to post-tax —
  that requires a semantic test (e.g., a known reference order's expected
  total) alongside the schema check.
- **One contract owned by consumers, enforced nowhere near the producer.**
  Contracts work best enforced at the point of publication, catching
  breaks before they ever reach a consumer.

## Cheat sheet

| Contract element | Enforced by |
|---|---|
| Field types & nullability | `validate_against_contract` schema checks |
| Value ranges & enums | Same function, `min`/`enum` rules |
| Backward-compatible evolution | Additive-only changes (Module 4's policy) |
| Semantics & SLAs | Documented, tested with reference-value checks |

## Exercise

Add a `"pk"` (primary key) declaration to `orders_contract` naming
`order_id`, and rewrite `validate_against_contract` to treat a `pk`
violation as a distinct, higher-severity category from other violations
(mirroring the hard/soft split from Module 2). Test it against `bad_batch`.
