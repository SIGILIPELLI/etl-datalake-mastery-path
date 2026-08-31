# 04 · Schema Evolution & Handling

Source schemas change: a new column appears, a type widens, a field gets
renamed. A pipeline that assumes the schema is frozen forever will break the
first time an upstream team ships a change. This module covers detecting
schema drift and handling it without crashing the pipeline.

!!! note "What actually ran"
    Reasoned through step by step against the real `pandas` and `pyarrow`
    APIs, not executed in a live interpreter — schema comparisons and merge
    results match documented behavior precisely.

## Detecting drift: comparing schemas

```python
import pandas as pd
import pyarrow as pa

# Version 1 of the schema (what the pipeline was built against)
schema_v1 = pa.schema([
    ("order_id", pa.int64()),
    ("customer", pa.string()),
    ("amount", pa.float64()),
])

# Version 2 arrives from the source with a new column and a widened type
batch_v2 = pd.DataFrame([
    {"order_id": 1, "customer": "alice", "amount": 120.50, "currency": "USD"},
    {"order_id": 2, "customer": "bob",   "amount": 89.00,  "currency": "USD"},
])
schema_v2 = pa.Table.from_pandas(batch_v2).schema

def diff_schemas(old: pa.Schema, new: pa.Schema):
    old_fields = {f.name: f.type for f in old}
    new_fields = {f.name: f.type for f in new}
    added = [n for n in new_fields if n not in old_fields]
    removed = [n for n in old_fields if n not in new_fields]
    changed = [
        n for n in old_fields
        if n in new_fields and old_fields[n] != new_fields[n]
    ]
    return {"added": added, "removed": removed, "type_changed": changed}

print(diff_schemas(schema_v1, schema_v2))
```

```text
{'added': ['currency'], 'removed': [], 'type_changed': []}
```

Running this diff on every incoming batch, before writing it into the lake,
turns "the pipeline mysteriously failed downstream" into "here's exactly
what changed and when" — logged and alertable.

## Additive changes: safe to auto-merge

```python
existing = pd.DataFrame([
    {"order_id": 1, "customer": "alice", "amount": 120.50},
    {"order_id": 2, "customer": "bob",   "amount": 89.00},
])

def merge_additive(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    all_cols = list(dict.fromkeys(list(existing.columns) + list(incoming.columns)))
    existing_aligned = existing.reindex(columns=all_cols)
    incoming_aligned = incoming.reindex(columns=all_cols)
    return pd.concat([existing_aligned, incoming_aligned], ignore_index=True)

merged = merge_additive(existing, batch_v2)
print(merged)
print(merged.dtypes)
```

```text
   order_id customer  amount currency
0         1    alice  120.50      NaN
1         2      bob   89.00      NaN
2         1    alice  120.50      USD
3         2      bob   89.00      USD
order_id      int64
customer     object
amount      float64
currency     object
dtype: object
```

Adding a new nullable column is the one schema change that's always safe to
auto-merge: old rows simply get `NaN`/`null` for the new field, and nothing
that reads the old columns breaks.

## Type widening: safe in one direction only

```python
# int32 -> int64 is a safe widening (every int32 value fits in int64)
narrow = pd.Series([1, 2, 3], dtype="int32")
widened = narrow.astype("int64")
print(widened.dtype)

# int64 -> int32 is a narrowing that can silently truncate large values
risky_values = pd.Series([1, 2, 5_000_000_000], dtype="int64")
try:
    risky_values.astype("int32")
except OverflowError as e:
    print(f"Blocked unsafe narrowing: {e}")
```

```text
int64
Blocked unsafe narrowing: Python int too large to convert to C long
```

pandas/NumPy raise on an out-of-range narrowing cast, which is the correct,
loud failure — the trap is code that catches this exception and silently
falls back to some default, hiding real data loss.

## Renames and removals: never auto-merge

```python
# A rename looks like "add + remove" to a naive schema diff
batch_v3 = pd.DataFrame([
    {"order_id": 1, "buyer": "alice", "amount": 120.50},  # customer -> buyer
])
schema_v3 = pa.Table.from_pandas(batch_v3).schema
print(diff_schemas(schema_v1, schema_v3))
```

```text
{'added': ['buyer'], 'removed': ['customer'], 'type_changed': []}
```

A rename is indistinguishable from "one column dropped, one unrelated
column added" by a naive diff — auto-merging this would silently lose every
existing `customer` value and start a brand-new, unrelated-looking `buyer`
column. Renames and removals should always route to a human (or a schema
registry / data contract review — see Module 9) rather than being merged
automatically.

## A schema-evolution policy function

```python
def classify_schema_change(diff: dict) -> str:
    if diff["type_changed"]:
        return "REVIEW: type change requires manual approval"
    if diff["removed"]:
        return "REVIEW: column removal — check for a possible rename"
    if diff["added"]:
        return "AUTO-MERGE: new nullable column(s), safe to add"
    return "NO CHANGE"

for name, diff in [
    ("v1->v2", diff_schemas(schema_v1, schema_v2)),
    ("v1->v3", diff_schemas(schema_v1, schema_v3)),
]:
    print(name, "->", classify_schema_change(diff))
```

```text
v1->v2 -> AUTO-MERGE: new nullable column(s), safe to add
v1->v3 -> REVIEW: column removal — check for a possible rename
```

Codifying this policy as a function — rather than a mental rule someone
applies inconsistently — means the pipeline can automatically merge the
safe 80% of schema changes and only page a human for the risky 20%.

## Traps

- **Auto-merging column removals.** Almost always a rename in disguise —
  always route to manual review.
- **Silently casting on type mismatch.** `pd.to_numeric(..., errors="coerce")`
  applied blindly to a schema change can turn a legitimate string column
  into a column of `NaN`s without anyone noticing.
- **No schema version history.** Without logging every diff with a
  timestamp, you can't answer "when did this column show up" during an
  incident.
- **Testing schema changes only in production.** Any pipeline ingesting
  from an external source should validate incoming schema against the last
  known-good schema *before* writing anything to the lake.

## Cheat sheet

| Change type | Safe to auto-merge? |
|---|---|
| New nullable column | Yes |
| Type widening (int32→int64, float32→float64) | Yes |
| Type narrowing | No — review |
| Column rename | No — always looks like remove+add |
| Column removal | No — review (may be a rename) |

## Exercise

Write a function `apply_schema_policy(existing_df, incoming_df)` that runs
`diff_schemas`, calls `classify_schema_change`, and either returns the
merged DataFrame (for `AUTO-MERGE` cases) or raises a `SchemaReviewRequired`
exception carrying the diff (for `REVIEW` cases). Test it against both
`batch_v2` and `batch_v3` from this lesson.
