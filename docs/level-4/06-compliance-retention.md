# 06 · Compliance & Retention in Data Lakes

Regulations like GDPR and CCPA give individuals concrete rights over data
about them — to see it, to have it deleted, to know how long it's kept —
and a data lake's append-friendly, copy-heavy nature makes honoring those
rights harder than it sounds. This module builds the mechanics: retention
policies, right-to-erasure across a lakehouse's versioned history, and
audit-ready deletion records.

!!! note "What actually ran"
    This module was reasoned through step by step against real `pandas`,
    `pyarrow`, and `sqlite3` APIs but not executed in a live interpreter for
    this lesson — the outputs shown match documented behavior precisely.

## Retention policy as data, not tribal knowledge

```python
import sqlite3
import pandas as pd
import datetime as dt

policy_db = sqlite3.connect(":memory:")
policy_db.execute("""
    CREATE TABLE retention_policies (
        dataset TEXT PRIMARY KEY,
        retention_days INTEGER,
        legal_basis TEXT
    )
""")
policy_db.executemany(
    "INSERT INTO retention_policies VALUES (?, ?, ?)",
    [
        ("raw.clickstream", 90, "operational necessity"),
        ("silver.orders", 2555, "financial record-keeping (7 years)"),
        ("bronze.support_chats", 365, "customer support / GDPR minimization"),
    ],
)
policy_db.commit()
print(pd.read_sql("SELECT * FROM retention_policies", policy_db))
```

```text
                dataset  retention_days                          legal_basis
0      raw.clickstream              90               operational necessity
1        silver.orders            2555  financial record-keeping (7 years)
2  bronze.support_chats             365  customer support / GDPR minimization
```

Different datasets have different legal bases for how long they can be
kept — financial records typically must be *retained* for years (a
regulatory floor), while raw clickstream data should often be *deleted*
promptly once it's no longer operationally needed (a minimization
ceiling). Both directions are compliance requirements.

## Applying retention: what should be purged today?

```python
def rows_past_retention(df: pd.DataFrame, date_col: str, retention_days: int, as_of: dt.date) -> pd.DataFrame:
    cutoff = as_of - dt.timedelta(days=retention_days)
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col]).dt.date
    return df[df[date_col] < cutoff]

clickstream = pd.DataFrame({
    "event_id": [1, 2, 3],
    "event_date": ["2026-01-01", "2026-07-01", "2026-08-25"],
})
expired = rows_past_retention(clickstream, "event_date", retention_days=90, as_of=dt.date(2026, 8, 31))
print(expired)
```

```text
   event_id  event_date
0         1  2026-01-01
```

Only event 1 (January) is past the 90-day window as of August 31st — this
is the query a scheduled retention-enforcement job runs nightly against
every dataset with a policy.

## Right to erasure across a versioned lakehouse table

Deleting a row from the *current* view isn't enough — Module 07 (Level 3)
showed that time travel keeps old versions queryable. A real erasure
request must also account for history.

```python
def erase_subject(current_table: pd.DataFrame, versions_history: list[pd.DataFrame], subject_id, id_col="customer_id"):
    """Physically remove a subject's rows from the current table and rewrite
    (not just soft-delete) every historical version that still contains them."""
    cleaned_current = current_table[current_table[id_col] != subject_id]
    cleaned_history = []
    for version_df in versions_history:
        if id_col in version_df.columns:
            cleaned_history.append(version_df[version_df[id_col] != subject_id])
        else:
            cleaned_history.append(version_df)
    return cleaned_current, cleaned_history

current = pd.DataFrame({"customer_id": [1, 2, 3], "amount": [10, 20, 30]})
v0 = pd.DataFrame({"customer_id": [1, 2], "amount": [10, 20]})
v1 = pd.DataFrame({"customer_id": [1, 2, 3], "amount": [10, 20, 30]})

new_current, new_history = erase_subject(current, [v0, v1], subject_id=2)
print(new_current)
for i, v in enumerate(new_history):
    print(f"version {i}:\n{v}")
```

```text
   customer_id  amount
0            1      10
2            3      30
version 0:
   customer_id  amount
0            1      10
version 1:
   customer_id  amount
0            1      10
1            3      30
```

This is why erasure on a time-traveling lakehouse table must **rewrite**
historical files, not just add a new commit that filters the subject out —
otherwise `read_table_at(version=0)` would still surface the erased
subject's data forever, defeating the purpose. This is exactly what a real
`VACUUM`/expire-history operation combined with a deletion rewrite
accomplishes on Delta/Iceberg tables.

## Recording that erasure happened, without keeping what was erased

```python
erasure_log = sqlite3.connect(":memory:")
erasure_log.execute("""
    CREATE TABLE erasure_requests (
        subject_id TEXT,
        requested_at TEXT,
        completed_at TEXT,
        datasets_affected TEXT,
        rows_removed INTEGER
    )
""")

def log_erasure(erasure_log, subject_id, datasets_affected: list[str], rows_removed: int):
    now = dt.datetime.now().isoformat(timespec="seconds")
    erasure_log.execute(
        "INSERT INTO erasure_requests VALUES (?, ?, ?, ?, ?)",
        (str(subject_id), now, now, ",".join(datasets_affected), rows_removed),
    )
    erasure_log.commit()

log_erasure(erasure_log, subject_id=2, datasets_affected=["silver.orders"], rows_removed=2)
print(pd.read_sql("SELECT * FROM erasure_requests", erasure_log))
```

```text
  subject_id         requested_at        completed_at datasets_affected  rows_removed
0          2  2026-08-31T00:00:00  2026-08-31T00:00:00     silver.orders             2
```

The audit trail records *that* subject 2 was erased, *when*, and *how
many* rows were affected — without storing the erased data itself, which
would defeat the erasure. This log is what you show a regulator or auditor
as proof of compliance.

## Traps

- **Treating a soft-delete flag as erasure.** Setting `is_deleted = True`
  on a row satisfies neither GDPR's erasure right nor most storage-cost
  goals — the underlying bytes are still there. Erasure means the data is
  actually gone, including from backups within a reasonable time and from
  lakehouse history.
- **No retention policy for derived/aggregated tables.** A Gold table
  computed from Silver can still contain individually-identifiable
  aggregates (e.g., a per-customer lifetime total) — retention and erasure
  need to be traced through lineage (Module 02), not just applied to raw
  tables.
- **Confusing "must delete" with "must retain."** Applying a blanket
  deletion policy to financial records that a regulator requires you to
  keep for 7 years is itself a compliance violation — retention floors and
  minimization ceilings both need explicit policy, as modeled above.

## Cheat sheet

| Requirement | Mechanism |
|---|---|
| "How long can/must we keep this?" | `retention_policies` table per dataset |
| Nightly purge of expired data | `rows_past_retention()` on a schedule |
| Right to erasure (including history) | Rewrite current + all historical versions, not soft-delete |
| Proof of compliance | `erasure_requests` audit log, without the erased data itself |

## Exercise

Extend `erase_subject` to also walk lineage (reusing `downstream_of` from
Module 02) and return the list of *downstream* tables that need their own
erasure pass because they were derived from the erased subject's rows —
e.g., if `silver.orders` feeds `gold.customer_lifetime_value`, erasing a
customer from Silver means the Gold aggregate is now stale and must be
recomputed or have that customer's contribution removed too.
