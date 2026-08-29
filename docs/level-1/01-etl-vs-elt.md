# 01 · What Is ETL vs. ELT?

Every data pipeline moves data from a source to a place where it can be
queried and trusted. The two dominant shapes for doing that are **ETL**
(Extract, Transform, Load) and **ELT** (Extract, Load, Transform) — the
letters are the same, the order is not, and that order changes almost
everything about how the pipeline behaves.

!!! note "What actually ran"
    The code in this lesson was run locally with the Python standard library
    (`sqlite3`) plus `pandas` — no external services.

## ETL: transform before it lands

In classic ETL, data is pulled from the source, cleaned and reshaped **in
flight** (usually in the pipeline's own memory or a dedicated processing
engine), and only the already-clean result is written to the destination.

```python
import csv, io, sqlite3

raw_csv = """id,name,signup_date,plan
1, Ava Chen ,2026-01-15,pro
2,ravi kumar,2026-01-16,FREE
3,Ava Chen,2026-01-15,pro
"""

def extract(text):
    return list(csv.DictReader(io.StringIO(text)))

def transform(rows):
    seen = set()
    clean = []
    for r in rows:
        key = (r["name"].strip().lower(), r["signup_date"])
        if key in seen:
            continue          # drop duplicate before it ever reaches storage
        seen.add(key)
        clean.append({
            "id": int(r["id"]),
            "name": r["name"].strip().title(),
            "signup_date": r["signup_date"],
            "plan": r["plan"].strip().lower(),
        })
    return clean

def load(rows):
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE users (id INT, name TEXT, signup_date TEXT, plan TEXT)")
    conn.executemany("INSERT INTO users VALUES (:id, :name, :signup_date, :plan)", rows)
    return conn

rows = extract(raw_csv)
clean = transform(rows)
conn = load(clean)
print(f"Extracted {len(rows)} raw rows, loaded {len(clean)} clean rows")
for row in conn.execute("SELECT * FROM users"):
    print(" ", row)
```

```text
Extracted 3 raw rows, loaded 2 clean rows
  (1, 'Ava Chen', '2026-01-15', 'pro')
  (2, 'Ravi Kumar', '2026-01-16', 'free')
```

Notice the duplicate row (id 3, same name and signup date as id 1) never
makes it into `users` at all — ETL's transform step acts as a gatekeeper
*before* storage. The database only ever holds the clean shape.

## ELT: land first, transform where it lives

ELT flips the order: raw data is extracted and loaded into the destination
**as-is**, and transformation happens afterward, typically with SQL running
inside the destination itself (a warehouse, lake engine, or database).

```python
def load_raw(rows):
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE users_raw (id TEXT, name TEXT, signup_date TEXT, plan TEXT)")
    conn.executemany("INSERT INTO users_raw VALUES (:id, :name, :signup_date, :plan)", rows)
    return conn

conn = load_raw(rows)  # rows = the *raw* extract, untouched
print("Raw table (ELT lands everything, warts and all):")
for row in conn.execute("SELECT * FROM users_raw"):
    print(" ", row)

# Transform happens afterward, as SQL against the landed data
conn.execute("""
CREATE TABLE users_clean AS
SELECT MIN(id) AS id,
       TRIM(name) AS name,
       signup_date,
       LOWER(TRIM(plan)) AS plan
FROM users_raw
GROUP BY LOWER(TRIM(name)), signup_date
""")
print("Clean table, built with SQL against the already-landed raw table:")
for row in conn.execute("SELECT * FROM users_clean"):
    print(" ", row)
```

```text
Raw table (ELT lands everything, warts and all):
  ('1', ' Ava Chen ', '2026-01-15', 'pro')
  ('2', 'ravi kumar', '2026-01-16', 'FREE')
  ('3', 'Ava Chen', '2026-01-15', 'pro')
Clean table, built with SQL against the already-landed raw table:
  (1, 'Ava Chen', '2026-01-15', 'pro')
  (2, 'ravi kumar', '2026-01-16', 'free')
```

The raw, messy rows (including the duplicate) are all sitting in
`users_raw` — nothing was thrown away at ingestion time. The cleanup
happened afterward, as a second SQL step, and the original raw data is still
there if you ever need to recompute the transform differently.

## Why the order matters

| | ETL | ELT |
|---|---|---|
| Where transform runs | In the pipeline (Python, Spark, a dedicated ETL tool) | In the destination (warehouse/lake SQL engine) |
| What lands in storage | Only the clean result | Raw data first, clean views/tables built on top |
| Reprocessing history | Requires re-running extraction against the source | Just re-run the transform SQL against already-landed raw data |
| Compute cost model | Pipeline infra pays for transform compute | Destination (warehouse/lake engine) pays for transform compute |
| Best fit | Sources that are hard to re-extract; strict pre-load validation | Cheap, elastic compute at the destination (cloud warehouses, lakehouses) |

ELT became dominant with cloud data warehouses and lakes precisely because
storage got cheap and destination compute got elastic — landing raw data
"just in case" stopped being wasteful. But ETL never disappeared: whenever a
source can't be re-queried cheaply (a paid API with a strict rate limit, a
production database you can only touch once a night), or when strict
validation must happen *before* anything lands (financial records, PII
redaction), transforming in flight is still the right call.

## Traps

- **Treating ELT as "no transformation needed."** ELT still transforms — it
  just does so after loading, not before. Skipping the transform step
  entirely (shipping raw data straight to dashboards) is neither ETL nor ELT;
  it's just untrusted data with a warehouse bill attached.
- **Assuming ELT is always cheaper.** Landing raw data has a storage cost and
  a governance cost (more raw, unvalidated data sitting around, potentially
  containing PII you now have to secure). It shifts cost, it doesn't erase it.
- **Forgetting the raw layer in ELT.** The entire point of ELT is that raw
  data survives landing so transforms are re-runnable. If you overwrite
  `users_raw` on every run, you've lost that advantage and just built a
  slower, SQL-based ETL pipeline.

## Cheat sheet

| Question | Points toward |
|---|---|
| Is the source expensive/rate-limited to re-query? | ETL |
| Do you need every raw byte preserved for audit or reprocessing? | ELT |
| Is destination compute cheap and elastic (cloud warehouse/lakehouse)? | ELT |
| Must invalid data be blocked before it's ever stored? | ETL |
| Is the transform simple enough to express in SQL? | ELT |

## Exercise

Take the `raw_csv` above and add two more rows: one with a `plan` value of
`"Enterprise "` (mixed case, trailing space) and one that is an exact
duplicate of an existing row but with different casing in `name`. Run both
the ETL and ELT versions of the pipeline against the new data and compare:
which duplicate-detection rule (the Python `seen` set vs. the SQL `GROUP BY`)
catches the case-different duplicate, and does either miss a case the other
catches? Write down which one you would trust more in production and why.
