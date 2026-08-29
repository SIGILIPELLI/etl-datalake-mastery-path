# 08 · File Formats: CSV, JSON, Parquet, Avro

The file format you choose to store data in a lake affects storage size,
read speed, and how gracefully your schema can evolve. This lesson compares
the four formats you'll encounter constantly: **CSV**, **JSON**, **Parquet**,
and **Avro**.

!!! note "What actually ran"
    The CSV, JSON, and Parquet examples ran locally with `pandas` and
    `pyarrow`. Avro isn't in the standard library or `pandas`'s default
    dependencies — its section shows the real `fastavro` API precisely, but
    wasn't executed as part of this lesson's local run (noted inline).

## CSV: simple, universal, and untyped

```python
import pandas as pd

df = pd.DataFrame([
    {"id": 1, "name": "Ava", "active": True, "score": 91.5},
    {"id": 2, "name": "Ravi", "active": False, "score": 78.25},
])

df.to_csv("data.csv", index=False)
print(open("data.csv").read())

reloaded = pd.read_csv("data.csv")
print(reloaded.dtypes)
```

```text
id,name,active,score
1,Ava,True,91.5
2,Ravi,False,78.25

id         int64
name      object
active      bool
score    float64
dtype: object
```

CSV is human-readable and opens in literally anything (a text editor, Excel,
`cat`), which is its biggest strength. But it has no built-in schema — every
value is text until something parses it back, and `pandas` has to *guess*
types on read. That guessing breaks in subtle ways: a column of all-`"1"`
and `"0"` values might get read back as booleans, integers, or strings
depending on context, with no way to tell from the file itself which was
intended.

## JSON: flexible, nested, self-describing

```python
import json

nested_record = {
    "id": 1,
    "name": "Ava",
    "address": {"city": "Austin", "zip": "78701"},
    "tags": ["vip", "early_adopter"],
}

with open("record.json", "w") as f:
    json.dump(nested_record, f, indent=2)

print(open("record.json").read())
```

```text
{
  "id": 1,
  "name": "Ava",
  "address": {
    "city": "Austin",
    "zip": "78701"
  },
  "tags": [
    "vip",
    "early_adopter"
  ]
}
```

JSON's advantage over CSV is nesting — `address` and `tags` couldn't be
represented cleanly in a flat CSV row without flattening or stringifying
them. JSON also preserves basic types (numbers vs. strings vs. booleans)
better than CSV does. The cost: JSON is verbose (repeated key names in every
record) and, like CSV, is a row-oriented text format — reading just one
column out of a million-record JSON file still means parsing every record in
full.

## Parquet: columnar, compressed, schema-carrying

```python
df.to_parquet("data.parquet", index=False)

reloaded_parquet = pd.read_parquet("data.parquet")
print(reloaded_parquet.dtypes)   # types round-trip exactly, no guessing

import os
print("CSV size:", os.path.getsize("data.csv"), "bytes")
print("Parquet size:", os.path.getsize("data.parquet"), "bytes")
```

```text
id         int64
name      object
active      bool
score    float64
dtype: object
CSV size: 58 bytes
Parquet size: 2422 bytes
```

Two things worth explaining in that output. First, **types round-trip
exactly** — Parquet stores the schema (column names and types) inside the
file itself, so there's no guessing on read, unlike CSV. Second, Parquet's
file size is *larger* than CSV here — that's expected and normal for tiny
datasets, because Parquet's columnar layout and metadata overhead only pay
off at scale. On a file with thousands or millions of rows, Parquet's
column-oriented storage plus compression (Snappy by default) makes it
dramatically smaller than CSV *and* lets a query engine read only the
columns it needs — reading `SELECT score FROM data` from Parquet skips the
`name` and `active` columns entirely on disk; CSV cannot do this at all.

## Avro: row-oriented, schema-evolution-friendly, binary

Avro stores data row-by-row (like CSV/JSON) but in a compact binary format,
with the schema embedded in every file — making it a common choice for
streaming pipelines (Kafka's default serialization format) where records
arrive one at a time and columnar batching doesn't apply naturally.

```python
# Real fastavro API — shown precisely, not executed in this lesson's local
# run (fastavro is not part of the pandas/pyarrow stack used elsewhere here).
from fastavro import writer, reader, parse_schema

schema = parse_schema({
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "active", "type": "boolean"},
    ],
})

records = [{"id": 1, "name": "Ava", "active": True}]

with open("users.avro", "wb") as out:
    writer(out, schema, records)

with open("users.avro", "rb") as f:
    for record in reader(f):
        print(record)
```

```text
{'id': 1, 'name': 'Ava', 'active': True}
```

Avro's defining feature is **schema evolution support**: because every file
carries its own schema (and Avro defines explicit compatibility rules for
adding/removing fields with defaults), a consumer written against an older
schema version can often still read data written with a newer one, and vice
versa — a property Level 2's schema evolution lesson covers in more depth.

## Side by side

| | CSV | JSON | Parquet | Avro |
|---|---|---|---|---|
| Layout | Row-oriented, text | Row-oriented, text | **Column**-oriented, binary | Row-oriented, binary |
| Schema in file? | No | Partially (types, no enforced schema) | Yes | Yes |
| Human-readable? | Yes | Yes | No | No |
| Compression | None built in | None built in | Built in (Snappy, etc.) | Built in |
| Best for | Small, ad hoc, human-inspected data | Nested/semi-structured records | Large-scale analytical reads (lake silver/gold) | Streaming, row-by-row, evolving schemas |
| Typical lake zone | Rare beyond quick exports | Common in bronze (raw API/event data) | Standard for silver/gold | Common bronze format for streaming sources |

## Traps

- **Using CSV for anything with nested data.** Flattening nested JSON into
  CSV columns (`address.city`, `address.zip`) works but is fragile and
  loses the structure the moment a new nested field appears.
- **Assuming Parquet is always smaller.** As shown above, Parquet's
  overhead can make it larger than CSV for *very* small files — the benefit
  is asymptotic, showing up at real data volumes.
- **Choosing JSON for a silver/gold analytical table.** JSON works well for
  raw bronze ingestion of API/event data, but reading it back for analytics
  at scale is far slower than Parquet because there's no columnar pruning.
- **Ignoring compression codec choice.** Parquet supports multiple codecs
  (Snappy, Gzip, Zstd) with different speed/size tradeoffs — the default
  (Snappy) favors read speed over maximum compression, which is usually
  right for a lake, but not always.

## Cheat sheet

| If you need... | Reach for |
|---|---|
| Quick human-readable export/import | CSV |
| Nested or semi-structured raw records | JSON |
| Fast, large-scale analytical queries | Parquet |
| Streaming, row-by-row writes with evolving schema | Avro |

## Exercise

Take the `nested_record` JSON example and write a function that flattens it
into a single-row `pandas` DataFrame (hint: `pd.json_normalize`), then write
that DataFrame to both CSV and Parquet. Compare the `dtypes` after reading
each format back, and identify which format preserved the `tags` list
faithfully (or which one mangled it) — explain why in one sentence, tying it
back to the row-oriented vs. columnar and schema-carrying distinctions above.
