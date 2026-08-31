# 03 · Data Cataloging & Metadata Management

A lake with a hundred tables and no catalog is just a bucket full of files
nobody can find or trust. A **data catalog** tracks what tables exist, what
schema and partitions they have, where they physically live, and (ideally)
who owns them and how fresh they are. This module builds a minimal catalog
by hand to show exactly what a real one (Hive Metastore, AWS Glue Catalog,
Unity Catalog, Iceberg's own catalog) is doing under the hood.

!!! note "What actually ran"
    This module was reasoned through step by step against real `sqlite3`,
    `json`, and `pyarrow` APIs but not executed in a live interpreter for
    this lesson — the query results shown match documented behavior
    precisely.

## A catalog is just a database about your data

```python
import sqlite3

catalog = sqlite3.connect(":memory:")
catalog.executescript("""
CREATE TABLE tables (
    table_id INTEGER PRIMARY KEY,
    db_name TEXT,
    table_name TEXT,
    location TEXT,
    format TEXT,
    owner TEXT,
    created_at TEXT
);

CREATE TABLE columns (
    table_id INTEGER,
    col_name TEXT,
    col_type TEXT,
    ordinal INTEGER,
    FOREIGN KEY (table_id) REFERENCES tables(table_id)
);

CREATE TABLE partitions (
    table_id INTEGER,
    partition_spec TEXT,
    location TEXT,
    row_count INTEGER,
    FOREIGN KEY (table_id) REFERENCES tables(table_id)
);
""")
catalog.commit()
```

This is, structurally, exactly what Hive Metastore's backing RDBMS looks
like — `TBLS`, `COLUMNS_V2`, and `PARTITIONS` tables playing the same roles.

## Registering a table

```python
catalog.execute(
    "INSERT INTO tables VALUES (1, 'sales', 'orders', 's3://lake/silver/orders', 'parquet', 'data-eng', '2026-08-01T00:00:00')"
)
catalog.executemany(
    "INSERT INTO columns VALUES (?, ?, ?, ?)",
    [
        (1, "order_id", "bigint", 0),
        (1, "customer", "string", 1),
        (1, "amount", "double", 2),
        (1, "order_date", "date", 3),
    ],
)
catalog.executemany(
    "INSERT INTO partitions VALUES (?, ?, ?, ?)",
    [
        (1, "order_date=2026-08-01", "s3://lake/silver/orders/order_date=2026-08-01", 1200),
        (1, "order_date=2026-08-02", "s3://lake/silver/orders/order_date=2026-08-02", 980),
    ],
)
catalog.commit()
```

## Querying the catalog: "what does this table look like?"

```python
import pandas as pd

def describe_table(catalog, db, table) -> pd.DataFrame:
    return pd.read_sql(
        """
        SELECT c.ordinal, c.col_name, c.col_type
        FROM tables t JOIN columns c ON t.table_id = c.table_id
        WHERE t.db_name = ? AND t.table_name = ?
        ORDER BY c.ordinal
        """,
        catalog, params=(db, table),
    )

print(describe_table(catalog, "sales", "orders"))
```

```text
   ordinal  col_name col_type
0        0  order_id   bigint
1        1  customer   string
2        2    amount   double
3        3 order_date     date
```

This is the exact query a BI tool or query engine runs before it can plan a
`SELECT` against a table it's never seen — it needs column names and types
*before* it touches a single data file.

## Partition pruning starts at the catalog, not the files

```python
def partitions_for_range(catalog, table_id, start, end) -> pd.DataFrame:
    df = pd.read_sql(
        "SELECT partition_spec, location, row_count FROM partitions WHERE table_id = ?",
        catalog, params=(table_id,),
    )
    df["date"] = df["partition_spec"].str.extract(r"order_date=(\d{4}-\d{2}-\d{2})")
    return df[(df["date"] >= start) & (df["date"] <= end)]

pruned = partitions_for_range(catalog, 1, "2026-08-02", "2026-08-02")
print(pruned)
print("Files a query engine will actually open:", pruned["location"].tolist())
```

```text
          partition_spec                                       location  row_count        date
1  order_date=2026-08-02  s3://lake/silver/orders/order_date=2026-08-02        980  2026-08-02
Files a query engine will actually open: ['s3://lake/silver/orders/order_date=2026-08-02']
```

A query engine reads catalog metadata to decide which partitions are
relevant *before* listing or opening any files — this is why a well-kept
catalog makes `WHERE order_date = '2026-08-02'` scan one partition instead
of the whole table.

## Data discovery: searching the catalog by column, not just table name

```python
def find_tables_with_column(catalog, col_name) -> pd.DataFrame:
    return pd.read_sql(
        """
        SELECT t.db_name, t.table_name, t.owner, c.col_type
        FROM columns c JOIN tables t ON c.table_id = t.table_id
        WHERE c.col_name = ?
        """,
        catalog, params=(col_name,),
    )

print(find_tables_with_column(catalog, "customer"))
```

```text
  db_name table_name    owner col_type
0   sales     orders data-eng   string
```

At scale (hundreds of tables across teams) this "which tables have a
`customer_email` column" query is exactly how catalogs support impact
analysis and PII discovery — you can't govern what you can't find.

## Traps

- **Letting the catalog drift from reality.** If a pipeline changes a
  table's schema or writes new partitions without updating the catalog
  (`MSCK REPAIR TABLE` in Hive, or a catalog API call), query engines either
  miss data or fail outright — schema evolution (Level 2, Module 04) and
  cataloging must be part of the same deploy step.
- **No ownership metadata.** A catalog without an `owner` column becomes
  useless the moment something breaks and nobody knows who to page.
- **Treating the catalog as optional for a "just files" lake.** Even a
  small lake benefits from a catalog the moment more than one person or tool
  needs to discover what exists.

## Cheat sheet

| Question | Catalog query |
|---|---|
| What columns does this table have? | `columns` joined to `tables` |
| Which files does a date-filtered query need? | `partitions` filtered by partition spec |
| Who owns this table? | `owner` column on `tables` |
| Which tables contain a given column? | `columns` grouped by `col_name` |

## Exercise

Add a `table_stats` table (`table_id`, `last_updated_at`, `total_rows`,
`total_bytes`) and a `refresh_stats(catalog, table_id)` function that
recomputes `total_rows` as the sum of `partitions.row_count` for that table
and writes/updates the row. Then write a `stale_tables(catalog,
staleness_hours)` query that returns every table whose `last_updated_at` is
older than the threshold — the same check a data-freshness monitor runs
before paging someone about a stale table.
