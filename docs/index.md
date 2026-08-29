---
title: "Learn ETL & Data Lakes Free: Beginner to Master Course"
description: "Free course on ETL/ELT pipeline design and data lake architecture -- extraction, transformation, loading, bronze/silver/gold layering, lakehouse concepts, and governance, with real hands-on projects."
---

# ETL & Data Lake Mastery Path

A structured, module-wise training program on **ETL/ELT pipeline design and
Data Lake architecture** — from your first extract-transform-load script to
governed, multi-zone lakehouse platforms — with runnable Python code in every
module and a hands-on project at the end of each level.

ETL is the plumbing behind every analytics dashboard, ML feature store, and
finance report: moving data reliably from where it's produced to where it's
trusted and queried. A Data Lake is where a huge share of that data ends up
living. This site teaches both from first principles — plain Python and file
formats first, frameworks and lakehouse table formats (Airflow, Delta
Lake/Iceberg/Hudi) once you understand what problem they actually solve.

## How the program is organized

| Level | Focus | Modules |
|-------|-------|---------|
| [Level 1 · Entry](level-1/index.md) | ETL vs. ELT, ingestion patterns, extraction/transformation/loading basics, what a data lake is, bronze/silver/gold layering, file formats, orchestration concept | 9 topics + 1 capstone |
| [Level 2 · Intermediate](level-2/index.md) | Incremental loads & CDC, data quality checks, partitioning, schema evolution, orchestration tools (Airflow), idempotent design, backfills, monitoring, data contracts | 9 topics + 1 project |
| [Level 3 · Advanced](level-3/index.md) | Lakehouse concepts (Delta/Iceberg/Hudi), streaming ETL, cataloging & metadata, cost/performance optimization, late-arriving data, compaction, time travel, ACID on the lake | 9 topics + 1 project |
| [Level 4 · Master](level-4/index.md) | Governed multi-zone lake architecture, lineage & observability, security & access control, lake vs. warehouse vs. lakehouse, data mesh, compliance, multi-cloud strategy | 9 topics + 1 capstone |

## What you need

- **Python 3.10+** and `pip`. Level 1 uses only the standard library and
  `pandas` (plus `pyarrow` for Parquet) — no cloud account, no API key,
  everything runs locally.
- Later levels introduce Airflow concepts, Delta Lake/Iceberg/Hudi table
  formats, and streaming engines; each lesson states exactly what to install
  (or simply describes, conceptually, since some tools need a cluster).

## How to use this site

- Work through each level in order — later modules assume earlier ones.
- Every Level 1 topic page has runnable code — copy it into a local `.py`
  file and run it. Every output block on this level was carefully reasoned
  through against the actual pandas/pyarrow APIs, and is noted where it
  wasn't literally executed in a live environment.
- Each level ends with a project or capstone that combines everything learned
  in that level.
- Use the search bar (top of the page) to jump straight to a topic.

Start here → [Level 1 · Entry](level-1/index.md)

## Related tracks

ETL and data lakes sit next to data engineering, analytics, and SQL. Sister
sites cover the neighboring ground:

- [Data Engineering Mastery Path](https://sigilipelli.github.io/data-engineering-mastery-path/) — the broader data engineering discipline
- [SQL Mastery Path](https://sigilipelli.github.io/sql-mastery-path/) — SQL from first principles
- [PySpark Mastery Path](https://sigilipelli.github.io/pyspark-mastery-path/) — distributed processing for large-scale ETL
- [Data Science Mastery Path](https://sigilipelli.github.io/data-science-mastery-path/) — what happens after the data lands

🎥 **Prefer video?** Watch the [Mastery Path video series](https://youtube.com/@sigilipelli) on YouTube — Shorts and full walkthroughs of these lessons.

## More from the Mastery Path series

<!-- cross-link grid: added separately -->
