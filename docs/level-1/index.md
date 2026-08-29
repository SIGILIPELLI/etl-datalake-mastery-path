# Level 1 · Entry <span class="level-badge">Foundations</span>

Goal: understand every moving part of an ETL pipeline and the data lake it
feeds — extraction, transformation, loading, file formats, layering, and why
you need a scheduler — and ship a working ETL pipeline that lands data into a
bronze/silver/gold lake, built from those parts in plain Python.

## Modules

1. [What Is ETL vs. ELT?](01-etl-vs-elt.md)
2. [Data Sources & Ingestion Patterns](02-data-sources-ingestion-patterns.md)
3. [Extraction Basics (Files, APIs, Databases)](03-extraction-basics.md)
4. [Transformation Basics (Clean, Cast, Dedupe)](04-transformation-basics.md)
5. [Loading Into a Target](05-loading-into-a-target.md)
6. [What Is a Data Lake? (vs. Data Warehouse)](06-what-is-a-data-lake.md)
7. [Bronze/Silver/Gold Layering](07-bronze-silver-gold-layering.md)
8. [File Formats: CSV, JSON, Parquet, Avro](08-file-formats.md)
9. [Why You Need a Scheduler](09-orchestration-basics.md)
10. [Capstone — End-to-End ETL to a Bronze/Silver/Gold Lake](10-capstone-etl-lake-pipeline.md)

By the end of this level you'll be able to take a folder of raw files, run
them through an extract → transform → load pipeline, and organize the result
into layered zones on disk the way a real data lake does — recognizing the
tradeoffs between CSV, JSON, and Parquet along the way.

!!! info "Setup for this level"
    ```bash
    pip install pandas pyarrow
    ```
    Everything else — `csv`, `json`, `pathlib`, `sqlite3` — is in the Python
    standard library. No cloud account, no API key, no server to install. All
    code on this level runs fully offline.
