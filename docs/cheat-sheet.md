---
title: "ETL & Data Lake Cheat Sheet: Quick Reference, Beginner to Advanced"
description: "A free ETL & Data Lake cheat sheet: 38 quick-reference tables from every level of the ETL & Data Lake Mastery Path, each linked to its full lesson."
---

# ETL & Data Lake Cheat Sheet

Quick-reference tables from **38 lessons** of the free ETL & Data Lake Mastery Path, in course order from beginner to advanced. Each section links to the full lesson with explanations and runnable examples.

## Level 1 · Entry

### [01 · What Is ETL vs. ELT?](level-1/01-etl-vs-elt.md)

| Question | Points toward |
|---|---|
| Is the source expensive/rate-limited to re-query? | ETL |
| Do you need every raw byte preserved for audit or reprocessing? | ELT |
| Is destination compute cheap and elastic (cloud warehouse/lakehouse)? | ELT |
| Must invalid data be blocked before it's ever stored? | ETL |
| Is the transform simple enough to express in SQL? | ELT |

### [02 · Data Sources &amp; Ingestion Patterns](level-1/02-data-sources-ingestion-patterns.md)

| Source | Typical ingestion pattern | "Am I done?" signal |
|---|---|---|
| Flat files | Batch | End of file / all files in folder processed |
| REST API | Batch (with pagination) | Empty page / no `next` token |
| Operational database | Batch (scheduled query) or CDC (Level 2) | Query completes / no new watermark rows |
| Message queue (Kafka, etc.) | Streaming | Never — runs continuously |

### [03 · Extraction Basics (Files, APIs, Databases)](level-1/03-extraction-basics.md)

| Source | Extraction unit | Stop condition |
|---|---|---|
| File | Whole file / row iterator | End of file |
| API | One page per request | Empty page / no next-page token |
| Database | Query result set | Query returns 0 rows (or reaches the watermark, Level 2) |

### [04 · Transformation Basics (Cleaning, Type Casting, Deduplication)](level-1/04-transformation-basics.md)

| Task | pandas idiom |
|---|---|
| Strip whitespace | `.str.strip()` |
| Normalize casing | `.str.title()` / `.str.lower()` / `.str.upper()` |
| Safe numeric cast | `pd.to_numeric(col, errors="coerce")` |
| Fill missing + flag it | `col.isna()` then `col.fillna(default)` |
| Dedup on cleaned keys | `.drop_duplicates(subset=[...], keep="first")` |

### [05 · Loading Into a Target](level-1/05-loading-into-a-target.md)

| Strategy | Idempotent? | Scales to large sources? | Typical use |
|---|---|---|---|
| Full overwrite | Yes, trivially | No — re-extracts everything | Small reference/dimension tables |
| Append/upsert (DB) | Yes, with a primary key | Yes | Fact tables, growing datasets |
| Write Parquet (lake) | Only if the whole file is rewritten | Depends on partitioning (Level 2) | Data lake bronze/silver/gold zones |

### [06 · What Is a Data Lake? (vs. Data Warehouse)](level-1/06-what-is-a-data-lake.md)

| Question | Points toward |
|---|---|
| Is the data structured and known ahead of time? | Warehouse |
| Do you need to store raw files, logs, images, or evolving JSON? | Lake |
| Do downstream consumers need strict, enforced tables? | Warehouse |
| Do you need cheap, flexible storage before deciding on structure? | Lake |
| Do you want both — flexible raw storage AND reliable curated tables? | Lakehouse (Level 3) |

### [07 · Bronze/Silver/Gold Layering](level-1/07-bronze-silver-gold-layering.md)

| Zone | Contains | Mutability | Who reads it |
|---|---|---|---|
| Bronze | Raw, as-arrived data | Append-only, never edited | Pipeline engineers (reprocessing) |
| Silver | Cleaned, typed, deduplicated | Regenerable from bronze | Analysts, other pipelines |
| Gold | Aggregated, business-shaped | Regenerable from silver | Dashboards, reports, end users |

### [08 · File Formats: CSV, JSON, Parquet, Avro](level-1/08-file-formats.md)

| If you need... | Reach for |
|---|---|
| Quick human-readable export/import | CSV |
| Nested or semi-structured raw records | JSON |
| Fast, large-scale analytical queries | Parquet |
| Streaming, row-by-row writes with evolving schema | Avro |

### [09 · Why You Need a Scheduler](level-1/09-orchestration-basics.md)

| Concept | What it solves | Plain cron? | Airflow (Level 2)? |
|---|---|---|---|
| Scheduling | "Run this automatically at time X" | Yes | Yes |
| Dependencies (DAGs) | "Run B only after A succeeds" | No | Yes |
| Retries | "Recover from transient failures automatically" | No (needs custom scripting) | Yes |
| Visibility/alerting | "Tell me what failed, without me checking logs" | No | Yes |

## Level 2 · Intermediate

### [01 · Incremental Loads &amp; Change Data Capture](level-2/01-incremental-loads-cdc.md)

| Task | Pattern |
|---|---|
| Pull only new/changed rows | `WHERE updated_at > :watermark` |
| Persist progress | Write new watermark only after successful load |
| Merge new+existing on key | `pd.concat([...]).drop_duplicates(subset=key, keep="last")` |
| Catch deletes | CDC log stream with explicit `op` field, or soft-delete flag |

### [02 · Data Quality Checks &amp; Validation](level-2/02-data-quality-validation.md)

| Check | pandas idiom |
|---|---|
| Not null | `df[col].isna()` |
| Range/positivity | boolean comparison, e.g. `df[col] <= 0` |
| Pattern match | `df[col].str.match(pattern, na=False)` |
| Enum membership | `~df[col].isin(allowed_set)` |
| Uniqueness | `df.duplicated(subset=col, keep=False)` |

### [03 · Partitioning Strategy for a Data Lake](level-2/03-partitioning-strategy.md)

| Task | Code |
|---|---|
| Write partitioned Parquet | `pq.write_to_dataset(table, root_path=..., partition_cols=[...])` |
| Read with pruning | `ds.dataset(path, partitioning="hive").to_table(filter=...)` |
| Check column cardinality | `series.nunique()` |
| Rule of thumb | Partition on what's low-cardinality *and* commonly filtered |

### [04 · Schema Evolution &amp; Handling](level-2/04-schema-evolution.md)

| Change type | Safe to auto-merge? |
|---|---|
| New nullable column | Yes |
| Type widening (int32→int64, float32→float64) | Yes |
| Type narrowing | No — review |
| Column rename | No — always looks like remove+add |
| Column removal | No — review (may be a rename) |

### [05 · Orchestration Tools Overview (Airflow DAGs)](level-2/05-orchestration-tools-airflow.md)

| Concept | TaskFlow API |
|---|---|
| Define a task | `@task` decorator on a function |
| Wire dependency | call one task with another's return value, or `a >> b` |
| Retry a task | `@task(retries=N, retry_delay=timedelta(...))` |
| Wait on a condition | `@task.sensor(poke_interval=..., mode="reschedule")` |
| Backfill | `airflow dags backfill <dag_id> --start-date ... --end-date ...` |

### [06 · Idempotent Pipeline Design](level-2/06-idempotent-pipeline-design.md)

| Pattern | When to use |
|---|---|
| Delete-then-insert (by partition) | Batch loads scoped to a date/partition |
| Upsert on natural key | Row-level incremental loads (Module 1) |
| Deterministic file naming | Any file-based bronze/silver write |
| Partition-scoped task logic | Any pipeline that will ever need backfilling |

### [07 · Backfills &amp; Reprocessing](level-2/07-backfills-reprocessing.md)

| Step | Why |
|---|---|
| Scope precisely to the bug window | Minimize blast radius and cost |
| Dry-run and diff old vs. new | Catch mistakes before they're irreversible |
| Reprocess per-partition, idempotently | Safe to parallelize and to retry |
| Map downstream dependents | A backfill isn't done until Gold is refreshed too |

### [08 · Monitoring &amp; Alerting for Pipelines](level-2/08-monitoring-alerting.md)

| Signal | Detection |
|---|---|
| Volume anomaly | Compare `rows_in` to rolling mean of recent runs |
| Quality anomaly | `row_drop_pct` over a fixed threshold |
| Performance anomaly | `duration_sec` over N× the rolling median |
| Alert payload | Name the DAG, the run date, the metric, and the threshold |

### [09 · Data Contracts Between Producers &amp; Consumers](level-2/09-data-contracts.md)

| Contract element | Enforced by |
|---|---|
| Field types & nullability | `validate_against_contract` schema checks |
| Value ranges & enums | Same function, `min`/`enum` rules |
| Backward-compatible evolution | Additive-only changes (Module 4's policy) |
| Semantics & SLAs | Documented, tested with reference-value checks |

### [10 · Project — Incremental, Validated Lake Pipeline](level-2/10-project-incremental-validated-pipeline.md)

| Step | Function |
|---|---|
| Read progress | `read_watermark()` |
| Pull only new/changed rows | `extract(conn, watermark)` |
| Fail fast on bad data | `validate(df)` |
| Durable raw copy | `write_bronze(df, run_date)` |
| Deduplicated current view | `upsert_silver(df)` |
| Advance state only on success | `STATE_FILE.write_text(new_watermark)` |

## Level 3 · Advanced

### [01 · Data Lakehouse Concepts (Delta Lake/Iceberg/Hudi)](level-3/01-lakehouse-concepts.md)

| Concept | What it gives you |
|---|---|
| Transaction log | Atomic multi-file commits, no partial reads |
| Versioned log entries | Time travel (`read_table_at(version=N)`) |
| Manifest/checkpoint files | Fast metadata reads without listing every data file |
| Copy-on-write vs. merge-on-read | Read-optimized vs. write-optimized update trade-off |

### [02 · Streaming ETL Basics](level-3/02-streaming-etl-basics.md)

| Concept | What it does |
|---|---|
| Tumbling window | Fixed, non-overlapping time buckets keyed by event time |
| Watermark | "I don't expect events older than this anymore" |
| `max_lateness` | How long a window stays open after its nominal end |
| Late-arrival handling | Drop, side-output, or corrective re-emit — a policy choice |

### [03 · Data Cataloging &amp; Metadata Management](level-3/03-data-cataloging-metadata.md)

| Question | Catalog query |
|---|---|
| What columns does this table have? | `columns` joined to `tables` |
| Which files does a date-filtered query need? | `partitions` filtered by partition spec |
| Who owns this table? | `owner` column on `tables` |
| Which tables contain a given column? | `columns` grouped by `col_name` |

### [04 · Cost &amp; Performance Optimization for Lake Storage](level-3/04-cost-performance-optimization.md)

| Lever | Effect |
|---|---|
| Columnar format (Parquet) | Read only needed columns, compress similar values together |
| Column pruning | `columns=[...]` at read time — skip unrequested column bytes |
| Partition pruning | Skip whole directories that can't match the filter |
| Compression codec | `snappy` (fast reads) vs. `zstd`/`gzip` (smaller, more CPU) |
| Row group size | Statistics granularity for predicate pushdown vs. per-group overhead |

### [05 · Handling Late-Arriving &amp; Out-of-Order Data](level-3/05-late-arriving-data.md)

| Pattern | When to use |
|---|---|
| Lookback window on batch re-run | Simple, works with existing idempotent batch jobs |
| Corrective recompute-and-overwrite | Streaming/near-real-time, consumers can handle updated values |
| Quarantine + separate reconciliation | Consumers need append-only/immutable published data |
| Hard cutoff (drop past N) | Bounds cost/complexity when perfect correctness isn't required |

### [06 · Small Files &amp; Compaction Strategies](level-3/06-small-files-compaction.md)

| Symptom | Fix |
|---|---|
| Many files far below target size | Run compaction |
| Compaction breaking partition pruning | Compact within one partition at a time |
| Readers seeing partial state during compaction | Use a transaction log; commit only after full write |
| Compacting too often | Gate on `should_compact`-style thresholds, run on a schedule |

### [07 · Time Travel &amp; Data Versioning](level-3/07-time-travel-versioning.md)

| Need | How |
|---|---|
| Query historical state | `read_table_at(version=N)` / `AS OF` |
| Undo a bad write | New commit that restores prior state (never edit history) |
| Reproducible downstream job | Pin reads to a specific version number |
| Reclaim old file storage | Vacuum/expire past a retention window |

### [08 · ACID Transactions on the Lake](level-3/08-acid-transactions-lake.md)

| ACID property | Lakehouse mechanism |
|---|---|
| Atomicity | Write-then-atomic-rename of the log entry |
| Consistency | Validate constraints before committing |
| Isolation | Optimistic concurrency control — check base version at commit time |
| Durability | `fsync` (or object store's durability guarantee) before making the commit visible |

### [09 · Query Engines Over the Lake (Presto/Trino/Athena)](level-3/09-query-engines.md)

| Stage | What happens |
|---|---|
| Catalog lookup | Determine which partitions could possibly match |
| Partition pruning | Skip directories that can't match filters |
| Distributed scan | Read matching files in parallel across workers |
| Predicate pushdown | Skip row groups using Parquet statistics before decompressing |
| Aggregation | Combine partial results (map-reduce style) into the final answer |

### [10 · Project — Streaming-Ready Lakehouse Table](level-3/10-project-streaming-lakehouse-table.md)

| Concern | Module | Mechanism used here |
|---|---|---|
| Atomic multi-file commits | 01 / 08 | `commit()` with temp-write-then-rename |
| Concurrent writer safety | 08 | `expected_base` optimistic concurrency check |
| Late data | 05 | `watermarked_view()` splits on/late without dropping |
| Small files | 06 | `compact_if_needed()` as its own logged commit |
| Time travel | 07 | Every version stays queryable via the log |

## Level 4 · Master

### [01 · Building a Governed Multi-Zone Data Lake](level-4/01-governed-multi-zone-lake.md)

| Governance need | Mechanism |
|---|---|
| Who can write/read a zone | `zones` registry + `check_write_access` |
| Data-quality gate between zones | `promote()` with zone-specific checks |
| PII protection | Column-level tags + `mask_pii_columns` at read time |
| Auditability | Log every promotion/access decision, not just the data move |

### [02 · Data Lineage &amp; Observability](level-4/02-data-lineage-observability.md)

| Question | Query |
|---|---|
| What fed this table? | `upstream_of(lineage, node)` |
| What breaks if I change this? | `downstream_of(lineage, node)` |
| Did volume/schema change unexpectedly? | `detect_anomalies(metrics, table)` |
| Is this table fresh enough? | `check_freshness(metrics, table, sla_hours)` |

### [03 · Security &amp; Access Control Patterns for Data Lakes](level-4/03-security-access-control.md)

| Pattern | Good for |
|---|---|
| RBAC | Coarse, auditable permissions with a small number of roles |
| ABAC | Fine-grained rules that combine multiple attributes without role explosion |
| Row-level security | Same physical table, different visible rows per requester |

### [04 · Lake vs. Warehouse vs. Lakehouse: Choosing for a Use Case](level-4/04-lake-warehouse-lakehouse-choice.md)

| Signal | Leans toward |
|---|---|
| Mostly unstructured, cheap archival, ML raw data | Data Lake |
| Sub-second BI, mature tooling, no ML/unification need | Data Warehouse |
| Same data needs both BI and ML, or needs ACID + existing lake tooling | Lakehouse |
| No update/ACID need at all | Whichever is simplest to operate — usually not a lakehouse |

### [05 · Data Mesh &amp; Domain-Oriented Lake Ownership](level-4/05-data-mesh-domain-ownership.md)

| Mesh principle | What it looks like in practice |
|---|---|
| Domain ownership | The team closest to the source owns its pipeline |
| Data as a product | Explicit schema, SLA, owner, discoverability contract |
| Self-serve platform | Shared tooling (catalog, orchestration, storage) domains build on |
| Federated governance | Small set of global standards; domain-local implementation |

### [06 · Compliance &amp; Retention in Data Lakes](level-4/06-compliance-retention.md)

| Requirement | Mechanism |
|---|---|
| "How long can/must we keep this?" | `retention_policies` table per dataset |
| Nightly purge of expired data | `rows_past_retention()` on a schedule |
| Right to erasure (including history) | Rewrite current + all historical versions, not soft-delete |
| Proof of compliance | `erasure_requests` audit log, without the erased data itself |

### [07 · Multi-Cloud &amp; Hybrid Data Lake Strategy](level-4/07-multi-cloud-hybrid-strategy.md)

| Concern | Pattern |
|---|---|
| Pipeline code shouldn't hardcode a cloud provider | URI-based abstraction (`parse_lake_uri`) |
| Same logical table, different physical location per region | Table registry keyed on `(logical_table, region)` |
| Recurring cross-cloud queries are expensive | Run compute where the data lives (data gravity) |
| Discovering data across clouds without copying it | Federated catalog with metadata only |

### [08 · Cost Governance at Platform Scale](level-4/08-cost-governance-platform-scale.md)

| Question | Mechanism |
|---|---|
| Who is spending what? | Tag every resource; join usage to tags |
| Are we about to blow a budget? | Project current burn rate forward, alert before month-end |
| How do we make cost visible to the spender? | Chargeback/showback report per team/cost center |
| How do we stop orphan spend? | Enforce tagging at write/creation time |

### [09 · Building a Data Platform Team](level-4/09-building-a-platform-team.md)

| Platform team responsibility | Domain team responsibility |
|---|---|
| Shared infra, catalog, orchestration engine | Pipeline business logic |
| Golden-path templates, CI/CD | Their own data product's schema & SLA |
| Platform-wide incident response | On-call for their own pipeline's failures |
| Self-service tooling, adoption tracking | Actually using the self-service tools |

---

Go deeper with the full [ETL & Data Lake Mastery Path](index.md) curriculum or test yourself with the [interview questions](interview-questions.md).
