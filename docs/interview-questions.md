---
title: "ETL & Data Lake Interview Questions and Answers (179 Questions)"
description: "179 ETL & Data Lake interview questions with answers and explanations, from beginner to advanced, organised by level. Free, from the ETL & Data Lake Mastery Path."
---

# ETL & Data Lake Interview Questions and Answers

**179 questions** with the correct answer and a short explanation, organised from beginner (Level 1) to advanced (Level 4). Every question comes from a lesson in the free ETL & Data Lake Mastery Path, so you can open the lesson to go deeper.

!!! tip "Practise, don't just read"
    Each lesson also ends with an interactive quiz, and the [final quiz](quiz.md) tests all four levels. Try to answer each question below before reading the answer.

## Level 1 · Entry

### [01 · What Is ETL vs. ELT?](level-1/01-etl-vs-elt.md)

**Q1.** What is the difference between ETL and ELT?

**Answer:** ETL transforms before loading; ELT loads raw data first and transforms inside the target

Cheap warehouse/lake compute favored ELT.

**Q2.** When is ETL still a good choice?

**Answer:** When data must be cleaned or masked before landing (e.g. PII) or the target can't transform

Compliance and constraints.

**Q3.** What does ELT enable?

**Answer:** Keeping raw data for reprocessing and transforming with SQL in the target

Flexibility.

**Q4.** Why keep raw data?

**Answer:** So logic can be fixed and reprocessed without re-extracting

Replayability.

### [02 · Data Sources &amp; Ingestion Patterns](level-1/02-data-sources-ingestion-patterns.md)

**Q5.** What are the two basic ingestion patterns?

**Answer:** Batch (periodic chunks) and streaming (continuous events)

Choose by latency need.

**Q6.** Which source type is semi-structured?

**Answer:** JSON logs or API payloads

Schema on read.

**Q7.** Why prefer batch for many analytics loads?

**Answer:** Simpler and cheaper when freshness of minutes-hours is fine

Match latency to need.

**Q8.** What does an API source require handling?

**Answer:** Pagination, rate limits, retries and auth

Resilient extraction.

### [03 · Extraction Basics (Files, APIs, Databases)](level-1/03-extraction-basics.md)

**Q9.** How do you read a large CSV without exhausting memory?

**Answer:** Read in chunks/streams

chunksize.

**Q10.** What should extraction preserve?

**Answer:** The raw data as received, plus metadata like extract time

Auditability.

**Q11.** What does a watermark column support?

**Answer:** Incremental extraction of only new/changed rows

updated_at.

**Q12.** What risk does querying a production DB for extraction bring?

**Answer:** Load on a transactional system; use replicas or off-peak windows

Be polite.

### [04 · Transformation Basics (Cleaning, Type Casting, Deduplication)](level-1/04-transformation-basics.md)

**Q13.** What does this print?

```python
rows=[(' Ada ','10'),('bo','x'),('ADA','10')]
out=[]
for n,v in rows:
    try: v=int(v)
    except ValueError: continue
    out.append((n.strip().lower(),v))
print(sorted(set(out)))
```

**Answer:** [('ada', 10)]

Clean, cast, drop bad, dedupe. Verified by running it.

**Q14.** What are the three common transformations?

**Answer:** Cleaning, type casting and deduplication

Also joins and aggregations.

**Q15.** Why cast types early?

**Answer:** String-typed data causes wrong sorts, sums and comparisons

Schema enforcement.

**Q16.** How should invalid rows be handled?

**Answer:** Quarantined with reasons rather than silently dropped

Reject table.

### [05 · Loading Into a Target](level-1/05-loading-into-a-target.md)

**Q17.** What's a full load vs incremental load?

**Answer:** Full reloads everything; incremental loads only new/changed data

Trade simplicity for efficiency.

**Q18.** What does this upsert produce?

```sql
create table t(id int primary key, v text); insert into t values(1,'a'),(2,'b'); insert into t values(2,'B'),(3,'c') on conflict(id) do update set v=excluded.v;

select * from t order by id;
```

**Answer:** 1,a; 2,B; 3,c

Upsert updates matches and inserts new rows. Verified by running it in SQLite (rows shown as comma-separated, separated by semicolons).

**Q19.** What makes a load idempotent?

**Answer:** Upsert/merge or overwriting a partition instead of blindly appending

Rerun-safe.

**Q20.** Why load to a staging table first?

**Answer:** Validate and swap atomically

Safe publish.

### [06 · What Is a Data Lake? (vs. Data Warehouse)](level-1/06-what-is-a-data-lake.md)

**Q21.** What is a data lake?

**Answer:** Cheap object storage holding raw and processed data in open file formats

Schema on read.

**Q22.** What's the main difference from a warehouse?

**Answer:** Lakes store files of any structure with schema on read; warehouses store curated schema-on-write tables

Complement each other.

**Q23.** What's a data swamp?

**Answer:** A lake with no organization, metadata or quality

Governance.

**Q24.** Why is object storage used?

**Answer:** Cheap, durable and scalable

S3/GCS/ADLS.

### [07 · Bronze/Silver/Gold Layering](level-1/07-bronze-silver-gold-layering.md)

**Q25.** What are bronze, silver and gold?

**Answer:** Raw, cleaned/conformed and business-ready layers

Progressive quality.

**Q26.** What's stored in bronze?

**Answer:** Raw data exactly as ingested

Immutable.

**Q27.** Where does validation happen?

**Answer:** Between bronze and silver

Single place.

**Q28.** Who uses gold tables?

**Answer:** Analysts and BI tools

Stable and trusted.

### [08 · File Formats: CSV, JSON, Parquet, Avro](level-1/08-file-formats.md)

**Q29.** Why is Parquet preferred for analytics?

**Answer:** Columnar, compressed and typed, so queries read only needed columns

Predicate pushdown.

**Q30.** What's a downside of CSV?

**Answer:** No types or schema and ambiguous quoting/encodings

Use for interchange only.

**Q31.** When is JSON good?

**Answer:** Nested/semi-structured data and APIs

Convert to Parquet for analytics.

**Q32.** What is Avro suited for?

**Answer:** Row-oriented streaming/record storage with embedded schema

Schema evolution.

### [09 · Why You Need a Scheduler](level-1/09-orchestration-basics.md)

**Q33.** What does a scheduler provide beyond cron?

**Answer:** Dependencies, retries, backfills and monitoring

Orchestration.

**Q34.** What is a DAG?

**Answer:** A directed acyclic graph of tasks defining order

No cycles.

**Q35.** Why retry with backoff?

**Answer:** Transient failures often pass on retry

Idempotent tasks only.

**Q36.** What should an orchestrator not do?

**Answer:** Heavy data processing itself; delegate to compute engines

Orchestrate, don't compute.

### [10 · Capstone — End-to-End ETL to a Bronze/Silver/Gold Lake](level-1/10-capstone-etl-lake-pipeline.md)

**Q37.** What does the capstone produce?

**Answer:** A bronze/silver/gold lake from a raw CSV with cleaning and deduplication

End to end.

**Q38.** How do you verify it?

**Answer:** Row counts and checks between layers

Reconcile.

**Q39.** Why partition gold by date?

**Answer:** Efficient queries and idempotent partition overwrites

Pruning.

**Q40.** What does the pipeline log?

**Answer:** Counts, rejects and timings for observability

Monitor.

## Level 2 · Intermediate

### [01 · Incremental Loads &amp; Change Data Capture](level-2/01-incremental-loads-cdc.md)

**Q41.** What is CDC?

**Answer:** Change data capture: reading inserts, updates and deletes from a source's log

Log-based replication.

**Q42.** Why use CDC over full loads?

**Answer:** Lower load and latency and captures deletes

Efficiency.

**Q43.** What's a limitation of updated_at watermarks?

**Answer:** They miss hard deletes and rely on reliable timestamps

Hybrid with CDC.

**Q44.** How do you apply CDC events?

**Answer:** MERGE by key respecting order and deletes

Latest wins.

### [02 · Data Quality Checks &amp; Validation](level-2/02-data-quality-validation.md)

**Q45.** Why validate data before loading?

**Answer:** Garbage loaded quietly is worse than a loud failure

Gate.

**Q46.** Which checks are basic?

**Answer:** Nulls, uniqueness, ranges, referential integrity and freshness

Data quality.

**Q47.** What should failing validation do?

**Answer:** Quarantine or halt and alert

Visible failure.

**Q48.** Where do expectations live?

**Answer:** As code in the pipeline (e.g. Great Expectations/dbt tests)

Versioned.

### [03 · Partitioning Strategy for a Data Lake](level-2/03-partitioning-strategy.md)

**Q49.** What does partitioning achieve?

**Answer:** Queries skip irrelevant directories (partition pruning)

Fewer bytes read.

**Q50.** What's a good partition key?

**Answer:** Low-cardinality column frequently filtered, like date

Avoid over-partitioning.

**Q51.** What problem does over-partitioning cause?

**Answer:** Many tiny files and slow metadata operations

Small files.

**Q52.** Hive-style partition paths look like...

**Answer:** year=2025/month=01/part.parquet

Directory per value.

### [04 · Schema Evolution &amp; Handling](level-2/04-schema-evolution.md)

**Q53.** What is schema evolution?

**Answer:** Handling added, widened or renamed columns without breaking pipelines

Plan for change.

**Q54.** Which change is usually backward compatible?

**Answer:** Adding a nullable column

Consumers ignore new columns.

**Q55.** What's the danger of silent schema drift?

**Answer:** Wrong or missing data downstream

Detect and alert.

**Q56.** How do you manage breaking changes?

**Answer:** Versioned contracts and coordinated migrations

Communicate.

### [05 · Orchestration Tools Overview (Airflow DAGs)](level-2/05-orchestration-tools-airflow.md)

**Q57.** What does an Airflow DAG define?

**Answer:** Tasks and their dependencies with a schedule

Python code.

**Q58.** What's the logical date (data interval)?

**Answer:** The period of data a run processes, not the wall-clock time it runs

Key for idempotency.

**Q59.** What does `catchup` do?

**Answer:** Backfills missed schedule intervals

Use deliberately.

**Q60.** Why keep task logic out of the DAG file?

**Answer:** Testability and fast parsing

Thin DAGs.

### [06 · Idempotent Pipeline Design](level-2/06-idempotent-pipeline-design.md)

**Q61.** What's an idempotent pipeline?

**Answer:** Same result no matter how many times it runs for the same input

Retry-safe.

**Q62.** Which write pattern is idempotent?

**Answer:** Overwrite the partition for the run date

Replace, don't add.

**Q63.** Why generate deterministic IDs or keys?

**Answer:** So reruns produce the same rows

Hash business keys.

**Q64.** What breaks idempotency?

**Answer:** Appending, using now() in logic or side effects

Avoid.

### [07 · Backfills &amp; Reprocessing](level-2/07-backfills-reprocessing.md)

**Q65.** What is a backfill?

**Answer:** Re-running a pipeline over past dates to fix or populate data

Needs idempotency.

**Q66.** How should backfills run?

**Answer:** In controlled batches without overwhelming sources or targets

Rate limits.

**Q67.** Why snapshot/version before a backfill?

**Answer:** So you can roll back if the result is wrong

Safety.

**Q68.** What do you verify after?

**Answer:** Reconcile counts and compare with expectations

Quality.

### [08 · Monitoring &amp; Alerting for Pipelines](level-2/08-monitoring-alerting.md)

**Q69.** What's a good pipeline alert?

**Answer:** Actionable: freshness SLA breach or failed run with an owner and runbook

Avoid fatigue.

**Q70.** What metrics matter?

**Answer:** Freshness, volume, failures and data quality results

Observability.

**Q71.** Why track row counts per run?

**Answer:** Sudden drops/spikes signal upstream issues

Anomaly detection.

**Q72.** Why page only on user impact?

**Answer:** Reduces alert fatigue and focuses response

SLOs.

### [09 · Data Contracts Between Producers &amp; Consumers](level-2/09-data-contracts.md)

**Q73.** What does a data contract specify?

**Answer:** Schema, semantics, SLAs and ownership agreed between producers and consumers

Enforced and versioned.

**Q74.** Why enforce contracts in CI?

**Answer:** Catch breaking changes before they reach consumers

Shift left.

**Q75.** Who owns the contract?

**Answer:** The producing team, with consumer input

Accountability.

**Q76.** What's included in semantics?

**Answer:** Definitions like units, timezone and business meaning

Avoid ambiguity.

### [10 · Project — Incremental, Validated Lake Pipeline](level-2/10-project-incremental-validated-pipeline.md)

**Q77.** What does the incremental validated pipeline combine?

**Answer:** CDC/watermark loads, validation gates, partitioning and orchestration

Level 2 capstone.

**Q78.** What happens to failed validations?

**Answer:** Quarantine with reasons and alert

Visible.

**Q79.** How do you prove idempotency?

**Answer:** Run twice and compare outputs

Test it.

**Q80.** What do you monitor?

**Answer:** Freshness, counts and quality results

Operate.

## Level 3 · Advanced

### [01 · Data Lakehouse Concepts (Delta Lake/Iceberg/Hudi)](level-3/01-lakehouse-concepts.md)

**Q81.** What is a lakehouse?

**Answer:** A lake with a transactional table layer (Delta/Iceberg/Hudi) providing warehouse-like reliability

Open formats plus ACID.

**Q82.** What do table formats add to Parquet files?

**Answer:** ACID transactions, schema evolution, time travel and efficient metadata

A log of commits.

**Q83.** Why is the lakehouse attractive?

**Answer:** One copy of data serves BI, ML and streaming

Less duplication.

**Q84.** What's still required for success?

**Answer:** Governance, quality and good modeling

Not magic.

### [02 · Streaming ETL Basics](level-3/02-streaming-etl-basics.md)

**Q85.** What is streaming ETL?

**Answer:** Processing events continuously as they arrive instead of in batches

Low latency.

**Q86.** What's a windowed aggregation?

**Answer:** Computing over time windows of events

Tumbling/sliding.

**Q87.** What does exactly-once require?

**Answer:** Idempotent/transactional sinks plus checkpointing

End to end.

**Q88.** When is streaming worth the complexity?

**Answer:** When freshness of seconds or minutes creates value

Cost vs benefit.

### [03 · Data Cataloging &amp; Metadata Management](level-3/03-data-cataloging-metadata.md)

**Q89.** What does a data catalog hold?

**Answer:** Metadata: schemas, owners, descriptions and lineage

Discoverability.

**Q90.** Why enrich metadata?

**Answer:** So people can find and trust datasets

Documentation.

**Q91.** What is a business glossary?

**Answer:** Agreed definitions of business terms

Shared language.

**Q92.** What stops catalogs rotting?

**Answer:** Automation and ownership

Sync from sources.

### [04 · Cost &amp; Performance Optimization for Lake Storage](level-3/04-cost-performance-optimization.md)

**Q93.** What drives lake query cost?

**Answer:** Bytes scanned and compute time

Optimize layout.

**Q94.** Which reduces scan cost?

**Answer:** Columnar formats, partition pruning and compaction

Layout matters.

**Q95.** Why right-size compute?

**Answer:** Idle/oversized clusters waste money

Autoscale.

**Q96.** What's a cost guardrail?

**Answer:** Budgets, quotas and alerts per team

FinOps.

### [05 · Handling Late-Arriving &amp; Out-of-Order Data](level-3/05-late-arriving-data.md)

**Q97.** What is late-arriving data?

**Answer:** Events that arrive after their window or partition was processed

Common in streams.

**Q98.** How handle it in batch?

**Answer:** Reprocess a lookback window or upsert into affected partitions

Idempotent.

**Q99.** What does a watermark do in streaming?

**Answer:** Defines how late data is still accepted

State bounds.

**Q100.** What's a trade-off of long lateness windows?

**Answer:** More state and delayed results

Choose by need.

### [06 · Small Files &amp; Compaction Strategies](level-3/06-small-files-compaction.md)

**Q101.** Why do small files hurt?

**Answer:** Metadata and open overhead slow queries and listing

Compact.

**Q102.** What does compaction do?

**Answer:** Rewrites many small files into fewer larger ones

OPTIMIZE.

**Q103.** What target file size is typical?

**Answer:** About 128 MB to 1 GB

Balance parallelism.

**Q104.** What causes small files?

**Answer:** Streaming micro-batches and over-partitioning

Compact regularly.

### [07 · Time Travel &amp; Data Versioning](level-3/07-time-travel-versioning.md)

**Q105.** What does time travel allow?

**Answer:** Querying previous table versions

Via the commit log.

**Q106.** Why is it useful?

**Answer:** Audit, reproducibility and recovery from bad writes

Restore.

**Q107.** What limits it?

**Answer:** Retention: vacuumed files are gone

Set retention deliberately.

**Q108.** How is a version identified?

**Answer:** By version number or timestamp in the transaction log

VERSION AS OF.

### [08 · ACID Transactions on the Lake](level-3/08-acid-transactions-lake.md)

**Q109.** What is ACID on a lake?

**Answer:** Atomic, consistent, isolated, durable commits via a transaction log

Table formats.

**Q110.** What does optimistic concurrency do?

**Answer:** Writers proceed and conflict detection fails one commit to retry

Retry on conflict.

**Q111.** Why does atomicity matter?

**Answer:** Readers never see half-written data

Snapshot isolation.

**Q112.** What happens without ACID on object stores?

**Answer:** Partial writes and inconsistent reads on failures

Hence table formats.

### [09 · Query Engines Over the Lake (Presto/Trino/Athena)](level-3/09-query-engines.md)

**Q113.** What do query engines do?

**Answer:** Execute SQL over lake files (Trino, Spark SQL, Athena, DuckDB)

Compute layer.

**Q114.** Why separate storage and compute?

**Answer:** Scale and pay independently, and use multiple engines on one copy

Flexibility.

**Q115.** What improves engine performance?

**Answer:** Partition pruning, statistics and columnar formats

Layout.

**Q116.** How choose an engine?

**Answer:** By workload: interactive SQL, batch ETL or ad hoc exploration

Fit.

### [10 · Project — Streaming-Ready Lakehouse Table](level-3/10-project-streaming-lakehouse-table.md)

**Q117.** What does the streaming lakehouse project combine?

**Answer:** Streaming ingestion into a transactional table with compaction and time travel

Level 3 capstone.

**Q118.** How do you make streaming writes safe?

**Answer:** Checkpoints and idempotent/transactional sinks

Exactly once.

**Q119.** What keeps performance healthy?

**Answer:** Regular compaction and statistics maintenance

Operational.

**Q120.** How do you validate?

**Answer:** Reconcile counts and check late data handling

Verify.

## Level 4 · Master

### [01 · Building a Governed Multi-Zone Data Lake](level-4/01-governed-multi-zone-lake.md)

**Q121.** What is a governed multi-zone lake?

**Answer:** Separate zones (raw, curated, sandbox) with policies and ownership

Structure and control.

**Q122.** Why zones?

**Answer:** Different quality, access and retention per purpose

Policy boundaries.

**Q123.** What does governance include?

**Answer:** Ownership, classification, quality and access control

Accountable data.

**Q124.** What prevents a data swamp?

**Answer:** Cataloging, ownership and quality gates

Discipline.

### [02 · Data Lineage &amp; Observability](level-4/02-data-lineage-observability.md)

**Q125.** What is data lineage?

**Answer:** Tracing data from source through transformations to consumers

Impact and root-cause analysis.

**Q126.** Why useful?

**Answer:** Know what a change breaks and trace issues upstream

Observability.

**Q127.** What does data observability monitor?

**Answer:** Freshness, volume, schema and distribution anomalies

Detect before users.

**Q128.** How is lineage captured?

**Answer:** From orchestration/query metadata (OpenLineage, catalogs)

Automate.

### [03 · Security &amp; Access Control Patterns for Data Lakes](level-4/03-security-access-control.md)

**Q129.** What's least privilege for data?

**Answer:** Grant only the access needed, by role

RBAC.

**Q130.** What's column-level security?

**Answer:** Restricting or masking sensitive columns

PII.

**Q131.** How protect data at rest and in transit?

**Answer:** Encryption with managed keys and TLS

KMS.

**Q132.** Why audit access?

**Answer:** Detect misuse and prove compliance

Logs.

### [04 · Lake vs. Warehouse vs. Lakehouse: Choosing for a Use Case](level-4/04-lake-warehouse-lakehouse-choice.md)

**Q133.** When does a warehouse fit best?

**Answer:** Curated, structured BI with strong governance and performance needs

Schema on write.

**Q134.** When does a lake fit best?

**Answer:** Large, varied raw data and ML/exploration at low cost

Schema on read.

**Q135.** What does a lakehouse try to offer?

**Answer:** Both: open files with warehouse-like reliability

Trade-offs.

**Q136.** How decide?

**Answer:** By workloads, skills, governance and cost

Fit for purpose.

### [05 · Data Mesh &amp; Domain-Oriented Lake Ownership](level-4/05-data-mesh-domain-ownership.md)

**Q137.** What is data mesh?

**Answer:** Domain teams own and publish data products with federated governance

Decentralized ownership.

**Q138.** What's a data product?

**Answer:** A discoverable, trustworthy, documented dataset owned by a domain

SLAs.

**Q139.** What must the platform provide?

**Answer:** Self-service infrastructure and standards

Enable domains.

**Q140.** When is mesh a poor fit?

**Answer:** Small organizations with few domains

Overhead.

### [06 · Compliance &amp; Retention in Data Lakes](level-4/06-compliance-retention.md)

**Q141.** What does retention policy define?

**Answer:** How long data is kept before deletion or archiving

Compliance.

**Q142.** What does GDPR erasure require?

**Answer:** Locating and deleting a person's data across the platform

Design for deletion.

**Q143.** Why classify data?

**Answer:** Controls depend on sensitivity (PII, confidential, public)

Policies.

**Q144.** What's legal hold?

**Answer:** Suspending deletion for data relevant to litigation

Override retention.

### [07 · Multi-Cloud &amp; Hybrid Data Lake Strategy](level-4/07-multi-cloud-hybrid-strategy.md)

**Q145.** What is a risk of multi-cloud lakes?

**Answer:** Complexity, egress costs and inconsistent governance

Avoid unless needed.

**Q146.** What enables portability?

**Answer:** Open table/file formats and standard engines

Parquet/Iceberg.

**Q147.** When consider hybrid?

**Answer:** Data residency or on-prem sources that can't move

Constraints.

**Q148.** What manages cross-cloud security?

**Answer:** Consistent identity, encryption and policy

Federation.

### [08 · Cost Governance at Platform Scale](level-4/08-cost-governance-platform-scale.md)

**Q149.** How do you control platform cost at scale?

**Answer:** Tagging, budgets, quotas, lifecycle policies and showback/chargeback

FinOps.

**Q150.** What lifecycle policy saves money?

**Answer:** Tiering cold data to cheaper storage and expiring temp data

Automate.

**Q151.** What reveals waste?

**Answer:** Cost per team/pipeline and unused data/tables

Visibility.

**Q152.** Why involve data owners?

**Answer:** They decide the value and retention of their data

Accountability.

### [09 · Building a Data Platform Team](level-4/09-building-a-platform-team.md)

**Q153.** What should a platform team provide?

**Answer:** Paved roads: ingestion, storage, governance and observability as a product

Enable teams.

**Q154.** How measure platform success?

**Answer:** Adoption, reliability and time-to-first-pipeline

Outcomes.

**Q155.** What's a risk?

**Answer:** Becoming a bottleneck instead of self-service

Design for autonomy.

**Q156.** Why treat the platform as a product?

**Answer:** Users have needs and feedback loops

Roadmaps.

### [10 · Capstone — Governed Enterprise Data Lake Architecture](level-4/10-capstone-enterprise-lake-architecture.md)

**Q157.** What does the enterprise architecture capstone combine?

**Answer:** Governed zones, lineage, security, cost controls and mesh/platform operating model

Full picture.

**Q158.** How do you justify design choices?

**Answer:** Trade-offs against requirements, risks and costs

Decision records.

**Q159.** What proves readiness?

**Answer:** Failure drills, access reviews, cost reports and lineage coverage

Operate.

**Q160.** What sustains the architecture?

**Answer:** Ownership, standards and continuous improvement

Culture.

## Mixed questions across all levels

**Q161.** A source team renames a column and your pipeline silently produces nulls. What would have caught it?

**Answer:** A data contract with schema checks in CI and a validation gate before load

Fail loudly at the boundary.

**Q162.** Why keep a bronze layer of untouched raw data?

**Answer:** So logic can be fixed and the data reprocessed without re-extracting

Replayability.

**Q163.** Where should deduplication and type casting happen?

**Answer:** In the bronze-to-silver step, with rejects quarantined

One place for validation.

**Q164.** A rerun appends rows again. Which load is idempotent?

```sql
create table t(id int primary key, v int); insert into t values(1,10),(2,20); insert into t values(2,25),(3,30) on conflict(id) do update set v=excluded.v;

select * from t order by id;
```

**Answer:** 1,10; 2,25; 3,30

Upsert keyed on id converges to the same state however often it runs. Verified by running it in SQLite (rows shown as comma-separated, separated by semicolons).

**Q165.** A job reads an updated_at watermark but deletes in the source never reach the lake. What's missing?

**Answer:** Log-based CDC (or periodic reconciliation) to capture deletes

Timestamps can't see hard deletes.

**Q166.** Queries on a date-filtered table scan everything. What's the fix?

**Answer:** Partition (or cluster) by date and filter on it so pruning works

Layout drives cost.

**Q167.** A streaming job writes thousands of 1 MB files per day. What problem and fix?

**Answer:** Small files; compact regularly (OPTIMIZE/compaction) and tune batch sizes

Metadata overhead.

**Q168.** A bad transform ran for a week. How do you correct history?

**Answer:** Fix the logic and backfill the affected partitions idempotently, then reconcile counts

Backfills need idempotency.

**Q169.** Events arrive two days late. How do you keep daily aggregates correct?

**Answer:** Reprocess a lookback window of partitions (or upsert) so late data lands in the right day

Late data is normal.

**Q170.** What does a watermark in streaming control?

**Answer:** How late events may arrive before windows close and state is dropped

Trade-off between completeness and latency.

**Q171.** Why do lakehouse table formats (Delta/Iceberg/Hudi) matter?

**Answer:** They add ACID transactions, schema evolution and time travel on top of Parquet files

Reliable tables on object storage.

**Q172.** Two writers commit to the same table at once. What does optimistic concurrency do?

**Answer:** One commit succeeds and the other detects the conflict and retries

Transaction log arbitration.

**Q173.** A table was overwritten by mistake. What feature helps recovery?

**Answer:** Time travel/restore to a previous version (within retention)

Mind VACUUM retention.

**Q174.** Analysts cannot find or trust datasets. What do you add?

**Answer:** A data catalog with owners, descriptions, quality status and lineage

Discoverability and trust.

**Q175.** A column change would break unknown downstream reports. What helps you assess impact?

**Answer:** Data lineage from the catalog/orchestrator metadata

Impact analysis.

**Q176.** PII must be visible to some roles and masked for others. What control fits?

**Answer:** Column-level security/masking with role-based access

Least privilege.

**Q177.** A GDPR erasure request arrives. What must the platform be able to do?

**Answer:** Locate and delete a person's data across layers, including derived tables

Design for deletion.

**Q178.** Cloud costs spike. What do you check first?

**Answer:** Bytes scanned and idle compute by team/pipeline (tags), then fix layout and sizing

FinOps visibility.

**Q179.** A central data team is a bottleneck for every domain. Which model addresses it?

**Answer:** Data mesh: domain-owned data products on a self-serve platform with federated governance

Ownership scales.

## Practical coding questions

### Bronze to silver to gold

Implement to_silver (dedupe on order_id keeping the first, strip/upper the country, cast types, quarantine rows with a bad amount or missing country as (order_id, reason)) and to_gold (total amount per (ts, country)). Show the result is the same when the raw data is ingested twice.

Starter code:

```
from collections import defaultdict

BRONZE = [  # raw rows exactly as ingested (strings, duplicates and bad data included)
    {"order_id": "1", "country": " us ", "amount": "10.50", "ts": "2025-01-01"},
    {"order_id": "2", "country": "DE", "amount": "20", "ts": "2025-01-01"},
    {"order_id": "2", "country": "DE", "amount": "20", "ts": "2025-01-01"},   # exact duplicate
    {"order_id": "3", "country": "us", "amount": "abc", "ts": "2025-01-02"},  # bad amount
    {"order_id": "4", "country": "", "amount": "5", "ts": "2025-01-02"},      # missing country
    {"order_id": "5", "country": "De", "amount": "7.25", "ts": "2025-01-02"},
]

def to_silver(bronze):
    # dedupe on order_id (keep the first), strip/upper the country, cast amount to float and
    # order_id to int; send rows with a bad amount or a missing country to quarantine as
    # (order_id, reason). Return (silver_rows, quarantine).
    return [], []

def to_gold(silver):
    # total amount per (ts, country), rounded to 2 decimals, as a dict sorted by key
    return {}

silver, quarantine = to_silver(BRONZE)
print(len(BRONZE), len(silver), quarantine)
print(to_gold(silver))
print(to_gold(to_silver(BRONZE + BRONZE)[0]) == to_gold(silver))   # idempotent on rerun
```

Expected output:

```text
6 3 [('3', 'bad amount'), ('4', 'missing country')]
{('2025-01-01', 'DE'): 20.0, ('2025-01-01', 'US'): 10.5, ('2025-01-02', 'DE'): 7.25}
True
```

??? success "Show a solution"

    ```
    from collections import defaultdict

    BRONZE = [  # raw rows exactly as ingested (strings, duplicates and bad data included)
        {"order_id": "1", "country": " us ", "amount": "10.50", "ts": "2025-01-01"},
        {"order_id": "2", "country": "DE", "amount": "20", "ts": "2025-01-01"},
        {"order_id": "2", "country": "DE", "amount": "20", "ts": "2025-01-01"},   # exact duplicate
        {"order_id": "3", "country": "us", "amount": "abc", "ts": "2025-01-02"},  # bad amount
        {"order_id": "4", "country": "", "amount": "5", "ts": "2025-01-02"},      # missing country
        {"order_id": "5", "country": "De", "amount": "7.25", "ts": "2025-01-02"},
    ]

    def to_silver(bronze):
        silver, quarantine, seen = [], [], set()
        for r in bronze:
            if r["order_id"] in seen:
                continue                                   # deduplicate on the business key
            seen.add(r["order_id"])
            country = r["country"].strip().upper()
            try:
                amount = float(r["amount"])
            except ValueError:
                quarantine.append((r["order_id"], "bad amount"))
                continue
            if not country:
                quarantine.append((r["order_id"], "missing country"))
                continue
            silver.append({"order_id": int(r["order_id"]), "country": country, "amount": amount, "ts": r["ts"]})
        return silver, quarantine

    def to_gold(silver):
        totals = defaultdict(float)
        for r in silver:
            totals[(r["ts"], r["country"])] += r["amount"]
        return {k: round(v, 2) for k, v in sorted(totals.items())}

    silver, quarantine = to_silver(BRONZE)
    print(len(BRONZE), len(silver), quarantine)
    print(to_gold(silver))
    print(to_gold(to_silver(BRONZE + BRONZE)[0]) == to_gold(silver))   # idempotent on rerun
    ```

    Raw rows stay untouched in bronze, silver applies validation in one place, and gold is a deterministic aggregation. Deduplicating on the business key makes reruns idempotent. Verified by running the solution.

### Late-arriving events and idempotent partitions

Implement process(events, table, run_date, lookback_days): rebuild and overwrite every daily partition from run_date - lookback_days to run_date from all known events so a late event lands in its (earlier) day and reruns never double count.

Starter code:

```
from collections import defaultdict
from datetime import date, timedelta

class DailyPartitions:
    """A tiny partitioned table: one 'file' per event date, overwritten atomically."""
    def __init__(self):
        self.parts = {}

    def overwrite(self, day, rows):
        self.parts[day] = sorted(rows)

    def total(self, day):
        return sum(self.parts.get(day, []))

def process(events, table, run_date, lookback_days=2):
    # rebuild every partition from run_date - lookback_days to run_date (inclusive) from ALL events,
    # overwriting each (so late events land in the right earlier day and reruns never double count);
    # return the list of ISO dates touched
    return []

d1, d2, d3, d4 = date(2025, 3, 1), date(2025, 3, 2), date(2025, 3, 3), date(2025, 3, 4)
table = DailyPartitions()
events = [(d1, 10), (d2, 20), (d3, 30)]
process(events, table, d3)
print([table.total(d) for d in (d1, d2, d3)])
events.append((d2, 5))                      # a late event for yesterday arrives
print(process(events, table, d4))
print([table.total(d) for d in (d2, d3, d4)])
process(events, table, d4)                  # rerun: must not double count
print([table.total(d) for d in (d2, d3, d4)])
print(table.total(d1))                      # older than lookback: untouched
```

Expected output:

```text
[10, 20, 30]
['2025-03-02', '2025-03-03', '2025-03-04']
[25, 30, 0]
[25, 30, 0]
10
```

??? success "Show a solution"

    ```
    from collections import defaultdict
    from datetime import date, timedelta

    class DailyPartitions:
        """A tiny partitioned table: one 'file' per event date, overwritten atomically."""
        def __init__(self):
            self.parts = {}

        def overwrite(self, day, rows):
            self.parts[day] = sorted(rows)

        def total(self, day):
            return sum(self.parts.get(day, []))

    def process(events, table, run_date, lookback_days=2):
        """Reprocess partitions from run_date - lookback_days .. run_date from ALL known events,
        so late-arriving events land in the right (earlier) partition and reruns are idempotent."""
        by_day = defaultdict(list)
        for event_day, value in events:
            by_day[event_day].append(value)
        start = run_date - timedelta(days=lookback_days)
        day = start
        touched = []
        while day <= run_date:
            table.overwrite(day, by_day.get(day, []))
            touched.append(day.isoformat())
            day += timedelta(days=1)
        return touched

    d1, d2, d3, d4 = date(2025, 3, 1), date(2025, 3, 2), date(2025, 3, 3), date(2025, 3, 4)
    table = DailyPartitions()
    events = [(d1, 10), (d2, 20), (d3, 30)]
    process(events, table, d3)
    print([table.total(d) for d in (d1, d2, d3)])
    events.append((d2, 5))                      # a late event for yesterday arrives
    print(process(events, table, d4))
    print([table.total(d) for d in (d2, d3, d4)])
    process(events, table, d4)                  # rerun: must not double count
    print([table.total(d) for d in (d2, d3, d4)])
    print(table.total(d1))                      # older than lookback: untouched
    ```

    Overwriting whole partitions from the full event set is idempotent, and the lookback window bounds the reprocessing cost (older partitions stay untouched). Verified by running the solution.

---

More free practice: the [ETL & Data Lake Mastery Path](index.md) curriculum, the [Interview Prep Pack](https://learnmasterypath.com/interview-pack/) and the [free resume ATS check](https://learnmasterypath.com/resume/).
