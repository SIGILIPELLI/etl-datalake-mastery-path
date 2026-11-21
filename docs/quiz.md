---
title: "Final Quiz"
description: "Test what you have learned across all four levels of ETL \u0026 Data Lake Mastery Path with a 19-question final quiz."
---

# Final Quiz

19 questions drawn from all four levels of this course. You need **70% (14 out of 19)** to pass —
you can retake it as many times as you like. Your best result is saved in this browser.

<div id="mp-quiz-root"></div>

<script type="application/json" id="mp-quiz-data">
[
  {
    "question": "A source team renames a column and your pipeline silently produces nulls. What would have caught it?",
    "options": [
      "More retries",
      "A bigger cluster",
      "A dashboard",
      "A data contract with schema checks in CI and a validation gate before load"
    ],
    "correctIndex": 3,
    "explanation": "Fail loudly at the boundary."
  },
  {
    "question": "Why keep a bronze layer of untouched raw data?",
    "options": [
      "Speed",
      "Style",
      "So logic can be fixed and the data reprocessed without re-extracting",
      "Law"
    ],
    "correctIndex": 2,
    "explanation": "Replayability."
  },
  {
    "question": "Where should deduplication and type casting happen?",
    "options": [
      "In gold",
      "In the bronze-to-silver step, with rejects quarantined",
      "In BI",
      "Nowhere"
    ],
    "correctIndex": 1,
    "explanation": "One place for validation."
  },
  {
    "question": "A rerun appends rows again. Which load is idempotent?\n```sql\ncreate table t(id int primary key, v int); insert into t values(1,10),(2,20); insert into t values(2,25),(3,30) on conflict(id) do update set v=excluded.v;\n\nselect * from t order by id;\n```\n",
    "options": [
      "1,10; 3,30",
      "1,10; 2,20; 2,25; 3,30",
      "1,10; 2,25; 3,30",
      "2,25"
    ],
    "correctIndex": 2,
    "explanation": "Upsert keyed on id converges to the same state however often it runs. Verified by running it in SQLite (rows shown as comma-separated, separated by semicolons)."
  },
  {
    "question": "A job reads an updated_at watermark but deletes in the source never reach the lake. What's missing?",
    "options": [
      "Retries",
      "Log-based CDC (or periodic reconciliation) to capture deletes",
      "More watermarks",
      "Partitions"
    ],
    "correctIndex": 1,
    "explanation": "Timestamps can't see hard deletes."
  },
  {
    "question": "Queries on a date-filtered table scan everything. What's the fix?",
    "options": [
      "Select *",
      "More joins",
      "Bigger cluster",
      "Partition (or cluster) by date and filter on it so pruning works"
    ],
    "correctIndex": 3,
    "explanation": "Layout drives cost."
  },
  {
    "question": "A streaming job writes thousands of 1 MB files per day. What problem and fix?",
    "options": [
      "Schema drift",
      "Small files; compact regularly (OPTIMIZE/compaction) and tune batch sizes",
      "Large files",
      "Slow network"
    ],
    "correctIndex": 1,
    "explanation": "Metadata overhead."
  },
  {
    "question": "A bad transform ran for a week. How do you correct history?",
    "options": [
      "Delete lake",
      "Edit rows",
      "Fix the logic and backfill the affected partitions idempotently, then reconcile counts",
      "Ignore"
    ],
    "correctIndex": 2,
    "explanation": "Backfills need idempotency."
  },
  {
    "question": "Events arrive two days late. How do you keep daily aggregates correct?",
    "options": [
      "Drop late data",
      "Ignore",
      "Append to today",
      "Reprocess a lookback window of partitions (or upsert) so late data lands in the right day"
    ],
    "correctIndex": 3,
    "explanation": "Late data is normal."
  },
  {
    "question": "What does a watermark in streaming control?",
    "options": [
      "File size",
      "Encryption",
      "Sorting",
      "How late events may arrive before windows close and state is dropped"
    ],
    "correctIndex": 3,
    "explanation": "Trade-off between completeness and latency."
  },
  {
    "question": "Why do lakehouse table formats (Delta/Iceberg/Hudi) matter?",
    "options": [
      "Compression",
      "Encryption",
      "Speed only",
      "They add ACID transactions, schema evolution and time travel on top of Parquet files"
    ],
    "correctIndex": 3,
    "explanation": "Reliable tables on object storage."
  },
  {
    "question": "Two writers commit to the same table at once. What does optimistic concurrency do?",
    "options": [
      "Locks all",
      "Corrupts",
      "Both overwrite",
      "One commit succeeds and the other detects the conflict and retries"
    ],
    "correctIndex": 3,
    "explanation": "Transaction log arbitration."
  },
  {
    "question": "A table was overwritten by mistake. What feature helps recovery?",
    "options": [
      "Time travel/restore to a previous version (within retention)",
      "Rewrite",
      "Nothing",
      "Backups only"
    ],
    "correctIndex": 0,
    "explanation": "Mind VACUUM retention."
  },
  {
    "question": "Analysts cannot find or trust datasets. What do you add?",
    "options": [
      "Bigger lake",
      "More tables",
      "Faster compute",
      "A data catalog with owners, descriptions, quality status and lineage"
    ],
    "correctIndex": 3,
    "explanation": "Discoverability and trust."
  },
  {
    "question": "A column change would break unknown downstream reports. What helps you assess impact?",
    "options": [
      "Guess",
      "Rollback",
      "Data lineage from the catalog/orchestrator metadata",
      "Email"
    ],
    "correctIndex": 2,
    "explanation": "Impact analysis."
  },
  {
    "question": "PII must be visible to some roles and masked for others. What control fits?",
    "options": [
      "Passwords",
      "Column-level security/masking with role-based access",
      "Hiding tables",
      "Copies per role"
    ],
    "correctIndex": 1,
    "explanation": "Least privilege."
  },
  {
    "question": "A GDPR erasure request arrives. What must the platform be able to do?",
    "options": [
      "Ignore archives",
      "Locate and delete a person's data across layers, including derived tables",
      "Delete the lake",
      "Hash the table"
    ],
    "correctIndex": 1,
    "explanation": "Design for deletion."
  },
  {
    "question": "Cloud costs spike. What do you check first?",
    "options": [
      "Add columns",
      "Bytes scanned and idle compute by team/pipeline (tags), then fix layout and sizing",
      "Buy more",
      "Add users"
    ],
    "correctIndex": 1,
    "explanation": "FinOps visibility."
  },
  {
    "question": "A central data team is a bottleneck for every domain. Which model addresses it?",
    "options": [
      "Fewer domains",
      "More central staff",
      "One lake",
      "Data mesh: domain-owned data products on a self-serve platform with federated governance"
    ],
    "correctIndex": 3,
    "explanation": "Ownership scales."
  }
]
</script>

## Coding challenges

Try these on your own computer, then compare your output. Nothing you write is sent anywhere.

### Bronze to silver to gold

Implement to_silver (dedupe on order_id keeping the first, strip/upper the country, cast types, quarantine rows with a bad amount or missing country as (order_id, reason)) and to_gold (total amount per (ts, country)). Show the result is the same when the raw data is ingested twice.

**Starter code**

```python
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

**Expected output**

```text
6 3 [('3', 'bad amount'), ('4', 'missing country')]
{('2025-01-01', 'DE'): 20.0, ('2025-01-01', 'US'): 10.5, ('2025-01-02', 'DE'): 7.25}
True
```

<details><summary>Show solution and explanation</summary>

<pre><code>from collections import defaultdict

BRONZE = [  # raw rows exactly as ingested (strings, duplicates and bad data included)
    {&quot;order_id&quot;: &quot;1&quot;, &quot;country&quot;: &quot; us &quot;, &quot;amount&quot;: &quot;10.50&quot;, &quot;ts&quot;: &quot;2025-01-01&quot;},
    {&quot;order_id&quot;: &quot;2&quot;, &quot;country&quot;: &quot;DE&quot;, &quot;amount&quot;: &quot;20&quot;, &quot;ts&quot;: &quot;2025-01-01&quot;},
    {&quot;order_id&quot;: &quot;2&quot;, &quot;country&quot;: &quot;DE&quot;, &quot;amount&quot;: &quot;20&quot;, &quot;ts&quot;: &quot;2025-01-01&quot;},   # exact duplicate
    {&quot;order_id&quot;: &quot;3&quot;, &quot;country&quot;: &quot;us&quot;, &quot;amount&quot;: &quot;abc&quot;, &quot;ts&quot;: &quot;2025-01-02&quot;},  # bad amount
    {&quot;order_id&quot;: &quot;4&quot;, &quot;country&quot;: &quot;&quot;, &quot;amount&quot;: &quot;5&quot;, &quot;ts&quot;: &quot;2025-01-02&quot;},      # missing country
    {&quot;order_id&quot;: &quot;5&quot;, &quot;country&quot;: &quot;De&quot;, &quot;amount&quot;: &quot;7.25&quot;, &quot;ts&quot;: &quot;2025-01-02&quot;},
]

def to_silver(bronze):
    silver, quarantine, seen = [], [], set()
    for r in bronze:
        if r[&quot;order_id&quot;] in seen:
            continue                                   # deduplicate on the business key
        seen.add(r[&quot;order_id&quot;])
        country = r[&quot;country&quot;].strip().upper()
        try:
            amount = float(r[&quot;amount&quot;])
        except ValueError:
            quarantine.append((r[&quot;order_id&quot;], &quot;bad amount&quot;))
            continue
        if not country:
            quarantine.append((r[&quot;order_id&quot;], &quot;missing country&quot;))
            continue
        silver.append({&quot;order_id&quot;: int(r[&quot;order_id&quot;]), &quot;country&quot;: country, &quot;amount&quot;: amount, &quot;ts&quot;: r[&quot;ts&quot;]})
    return silver, quarantine

def to_gold(silver):
    totals = defaultdict(float)
    for r in silver:
        totals[(r[&quot;ts&quot;], r[&quot;country&quot;])] += r[&quot;amount&quot;]
    return {k: round(v, 2) for k, v in sorted(totals.items())}

silver, quarantine = to_silver(BRONZE)
print(len(BRONZE), len(silver), quarantine)
print(to_gold(silver))
print(to_gold(to_silver(BRONZE + BRONZE)[0]) == to_gold(silver))   # idempotent on rerun
</code></pre>

<p>Raw rows stay untouched in bronze, silver applies validation in one place, and gold is a deterministic aggregation. Deduplicating on the business key makes reruns idempotent. Verified by running the solution.</p>
</details>

### Late-arriving events and idempotent partitions

Implement process(events, table, run_date, lookback_days): rebuild and overwrite every daily partition from run_date - lookback_days to run_date from all known events so a late event lands in its (earlier) day and reruns never double count.

**Starter code**

```python
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

**Expected output**

```text
[10, 20, 30]
['2025-03-02', '2025-03-03', '2025-03-04']
[25, 30, 0]
[25, 30, 0]
10
```

<details><summary>Show solution and explanation</summary>

<pre><code>from collections import defaultdict
from datetime import date, timedelta

class DailyPartitions:
    &quot;&quot;&quot;A tiny partitioned table: one &#x27;file&#x27; per event date, overwritten atomically.&quot;&quot;&quot;
    def __init__(self):
        self.parts = {}

    def overwrite(self, day, rows):
        self.parts[day] = sorted(rows)

    def total(self, day):
        return sum(self.parts.get(day, []))

def process(events, table, run_date, lookback_days=2):
    &quot;&quot;&quot;Reprocess partitions from run_date - lookback_days .. run_date from ALL known events,
    so late-arriving events land in the right (earlier) partition and reruns are idempotent.&quot;&quot;&quot;
    by_day = defaultdict(list)
    for event_day, value in events:
        by_day[event_day].append(value)
    start = run_date - timedelta(days=lookback_days)
    day = start
    touched = []
    while day &lt;= run_date:
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
</code></pre>

<p>Overwriting whole partitions from the full event set is idempotent, and the lookback window bounds the reprocessing cost (older partitions stay untouched). Verified by running the solution.</p>
</details>
