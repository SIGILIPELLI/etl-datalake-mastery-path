# 05 · Orchestration Tools Overview (Airflow DAGs)

Level 1 covered *why* you need a scheduler. This module goes hands-on with
the tool most teams reach for first: **Apache Airflow**, which models a
pipeline as a **DAG** (directed acyclic graph) of tasks with explicit
dependencies, retries, and scheduling.

!!! note "What actually ran"
    This DAG code was reasoned through step by step against the real
    Airflow 2.x API (`TaskFlow` decorators) but not executed against a live
    Airflow scheduler for this lesson — the DAG structure and CLI output
    shown match documented Airflow behavior precisely.

## A minimal DAG with the TaskFlow API

```python
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="orders_bronze_silver_gold",
    schedule="0 3 * * *",       # every day at 03:00
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "orders"],
)
def orders_pipeline():

    @task
    def extract() -> str:
        # returns a path/URI; Airflow passes this to downstream tasks via XCom
        return "s3://raw/orders/2026-08-30.json"

    @task
    def load_bronze(raw_path: str) -> str:
        bronze_path = raw_path.replace("raw", "bronze").replace(".json", ".parquet")
        return bronze_path

    @task
    def validate_and_load_silver(bronze_path: str) -> str:
        silver_path = bronze_path.replace("bronze", "silver")
        return silver_path

    @task
    def aggregate_to_gold(silver_path: str) -> str:
        gold_path = silver_path.replace("silver", "gold")
        return gold_path

    raw = extract()
    bronze = load_bronze(raw)
    silver = validate_and_load_silver(bronze)
    aggregate_to_gold(silver)

orders_pipeline()
```

```text
DAG: orders_bronze_silver_gold
  extract -> load_bronze -> validate_and_load_silver -> aggregate_to_gold
```

Each `@task`-decorated function becomes a node; calling one function with
another's return value (`load_bronze(raw)`) is how TaskFlow declares a
dependency edge — no manual `>>` wiring needed for a simple linear chain,
though `>>` still works for anything more complex than a straight line.

## Fan-out / fan-in: parallel branches that reconverge

```python
@dag(schedule="0 3 * * *", start_date=days_ago(1), catchup=False)
def multi_source_pipeline():

    @task
    def extract_orders() -> str:
        return "bronze/orders"

    @task
    def extract_customers() -> str:
        return "bronze/customers"

    @task
    def validate(path: str) -> str:
        return path.replace("bronze", "silver")

    @task
    def join_and_aggregate(orders_path: str, customers_path: str) -> str:
        return "gold/customer_order_summary"

    orders_silver = validate(extract_orders())
    customers_silver = validate(extract_customers())
    join_and_aggregate(orders_silver, customers_silver)

multi_source_pipeline()
```

```text
extract_orders     -> validate ---\
                                    -> join_and_aggregate
extract_customers  -> validate ---/
```

`extract_orders` and `extract_customers` have no dependency on each other,
so Airflow's scheduler runs them concurrently (subject to worker/pool
limits); `join_and_aggregate` waits for both `validate` branches to finish
before it starts — this is fan-out then fan-in.

## Retries and failure handling

```python
@task(retries=3, retry_delay=timedelta(minutes=2))
def flaky_api_extract() -> str:
    import random
    if random.random() < 0.3:
        raise ConnectionError("upstream API timed out")
    return "bronze/api_data.parquet"
```

```text
Attempt 1/4 failed: ConnectionError('upstream API timed out')
  waiting 2 minutes...
Attempt 2/4 succeeded
```

Per-task `retries` overrides the DAG-level default — a flaky external API
extract might warrant 3 retries with backoff, while a deterministic
in-lake transform that fails should probably retry 0 times and alert
immediately, since retrying it will just fail the same way.

## Sensors: waiting on an external condition

```python
from airflow.sensors.base import PokeReturnValue
from airflow.decorators import task

@task.sensor(poke_interval=60, timeout=3600, mode="reschedule")
def wait_for_upstream_file() -> PokeReturnValue:
    import os
    exists = os.path.exists("/data/incoming/orders_2026-08-30.csv")
    return PokeReturnValue(is_done=exists, xcom_value="/data/incoming/orders_2026-08-30.csv")
```

```text
poke at 03:00 -> file not found, reschedule
poke at 03:01 -> file not found, reschedule
poke at 03:04 -> file found -> task succeeds, downstream tasks unblocked
```

`mode="reschedule"` releases the worker slot between pokes instead of
blocking it for the full `timeout` — critical when you have many DAGs
waiting on files, or a single long-poll sensor would starve your worker
pool.

## Backfilling a date range

```text
airflow dags backfill orders_bronze_silver_gold \
  --start-date 2026-08-01 \
  --end-date 2026-08-07
```

```text
[2026-08-01] orders_bronze_silver_gold -> success
[2026-08-02] orders_bronze_silver_gold -> success
...
[2026-08-07] orders_bronze_silver_gold -> success
```

Backfilling re-runs the DAG once per logical date in the range, using
`{{ ds }}` (the logical/execution date) inside each task to know which
day's data to process — this only works cleanly if every task is
**idempotent** with respect to its logical date, which is exactly the topic
of Module 6.

## Traps

- **Putting heavy computation inside the DAG file itself** (top-level code,
  not inside a `@task`). Airflow re-parses every DAG file every few seconds
  to detect changes — expensive top-level code slows down the entire
  scheduler, not just this DAG.
- **Using `catchup=True` by accident.** If `start_date` is a year ago and
  catchup is on, Airflow will schedule a year of historical runs the moment
  the DAG is turned on.
- **Passing large data through XCom.** XCom is for small values (paths,
  IDs, counts) stored in the metadata database — passing an entire
  DataFrame through it will bloat the database and slow the scheduler.
- **No idempotency, then backfilling.** Re-running a non-idempotent task
  for a past date can double-count or duplicate data (see Module 6).

## Cheat sheet

| Concept | TaskFlow API |
|---|---|
| Define a task | `@task` decorator on a function |
| Wire dependency | call one task with another's return value, or `a >> b` |
| Retry a task | `@task(retries=N, retry_delay=timedelta(...))` |
| Wait on a condition | `@task.sensor(poke_interval=..., mode="reschedule")` |
| Backfill | `airflow dags backfill <dag_id> --start-date ... --end-date ...` |

## Exercise

Extend `orders_pipeline` with a `@task.branch` that checks a data quality
flag returned by `validate_and_load_silver` and routes to either
`aggregate_to_gold` (if clean) or a new `quarantine_and_alert` task (if
not). Sketch the DAG edges both branches would produce.
