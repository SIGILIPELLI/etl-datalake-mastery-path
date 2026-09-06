# 09 · Why You Need a Scheduler

You could run your ETL script by hand every morning. For a while, that
works. This lesson explains exactly where that breaks down, and introduces
the core concepts — scheduling, dependencies, and retries — that every
orchestration tool (Airflow, in Level 2) exists to solve.

!!! note "What actually ran"
    This lesson uses only the Python standard library (`time`, `datetime`,
    `pathlib`) to demonstrate the concepts concretely, without depending on
    Airflow or any other orchestration framework — those are introduced in
    Level 2 once the underlying problem is clear.

## The manual approach, and where it breaks

```python
def extract(): print("Extracting orders..."); return [{"id": 1, "amount": 50}]
def transform(rows): print("Transforming..."); return [{**r, "amount": r["amount"] * 1.0} for r in rows]
def load(rows): print("Loading..."); return len(rows)

# Running it by hand, once
raw = extract()
clean = transform(raw)
count = load(clean)
print(f"Loaded {count} rows")
```

```text
Extracting orders...
Transforming...
Loading...
Loaded 1 rows
```

This is fine for a single run you trigger yourself. It stops being fine the
moment any of these become true — and in a real pipeline, they all
eventually do:

- The pipeline needs to run **every day at 2am**, whether or not you're
  awake.
- A downstream pipeline (say, a report) must only start **after** this one
  finishes successfully — a dependency.
- The `extract()` step occasionally fails because a source API had a
  transient outage, and needs to **retry** rather than fail the whole run.
- You need to know, at a glance, **which of last night's 40 pipelines
  failed**, without SSHing in and reading logs one by one.

None of these are solved by a script and a person remembering to run it.
That's the actual job of a scheduler/orchestrator.

## Concept 1: scheduling (triggering runs automatically)

```python
import datetime

def should_run_now(last_run: datetime.datetime, interval_hours: int, now: datetime.datetime) -> bool:
    return (now - last_run) >= datetime.timedelta(hours=interval_hours)

last_run = datetime.datetime(2026, 8, 28, 2, 0)
now = datetime.datetime(2026, 8, 29, 2, 5)

if should_run_now(last_run, interval_hours=24, now=now):
    print("24 hours have passed — triggering the pipeline run")
else:
    print("Not time yet")
```

```text
24 hours have passed — triggering the pipeline run
```

This is the simplest possible model of what a scheduler does continuously,
in the background, without a human involved: it tracks time (or an event)
and decides when to kick off a run. Real schedulers (cron, Airflow) express
this as a schedule expression (`0 2 * * *` = every day at 2am) rather than
hand-written interval math, but the underlying decision is exactly this.

## Concept 2: dependencies (DAGs)

Real pipelines are rarely one function — they're a chain, where each step
depends on the previous one succeeding. This dependency chain, drawn out, is
a **Directed Acyclic Graph (DAG)**: directed because dependencies point one
way, acyclic because a pipeline can't depend on its own future output.

```python
def run_pipeline_step(name, func, *args):
    print(f"-> Running: {name}")
    try:
        result = func(*args)
        print(f"   OK: {name}")
        return result, True
    except Exception as e:
        print(f"   FAILED: {name} ({e})")
        return None, False

def run_dag(steps):
    """steps: list of (name, func, args) in dependency order."""
    prev_result = None
    for name, func, args in steps:
        actual_args = args if args else (prev_result,) if prev_result is not None else ()
        prev_result, ok = run_pipeline_step(name, func, *actual_args)
        if not ok:
            print(f"Stopping DAG: downstream steps depend on '{name}' and won't run")
            return False
    return True

dag = [
    ("extract", extract, ()),
    ("transform", transform, None),   # None means "use previous step's output"
    ("load", load, None),
]
run_dag(dag)
```

```text
-> Running: extract
   OK: extract
-> Running: transform
   OK: transform
-> Running: load
   OK: load
```

The critical behavior: if `transform` fails, `load` **never runs** —
because it depends on transform's output. A scheduler's DAG engine encodes
exactly this: "run B only after A succeeds," at arbitrary depth and fan-out
(one extract feeding three parallel transforms, for example), which becomes
unmanageable to hand-code correctly once you have more than a few steps.

## Concept 3: retries

```python
import random

def flaky_extract(attempt_tracker={"count": 0}):
    attempt_tracker["count"] += 1
    if attempt_tracker["count"] < 3:
        raise ConnectionError("API timed out")
    return [{"id": 1, "amount": 50}]

def run_with_retries(func, max_attempts=3):
    for attempt in range(1, max_attempts + 1):
        try:
            result = func()
            print(f"Succeeded on attempt {attempt}")
            return result
        except Exception as e:
            print(f"Attempt {attempt} failed: {e}")
            if attempt == max_attempts:
                raise
    return None

run_with_retries(flaky_extract, max_attempts=3)
```

```text
Attempt 1 failed: API timed out
Attempt 2 failed: API timed out
Succeeded on attempt 3
```

A transient failure — a source API hiccup, a brief network blip — shouldn't
mean the whole pipeline (and everything depending on it) fails for the day.
Automatic retry-with-backoff is one of the most valuable, least glamorous
things a scheduler does for you, and it's exactly the kind of thing that's
tedious and error-prone to hand-roll correctly across dozens of pipelines.

## Why plain cron isn't usually enough

Cron solves scheduling (concept 1) but nothing else — no dependency
tracking between jobs, no retry logic, no visibility into what failed and
why, no easy way to backfill a missed run. That gap is exactly what
dedicated orchestrators like Airflow fill, which is why Level 2 introduces
Airflow DAGs as soon as pipelines have more than one dependent step.

## Traps

- **Relying on a human to trigger runs.** Works until someone is sick,
  travels, or simply forgets — pipelines that matter need automated
  scheduling, not tribal memory.
- **No retry logic for transient failures.** A pipeline that hard-fails on
  the first network blip will page someone unnecessarily at 2am for a
  problem that would have resolved itself on a second attempt.
- **Steps that don't declare their real dependencies.** If `load` doesn't
  actually need `transform`'s output but is written as if it does, you lose
  the ability to run independent steps in parallel — costing time for no
  reason.
- **Retrying without limits or backoff.** Retrying forever, or immediately
  with no delay, can turn a transient source outage into a self-inflicted
  denial-of-service against your own source system.

## Cheat sheet

| Concept | What it solves | Plain cron? | Airflow (Level 2)? |
|---|---|---|---|
| Scheduling | "Run this automatically at time X" | Yes | Yes |
| Dependencies (DAGs) | "Run B only after A succeeds" | No | Yes |
| Retries | "Recover from transient failures automatically" | No (needs custom scripting) | Yes |
| Visibility/alerting | "Tell me what failed, without me checking logs" | No | Yes |

## How It Actually Works

An orchestrator's core mechanism is maintaining a **directed acyclic graph (DAG)** of tasks
in a metadata database, and repeatedly asking a scheduler process "which tasks are now
eligible to run?" rather than executing steps in a fixed sequence itself.

Each task node records its upstream dependencies; the scheduler polls the metadata DB on an
interval (Airflow's scheduler loop, for instance, runs every few seconds) and marks a task
"eligible" only once every upstream task's state is `success` in that same database. This is
why orchestrators are resilient to their own restarts — task state lives in a database, not
in scheduler process memory, so killing and restarting the scheduler just means it re-reads
the same state and resumes deciding what's eligible.

Actual task execution is handed off to a separate **executor** — a subprocess, a Celery
worker pulling from a queue, or a Kubernetes pod spun up per task — which is a deliberate
separation: the scheduler's job is graph bookkeeping (cheap, must stay responsive), while
executors do the heavy lifting (a Spark job, a SQL query) and can be scaled independently or
run on entirely different machines. When a worker reports back, it writes task state and
`try_number` back to the metadata DB, and retries are implemented as literally re-invoking
the same task function with an incremented try counter — which is why retried tasks must be
idempotent: the orchestrator has no way to know if a "failed" task partially wrote data
before crashing, it only reruns the task's code from the top.

Scheduling on a calendar interval (a cron-like `schedule_interval`) works by the scheduler
computing the next "data interval" boundary from the DAG's start date and cron expression,
then creating a new DAG run row for that interval once its end time has passed — this is why
a backfill for a past date range is mechanically identical to normal scheduling, just with
DAG run rows created for past intervals instead of waiting for wall-clock time to reach them.

## Exercise

Extend `run_dag` above to support **parallel independent branches** — modify
it so that `steps` can express "step C depends on both A and B" (rather than
a strict linear chain), and only run C once both A and B have succeeded. If
either A or B fails, C should be skipped and the DAG should report exactly
which step caused the stoppage.
