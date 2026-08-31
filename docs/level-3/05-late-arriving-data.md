# 05 · Handling Late-Arriving & Out-of-Order Data

Real event sources don't deliver in perfect order: mobile clients buffer
offline, retries reorder network packets, and clocks drift between servers.
"Late-arriving data" is any record whose *event time* is well before other
records you've already processed, but whose *arrival time* is after. This
module covers the concrete patterns for handling it in both batch and
streaming pipelines, building on the watermarking introduced in Module 02.

!!! note "What actually ran"
    This module was reasoned through step by step against real `pandas` and
    `datetime` APIs but not executed in a live interpreter for this lesson —
    the DataFrame shapes and printed values shown match documented behavior
    precisely.

## Event time vs. arrival time

```python
import pandas as pd

events = pd.DataFrame([
    {"event_id": 1, "event_time": "2026-08-01T09:00:00", "arrival_time": "2026-08-01T09:00:02", "amount": 10},
    {"event_id": 2, "event_time": "2026-08-01T09:00:05", "arrival_time": "2026-08-01T09:00:06", "amount": 20},
    {"event_id": 3, "event_time": "2026-08-01T08:59:50", "arrival_time": "2026-08-01T09:05:00", "amount": 15},  # late!
])
events["event_time"] = pd.to_datetime(events["event_time"])
events["arrival_time"] = pd.to_datetime(events["arrival_time"])
events["lateness_seconds"] = (events["arrival_time"] - events["event_time"]).dt.total_seconds()
print(events)
```

```text
   event_id          event_time        arrival_time  amount  lateness_seconds
0         1 2026-08-01 09:00:00 2026-08-01 09:00:02      10               2.0
1         2 2026-08-01 09:00:05 2026-08-01 09:00:06      20               6.0
2         3 2026-08-01 08:59:50 2026-08-01 09:05:00      15             310.0
```

Event 3 happened *before* events 1 and 2 (event time), but arrived last —
five minutes after the fact. Any pipeline that already closed out the
`09:00:00` batch/window before event 3 shows up needs a deliberate policy
for what happens next.

## Batch pattern: reprocess a bounded lookback window

```python
def batch_pull(events: pd.DataFrame, batch_start: str, batch_end: str, lookback_minutes: int = 10):
    """Pull rows whose event_time falls in [batch_start, batch_end), but re-scan
    a lookback window before batch_start too, to catch stragglers."""
    extended_start = pd.Timestamp(batch_start) - pd.Timedelta(minutes=lookback_minutes)
    window = events[
        (events["event_time"] >= extended_start) & (events["event_time"] < batch_end)
    ]
    return window

reprocessed = batch_pull(events, "2026-08-01T09:00:00", "2026-08-01T09:01:00")
print(reprocessed)
```

```text
   event_id          event_time        arrival_time  amount  lateness_seconds
0         1 2026-08-01 09:00:00 2026-08-01 09:00:02      10               2.0
1         2 2026-08-01 09:00:05 2026-08-01 09:00:06      20               6.0
2         3 2026-08-01 08:59:50 2026-08-01 09:05:00      15             310.0
```

This is the standard batch fix: re-run each batch's aggregation over a
lookback window wider than the batch itself, so a straggler that lands after
the original run still gets picked up on the *next* scheduled run — at the
cost of recomputing overlapping ranges, which is why the aggregation must be
idempotent (Level 2, Module 06).

## Correcting an already-emitted aggregate

```python
published = pd.DataFrame([
    {"window_start": "2026-08-01T09:00:00", "total": 30},   # only events 1+2 when first emitted
])
published["window_start"] = pd.to_datetime(published["window_start"])

def recompute_and_correct(published: pd.DataFrame, all_events: pd.DataFrame, window_start, window_seconds=60):
    window_end = window_start + pd.Timedelta(seconds=window_seconds)
    in_window = all_events[
        (all_events["event_time"] >= window_start) & (all_events["event_time"] < window_end)
    ]
    corrected_total = in_window["amount"].sum()

    result = published.copy()
    mask = result["window_start"] == window_start
    if mask.any():
        result.loc[mask, "total"] = corrected_total
    else:
        result = pd.concat([result, pd.DataFrame([{"window_start": window_start, "total": corrected_total}])])
    return result

corrected = recompute_and_correct(published, events, pd.Timestamp("2026-08-01T09:00:00"))
print(corrected)
```

```text
       window_start  total
0 2026-08-01 09:00:00     45
```

Instead of appending event 3's amount as a new row (which would double-count
events 1 and 2), the whole window is **recomputed from source** and the
published row is overwritten in place — this is a corrective update, and it
requires downstream consumers to treat published aggregates as mutable, not
append-only.

## Alternative: quarantine and reconcile separately

```python
def split_on_time(events: pd.DataFrame, watermark: pd.Timestamp):
    on_time = events[events["event_time"] >= watermark]
    late = events[events["event_time"] < watermark]
    return on_time, late

watermark = pd.Timestamp("2026-08-01T09:00:00")
on_time, late = split_on_time(events, watermark)
print("On time:\n", on_time[["event_id", "event_time"]])
print("\nLate (quarantined for a separate reconciliation job):\n", late[["event_id", "event_time"]])
```

```text
On time:
    event_id          event_time
0         1 2026-08-01 09:00:00
1         2 2026-08-01 09:00:05

Late (quarantined for a separate reconciliation job):
    event_id          event_time
2         3 2026-08-01 08:59:50
```

When corrective updates are too disruptive for downstream consumers (e.g., a
dashboard that can't handle numbers changing after the fact), the
alternative is to route late events to a separate table and publish a daily
or hourly "reconciliation" job that adjusts totals on its own schedule —
trading immediacy for a simpler consumer contract.

## Traps

- **No lookback window in a batch job.** Filtering `event_time` strictly to
  `[batch_start, batch_end)` with no overlap silently drops every straggler
  forever — they were never in a window that got re-run.
- **Appending instead of recomputing on correction.** Adding a late event's
  value to a published total (`total += late_amount`) works only if you're
  certain the total was never itself the product of a previous correction —
  recomputing from source events is far safer.
- **No cutoff at all.** Without *some* limit (a watermark, a lookback
  window, an "accept lateness up to N hours" rule), a pipeline must keep
  every historical aggregate open forever to theoretically handle
  arbitrarily late data — pick a business-appropriate cutoff and quarantine
  or drop past it.

## Cheat sheet

| Pattern | When to use |
|---|---|
| Lookback window on batch re-run | Simple, works with existing idempotent batch jobs |
| Corrective recompute-and-overwrite | Streaming/near-real-time, consumers can handle updated values |
| Quarantine + separate reconciliation | Consumers need append-only/immutable published data |
| Hard cutoff (drop past N) | Bounds cost/complexity when perfect correctness isn't required |

## Exercise

Extend `recompute_and_correct` to also record a `correction_count` column
that increments every time a given `window_start` is recomputed with a
different total than before, and a `last_corrected_at` timestamp. Then
simulate three late events arriving for the same window across three
separate reconciliation runs and confirm `correction_count` reaches 3 while
`total` reflects the sum of all events seen so far.
