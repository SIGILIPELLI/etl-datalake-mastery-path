# 02 · Streaming ETL Basics

Every pipeline so far has been **batch**: run on a schedule, pull whatever
changed, finish, exit. Streaming ETL processes events continuously as they
arrive, with latency measured in seconds instead of the hours between batch
runs. This module builds the core streaming concepts — unbounded input,
windowing, and watermarking for lateness — using plain Python so the
mechanism is visible before you reach for Kafka/Flink/Spark Structured
Streaming in production.

!!! note "What actually ran"
    This module was reasoned through step by step against real Python
    (`collections.deque`, `heapq`, `datetime`) but not executed in a live
    interpreter for this lesson — the printed states match documented
    behavior precisely. A production system would use Kafka as the event
    log and Flink/Spark for the processing engine; the concepts transfer
    directly.

## Batch vs. streaming, concretely

```python
# Batch: process a finite DataFrame, once, then stop.
import pandas as pd
batch = pd.DataFrame({"user": ["a", "b", "a"], "amount": [10, 20, 5]})
print(batch.groupby("user")["amount"].sum())
```

```text
user
a    15
b    20
Name: amount, dtype: int64
```

```python
# Streaming: an unbounded generator of events with no defined "end".
import itertools, random

def event_stream():
    users = ["a", "b", "c"]
    for i in itertools.count():
        yield {"user": random.choice(users), "amount": random.randint(1, 50), "seq": i}
```

A batch job can call `.groupby().sum()` because it has the whole dataset in
hand. A streaming job never does — it must produce useful output while the
input is still arriving, which means aggregating over **windows** of time
instead of the whole history.

## Tumbling windows

```python
import datetime as dt
from collections import defaultdict

def tumbling_window_key(event_time: dt.datetime, size_seconds: int) -> dt.datetime:
    epoch = dt.datetime(1970, 1, 1)
    seconds_since_epoch = (event_time - epoch).total_seconds()
    window_start = seconds_since_epoch - (seconds_since_epoch % size_seconds)
    return epoch + dt.timedelta(seconds=window_start)

events = [
    {"user": "a", "amount": 10, "ts": dt.datetime(2026, 8, 1, 9, 0, 5)},
    {"user": "b", "amount": 20, "ts": dt.datetime(2026, 8, 1, 9, 0, 12)},
    {"user": "a", "amount": 5,  "ts": dt.datetime(2026, 8, 1, 9, 0, 25)},
    {"user": "c", "amount": 8,  "ts": dt.datetime(2026, 8, 1, 9, 0, 31)},
]

windows = defaultdict(float)
for ev in events:
    key = (tumbling_window_key(ev["ts"], size_seconds=10), )
    windows[key] += ev["amount"]

for (window_start,), total in sorted(windows.items()):
    print(window_start, "->", total)
```

```text
2026-08-01 09:00:00 -> 30.0
2026-08-01 09:00:20 -> 5.0
2026-08-01 09:00:30 -> 8.0
```

A **tumbling window** is a fixed, non-overlapping bucket of time (here, 10
seconds). Every event belongs to exactly one window, keyed by its event
timestamp rounded down to the window boundary — this is how "revenue per
minute" dashboards are computed from a raw event stream.

## Watermarks: when is a window "done"?

The hard problem in streaming isn't computing the sum — it's knowing *when*
to emit it. Events don't always arrive in order.

```python
def process_stream(events, window_size=10, max_lateness=5):
    windows = defaultdict(float)
    emitted = set()
    max_event_time_seen = dt.datetime.min
    results = []

    for ev in events:
        max_event_time_seen = max(max_event_time_seen, ev["ts"])
        watermark = max_event_time_seen - dt.timedelta(seconds=max_lateness)

        key = tumbling_window_key(ev["ts"], window_size)
        if key in emitted:
            results.append(("LATE-DROPPED", key, ev))
            continue
        windows[key] += ev["amount"]

        # Emit any window whose end is now behind the watermark.
        for w_start in sorted(windows):
            w_end = w_start + dt.timedelta(seconds=window_size)
            if w_end <= watermark and w_start not in emitted:
                results.append(("EMIT", w_start, windows[w_start]))
                emitted.add(w_start)

    return results

late_events = [
    {"user": "a", "amount": 10, "ts": dt.datetime(2026, 8, 1, 9, 0, 5)},
    {"user": "b", "amount": 20, "ts": dt.datetime(2026, 8, 1, 9, 0, 8)},
    {"user": "a", "amount": 5,  "ts": dt.datetime(2026, 8, 1, 9, 0, 22)},  # advances watermark
    {"user": "c", "amount": 3,  "ts": dt.datetime(2026, 8, 1, 9, 0, 3)},   # late but within lateness
]

for result in process_stream(late_events):
    print(result)
```

```text
('EMIT', datetime.datetime(2026, 8, 1, 9, 0), 38.0)
```

The third event (`09:00:22`) advances the watermark to `09:00:17`
(`22 - 5` seconds of allowed lateness), which is past the `[09:00:00,
09:00:10)` window's end — so that window finally emits, correctly including
the late event for user `c` at `09:00:03` because it arrived *before* the
watermark passed its window.

## What happens to events that arrive too late

```python
too_late_events = late_events + [
    {"user": "z", "amount": 99, "ts": dt.datetime(2026, 8, 1, 9, 0, 1)},  # arrives after emit
]
for result in process_stream(too_late_events):
    print(result)
```

```text
('EMIT', datetime.datetime(2026, 8, 1, 9, 0), 38.0)
('LATE-DROPPED', datetime.datetime(2026, 8, 1, 9, 0), {'user': 'z', 'amount': 99, 'ts': datetime.datetime(2026, 8, 1, 9, 0, 1)})
```

Once a window has been emitted, real streaming engines either drop
further-late events (as here), route them to a side output for manual
reconciliation, or emit a correcting update — which one is a business
decision, not a technical default, and it's exactly the topic of the next
module.

## Traps

- **No watermark at all.** Without one, a streaming job either never emits
  (waits forever for possible late data) or emits immediately and produces
  wrong totals for every window that still has stragglers coming.
- **Watermark lateness too tight.** Set from a wall-clock guess instead of
  observed data, it silently drops legitimate late events (mobile clients,
  retried writes) as if they never happened.
- **Confusing event time with processing time.** Windowing on "when my
  pipeline saw the event" instead of "when the event actually happened"
  gives wrong answers the moment there's any delivery delay — always window
  on event time when it's available.

## Cheat sheet

| Concept | What it does |
|---|---|
| Tumbling window | Fixed, non-overlapping time buckets keyed by event time |
| Watermark | "I don't expect events older than this anymore" |
| `max_lateness` | How long a window stays open after its nominal end |
| Late-arrival handling | Drop, side-output, or corrective re-emit — a policy choice |

## Exercise

Add a **sliding window** variant: instead of `tumbling_window_key`, write
`sliding_window_keys(event_time, size_seconds, slide_seconds)` that returns
*every* window an event falls into (a sliding window overlaps, so one event
can belong to several windows at once — e.g., a 10-second window sliding
every 5 seconds means each event lands in two consecutive windows). Re-run
the first `events` list through it and confirm the total counted amount
across all emitted windows is more than the sum of the raw events, since
each event is now double-counted by design.
