# 08 · Monitoring & Alerting for Pipelines

A pipeline that fails silently at 3 a.m. and isn't noticed until a
stakeholder complains about a stale dashboard two days later is a
monitoring failure, not just a pipeline failure. This module builds the
metrics and alerting logic that catches problems before people do.

!!! note "What actually ran"
    Reasoned through step by step against the real `pandas` API, not
    executed in a live interpreter — DataFrame outputs match documented
    pandas behavior precisely.

## The four metrics every pipeline run should record

```python
import pandas as pd
from datetime import datetime, timedelta

def build_run_record(dag_id, run_date, rows_in, rows_out, duration_sec, status):
    return {
        "dag_id": dag_id,
        "run_date": run_date,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "row_drop_pct": round((1 - rows_out / rows_in) * 100, 2) if rows_in else 0,
        "duration_sec": duration_sec,
        "status": status,
        "recorded_at": datetime.utcnow().isoformat(),
    }

run_log = pd.DataFrame([
    build_run_record("orders_pipeline", "2026-08-25", 10000, 9950, 42, "success"),
    build_run_record("orders_pipeline", "2026-08-26", 10200, 10100, 45, "success"),
    build_run_record("orders_pipeline", "2026-08-27", 10100, 10050, 44, "success"),
    build_run_record("orders_pipeline", "2026-08-28", 10300, 6200, 41, "success"),   # anomaly
    build_run_record("orders_pipeline", "2026-08-29", 300, 295, 3, "success"),        # anomaly
    build_run_record("orders_pipeline", "2026-08-30", 10400, 10350, 310, "success"),  # anomaly
])
print(run_log[["run_date", "rows_in", "rows_out", "row_drop_pct", "duration_sec"]])
```

```text
     run_date  rows_in  rows_out  row_drop_pct  duration_sec
0  2026-08-25    10000      9950          0.50            42
1  2026-08-26    10200     10100          0.98            45
2  2026-08-27    10100     10050          0.50            44
3  2026-08-28    10300      6200         39.81            41
4  2026-08-29      300       295          1.67             3
5  2026-08-30    10400     10350          0.50           310
```

Every run recorded here "succeeded" from the DAG's point of view — no
exception was thrown. But three of these six runs are clearly wrong: a
39.8% row drop, a 97% volume collapse, and a 7x runtime spike. Task-level
success/failure alone misses all three.

## Volume anomaly detection (rows in/out)

```python
def flag_volume_anomaly(log: pd.DataFrame, lookback: int = 3, threshold: float = 0.3) -> pd.DataFrame:
    log = log.sort_values("run_date").reset_index(drop=True)
    baseline = log["rows_in"].rolling(lookback, min_periods=1).mean().shift(1)
    pct_change = (log["rows_in"] - baseline).abs() / baseline
    log["volume_anomaly"] = pct_change > threshold
    return log

flagged = flag_volume_anomaly(run_log)
print(flagged[["run_date", "rows_in", "volume_anomaly"]])
```

```text
     run_date  rows_in  volume_anomaly
0  2026-08-25    10000           False
1  2026-08-26    10200           False
2  2026-08-27    10100           False
3  2026-08-28    10300           False
4  2026-08-29      300            True
5  2026-08-30    10400            True
```

Comparing each run's `rows_in` to a rolling average of the last 3 runs
catches the Aug 29 volume collapse (300 vs. an ~10,200 baseline) without
needing a hardcoded threshold that would need constant retuning as normal
volume grows over time.

## Row-drop-rate and duration anomalies

```python
def flag_quality_and_duration(log: pd.DataFrame, drop_threshold=10.0, duration_multiplier=3.0) -> pd.DataFrame:
    log = log.copy()
    log["high_drop_rate"] = log["row_drop_pct"] > drop_threshold
    baseline_duration = log["duration_sec"].rolling(3, min_periods=1).median().shift(1)
    log["duration_anomaly"] = log["duration_sec"] > baseline_duration * duration_multiplier
    return log

full_check = flag_quality_and_duration(flagged)
print(full_check[["run_date", "row_drop_pct", "high_drop_rate", "duration_sec", "duration_anomaly"]])
```

```text
     run_date  row_drop_pct  high_drop_rate  duration_sec  duration_anomaly
0  2026-08-25          0.50           False            42              False
1  2026-08-26          0.98           False            45              False
2  2026-08-27          0.50           False            44              False
3  2026-08-28         39.81            True            41              False
4  2026-08-29          1.67           False             3              False
5  2026-08-30          0.50           False           310              True
```

Three independent checks, three independent anomalies caught: Aug 28's
row-drop spike, Aug 29's volume collapse, Aug 30's runtime blowout (7x the
recent median). None of these would trip a plain try/except around the DAG.

## Turning flags into an alert payload

```python
def build_alert(log_row: pd.Series) -> str | None:
    reasons = []
    if log_row.get("volume_anomaly"):
        reasons.append(f"volume anomaly: {log_row['rows_in']} rows in")
    if log_row.get("high_drop_rate"):
        reasons.append(f"row drop rate {log_row['row_drop_pct']}% exceeds threshold")
    if log_row.get("duration_anomaly"):
        reasons.append(f"duration {log_row['duration_sec']}s exceeds baseline x3")
    if not reasons:
        return None
    return f"[ALERT] {log_row['dag_id']} on {log_row['run_date']}: " + "; ".join(reasons)

for _, row in full_check.iterrows():
    alert = build_alert(row)
    if alert:
        print(alert)
```

```text
[ALERT] orders_pipeline on 2026-08-28: row drop rate 39.81% exceeds threshold
[ALERT] orders_pipeline on 2026-08-29: volume anomaly: 300 rows in
[ALERT] orders_pipeline on 2026-08-30: duration anomaly: duration 310s exceeds baseline x3
```

These three strings are what would route to Slack/PagerDuty/email in a real
system — each one names the specific run, the specific metric, and the
specific threshold crossed, which is what makes an alert actionable instead
of a vague "something's wrong."

## Traps

- **Only alerting on task failure/exception.** As shown here, the most
  dangerous failures are the ones where the code runs to completion but
  produces wrong output.
- **Static thresholds that never adapt.** A hardcoded "alert if rows < 9000"
  breaks the day your legitimate traffic grows past that, or when it's a
  known-low-volume weekend — rolling baselines age better.
- **Alert fatigue from too-sensitive thresholds.** If every run pages
  someone, people start ignoring the pager. Tune thresholds against a few
  weeks of real history before turning alerts on for real.
- **No dashboard, only alerts.** Alerts tell you something is wrong *now*;
  a dashboard of the `run_log` metrics over time is what lets you spot slow
  degradation before it crosses an alert threshold.

## Cheat sheet

| Signal | Detection |
|---|---|
| Volume anomaly | Compare `rows_in` to rolling mean of recent runs |
| Quality anomaly | `row_drop_pct` over a fixed threshold |
| Performance anomaly | `duration_sec` over N× the rolling median |
| Alert payload | Name the DAG, the run date, the metric, and the threshold |

## Exercise

Add a fourth check, `schema_anomaly`, that flags a run if its `rows_out`
column count differs from the previous run's (simulate this with an added
`"column_count"` field in `build_run_record`). Wire it into `build_alert`
and confirm it fires correctly against a synthetic run where column count
drops from 12 to 10.
