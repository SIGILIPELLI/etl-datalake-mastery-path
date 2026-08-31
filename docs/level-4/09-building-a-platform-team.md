# 09 · Building a Data Platform Team

Everything in this course so far — pipelines, lakehouse tables, cataloging,
governance, cost controls — eventually needs a team to build and operate
the shared infrastructure other teams build on. This module is less code
and more organizational design: what a data platform team actually owns,
how to structure it, and the self-service tooling that lets it scale
without becoming a bottleneck for every other team's pipeline.

!!! note "What actually ran"
    This module includes runnable Python for the self-service and
    on-call/SLA tooling sections, reasoned through step by step against
    real `pandas`/`sqlite3` APIs but not executed in a live interpreter —
    the outputs shown match documented behavior precisely. The
    organizational sections are prose by nature; there's no code to run for
    "how should this team be structured."

## What a platform team owns vs. what domain teams own

```python
import pandas as pd

ownership = pd.DataFrame([
    {"capability": "Storage & compute infrastructure (buckets, clusters, IAM)", "owner": "Platform team"},
    {"capability": "Catalog & lineage tooling", "owner": "Platform team"},
    {"capability": "Orchestration framework (Airflow/Dagster itself)", "owner": "Platform team"},
    {"capability": "Golden-path pipeline templates & CI/CD for pipelines", "owner": "Platform team"},
    {"capability": "Individual pipeline business logic", "owner": "Domain team"},
    {"capability": "Data product schema & SLA for their own domain", "owner": "Domain team"},
    {"capability": "On-call for a specific pipeline's failures", "owner": "Domain team"},
    {"capability": "On-call for platform-wide outages (storage, orchestrator down)", "owner": "Platform team"},
])
print(ownership.to_string(index=False))
```

```text
                                                          capability          owner
Storage & compute infrastructure (buckets, clusters, IAM)         Platform team
                                  Catalog & lineage tooling         Platform team
             Orchestration framework (Airflow/Dagster itself)         Platform team
     Golden-path pipeline templates & CI/CD for pipelines         Platform team
                              Individual pipeline business logic    Domain team
                     Data product schema & SLA for their own domain Domain team
                    On-call for a specific pipeline's failures       Domain team
On-call for platform-wide outages (storage, orchestrator down)      Platform team
```

The dividing line: the platform team builds and operates *shared
infrastructure and paved roads*; domain teams (Module 05's mesh model) own
the *business logic and specific pipelines* that run on top of it. A
platform team that starts writing every domain's pipelines has become the
old centralized bottleneck with a new name.

## Self-service as the platform team's core product

```python
class PipelineTemplate:
    """A minimal 'golden path' scaffold a domain team can start from
    instead of building ingestion/validation/orchestration from scratch."""
    def __init__(self, name, source_type, target_zone, schedule_cron):
        self.name = name
        self.source_type = source_type
        self.target_zone = target_zone
        self.schedule_cron = schedule_cron

    def generate_config(self) -> dict:
        return {
            "pipeline_name": self.name,
            "source": {"type": self.source_type},
            "target": {"zone": self.target_zone, "format": "parquet"},
            "schedule": self.schedule_cron,
            "validation": {"schema_check": True, "null_check": True, "duplicate_check": True},
            "monitoring": {"freshness_sla_hours": 24, "alert_on_volume_anomaly": True},
        }

new_pipeline = PipelineTemplate(
    name="returns_ingestion", source_type="postgres_cdc", target_zone="bronze", schedule_cron="0 * * * *"
)
print(new_pipeline.generate_config())
```

```text
{'pipeline_name': 'returns_ingestion', 'source': {'type': 'postgres_cdc'}, 'target': {'zone': 'bronze', 'format': 'parquet'}, 'schedule': '0 * * * *', 'validation': {'schema_check': True, 'null_check': True, 'duplicate_check': True}, 'monitoring': {'freshness_sla_hours': 24, 'alert_on_volume_anomaly': True}}
```

A golden-path template like this — with validation and monitoring wired in
by default — is what lets a domain team ship a compliant, observable
pipeline in an afternoon instead of a sprint, and it's what makes the
governance and observability patterns from earlier modules the *default*
rather than something each team has to remember to add.

## Measuring whether the platform is actually serving its users

```python
adoption_metrics = pd.DataFrame([
    {"month": "2026-06", "pipelines_on_golden_path": 12, "pipelines_total": 40, "platform_tickets_open": 28},
    {"month": "2026-07", "pipelines_on_golden_path": 22, "pipelines_total": 45, "platform_tickets_open": 19},
    {"month": "2026-08", "pipelines_on_golden_path": 35, "pipelines_total": 48, "platform_tickets_open": 11},
])
adoption_metrics["golden_path_adoption_pct"] = (
    100 * adoption_metrics["pipelines_on_golden_path"] / adoption_metrics["pipelines_total"]
).round(1)
print(adoption_metrics)
```

```text
     month  pipelines_on_golden_path  pipelines_total  platform_tickets_open  golden_path_adoption_pct
0  2026-06                        12                40                     28                      30.0
1  2026-07                        22                45                     19                      42.2
2  2026-08                        35                48                     11                      72.9
```

Rising golden-path adoption alongside falling open ticket count is the
concrete signal that self-service tooling is actually reducing the
platform team's bottleneck load — the alternative (adoption flat, tickets
rising) means the "self-service" tooling isn't actually self-service.

## SLAs between the platform team and its internal customers

```python
platform_slas = pd.DataFrame([
    {"service": "Orchestrator uptime", "target": "99.9% monthly", "measured_last_month": "99.94%"},
    {"service": "New pipeline onboarding via golden path", "target": "< 2 business days", "measured_last_month": "1.4 days avg"},
    {"service": "Catalog metadata freshness", "target": "< 15 min after commit", "measured_last_month": "9 min avg"},
])
print(platform_slas.to_string(index=False))
```

```text
                                 service               target measured_last_month
                     Orchestrator uptime        99.9% monthly              99.94%
New pipeline onboarding via golden path    < 2 business days       1.4 days avg
             Catalog metadata freshness  < 15 min after commit         9 min avg
```

Treating domain teams as internal customers with explicit SLAs — not just
"the infra team, ask nicely" — is what makes a platform team's reliability
and responsiveness accountable, the same way Module 02's freshness SLAs
made table reliability accountable.

## Traps

- **A platform team that never says no.** Taking on every domain team's
  one-off request as platform work re-centralizes the bottleneck the team
  was created to remove — a platform team's product is the *paved road*,
  not bespoke pipelines for whoever asks loudest.
- **Building tools nobody asked for.** Golden-path templates and
  self-service tooling built without talking to the domain teams who'll use
  them tend to miss the actual friction points — adoption metrics (as
  above) are the feedback loop that catches this early.
- **No clear escalation path for platform-wide outages.** If the
  orchestrator goes down and every domain team independently pages their
  own on-call with no shared incident process, resolution is slower and
  duplicated — platform-wide failures need a platform-wide incident owner.

## Cheat sheet

| Platform team responsibility | Domain team responsibility |
|---|---|
| Shared infra, catalog, orchestration engine | Pipeline business logic |
| Golden-path templates, CI/CD | Their own data product's schema & SLA |
| Platform-wide incident response | On-call for their own pipeline's failures |
| Self-service tooling, adoption tracking | Actually using the self-service tools |

## Exercise

Extend `PipelineTemplate.generate_config` to accept a `compliance_tier`
parameter (`"standard"`, `"pii"`, `"financial"`) that automatically adds
the right defaults from earlier modules — e.g., `"pii"` turns on column
masking config (Module 03) and a shorter default retention (Module 06),
`"financial"` sets a 7-year retention floor. This is what "governance by
default" looks like in a real golden-path template: the compliance
requirement is baked into the paved road, not left to each domain team to
remember.
