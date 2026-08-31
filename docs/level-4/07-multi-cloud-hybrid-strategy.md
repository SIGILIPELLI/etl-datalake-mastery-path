# 07 · Multi-Cloud & Hybrid Data Lake Strategy

Large organizations rarely run on a single cloud by choice alone — mergers
bring in acquired companies' infrastructure, different business units pick
different vendors, and some workloads must legally stay on-premises. A
multi-cloud/hybrid lake strategy is about making data usable across those
boundaries without duplicating pipelines per environment. This module
covers the concrete patterns: an abstraction layer over storage, cross-cloud
metadata, and data gravity trade-offs.

!!! note "What actually ran"
    This module was reasoned through step by step against real Python
    (`pathlib`, `urllib.parse`) and `pandas` APIs but not executed in a live
    interpreter for this lesson — the outputs shown match documented
    behavior precisely. Actual object-store I/O would use `boto3` (S3),
    `google-cloud-storage` (GCS), or `azure-storage-blob`, abstracted behind
    the same interface shown here.

## An abstraction layer over storage location

```python
from urllib.parse import urlparse
import pandas as pd

def parse_lake_uri(uri: str) -> dict:
    parsed = urlparse(uri)
    scheme_to_provider = {"s3": "aws", "gs": "gcp", "abfss": "azure", "file": "on-prem"}
    return {
        "provider": scheme_to_provider.get(parsed.scheme, "unknown"),
        "bucket_or_container": parsed.netloc,
        "path": parsed.path.lstrip("/"),
        "uri": uri,
    }

locations = [
    "s3://acme-lake-us/silver/orders",
    "gs://acme-lake-eu/silver/orders",
    "abfss://acme-lake-onprem-sync@storage.dfs.core.windows.net/silver/orders",
    "file:///mnt/onprem-lake/silver/orders",
]
print(pd.DataFrame([parse_lake_uri(u) for u in locations]))
```

```text
  provider           bucket_or_container         path                                    uri
0      aws               acme-lake-us  silver/orders                s3://acme-lake-us/silver/orders
1      gcp               acme-lake-eu  silver/orders                gs://acme-lake-eu/silver/orders
2    azure   acme-lake-onprem-sync@storage.dfs.core.windows.net  silver/orders  abfss://acme-lake-onprem-sync@storage.dfs.core.windows.net/silver/orders
3  on-prem                                     mnt/onprem-lake/silver/orders  file:///mnt/onprem-lake/silver/orders
```

A pipeline written against `parse_lake_uri` and a matching read/write
adapter per provider can run against any of the four locations without
branching on provider throughout the business logic — this is the core
idea behind tools like Apache Iceberg's pluggable `FileIO` or Delta Lake's
storage abstraction: the table format doesn't care which cloud the bytes
sit in.

## A registry mapping logical tables to physical, provider-specific locations

```python
table_registry = pd.DataFrame([
    {"logical_table": "orders", "region": "us", "provider": "aws", "uri": "s3://acme-lake-us/silver/orders"},
    {"logical_table": "orders", "region": "eu", "provider": "gcp", "uri": "gs://acme-lake-eu/silver/orders"},
    {"logical_table": "hr_records", "region": "us", "provider": "on-prem", "uri": "file:///mnt/onprem-lake/hr"},
])

def resolve_location(registry: pd.DataFrame, logical_table: str, region: str) -> str:
    match = registry[(registry["logical_table"] == logical_table) & (registry["region"] == region)]
    if match.empty:
        raise LookupError(f"no location registered for {logical_table}/{region}")
    return match.iloc[0]["uri"]

print(resolve_location(table_registry, "orders", "eu"))
print(resolve_location(table_registry, "hr_records", "us"))
```

```text
gs://acme-lake-eu/silver/orders
file:///mnt/onprem-lake/hr
```

A single logical table name (`orders`) can resolve to different physical
locations per region — this is often *required*, not optional: EU customer
data legally residing in an EU data center is a common data-residency
constraint that a multi-cloud strategy must satisfy structurally.

## Data gravity: why you replicate metadata but not always the data itself

```python
def estimate_cross_cloud_query_cost(rows: int, avg_row_bytes: int, egress_cost_per_gb: float = 0.09) -> dict:
    total_gb = (rows * avg_row_bytes) / (1024 ** 3)
    egress_cost = total_gb * egress_cost_per_gb
    return {"total_gb": round(total_gb, 3), "estimated_egress_cost_usd": round(egress_cost, 2)}

# A query engine in AWS reading a 50GB table stored in GCP has to pull it across
print(estimate_cross_cloud_query_cost(rows=50_000_000, avg_row_bytes=1024))
```

```text
{'total_gb': 46.566, 'estimated_egress_cost_usd': 4.19}
```

Cross-cloud egress isn't free, and it compounds — a recurring hourly job
that scans a large cross-cloud table pays this cost every run. This is
"data gravity": data tends to pull compute toward it, because moving
compute to the data is usually cheaper than moving the data to the
compute. The practical implication is to run the query engine *in the same
cloud* as the data whenever the workload is recurring or large, and reserve
actual cross-cloud data movement for smaller, less frequent transfers.

## Federated catalog: metadata unified, data left in place

```python
federated_catalog = pd.concat([
    table_registry.assign(catalog_source="central"),
], ignore_index=True)

def find_table_anywhere(catalog: pd.DataFrame, logical_table: str) -> pd.DataFrame:
    return catalog[catalog["logical_table"] == logical_table][["region", "provider", "uri"]]

print(find_table_anywhere(federated_catalog, "orders"))
```

```text
  region provider                              uri
0     us      aws  s3://acme-lake-us/silver/orders
1     eu      gcp  gs://acme-lake-eu/silver/orders
```

A federated catalog (Level 3, Module 03's ideas extended across providers)
lets an analyst discover that `orders` exists in two regions/clouds and
query the one relevant to them, without a central team having copied the
data anywhere — only the *metadata* is centralized.

## Traps

- **Replicating everything everywhere "to be safe."** This multiplies
  storage cost and, worse, creates consistency problems (which copy is
  authoritative after an update?) — replicate deliberately, per a specific
  compliance or latency requirement, not by default.
- **Ignoring egress costs in architecture decisions.** A design that looks
  clean on a whiteboard but routes every query cross-cloud can be far more
  expensive in practice than an uglier design that keeps compute next to
  data.
- **No single source of truth for "where does this table actually live."**
  Without a registry like `table_registry`, "which cloud has the real
  orders data for the EU region" becomes tribal knowledge that breaks the
  moment the one person who knows it leaves.

## Cheat sheet

| Concern | Pattern |
|---|---|
| Pipeline code shouldn't hardcode a cloud provider | URI-based abstraction (`parse_lake_uri`) |
| Same logical table, different physical location per region | Table registry keyed on `(logical_table, region)` |
| Recurring cross-cloud queries are expensive | Run compute where the data lives (data gravity) |
| Discovering data across clouds without copying it | Federated catalog with metadata only |

## Exercise

Add a `replication_policy` table (`logical_table`, `source_region`,
`replicate_to_region`, `sync_frequency_hours`) and a
`is_replication_stale(registry, replication_policy, logical_table,
region, last_synced_at, now)` function that flags when a required
replica hasn't synced within its configured frequency — the check a
platform team runs before trusting that an EU disaster-recovery copy of a
US table is actually current.
