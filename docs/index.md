---
title: "Learn ETL & Data Lakes Free: Beginner to Master Course"
description: "Free course on ETL/ELT pipeline design and data lake architecture -- extraction, transformation, loading, bronze/silver/gold layering, lakehouse concepts, and governance, with real hands-on projects."
---

# ETL & Data Lake Mastery Path

A structured, module-wise training program on **ETL/ELT pipeline design and
Data Lake architecture** — from your first extract-transform-load script to
governed, multi-zone lakehouse platforms — with runnable Python code in every
module and a hands-on project at the end of each level.

ETL is the plumbing behind every analytics dashboard, ML feature store, and
finance report: moving data reliably from where it's produced to where it's
trusted and queried. A Data Lake is where a huge share of that data ends up
living. This site teaches both from first principles — plain Python and file
formats first, frameworks and lakehouse table formats (Airflow, Delta
Lake/Iceberg/Hudi) once you understand what problem they actually solve.

## How the program is organized

| Level | Focus | Modules |
|-------|-------|---------|
| [Level 1 · Entry](level-1/index.md) | ETL vs. ELT, ingestion patterns, extraction/transformation/loading basics, what a data lake is, bronze/silver/gold layering, file formats, orchestration concept | 9 topics + 1 capstone |
| [Level 2 · Intermediate](level-2/index.md) | Incremental loads & CDC, data quality checks, partitioning, schema evolution, orchestration tools (Airflow), idempotent design, backfills, monitoring, data contracts | 9 topics + 1 project |
| [Level 3 · Advanced](level-3/index.md) | Lakehouse concepts (Delta/Iceberg/Hudi), streaming ETL, cataloging & metadata, cost/performance optimization, late-arriving data, compaction, time travel, ACID on the lake | 9 topics + 1 project |
| [Level 4 · Master](level-4/index.md) | Governed multi-zone lake architecture, lineage & observability, security & access control, lake vs. warehouse vs. lakehouse, data mesh, compliance, multi-cloud strategy | 9 topics + 1 capstone |

## What you need

- **Python 3.10+** and `pip`. Level 1 uses only the standard library and
  `pandas` (plus `pyarrow` for Parquet) — no cloud account, no API key,
  everything runs locally.
- Later levels introduce Airflow concepts, Delta Lake/Iceberg/Hudi table
  formats, and streaming engines; each lesson states exactly what to install
  (or simply describes, conceptually, since some tools need a cluster).

## How to use this site

- Work through each level in order — later modules assume earlier ones.
- Every Level 1 topic page has runnable code — copy it into a local `.py`
  file and run it. Every output block on this level was carefully reasoned
  through against the actual pandas/pyarrow APIs, and is noted where it
  wasn't literally executed in a live environment.
- Each level ends with a project or capstone that combines everything learned
  in that level.
- Use the search bar (top of the page) to jump straight to a topic.

Start here → [Level 1 · Entry](level-1/index.md)

## Related tracks

ETL and data lakes sit next to data engineering, analytics, and SQL. Sister
sites cover the neighboring ground:

- [Data Engineering Mastery Path](https://sigilipelli.github.io/data-engineering-skillmastery/) — the broader data engineering discipline
- [SQL Mastery Path](https://sigilipelli.github.io/sql-skillmastery/) — SQL from first principles
- [PySpark Mastery Path](https://sigilipelli.github.io/pyspark-skillmastery/) — distributed processing for large-scale ETL
- [Data Science Mastery Path](https://sigilipelli.github.io/data-science-skillmastery/) — what happens after the data lands

🎥 **Prefer video?** Watch the [Mastery Path video series](https://youtube.com/@sigilipelli) on YouTube — Shorts and full walkthroughs of these lessons.

## More from the Mastery Path series

Free, structured, module-wise training across 59 other languages, platforms and disciplines:

<div class="mastery-grid-wrap">
<p class="mastery-grid-category">Languages</p>
<div class="mastery-grid">
  <a href="https://sigilipelli.github.io/python-skillmastery/">🐍 Python</a>
  <a href="https://sigilipelli.github.io/java-skillmastery/">☕ Java</a>
  <a href="https://sigilipelli.github.io/javascript-skillmastery/">🟨 JavaScript</a>
  <a href="https://sigilipelli.github.io/typescript-skillmastery/">🔷 TypeScript</a>
  <a href="https://sigilipelli.github.io/csharp-skillmastery/">🔵 C#</a>
  <a href="https://sigilipelli.github.io/shell-skillmastery/">🐚 Shell/Bash</a>
  <a href="https://sigilipelli.github.io/powershell-skillmastery/">💻 PowerShell</a>
  <a href="https://sigilipelli.github.io/c-skillmastery/">🇨 C</a>
  <a href="https://sigilipelli.github.io/cpp-skillmastery/">➕ C++</a>
  <a href="https://sigilipelli.github.io/go-skillmastery/">🐹 Go</a>
  <a href="https://sigilipelli.github.io/rust-skillmastery/">🦀 Rust</a>
  <a href="https://sigilipelli.github.io/sql-skillmastery/">🗄️ SQL</a>
  <a href="https://sigilipelli.github.io/ruby-skillmastery/">💎 Ruby</a>
  <a href="https://sigilipelli.github.io/php-skillmastery/">🐘 PHP</a>
  <a href="https://sigilipelli.github.io/kotlin-skillmastery/">🟣 Kotlin</a>
  <a href="https://sigilipelli.github.io/swift-skillmastery/">🐦 Swift</a>
  <a href="https://sigilipelli.github.io/dart-skillmastery/">🎯 Dart</a>
  <a href="https://sigilipelli.github.io/scala-skillmastery/">🔴 Scala</a>
  <a href="https://sigilipelli.github.io/r-skillmastery/">📊 R</a>
  <a href="https://sigilipelli.github.io/matlab-skillmastery/">🟧 MATLAB</a>
</div>
<p class="mastery-grid-category">Testing & QA</p>
<div class="mastery-grid">
  <a href="https://sigilipelli.github.io/java-testing-skillmastery/">🧪 Java Testing</a>
  <a href="https://sigilipelli.github.io/cpp-testing-skillmastery/">🧪 C/C++ Testing</a>
  <a href="https://sigilipelli.github.io/python-testing-skillmastery/">🧪 Python Testing</a>
  <a href="https://sigilipelli.github.io/automotive-testing-skillmastery/">🚗 Automotive Testing</a>
</div>
<p class="mastery-grid-category">Security</p>
<div class="mastery-grid">
  <a href="https://sigilipelli.github.io/cybersecurity-skillmastery/">🛡️ Cybersecurity</a>
</div>
<p class="mastery-grid-category">Cloud Platforms</p>
<div class="mastery-grid">
  <a href="https://sigilipelli.github.io/aws-skillmastery/">☁️ AWS</a>
  <a href="https://sigilipelli.github.io/azure-skillmastery/">☁️ Azure</a>
  <a href="https://sigilipelli.github.io/gcp-skillmastery/">☁️ GCP</a>
  <a href="https://sigilipelli.github.io/ibm-cloud-skillmastery/">☁️ IBM Cloud</a>
  <a href="https://sigilipelli.github.io/adobe-skillmastery/">🎨 Adobe</a>
</div>
<p class="mastery-grid-category">Data & Analytics</p>
<div class="mastery-grid">
  <a href="https://sigilipelli.github.io/data-engineering-skillmastery/">🛠️ Data Engineering</a>
  <a href="https://sigilipelli.github.io/data-science-skillmastery/">📈 Data Science</a>
  <a href="https://sigilipelli.github.io/tableau-skillmastery/">📊 Tableau</a>
  <a href="https://sigilipelli.github.io/excel-skillmastery/">📗 Excel</a>
  <a href="https://sigilipelli.github.io/pyspark-skillmastery/">⚡ PySpark</a>
</div>
<p class="mastery-grid-category">AI / ML / LLM</p>
<div class="mastery-grid">
  <a href="https://sigilipelli.github.io/ai-ml-skillmastery/">🤖 AI/ML</a>
  <a href="https://sigilipelli.github.io/llm-dev-skillmastery/">🧠 LLM Dev</a>
  <a href="https://sigilipelli.github.io/rag-skillmastery/">📚 RAG</a>
  <a href="https://sigilipelli.github.io/edge-ai-skillmastery/">📱 Edge AI</a>
  <a href="https://sigilipelli.github.io/claude-training-skillmastery/">🔶 Claude Training</a>
  <a href="https://sigilipelli.github.io/ai-tools-skillmastery/">🧰 AI Tools</a>
  <a href="https://sigilipelli.github.io/ml-math-skillmastery/">➗ ML Math Foundations</a>
</div>
<p class="mastery-grid-category">Embedded Systems</p>
<div class="mastery-grid">
  <a href="https://sigilipelli.github.io/embedded-skillmastery/">🔌 Embedded</a>
  <a href="https://sigilipelli.github.io/embedded-linux-skillmastery/">🐧 Embedded Linux</a>
  <a href="https://sigilipelli.github.io/embedded-python-skillmastery/">🐍 Embedded Python</a>
  <a href="https://sigilipelli.github.io/freertos-skillmastery/">⏱️ FreeRTOS</a>
  <a href="https://sigilipelli.github.io/s32k-skillmastery/">🔧 S32K</a>
</div>
<p class="mastery-grid-category">Leadership & Management</p>
<div class="mastery-grid">
  <a href="https://sigilipelli.github.io/product-manager-skillmastery/">📋 Product Manager</a>
  <a href="https://sigilipelli.github.io/product-lead-skillmastery/">🧭 Product Lead</a>
  <a href="https://sigilipelli.github.io/project-manager-skillmastery/">📅 Project Manager</a>
  <a href="https://sigilipelli.github.io/ai-manager-skillmastery/">🤖 AI Manager</a>
  <a href="https://sigilipelli.github.io/servant-leadership-skillmastery/">🤝 Servant Leadership</a>
</div>
<p class="mastery-grid-category">Professional Skills</p>
<div class="mastery-grid">
  <a href="https://sigilipelli.github.io/english-fluency-skillmastery/">🗣️ English Fluency & IELTS</a>
  <a href="https://sigilipelli.github.io/workday-skillmastery/">🧑‍💼 Workday</a>
</div>
<p class="mastery-grid-category">Process & APIs</p>
<div class="mastery-grid">
  <a href="https://sigilipelli.github.io/agile-skillmastery/">🔄 Agile/Scrum/Kanban</a>
  <a href="https://sigilipelli.github.io/rest-api-skillmastery/">🔗 REST API</a>
  <a href="https://sigilipelli.github.io/playwright-skillmastery/">🎭 Playwright</a>
</div>
<p class="mastery-grid-category">Infrastructure & Ops</p>
<div class="mastery-grid">
  <a href="https://sigilipelli.github.io/server-ops-skillmastery/">🖥️ Server Ops</a>
  <a href="https://sigilipelli.github.io/nodemcu-skillmastery/">📶 NodeMCU/IoT</a>
</div>
</div>
