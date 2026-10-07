# Project Windrose — Revised Brief

*A multi-city weather pipeline · ETL to PostgreSQL*

**Team:** Idempotent and Proud — Julius, Femi, Michael
**Revised:** 8 October 2026
**Demo day:** Friday 30 October 2026

Latent Ed | Data Engineering Programme | Cohort 2026

> **This brief replaces the original.** The architecture has changed from
> ETL-into-Snowflake to ETL-into-PostgreSQL. Section 1 explains why, because the
> reasoning matters more than the instruction.

---

## 1. Why this changed

Your original brief asked for an ETL pipeline landing in Snowflake. You built it,
and the work was good — transactional partition replacement, validated inputs,
proper setup scripts. None of that was wasted; you'll use the same patterns here.

But there was a mismatch worth naming.

ETL exists for a historical reason. Warehouses used to be expensive, slow and
hard to scale, so transformation happened *outside* them — in Informatica, in
DataStage, in SSIS, in hand-written code on a server somewhere. Data was shaped
before it ever reached the database, because the database couldn't do the shaping.

ELT reverses that, and it reversed because engines like Snowflake and BigQuery
made the warehouse the cheapest place to compute.

Doing ETL *into Snowflake* is therefore slightly ahistorical. It works, but it
uses a 2005 pattern against an engine that makes it unnecessary — and the reason
the pattern exists never becomes visible to you.

PostgreSQL makes it visible. Postgres genuinely cannot do the heavy lifting at
scale, so Python genuinely has to. The constraint is real, so the design decisions
have teeth.

**What you end up with is a portfolio that shows range.** Dockside is a modern
cloud ELT — raw into the warehouse, transform in SQL. Windrose is a traditional
ETL — transform in code, load the result. Two architectures, two eras, and you
will be able to explain exactly when each is appropriate and why. That is a far
stronger position in an interview than two variations on the same stack.

> ★ **The thing to take away**
> Staging → core → mart *is* bronze → silver → gold. Same architecture, thirty
> years apart, different vocabulary. Medallion is not a cloud invention — it is a
> renaming of the layered warehouse pattern that Kimball and Inmon were arguing
> about in the 1990s. Every data warehouse has had these layers. Only the names
> and the engines changed.

---

## 2. Defining 'done'

- Extract writes **raw, untransformed** API responses to S3
- Transform reads from raw, shapes the data, writes **cleaned Parquet** to a separate processed path in S3
- Load reads cleaned Parquet and writes to **PostgreSQL tables** — not external tables, not a lake query engine
- PostgreSQL holds three layers: **staging → core → mart**
- Loading is **incremental**, driven by a control table rather than hardcoded dates
- Re-running any date range produces identical results. No duplicates, no drift
- A failed run can be re-run without manual intervention and without re-extracting from the API
- The whole thing runs on an Airflow schedule, unattended

---

## 3. The flow

```
Open-Meteo API
      │  extract
      ▼
    S3  raw/openmeteo/{dataset}/dt=YYYY-MM-DD/response.json
      │  exactly as the API returned it — no shaping
      │
      │  transform
      ▼
    S3  processed/openmeteo/{dataset}/dt=YYYY-MM-DD/data.parquet
      │  transposed, typed, audit columns added
      │
      │  load (COPY)
      ▼
 POSTGRES  stg_openmeteo.*     truncate + load, mirrors the file
      │
      ▼
           core.*              conformed, incremental, historised
      │
      ▼
           mart_weather.*      per-consumer views and aggregates
```

**Why the intermediate files are not incidental.** Informatica, DataStage and SSIS
all wrote to a staging filesystem between steps. S3 is standing in for the network
share. The point was restartability: a load that failed at 3am could be re-run at
7am without going back to the source system — which mattered enormously when the
source was a mainframe with a two-hour batch window.

That property is why each of your three tasks must be independently re-runnable.
Extract writes raw and stops. Transform reads raw and writes processed, and stops.
Load reads processed and writes to Postgres.

---

## 4. The PostgreSQL layout

**One database. Multiple schemas.** Use the Postgres instance already running in
the platform's Docker stack — do not provision a new one.

```
latent_dw
├── etl                  control and audit tables
├── stg_openmeteo        staging — one schema per source system
├── core                 conformed, the engine-agnostic layer
└── mart_weather         one schema per consumer or use case
```

### Why one database and not two

This is worth understanding properly, because the instinct to split staging and
presentation into separate databases is a **SQL Server** instinct — and in SQL
Server it is perfectly reasonable, because three-part naming lets you join across
databases freely.

**Postgres cannot do that.** A connection is scoped to exactly one database.
Crossing between databases requires `postgres_fdw` or `dblink` — extensions you
must install and maintain, with worse performance and no transactional guarantee
across the boundary. Your transform step could not read staging and write core in
a single transaction.

Schemas give you the separation without the cost. And they give you something a
two-database design does not: clean permission boundaries inside one connection.
Your ETL role gets write on `stg_*` and `core`; an analyst role gets read-only on
`mart_*` and nothing else.

> ★ **The lesson underneath**
> Architecture does not port between engines just because both say "database" on
> the tin. The right design for SQL Server is the wrong design for Postgres.

### What each layer is for

**Staging** mirrors the processed file. Column names as they arrive, minimal
typing, one schema per source system. **Truncate and reload on every run** — this
is a workspace, not a record. The file in S3 is the durable copy.

**Core** is where conforming happens: `dim_location`, `dim_date`,
`fct_hourly_weather`, `fct_daily_weather`. This layer survives a source system
being replaced, and it is loaded **incrementally**.

**Mart** is per consumer. Mostly views or small aggregates over core. If two teams
want different cuts, they get different schemas rather than arguing over one set
of tables.

---

## 5. Incremental loading

This is the most valuable thing in the revised project, and the part most likely
to come up in an interview.

### The control table

```sql
CREATE TABLE etl.load_control (
    source_system     VARCHAR(50)  NOT NULL,
    dataset           VARCHAR(50)  NOT NULL,
    last_loaded_date  DATE,
    rows_loaded       INTEGER,
    status            VARCHAR(20),
    run_timestamp     TIMESTAMPTZ,
    PRIMARY KEY (source_system, dataset)
);
```

Each run reads the watermark, extracts from there forward, loads, and advances the
watermark — **inside the same transaction as the load**. A failed load never
advances the marker, so the next run picks up exactly where the last one left off.

Every Informatica and SSIS shop built this, usually by hand, usually called
something like `ETL_AUDIT`. It is what made nightly batch survivable.

**It also removes a bug from your current code.** The hardcoded `start_date` and
`end_date` in `run_pipeline.py` disappear — the dates come from the control table.
You will remember that you narrowed `end_date` to a single day while testing and
committed it by accident. With a watermark, there is nothing to narrow and nothing
to forget to change back.

### Incremental happens at three independent points

| Stage | Incremental means |
|---|---|
| Extract | Fetch only dates after the watermark, not a year every run |
| Transform | Process only raw partitions that are new |
| Load | Write only affected partitions into core |

A pipeline can be incremental at one and full-refresh at another. Decide each
deliberately.

### The strategy depends on the data, not the engine

Windrose demonstrates both cases, which is why it is a good vehicle for this.

**Historical weather is immutable.** Once 1 August has happened, it never changes.
So: watermark-driven extract, partition replacement on load. Yesterday's data
arrives; nothing before it is touched.

**Forecast data is mutable by nature.** Today's prediction for Saturday differs
from yesterday's. You cannot append — you would accumulate contradictions. So it
is either full snapshot replacement (what you built for Snowflake) or an upsert:

```sql
INSERT INTO core.fct_hourly_forecast (...)
VALUES (...)
ON CONFLICT (location_id, forecast_time)
DO UPDATE SET
    temperature_2m = EXCLUDED.temperature_2m,
    ...
```

That is Postgres's MERGE. It is the same operation as Snowflake's `MERGE INTO`.

Use the upsert for forecasts in core. You will then have solved idempotency three
ways on two engines — delete-and-reload, truncate-and-replace, and upsert. Being
able to explain when each applies is worth more than any single implementation.

> ★ **Key takeaway**
> The data's mutability decides the pattern. Not the engine, not the framework.

---

## 6. Loading into Postgres

**Use `COPY`, not `pandas.to_sql`.** `COPY FROM STDIN` is Postgres's bulk loader —
the direct equivalent of `bcp` or SQL\*Loader, and the historically correct tool.
`to_sql` issues row-by-row inserts and is dramatically slower; the difference is
visible even at your data volumes.

**Postgres cannot read Parquet natively.** Your loader reads the Parquet with
pyarrow and streams it to `COPY` as an in-memory CSV buffer. This is a real
constraint of the old world and worth feeling — Snowflake reads Parquet directly,
Postgres does not.

**Idempotency transfers directly.** `DELETE WHERE date = x`, then `COPY`, both
inside a transaction. You have already written exactly this against Snowflake. It
is the same shape against a completely different engine, which is the point.

---

## 7. What to remove, what to keep

**Remove:** the `snowflake/` SQL directory, `snowflake_loader.py`,
`run_snowflake_loader.py`, the `dbt-snowflake` and `snowflake-connector-python`
entries in `requirements.txt`, and the Snowflake variables in `.env.example`.

Do this in its own commit with a clear message. Git keeps the history, so the work
is not lost — and `git log` showing a deliberate architectural change is a better
story than a repo with dead code in it.

**Keep:** everything in `pipeline/ingestion`, `pipeline/transformations`,
`config/`, `scripts/api_parameters.py`, and your S3 loader. The transform is the
hardest thing you have written and it does not change.

**Mention in your README** that the project was originally built against Snowflake
and moved to Postgres, with the reasoning. A documented architectural change is
evidence of judgement, not evidence of indecision.

---

## 8. The DAG

You have already built one. Keep the structure; the tasks change.

```
extract_to_raw      API → S3 raw. Returns the S3 prefix written.
       ↓
transform_to_processed    S3 raw → S3 processed. Returns the prefix.
       ↓
load_to_staging     processed → stg_openmeteo (truncate + COPY)
       ↓
load_to_core        staging → core (incremental; advances the watermark)
       ↓
refresh_mart        core → mart_weather
```

Five tasks. When something fails, the graph tells you which stage broke before you
open a log.

**Do not pass DataFrames between tasks.** Airflow moves values between tasks
through XCom, which stores them in its metadata database and is built for small
things — a prefix, a count, a filename. Pass the S3 path; the next task reads the
file. S3 is the handoff.

---

## 9. Scope

**In scope:** eight UK cities from `locations.csv`. Raw and processed S3 paths.
Staging, core and mart schemas in Postgres. Control table and watermark-driven
incremental load. Twelve months of history, seven-day forecast refreshed daily.
Five-task DAG on a daily schedule.

**Explicitly out of scope:**

- Snowflake, in any form
- dbt
- Slowly changing dimensions beyond type 1 — overwrite is fine
- More than two mart objects
- Dashboards or BI tools
- Alerting beyond Airflow's retries
- `postgres_fdw`, `dblink`, or anything that crosses databases

---

## 10. Milestones

| Date | Checkpoint |
|---|---|
| Thu 8 – Fri 9 Oct | Snowflake work removed. Postgres schemas created. Control table created |
| Sat 10 – Sun 11 | Extract writes raw JSON to S3. Transform reads raw, writes processed Parquet |
| Mon 12 – Tue 13 | Loader working: processed Parquet → `stg_openmeteo` via `COPY` |
| **Wed 14 Oct** | **End-to-end by hand: API → raw → processed → staging. Hard checkpoint** |
| Thu 15 – Fri 16 | Core layer. Historical partition replacement, forecast upsert |
| Sat 17 – Sun 18 | Watermark logic. Prove a failed run resumes correctly |
| Mon 19 – Tue 20 | DAG wired up, five tasks, running on schedule |
| Wed 21 – Thu 22 | Mart layer. Deliberately break the pipeline and watch it recover |
| Fri 23 – Mon 26 | Buffer |
| **Wed 28 Oct** | **Code freeze.** README written, demo prepared |
| Thu 29 Oct | Full dry run |
| **Fri 30 Oct** | **Demo day** |

If a hard checkpoint slips, tell me the same day.

---

## 11. Ways of working

Unchanged. `main` is protected, every change by pull request approved by the
instructor, branch naming `feature/<initials>-<description>`.

### Who owns what

This needs to change. Both of your pull requests so far contain only your commits.
Criterion 4 is 15 marks and it assesses all three of you contributing across the
project — the rebuild is a natural point to split the work.

Suggested division, to agree between you and post in your team channel:

- **Extract and transform to S3** — raw and processed paths, the file layout
- **Postgres schemas, staging load, the `COPY` mechanics**
- **Control table, incremental logic into core, the DAG**

README, testing and the demo are shared.

---

## 12. Assessment

Marked out of 100. **Meets** is the expected standard — a pass, not a compliment.

**This is a team mark.** There is no individual adjustment.

| # | Criterion | Wt | Meets | Exceeds |
|---|---|---|---|---|
| 1 | **Pipeline function** | 25 | Five-task DAG runs unattended on schedule, populating raw, processed, staging, core and mart | Recovers from a transient failure without intervention; a gap in collection is detectable rather than invisible |
| 2 | **Incremental & idempotency** | 20 | Watermark drives extract and load; re-running any date range leaves row counts unchanged; a failed run resumes without manual intervention | Historical and forecast use different strategies, chosen deliberately and explained; watermark advances inside the load transaction |
| 3 | **Architecture & modelling** | 15 | Clear staging/core/mart separation; one database, schema-separated; `COPY` used for bulk load; no transformation in the load layer | Core is genuinely engine-agnostic; grants reflect the layer boundaries; naming consistent and predictable |
| 4 | **Collaboration** | 15 | Every change via PR; no direct commits; all three members have meaningful commits spread across the period; branch naming followed; PR descriptions completed | Teammates review each other's work without being required to; PRs small and single-purpose; review shared across all three |
| 5 | **Config & secrets** | 10 | Nothing hardcoded — including date ranges, which come from the control table; `.env` never committed; `.env.example` current | Configuration validated at startup with a clear error when something is missing |
| 6 | **Reproducibility & docs** | 10 | A stranger can clone, follow the README, and run it; Open-Meteo attribution present | The Snowflake-to-Postgres change documented with its reasoning; known limitations stated honestly |
| 7 | **Demo & reflection** | 5 | Architecture explained clearly; pipeline demonstrated live | Can articulate when ETL beats ELT and why, with reference to both your projects |

Criterion 2 has gone from 15 to 20 marks, taken from criterion 7. Incremental
loading is the heart of this project now and the marking should say so.

---

## 13. Before you start

- [ ] Postgres container running and reachable from Airflow
- [ ] `latent_dw` database and the four schemas created
- [ ] Control table created and seeded with an initial watermark
- [ ] Snowflake work removed in its own commit
- [ ] Work split agreed between the three of you and posted

---

## 14. When you are stuck

`fpl-integration` — module structure and the extract/transform/load split ·
The API Integration Pipeline guide, sections 5 to 7 · Postgres docs on `COPY` and
`INSERT ... ON CONFLICT` · Your own Snowflake loader, which is the same logic
against a different engine

---

*© Latent Ed 2026 | For cohort use only | latented.co.uk*