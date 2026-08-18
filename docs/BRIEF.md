# Project Windrose

*A multi-city weather pipeline · ETL*

**Team:** Idempotent and Proud — Julius, Femi, Michael
**Kickoff:** Wednesday 19 August 2026
**Demo day:** Tuesday 1 September 2026

Latent Ed | Data Engineering Programme | Cohort 2026

---

## 1. The brief

Northlight Events runs outdoor markets and festivals in eight cities across the north of England and Scotland. Weather makes or breaks a trading day, and at the moment their planning works like this: someone checks a weather app the night before and makes a call.

They want something better. They want to know what the weather actually did at each of their sites over the past year, so they can see which locations are reliably wet and which trade well in the cold. They also want the coming week's forecast for every site in one place, refreshed automatically, so staffing and stock decisions stop being guesswork.

They have no data team. You are it.

---

## 2. Defining 'done'

- Hourly and daily weather for the eight defined cities is queryable in Snowflake
- At least twelve months of history is loaded
- The next seven days of forecast are refreshed automatically each day
- Data is written to S3 as Parquet, partitioned by date, before it reaches the warehouse
- Every table carries audit columns showing when the data was captured
- **Re-running any date range produces the same result.** No duplicates, no drift
- The whole thing runs on an Airflow schedule without anyone touching it

> ★ **The sixth point is the one your team is named after**
> It is also the hardest thing on that list. Treat it as a design constraint from day one rather than a bug you fix in week two. Retrofitting idempotency is significantly harder than designing for it.

---

## 3. The data source

Open-Meteo. Free, no key, no registration, up to 10,000 calls a day for non-commercial use. Data is licensed CC BY 4.0, so **attribution is required** — put it in your README.

| Endpoint | Purpose |
|---|---|
| `api.open-meteo.com/v1/forecast` | Up to 16 days ahead |
| `archive-api.open-meteo.com/v1/archive` | Historical, takes `start_date` and `end_date` |

### 3.1 The response shape is the whole challenge

You do not get records. You get parallel arrays:

```json
{
  "latitude": 53.8,
  "longitude": -1.55,
  "timezone": "Europe/London",
  "hourly": {
    "time":           ["2026-08-01T00:00", "2026-08-01T01:00", "..."],
    "temperature_2m": [14.2, 13.9, "..."],
    "precipitation":  [0.0, 0.2, "..."]
  },
  "hourly_units": { "temperature_2m": "°C" }
}
```

Index 0 of `temperature_2m` belongs to index 0 of `time`. `pd.DataFrame(records)` will not help you. Your transform layer has to transpose column-oriented arrays into row-oriented records, then repeat that for every city and stack the results.

### 3.2 Three things that will catch you out

- **Misspell a variable name in your request and the API drops it silently.** No error, no warning — just a column that is not there, which you will discover three layers downstream when a query returns nulls. Validate that every requested variable came back.
- **Forecast and archive are separate endpoints with the same response shape.** Your transform code should not care which one it came from.
- **The archive lags real time by a few days.** Confirm how far before you assume yesterday is available, and handle the gap deliberately rather than letting it produce silent holes.

> 💡 **Real-world tip**
> The silent-drop behaviour is not a quirk to work around — it is the single most useful thing this API will teach you. Real sources fail quietly far more often than they fail loudly. A pipeline that notices is worth ten that merely run.

---

## 4. Required architecture — ETL

Transformation happens **before** the warehouse. Snowflake receives clean, typed, tabular data and nothing else.

```
Open-Meteo API
      |  requests
      v
  extract           raw JSON dicts, no cleaning
      |
      v
  transform         transpose arrays -> tidy rows
                    fan across cities, stack
                    add audit columns
      |
      v
     S3             Parquet, partitioned by date
                    {env}/openmeteo/{dataset}/dt=YYYY-MM-DD/
      |  COPY INTO
      v
  Snowflake         query-ready tables
```

Orchestrated by one Airflow DAG. Module structure follows `fpl-integration` — config, extract, transform and load, each with one responsibility.

> ★ **Key takeaway**
> If you find yourself cleaning data inside your extract module, that logic belongs in transform. The extract layer's job is to get data and return it. Nothing else.

---

## 5. Scope

### In scope

Eight UK cities, defined in config. Hourly variables: temperature, precipitation, wind speed, relative humidity. Daily variables: max and min temperature, precipitation sum, sunshine duration. Twelve months of history. Seven-day forecast, refreshed daily.

### Explicitly out of scope

Do not build these, even if you have time:

- The geocoding endpoint — city coordinates are hardcoded in config. Geocoding is a stretch goal only
- Air quality, marine or ensemble endpoints
- More than ten cities
- dbt or any SQL transformation layer
- A dashboard or web app
- Alerting beyond Airflow's built-in retries
- Incremental merge logic more sophisticated than replacing a date partition

> ⚠ **Watch out for**
> You have fourteen days. Scope creep is the most likely reason this project does not finish, and it always arrives disguised as a good idea.

---

## 6. Milestones

| Date | Checkpoint |
|---|---|
| Wed 19 Aug | Kickoff. Platform running, Snowflake reachable, one API call succeeds |
| Thu 20 – Fri 21 | Extract module complete. City list driven entirely by config |
| Sat 22 – Sun 23 | Lighter. Transform started |
| Mon 24 Aug | Transform complete. Row counts verified against source array lengths |
| **Tue 25 Aug** | **End-to-end run for a single day, into S3 and Snowflake.** Hard checkpoint |
| Wed 26 – Thu 27 | Backfill loaded. Idempotency proven by re-running a window and showing counts unchanged |
| **Fri 28 Aug** | **Code freeze.** DAG scheduled, retries configured, README written |
| Sat 29 – Sun 30 | Buffer and demo prep |
| Mon 31 Aug | Bank holiday — deliberately empty |
| Tue 1 Sep | Demo day |

> ⚠ **If Tuesday slips, come and find me the same day**
> Do not absorb it quietly and hope to catch up. A checkpoint missed on Tuesday and reported on Friday is a much larger problem than one reported on Tuesday.

---

## 7. Ways of working

`main` is protected. No direct commits, no force pushes, no exceptions.

Branch naming:

```
feature/<initials>-<short-description>
fix/<initials>-<short-description>

For example:  feature/dr-silver-pivot
```

Every change reaches `main` through a pull request, approved by the instructor. Your teammates cannot unblock a merge, but you should still review each other's work — it is assessed, and it catches things before I see them.

**PRs are reviewed twice daily, morning and evening.** If something is urgent outside those windows, message me.

### Who owns what

Agree this between you on day one and post it in your team channel. Pairing is encouraged — these are areas of ownership, not walls.

- **Extract and config** — owns the API contract, the city list and variable validation
- **Transform** — owns the array transpose, which is the hardest single piece of this project
- **Load, S3 layout and DAG** — owns idempotency, partitioning and scheduling
- README, integration testing and the demo are shared across all three

> ★ **At three people, one person unavailable is a third of the team**
> Plan so the project survives someone losing two days. The Friday code freeze exists partly for this. Do not build a plan that only works if all three of you are at full capacity for fourteen straight days.

---

## 8. Technical requirements

- **Environment variables for everything machine-specific.** No hardcoded bucket names, account identifiers, schemas or paths. Anywhere.
- `.env` is never committed. `.env.example` is kept current — if you add a variable, add it there in the same PR.
- Audit columns on every table: `DOWNLOAD_DATE` and `RUN_TIME_STAMP`, captured once per run so they are consistent across all rows.
- **Snowflake warehouse auto-suspend stays at 60 seconds.** A warehouse left running burns shared credits the other team also depends on.
- PEP 8. Type hints on function signatures.

---

## 9. Demo day

Fifteen minutes:

| Time | What you cover |
|---|---|
| 3 min | The problem and your architecture |
| 5 min | Trigger the pipeline live in Airflow and talk through what is happening |
| 4 min | Query Snowflake and answer Northlight's actual question |
| 3 min | What broke, what you would do differently, what you would build next |

The fourth section carries more weight than teams expect. An honest account of a bug you fought for a day is worth more than a slide claiming everything went smoothly.

---

## 10. Assessment

Marked out of 100. **Meets** is the standard expected of everyone — a pass, not a compliment. **Exceeds** describes work you would be comfortable showing an employer without caveat.

> ★ **This is a team mark**
> There is no individual adjustment. Ensuring everyone contributes meaningfully is the team's responsibility, not something resolved after the fact.

| # | Criterion | Wt | Meets | Exceeds |
|---|---|---|---|---|
| 1 | **Pipeline function** | 25 | Full DAG runs end to end unattended, on schedule, populating both S3 and Snowflake with all eight cities and both hourly and daily datasets | Recovers from a transient API failure without intervention; sensible timeouts and retry backoff; a deliberate failure can be triggered and survived during the demo |
| 2 | **Idempotency & correctness** | 15 | Re-running any date range leaves row counts unchanged; counts reconcile against source array lengths; audit columns present and consistent within a run | Idempotency enforced by design rather than checked afterwards; missing or partial responses handled explicitly rather than silently absorbed |
| 3 | **Architecture & code quality** | 15 | Four modules with clean separation — no transformation in extract, no API calls in transform, no business logic in the DAG; PEP 8; type hints on signatures | Config-driven throughout, so adding a city or variable requires no code change; functions small and independently testable; defensive patterns used deliberately, not decoratively |
| 4 | **Collaboration** | 15 | Every change reaches `main` through a pull request; no direct commits; all three members have meaningful commits spread across the fortnight, not concentrated at the end; branch naming followed; PR descriptions filled in rather than left as the blank template | Teammates review each other's PRs without being required to — comments that ask questions, spot problems or suggest alternatives, not "looks good"; PRs small and single-purpose; review participation shared across all three rather than one person doing it all |
| 5 | **Config & secrets** | 10 | No credentials, buckets, accounts or paths hardcoded anywhere; `.env` never committed; `.env.example` current and complete | Configuration validated at startup with a clear error when a variable is missing, rather than failing obscurely at runtime |
| 6 | **Reproducibility & docs** | 10 | A stranger can clone, follow the README, and run the pipeline without asking questions; Open-Meteo attribution present | Architectural decisions documented with their reasoning; known limitations stated honestly rather than omitted |
| 7 | **Demo & reflection** | 10 | Architecture explained clearly; pipeline demonstrated live; Northlight's question answered from the warehouse | Choices defended with reasoning, including ones that proved wrong; failures described specifically and what changed as a result |

*Instructor approval is what unblocks a merge, so reviewing each other's work is voluntary. That is deliberate. Doing it anyway — properly, not as a formality — is what criterion 4 measures.*

Below **Meets** on any criterion is recorded with a specific, actionable note — what was missing and what would close the gap — rather than a lower number on its own.

---

## 11. Before you start

- [ ] Snowflake login works and you can see your team's schema
- [ ] AWS credentials in your local `.env`, and you can list your S3 bucket
- [ ] Repo cloned into `projects/` inside your `data-engineering` monorepo
- [ ] `docker compose up` succeeds and the Airflow UI loads
- [ ] One Open-Meteo call made successfully in a browser or Postman

Anything unticked is a message to me straight away, not a discovery three days in.

---

## 12. When you are stuck

- `fpl-integration` — module structure and the extract/transform/load split
- The API Integration Pipeline guide, sections 5 to 7 — the reasoning behind those decisions
- `fpl-elt-dbt` — S3 key layouts and Snowflake connection patterns

---

*© Latent Ed 2026 | For cohort use only | latented.co.uk*