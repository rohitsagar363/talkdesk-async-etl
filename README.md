# talkdesk-async-etl

Async, production‑ready Talkdesk reporting ETL with:

- Three implementation scenarios you can learn from and use:
  - **Local** (SQLite + filesystem) — simple, fully self‑contained async ETL.
  - **Databricks (driver‑async)** — Delta + ADLS Gen2 + Key Vault, using asyncio on the driver for small/medium workloads.
  - **Databricks (Spark‑scalable pattern)** — documented design for distributing work with Spark partitions for large workloads.
- Config‑driven endpoints and reports
- Fully async HTTP with retries and exponential backoff
- Structured monitoring for jobs and reports

---

## 1. Tech Stack

**Languages & Runtime**

- Python 3.10+
- Async I/O with `asyncio` + `aiohttp`

**Data & Storage**

- Local:
  - SQLite (`monitoring.db`) for job/report monitoring
  - CSV output on local filesystem under `output/`
- Databricks:
  - Delta Lake tables in `talkdesk_{ENV}` databases (e.g., `talkdesk_prod`, `talkdesk_dev`)
  - ADLS Gen2 as storage backend
  - Azure Key Vault (via Databricks secret scopes)

**Libraries**

- `aiohttp` — async HTTP client (Talkdesk API calls)
- `pandas` — JSON normalization and CSV writing
- `pyspark` — Databricks SQL/Delta

---

## 2. Repository Structure

```text
talkdesk_local/
├── config.json                 # Local config for Talkdesk API (real or copied from example)
├── config.example.json         # Sample config you can copy/edit
├── requirements.txt            # Python dependencies
├── output/                     # Local output CSVs (local pipeline)
│
├── local/                      # Local async ETL
│   ├── __init__.py
│   ├── async_utils.py          # Async HTTP helpers, config loader
│   ├── monitoring_db.py        # SQLite monitoring (jobs/reports)
│   └── talkdesk_local_etl.py   # Local ETL entrypoint (async)
│
├── databricks/
│   ├── __init__.py
│   ├── talkdesk_databricks_etl.py              # Databricks ETL job (driver-async, small/medium workloads)
│   └── talkdesk_databricks_etl_distributed.py  # Databricks ETL job (Spark-distributed example)
│
└── ddl/                        # One‑time DDL + seeding for Delta tables
    ├── ddl_talkdesk_config.py      # report_config + endpoint_config
    └── ddl_talkdesk_monitoring.py  # job_monitoring + report_monitoring
```

---

## 3. High‑Level Architecture

### 3.0 Three Scenarios at a Glance

1. **Local async ETL (open‑source friendly)**
   - Runs entirely on your machine with `python -m local.talkdesk_local_etl`.
   - Uses `config.json`, SQLite monitoring, and writes CSVs under `output/`.
   - Good for development, demos, or small production jobs that don't need Databricks.

2. **Databricks driver‑async ETL (implemented)**
   - Implemented in `databricks/talkdesk_databricks_etl.py`.
   - Uses Delta + ADLS Gen2 + Key Vault + OAuth2 TokenManager.
   - Async HTTP orchestration runs on the driver, tuned for a small, bounded number of reports (e.g., ~8 reports × up to ~10,000 rows).
   - This is your primary production‑ready path today.

3. **Databricks Spark‑scalable pattern (design pattern)**
   - Demonstrated in `databricks/talkdesk_databricks_etl_distributed.py`.
   - Shows how to scale the same logic to hundreds/thousands of reports by:
     - Keeping `report_config`/`endpoint_config` as Spark DataFrames.
     - Repartitioning by report (`repartition(N)`) and using `foreachPartition` to process batches of reports on workers.
     - Running a small asyncio loop *within each partition* to parallelize API calls per worker.

### 3.0.1 Architecture Diagram

```mermaid
flowchart LR
  TD[(Talkdesk APIs)]

  subgraph Local[Local async ETL (Python + SQLite)]
    L_ETL[Local ETL\n(local.talkdesk_local_etl.py)]
    L_CFG[Config\n(config.json)]
    L_OUT[(CSV files\noutput/)]
    L_MON[(SQLite\nmonitoring.db)]
  end

  subgraph DB_Driver[Databricks driver-async ETL]
    D_ETL[Driver ETL\n(talkdesk_databricks_etl.py)]
    D_CFG[(Delta config\nreport_config + endpoint_config)]
    D_MON[(Delta monitoring\njob_monitoring + report_monitoring)]
    ADLS[(ADLS Gen2)]
  end

  subgraph DB_Dist[Databricks Spark-distributed ETL]
    SD_ETL[Distributed ETL\n(talkdesk_databricks_etl_distributed.py)]
    SD_PART[Workers\n(foreachPartition + asyncio)]
  end

  %% Local flow
  TD <-- HTTP (CSV) --> L_ETL
  L_ETL --> L_OUT
  L_ETL --> L_MON
  L_ETL --> L_CFG

  %% Databricks driver-async flow
  TD <-- HTTP (CSV) --> D_ETL
  D_ETL --> ADLS
  D_ETL --> D_MON
  D_ETL --> D_CFG

  %% Databricks distributed flow
  TD <-- HTTP (CSV) --> SD_PART
  SD_ETL --> SD_PART
  SD_PART --> ADLS
  SD_PART --> D_MON
  SD_ETL --> D_CFG

  %% Styling to highlight tools
  classDef api fill:#ffe6f0,stroke:#c2185b,stroke-width:1px;
  classDef etl fill:#e3f2fd,stroke:#1565c0,stroke-width:1px;
  classDef storage fill:#e8f5e9,stroke:#2e7d32,stroke-width:1px;
  classDef meta fill:#f3e5f5,stroke:#6a1b9a,stroke-width:1px;

  class TD api;
  class L_ETL,D_ETL,SD_ETL,SD_PART etl;
  class L_CFG,L_OUT,L_MON meta;
  class D_CFG,D_MON meta;
  class ADLS storage;
```

### 3.1 Conceptual Flow

1. **Config & Secrets**
   - Endpoint + report definitions live in Delta tables (`report_config`, `endpoint_config`) for Databricks, and in `config.json` for local.
   - Secrets (Talkdesk token, ADLS creds) are fetched from Key Vault in Databricks.

2. **Scheduling & Date Range**
   - Daily job runs for a date window, typically yesterday → today (UTC).
   - Databricks supports manual override via widgets (`from_date`, `to_date`).
   - Local supports CLI args (`--from` / `--to`).

3. **Async HTTP Calls**
   - For each enabled report:
     - `POST` to generate `report_id`
     - `GET` to download the report (CSV via Explore APIs)
   - All HTTP calls use retries with exponential backoff and respect per‑report `timeout_sec`.

4. **Persistence**
   - CSV responses from Talkdesk are:
     - Written directly to disk in the local pipeline.
     - Parsed with `pandas.read_csv` in the Databricks pipeline (to compute row counts before writing back to ADLS).
   - Final CSV results are written to:
     - Local filesystem under `output/{report}/{from}_to_{to}.csv`
     - ADLS Gen2 (Databricks) under `abfss://.../talkdesk/{report}/{from}_to_{to}.csv`

5. **Monitoring & Logging**
   - Each job and report is recorded with status, timings, row counts, and error messages.
   - Logs go to:
     - SQLite (`monitoring.db`) for local runs.
     - Delta tables (`job_monitoring`, `report_monitoring`) for Databricks.
   - Python logger (Databricks) and `print` (local) provide step‑level logs and retry details.

### 3.2 Two Pipelines

**Local pipeline**

- Uses `config.json` for:
  - `base_url`, `auth_endpoint`, `post_endpoint`, `get_endpoint`, `bearer_token`, `output_base_path`, `reports`.
- Monitors via `monitoring.db` with tables:
  - `jobs(id, started_at, finished_at, from_date, to_date, status, error_message)`
  - `reports(id, job_id, report_name, started_at, finished_at, status, error_message, output_path)`

**Databricks pipeline**

- Uses Delta config tables (created by `ddl/*.py`):
  - `{DB_NAME}.endpoint_config` (e.g., `talkdesk_prod.endpoint_config`)
    - `endpoint_type, base_url, auth_endpoint, post_endpoint, get_endpoint, env`
    - Seeded with `standard` endpoints for `dev` and `prod`.
  - `{DB_NAME}.report_config` (e.g., `talkdesk_prod.report_config`)
    - `report_name, enabled, endpoint_type, retries, timeout_sec, env`
    - Seeded with standard Talkdesk reports for both `dev` and `prod`.

- Monitoring tables:
  - `{DB_NAME}.job_monitoring`
    - `run_id, from_date, to_date, start_time, end_time, status (SUCCESS/PARTIAL_SUCCESS/FAILED), total_reports, success_count, failed_count, error_message`
  - `{DB_NAME}.report_monitoring`
    - `run_id, report_name, from_date, to_date, start_time, end_time, status, rows_written, error_message`

- Scale assumptions:
  - The primary Databricks implementation in `talkdesk_databricks_etl.py` is tuned for a small/medium, bounded number of reports (for example, up to ~50 reports, each up to ~50,000 rows).
  - At this scale, running the async HTTP orchestration on the driver with pandas is appropriate and simpler than a fully distributed Spark implementation.
  - For significantly larger workloads (hundreds or thousands of reports, or much larger per‑report sizes), you should revisit the design and consider distributing work across the cluster via `foreachPartition` or similar patterns.

---

## 4. Low‑Level Architecture

### 4.1 Local (`local/talkdesk_local_etl.py`)

Key components:

- `parse_args()` — optional `--from` / `--to` CLI date arguments.
- `auto_dates()` — default yesterday → today (local time).
- `load_config()` (in `local/async_utils.py`) — reads `config.json` from repo root.
- Async flow per report:
  1. Acquire access token via `get_auth_token`.
  2. `generate_report_id` → `report_id` via POST.
  3. `download_report` → CSV via GET.
  4. `write_to_csv` → writes that CSV directly under `output/{report}/{from}_to_{to}.csv`.
  5. Monitoring:
     - `start_job`, `finish_job` update job row with normalized status.
     - `start_report`, `finish_report` write each report’s status and output path.

Exception handling:

- Each report runs in its own task; `process_report` catches exceptions and marks the report `FAILED`.
- `main()` wraps the entire workflow and sets the job status to `SUCCESS` or `FAILED` accordingly.

---

### 4.2 Databricks (`databricks/talkdesk_databricks_etl.py`)

Key functions:

- `load_secrets() -> dict`
  - Reads required secrets from `SECRET_SCOPE = "talkdesk-scope"`.
  - Fails fast with a clear error if any secret is missing.

- `configure_adls(...) -> None`
  - Configures ADLS Gen2 OAuth for the given `storage_account`, `client_id`, `client_secret`, `tenant_id`.

- `get_dates() -> (from_date, to_date)`
  - If Databricks widgets `from_date` and `to_date` are defined and non‑empty, uses them.
  - Else uses yesterday → today (UTC).

- `load_report_configs(env)`, `load_endpoint_configs(env)`
  - Read from `f"{DB_NAME}.report_config"` / `f"{DB_NAME}.endpoint_config"` filtered by `env`.

- `log_job_start`, `log_job_end`, `log_report`
  - Write to `job_monitoring` and `report_monitoring` with safe SQL escaping for error messages.

- `retry_request(...)`
  - Shared async HTTP retry with:
    - Exponential backoff.
    - Optional per‑call timeout.
    - Retries on exceptions and 5xx; no retries on 4xx.

- `fetch_report_id`, `download_report`
  - Wrap remote calls using `retry_request`; `download_report` receives CSV text for Explore APIs.

- `process_report(...)`
  - Executes the report ETL for a single report name:
    - Logs each step.
    - Marks report success/failure in `report_monitoring`.
    - Returns `True`/`False`.

- `main()`
  - High‑level Databricks ETL:
    1. Load secrets; configure ADLS.
    2. Load configs for the configured `ENV` (`prod` by default).
    3. Determine date range via `get_dates()`.
    4. Create `run_id` and call `log_job_start`.
    5. Create a single `ClientSession` and instantiate a `TokenManager` that obtains and refreshes short‑lived OAuth2 access tokens as needed.
    6. Spawn tasks for each enabled report; each task calls `token_manager.get_token()` just‑in‑time before making Talkdesk API calls.
    7. Derive job `status` = `SUCCESS` / `FAILED` / `PARTIAL_SUCCESS`.
    8. Call `log_job_end` and log S/F counts.
    9. Any uncaught exception is caught at the top level and logged; job is marked `FAILED` with an error message.

---

## 5. Usage

### 5.1 Local Pipeline

**Install dependencies**

```bash
pip install -r requirements.txt
```

**Configure `config.json`**

You can either edit `config.json` directly or start from the example:

```bash
cp config.example.json config.json
```

Update `config.json` with:

- `base_url`, `auth_endpoint`, `post_endpoint`, `get_endpoint`
- `bearer_token`
- `output_base_path`
- `reports` list (report names)

**Run local ETL**

From repo root:

```bash
python -m local.talkdesk_local_etl
```

- Default dates: yesterday → today (local time).
- Override dates:

```bash
python -m local.talkdesk_local_etl --from 2024-11-01 --to 2024-11-02
```

Output:

- CSV files under `output/{report_name}/{from}_to_{to}.csv`
- Monitoring in `monitoring.db` (jobs and reports tables).

---

### 5.2 Databricks Pipeline (Driver‑Async)

**Prereqs**

- A Databricks workspace with:
  - Access to an Azure Key Vault–backed secret scope (e.g., `talkdesk-scope`).
  - Access to an ADLS Gen2 account.

**Step 1 — Secrets**

In Key Vault / Databricks secret scope `talkdesk-scope`, define:

- Talkdesk OAuth2 client credentials (for short‑lived access tokens):
  - `talkdesk-client-id`
  - `talkdesk-client-secret`
  - Optional: `talkdesk-token-url` (defaults to `https://api.talkdesk.com/oauth/token`)
- ADLS / storage:
  - `adls-client-id`
  - `adls-client-secret`
  - `adls-tenant-id`
  - `adls-storage-account`
  - `talkdesk-container`

**Step 2 — DDL / Seeding (run once)**

Upload and run as Python notebooks or scripts:

- `ddl/ddl_talkdesk_config.py`
- `ddl/ddl_talkdesk_monitoring.py`

These will (for `ENV=prod`):

- Create DB: `talkdesk_prod`
- Create tables:
  - `report_config`, `endpoint_config`
  - `job_monitoring`, `report_monitoring`
- Seed:
  - Dev + prod rows in `endpoint_config`
  - Default reports (agent_activity, call_volume, etc.) for both `dev` and `prod` in `report_config`

**Step 3 — Databricks ETL job (driver‑async)**

Upload `databricks/talkdesk_databricks_etl.py` as a notebook or script in a job.

Optional widgets:

```python
dbutils.widgets.text("from_date", "", "From date (YYYY-MM-DD)")
dbutils.widgets.text("to_date", "", "To date (YYYY-MM-DD)")
```

Run (driver‑async):

- Default (auto yesterday → today):

  - Leave widgets blank, run the job.

- Manual date range:

  - Set `from_date` / `to_date` widgets and run.

**What happens**

- Job fails fast if any required secrets are missing.
- For each enabled report in `report_config` for the current `env`:
  - Calls Talkdesk to generate and download the report.
  - Writes CSV to ADLS under:
    - `abfss://{container}@{storage_account}.dfs.core.windows.net/talkdesk/{report_name}/{from}_to_{to}.csv`
- Job and report metrics are written to the monitoring tables.

---

### 5.3 Databricks Pipeline (Spark‑Distributed Example)

For larger workloads (hundreds/thousands of reports, or much larger per‑report sizes), you can use the distributed variant as a scalability pattern.

**How it works**

- Uses the same secrets, `ENV`, and Delta tables as the driver‑async job.
- Joins `report_config` and `endpoint_config` into a single Spark DataFrame.
- Repartitions by report (using `PARTITION_TARGET_SIZE = 100`) and uses `foreachPartition` so each Spark executor:
  - Creates its own `aiohttp.ClientSession` + `TokenManager`.
  - Runs an asyncio loop to process the reports in that partition in parallel.
  - Writes CSVs to ADLS and logs into `{DB_NAME}.report_monitoring`.
- After all partitions complete, it derives job status from the monitoring table and calls `log_job_end`.

**When to use it**

- Prefer `talkdesk_databricks_etl.py` when:
  - You have up to ~50 reports, each up to ~50k rows.
  - You want a simpler, easier‑to‑debug driver‑based implementation.
- Prefer `talkdesk_databricks_etl_distributed.py` when:
  - Report counts are in the hundreds/thousands, or
  - Per‑report CSVs are large enough that driver memory or API rate limits become concerns.

**How to run**

- Upload `databricks/talkdesk_databricks_etl_distributed.py` as a separate notebook or job.
- Use the same widgets and secrets setup as the driver‑async job:

  ```python
  dbutils.widgets.text("env", "prod", "Environment (dev/prod/qa)")
  dbutils.widgets.text("from_date", "", "From date (YYYY-MM-DD)")
  dbutils.widgets.text("to_date", "", "To date (YYYY-MM-DD)")
  ```

- Configure the cluster with enough workers to benefit from partition‑level parallelism (e.g., several executors if you have many reports).

---

## 6. Monitoring & Analytics

### 6.1 Last 7 days job summary

```sql
SELECT
    date(start_time) AS run_date,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_runs,
    SUM(CASE WHEN status = 'PARTIAL_SUCCESS' THEN 1 ELSE 0 END) AS partial_runs,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs
FROM talkdesk_prod.job_monitoring
WHERE date(start_time) >= date('now', '-7 days')
GROUP BY date(start_time)
ORDER BY run_date DESC;
```

### 6.2 Per‑report status over last 7 days

```sql
SELECT
    report_name,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count
FROM talkdesk_prod.report_monitoring
WHERE date(start_time) >= date('now', '-7 days')
GROUP BY report_name
ORDER BY report_name;
```

### 6.3 Error details

```sql
SELECT
    report_name,
    run_id,
    error_message,
    start_time,
    end_time
FROM talkdesk_prod.report_monitoring
WHERE status = 'FAILED'
  AND date(start_time) >= date('now', '-7 days')
ORDER BY start_time DESC;
```

### 6.4 Daily row count summary

```sql
SELECT
    date(start_time) AS date,
    report_name,
    SUM(rows_written) AS total_rows
FROM talkdesk_prod.report_monitoring
WHERE date(start_time) >= date('now', '-7 days')
  AND status = 'SUCCESS'
GROUP BY date(start_time), report_name
ORDER BY date DESC, report_name;
```

---

## 7. Extensibility

- **Add a new report**:
  - Databricks:
    - Insert a row into `talkdesk_prod.report_config` for that `report_name` with `enabled=true` and correct `endpoint_type`, `retries`, `timeout_sec`, `env`.
  - Local:
    - Add the string to `reports` array in `config.json` (or the example and copy it).

- **Add a new environment**:
  - Add a new `env` value in both `endpoint_config` and `report_config`.
  - Use the Databricks `env` widget (read by `databricks/talkdesk_databricks_etl.py`) to select which `{DB_NAME} = talkdesk_{ENV}` database to target.

- **Change retry behavior**:
  - Adjust `_with_retries` in `local/async_utils.py` and/or `retry_request` in `databricks/talkdesk_databricks_etl.py`.

---

## 8. Helpful Files

- `README.md` (this file): full architecture + usage guide.
- `config.example.json`: starting point for your own `config.json`.
- `ddl/ddl_talkdesk_config.py`: one‑time script to create and seed config tables in Databricks.
- `ddl/ddl_talkdesk_monitoring.py`: one‑time script to create monitoring tables.
- `local/talkdesk_local_etl.py`: local entrypoint (run via `python -m local.talkdesk_local_etl`).
- `databricks/talkdesk_databricks_etl.py`: main Databricks job script.

## 9. References

- Talkdesk Developers: https://developers.talkdesk.com/
- Talkdesk Explore API: https://developers.talkdesk.com/docs/explore-api
- Python asyncio: https://docs.python.org/3/library/asyncio.html
- aiohttp client: https://docs.aiohttp.org/en/stable/
- Apache Spark SQL: https://spark.apache.org/docs/latest/sql-programming-guide.html
- Delta Lake: https://docs.delta.io/latest/index.html
- Databricks widgets: https://docs.databricks.com/en/notebooks/widgets.html
- Databricks secret scopes: https://docs.databricks.com/en/security/secrets/secret-scopes.html
- ADLS Gen2 + Databricks: https://learn.microsoft.com/azure/databricks/connect/storage/azure-storage
- pandas I/O: https://pandas.pydata.org/docs/user_guide/io.html
- SQLite: https://www.sqlite.org/docs.html
