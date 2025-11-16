# talkdesk-async-etl

Async, production‑ready Talkdesk reporting ETL with:

- Two runtimes:
  - **Local** (SQLite + filesystem)
  - **Databricks** (Delta Lake + ADLS Gen2 + Key Vault)
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
  - Delta Lake tables in `talkdesk_prod` database
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
│   └── talkdesk_databricks_etl.py  # Databricks ETL job
│
└── ddl/                        # One‑time DDL + seeding for Delta tables
    ├── ddl_talkdesk_config.py      # report_config + endpoint_config
    └── ddl_talkdesk_monitoring.py  # job_monitoring + report_monitoring
```

---

## 3. High‑Level Architecture

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
     - `GET` to download report JSON
   - All HTTP calls use retries with exponential backoff and respect per‑report `timeout_sec`.

4. **Persistence**
   - JSON is normalized using `pandas.json_normalize`.
   - CSV results are written to:
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
  - `talkdesk_prod.endpoint_config`
    - `endpoint_type, base_url, auth_endpoint, post_endpoint, get_endpoint, env`
    - Seeded with `standard` endpoints for `dev` and `prod`.
  - `talkdesk_prod.report_config`
    - `report_name, enabled, endpoint_type, retries, timeout_sec, env`
    - Seeded with standard Talkdesk reports for both `dev` and `prod`.

- Monitoring tables:
  - `talkdesk_prod.job_monitoring`
    - `run_id, from_date, to_date, start_time, end_time, status (SUCCESS/PARTIAL_SUCCESS/FAILED), total_reports, success_count, failed_count, error_message`
  - `talkdesk_prod.report_monitoring`
    - `run_id, report_name, from_date, to_date, start_time, end_time, status, rows_written, error_message`

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
  3. `download_report` → JSON via GET.
  4. `write_to_csv` → CSV under `output/{report}/{from}_to_{to}.csv`.
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
  - Wrap remote calls using `retry_request`.

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
    5. Create a single `ClientSession` and reuse the Talkdesk token across all reports.
    6. Spawn tasks for each enabled report; gather results.
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

### 5.2 Databricks Pipeline

**Prereqs**

- A Databricks workspace with:
  - Access to an Azure Key Vault–backed secret scope (e.g., `talkdesk-scope`).
  - Access to an ADLS Gen2 account.

**Step 1 — Secrets**

In Key Vault / Databricks secret scope `talkdesk-scope`, define:

- `talkdesk-token`
- `adls-client-id`
- `adls-client-secret`
- `adls-tenant-id`
- `adls-storage-account`
- `talkdesk-container`

**Step 2 — DDL / Seeding (run once)**

Upload and run as Python notebooks or scripts:

- `ddl/ddl_talkdesk_config.py`
- `ddl/ddl_talkdesk_monitoring.py`

These will:

- Create DB: `talkdesk_prod`
- Create tables:
  - `report_config`, `endpoint_config`
  - `job_monitoring`, `report_monitoring`
- Seed:
  - Dev + prod rows in `endpoint_config`
  - Default reports (agent_activity, call_volume, etc.) for both `dev` and `prod` in `report_config`

**Step 3 — Databricks ETL job**

Upload `databricks/talkdesk_databricks_etl.py` as a notebook or script in a job.

Optional widgets:

```python
dbutils.widgets.text("from_date", "", "From date (YYYY-MM-DD)")
dbutils.widgets.text("to_date", "", "To date (YYYY-MM-DD)")
```

Run:

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
  - Update `ENV` constant in `databricks/talkdesk_databricks_etl.py` or pass it via a job parameter.

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
