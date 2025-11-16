# =====================================================================
# 00 — IMPORTS
# =====================================================================

import aiohttp
import asyncio
import logging
import uuid
from datetime import datetime, timedelta
from typing import Dict, Tuple

import pandas as pd
from pyspark.sql import Row


SECRET_SCOPE = "talkdesk-scope"
DB_NAME = "talkdesk_prod"


# =====================================================================
# 01 — ENV + SECRETS / ADLS HELPERS
# =====================================================================

ENV = "prod"


def load_secrets() -> Dict[str, str]:
    """
    Load all required secrets from Key Vault.
    Raise a RuntimeError if any are missing so the job fails fast.
    """
    scope = SECRET_SCOPE
    keys = {
        "talkdesk_token": "talkdesk-token",
        "client_id": "adls-client-id",
        "client_secret": "adls-client-secret",
        "tenant_id": "adls-tenant-id",
        "storage_account": "adls-storage-account",
        "container": "talkdesk-container",
    }

    values = {}
    missing = []

    for logical, secret_name in keys.items():
        try:
            values[logical] = dbutils.secrets.get(scope, secret_name)
        except Exception:
            missing.append(secret_name)

    if missing:
        raise RuntimeError(f"Missing required secrets: {', '.join(missing)}")

    return values


def configure_adls(
    storage_account: str,
    client_id: str,
    client_secret: str,
    tenant_id: str,
) -> None:
    """
    Configure ADLS Gen2 OAuth using the provided credentials.
    """
    spark.conf.set(
        f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
        "OAuth",
    )
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
        client_id,
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
        client_secret,
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
    )

    print("ADLS Authentication configured.")


# =====================================================================
# 03 — LOGGING
# =====================================================================


def get_logger() -> logging.Logger:
    logger = logging.getLogger("talkdesk")
    logger.setLevel(logging.INFO)
    if len(logger.handlers) > 0:
        return logger
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s  %(levelname)s  %(message)s"))
    logger.addHandler(handler)
    return logger


logger = get_logger()


# =====================================================================
# 04 — CONFIG LOADERS (FROM DELTA; TABLES CREATED VIA SEPARATE DDL)
# =====================================================================


def load_report_configs(env: str) -> pd.DataFrame:
    df = spark.table(f"{DB_NAME}.report_config").filter(
        f"enabled = true AND env = '{env}'"
    )
    return df.toPandas()


def load_endpoint_configs(env: str) -> pd.DataFrame:
    df = spark.table(f"{DB_NAME}.endpoint_config").filter(f"env = '{env}'")
    return df.toPandas()


# =====================================================================
# 05 — MONITORING FUNCTIONS (TABLES CREATED VIA SEPARATE DDL)
# =====================================================================


def log_job_start(run_id: str, dfrom: str, dto: str, total: int) -> None:
    row = Row(
        run_id=run_id,
        from_date=dfrom,
        to_date=dto,
        start_time=datetime.utcnow(),
        end_time=None,
        status="RUNNING",
        total_reports=total,
        success_count=0,
        failed_count=0,
        error_message=None,
    )
    spark.createDataFrame([row]).write.mode("append").format("delta").saveAsTable(
        f"{DB_NAME}.job_monitoring"
    )


def _escape_sql_string(value: str) -> str:
    """Escape single quotes for safe embedding in Spark SQL string literals."""
    return value.replace("'", "''")


def log_job_end(
    run_id: str,
    status: str,
    ok: int,
    fail: int,
    error_message: str | None = None,
) -> None:
    # Escape single quotes in the error message
    if error_message is not None:
        escaped = _escape_sql_string(error_message)
        err_sql = f"'{escaped}'"
    else:
        err_sql = "NULL"

    spark.sql(f"""
        UPDATE talkdesk_prod.job_monitoring
        SET end_time = current_timestamp(),
            status = '{status}',
            success_count = {ok},
            failed_count = {fail},
            error_message = {err_sql}
        WHERE run_id = '{run_id}'
    """)


def log_report(
    run_id: str,
    report_name: str,
    status: str,
    rows: int,
    error: str | None,
    dfrom: str,
    dto: str,
) -> None:
    row = Row(
        run_id=run_id,
        report_name=report_name,
        from_date=dfrom,
        to_date=dto,
        start_time=datetime.utcnow(),
        end_time=datetime.utcnow(),
        status=status,
        rows_written=rows,
        error_message=error,
    )
    spark.createDataFrame([row]).write.mode("append").format("delta").saveAsTable(
        f"{DB_NAME}.report_monitoring"
    )


# =====================================================================
# 06 — HTTP + RETRY UTILITIES
# =====================================================================


def get_token():
    return dbutils.secrets.get("talkdesk-scope", "talkdesk-token")


async def retry_request(
    session,
    method: str,
    url: str,
    retries: int,
    backoff: float,
    timeout_sec: int | None = None,
    **kwargs,
) -> aiohttp.ClientResponse | None:
    """
    Generic HTTP retry with exponential backoff and optional timeout.
    Retries on exceptions and 5xx. Does not retry on <500.
    """
    for attempt in range(1, retries + 1):
        try:
            timeout = aiohttp.ClientTimeout(total=timeout_sec) if timeout_sec else None
            resp = await session.request(method, url, timeout=timeout, **kwargs)

            if resp.status < 500:
                return resp

            logger.warning(
                f"HTTP {method} {url} attempt {attempt} got {resp.status}"
            )
        except Exception as exc:
            logger.warning(
                f"HTTP {method} {url} attempt {attempt} failed: {exc}"
            )

        if attempt < retries:
            sleep_for = backoff * (2 ** (attempt - 1))
            await asyncio.sleep(sleep_for)

    return None


async def fetch_report_id(
    session,
    ep: dict,
    token: str,
    report_name: str,
    from_d: str,
    to_d: str,
) -> str | None:
    url = ep["base_url"] + ep["post_endpoint"]
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {"report_name": report_name, "from": from_d, "to": to_d}

    resp = await retry_request(
        session,
        "POST",
        url,
        ep["retries"],
        1,
        ep.get("timeout_sec"),
        json=body,
        headers=headers,
    )
    if not resp:
        return None
    return (await resp.json()).get("report_id")


async def download_report(
    session,
    ep: dict,
    token: str,
    report_id: str,
) -> dict | None:
    url = ep["base_url"] + ep["get_endpoint"]
    headers = {"Authorization": f"Bearer {token}"}

    resp = await retry_request(
        session,
        "GET",
        url,
        ep["retries"],
        1,
        ep.get("timeout_sec"),
        headers=headers,
        params={"report_id": report_id},
    )
    if not resp:
        return None
    return await resp.json()


# =====================================================================
# 07 — SINGLE REPORT PROCESSING (ASYNC)
# =====================================================================


async def process_report(
    session: aiohttp.ClientSession,
    run_id: str,
    rpt,
    ep: dict,
    dfrom: str,
    dto: str,
    token_val: str,
    storage_account: str,
    container: str,
) -> bool:
    try:
        logger.info(f"[{run_id}] Starting report: {rpt.report_name} ({dfrom} -> {dto})")

        # Step 1: Generate Report ID
        logger.info(f"[{run_id}] {rpt.report_name}: requesting report_id")
        r_id = await fetch_report_id(
            session, ep, token_val, rpt.report_name, dfrom, dto
        )
        if not r_id:
            log_report(
                run_id,
                rpt.report_name,
                "FAILED",
                0,
                "Report ID missing",
                dfrom,
                dto,
            )
            return False

        # Step 2: Download Data
        data = await download_report(session, ep, token_val, r_id)
        if not data:
            log_report(
                run_id,
                rpt.report_name,
                "FAILED",
                0,
                "No data returned",
                dfrom,
                dto,
            )
            return False
        logger.info(f"[{run_id}] {rpt.report_name}: data downloaded, normalizing JSON")

        df = pd.json_normalize(data)
        logger.info(f"[{run_id}] {rpt.report_name}: normalized to {len(df)} rows")

        # Step 3: Write to ADLS
        output_path = (
            f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
            f"talkdesk/{rpt.report_name}/{dfrom}_to_{dto}.csv"
        )
        logger.info(f"[{run_id}] {rpt.report_name}: writing CSV to {output_path}")
        df.to_csv(output_path, index=False)

        log_report(
            run_id,
            rpt.report_name,
            "SUCCESS",
            len(df),
            None,
            dfrom,
            dto,
        )
        logger.info(f"[{run_id}] Completed report: {rpt.report_name}")
        return True

    except Exception as exc:
        logger.error(f"[{run_id}] {rpt.report_name}: unexpected error: {exc}")
        log_report(
            run_id,
            rpt.report_name,
            "FAILED",
            0,
            str(exc),
            dfrom,
            dto,
        )
        return False


# =====================================================================
# 08 — MASTER ETL RUNNER
# =====================================================================


def get_dates() -> Tuple[str, str]:
    """
    Determine from/to dates.
    - If Databricks widgets 'from_date' and 'to_date' are defined and non-empty,
      use those.
    - Otherwise, default to yesterday -> today (UTC).
    """
    try:
        from_date = dbutils.widgets.get("from_date")
        to_date = dbutils.widgets.get("to_date")
        if from_date and to_date:
            return from_date, to_date
    except Exception:
        # Widgets not defined or not available; fall back to auto dates.
        pass

    today = datetime.utcnow()
    yesterday = today - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")


async def main() -> None:
    # Load secrets first; fail fast if anything is missing.
    secrets = load_secrets()

    storage_account = secrets["storage_account"]
    container = secrets["container"]

    configure_adls(
        storage_account,
        secrets["client_id"],
        secrets["client_secret"],
        secrets["tenant_id"],
    )

    report_cfg = load_report_configs(ENV)
    endpoint_cfg = load_endpoint_configs(ENV)

    dfrom, dto = get_dates()
    run_id = str(uuid.uuid4())

    log_job_start(run_id, dfrom, dto, len(report_cfg))
    logger.info(f"Started ETL Run: {run_id} for {dfrom} -> {dto}")

    ok = 0
    fail = 0

    try:
        async with aiohttp.ClientSession() as session:
            token_val = secrets["talkdesk_token"]

            tasks = []
            for _, rpt in report_cfg.iterrows():
                ep = (
                    endpoint_cfg[endpoint_cfg.endpoint_type == rpt.endpoint_type]
                    .iloc[0]
                    .to_dict()
                )
                ep["retries"] = rpt.retries
                ep["timeout_sec"] = rpt.timeout_sec
                tasks.append(
                    process_report(
                        session,
                        run_id,
                        rpt,
                        ep,
                        dfrom,
                        dto,
                        token_val,
                        storage_account,
                        container,
                    )
                )

            results = await asyncio.gather(*tasks)

        ok = sum(results)
        fail = len(results) - ok

        if fail == 0:
            status = "SUCCESS"
        elif ok == 0:
            status = "FAILED"
        else:
            status = "PARTIAL_SUCCESS"

        log_job_end(run_id, status, ok, fail)
        logger.info(f"Finished ETL — Status={status}, Success={ok}, Failed={fail}")
    except Exception as exc:
        msg = f"Job-level failure: {exc}"
        logger.error(msg)
        # Mark job as failed; ok/fail reflect whatever was counted before the exception.
        log_job_end(run_id, "FAILED", ok, fail, error_message=msg)


# =====================================================================
# 09 — EXECUTE ETL PIPELINE
# =====================================================================


asyncio.run(main())
print("Talkdesk ETL Complete.")
