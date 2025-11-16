"""
Distributed variant of the Talkdesk ETL that demonstrates how to
parallelize work across Spark workers using foreachPartition.

This script is intended as a scalability pattern for larger workloads
(hundreds/thousands of reports), complementary to the driver-async
implementation in `talkdesk_databricks_etl.py`.
"""

import asyncio
import io
import logging
import uuid
from datetime import datetime, timezone
from typing import Iterable

import aiohttp
import pandas as pd
from pyspark.sql import Row
from pyspark.sql import functions as F

from .talkdesk_databricks_etl import (
    DB_NAME,
    ENV,
    TokenManager,
    download_report,
    fetch_report_id,
    get_dates,
    load_secrets,
    log_job_end,
    log_job_start,
    log_report,
    configure_adls,
    logger,
)


# Target number of reports per partition when distributing work.
PARTITION_TARGET_SIZE = 100


def build_report_df():
    """
    Join report_config and endpoint_config into a single DataFrame
    so each row carries both report and endpoint metadata.
    """
    report_df = spark.table(f"{DB_NAME}.report_config").filter(
        f"enabled = true AND env = '{ENV}'"
    )
    endpoint_df = spark.table(f"{DB_NAME}.endpoint_config").filter(
        f"env = '{ENV}'"
    )
    return report_df.join(endpoint_df, "endpoint_type")


async def _process_partition_async(
    rows: list[Row],
    secrets: dict,
    run_id: str,
    dfrom: str,
    dto: str,
) -> None:
    """
    Async worker that processes all reports in a single Spark partition.
    Runs on a Spark executor.
    """
    storage_account = secrets["storage_account"]
    container = secrets["container"]

    async with aiohttp.ClientSession() as session:
        token_manager = TokenManager(
            session,
            secrets["talkdesk_client_id"],
            secrets["talkdesk_client_secret"],
            secrets["talkdesk_token_url"],
        )

        tasks = []
        for row in rows:
            ep = row.asDict()
            tasks.append(
                _process_single_report(
                    session,
                    run_id,
                    row,
                    ep,
                    dfrom,
                    dto,
                    token_manager,
                    storage_account,
                    container,
                )
            )

        await asyncio.gather(*tasks)


async def _process_single_report(
    session: aiohttp.ClientSession,
    run_id: str,
    rpt_row: Row,
    ep: dict,
    dfrom: str,
    dto: str,
    token_manager: TokenManager,
    storage_account: str,
    container: str,
) -> None:
    """
    Mirrors the logic from process_report in the driver-async script,
    but works with a Spark Row.
    """
    report_name = rpt_row.report_name
    try:
        logger.info(f"[{run_id}] (distributed) Starting report: {report_name} ({dfrom} -> {dto})")

        token_val = await token_manager.get_token()

        logger.info(f"[{run_id}] (distributed) {report_name}: requesting report_id")
        r_id = await fetch_report_id(
            session, ep, token_val, report_name, dfrom, dto
        )
        if not r_id:
            log_report(
                run_id,
                report_name,
                "FAILED",
                0,
                "Report ID missing",
                dfrom,
                dto,
            )
            return

        data = await download_report(session, ep, token_val, r_id)
        if not data:
            log_report(
                run_id,
                report_name,
                "FAILED",
                0,
                "No data returned",
                dfrom,
                dto,
            )
            return
        logger.info(
            f"[{run_id}] (distributed) {report_name}: CSV downloaded, loading into DataFrame"
        )

        df = pd.read_csv(io.StringIO(data))
        logger.info(
            f"[{run_id}] (distributed) {report_name}: normalized to {len(df)} rows"
        )

        output_path = (
            f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
            f"talkdesk/{report_name}/{dfrom}_to_{dto}.csv"
        )
        logger.info(
            f"[{run_id}] (distributed) {report_name}: writing CSV to {output_path}"
        )
        df.to_csv(output_path, index=False)

        log_report(
            run_id,
            report_name,
            "SUCCESS",
            len(df),
            None,
            dfrom,
            dto,
        )
        logger.info(
            f"[{run_id}] (distributed) Completed report: {report_name}"
        )
    except Exception as exc:
        logger.error(
            f"[{run_id}] (distributed) {report_name}: unexpected error: {exc}"
        )
        log_report(
            run_id,
            report_name,
            "FAILED",
            0,
            str(exc),
            dfrom,
            dto,
        )


def process_partition(
    partition: Iterable[Row],
    secrets_broadcast,
    run_id: str,
    dfrom: str,
    dto: str,
) -> None:
    """
    Entry point for Spark's foreachPartition.
    Runs on worker nodes; spins up an asyncio loop per partition.
    """
    rows = list(partition)
    if not rows:
        return

    secrets = secrets_broadcast.value

    asyncio.run(_process_partition_async(rows, secrets, run_id, dfrom, dto))


async def main() -> None:
    """
    Distributed ETL entrypoint.
    Uses Spark partitions + foreachPartition to spread work across the cluster.
    """
    secrets = load_secrets()

    storage_account = secrets["storage_account"]
    container = secrets["container"]

    configure_adls(
        storage_account,
        secrets["client_id"],
        secrets["client_secret"],
        secrets["tenant_id"],
    )

    dfrom, dto = get_dates()
    run_id = str(uuid.uuid4())

    report_df = build_report_df()
    total_reports = report_df.count()

    log_job_start(run_id, dfrom, dto, total_reports)
    logger.info(
        f"(distributed) Started ETL Run: {run_id} for {dfrom} -> {dto} with {total_reports} reports"
    )

    ok = 0
    fail = 0

    try:
        secrets_bc = spark.sparkContext.broadcast(secrets)

        # Decide number of partitions based on a simple target size.
        num_partitions = max(
            1, (total_reports + PARTITION_TARGET_SIZE - 1) // PARTITION_TARGET_SIZE
        )

        (
            report_df.repartition(num_partitions)
            .rdd.foreachPartition(
                lambda it: process_partition(it, secrets_bc, run_id, dfrom, dto)
            )
        )

        # After all partitions complete, derive job status from report_monitoring.
        rep_df = spark.table(f"{DB_NAME}.report_monitoring").filter(
            F.col("run_id") == run_id
        )
        ok = rep_df.filter(F.col("status") == "SUCCESS").count()
        fail = rep_df.filter(F.col("status") == "FAILED").count()

        if fail == 0:
            status = "SUCCESS"
        elif ok == 0:
            status = "FAILED"
        else:
            status = "PARTIAL_SUCCESS"

        log_job_end(run_id, status, ok, fail)
        logger.info(
            f"(distributed) Finished ETL — Status={status}, Success={ok}, Failed={fail}"
        )
    except Exception as exc:
        msg = f"(distributed) Job-level failure: {exc}"
        logger.error(msg)
        log_job_end(run_id, "FAILED", ok, fail, error_message=msg)


if __name__ == "__main__":
    asyncio.run(main())
    print("Talkdesk Distributed ETL Complete.")

