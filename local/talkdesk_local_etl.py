import argparse
import asyncio
from datetime import datetime, timedelta

import aiohttp

from .async_utils import (
    load_config,
    get_auth_token,
    generate_report_id,
    download_report,
    write_to_csv,
)
from .monitoring_db import (
    init_db,
    start_job,
    finish_job,
    start_report,
    finish_report,
)


def parse_args():
    parser = argparse.ArgumentParser(description="TalkDesk Async ETL")

    parser.add_argument("--from", dest="from_date", help="From date YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", help="To date YYYY-MM-DD")

    return parser.parse_args()


def auto_dates():
    today = datetime.today()
    yesterday = today - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")


async def process_report(
    session,
    report_name,
    from_date,
    to_date,
    token,
    config,
    job_id: int,
):
    report_row_id = start_report(job_id, report_name)
    try:
        print(f"Starting: {report_name}")

        report_id = await generate_report_id(
            session, report_name, from_date, to_date, token, config
        )
        if not report_id:
            msg = f"No report_id for {report_name}. Skipping."
            print(msg)
            finish_report(report_row_id, "FAILED", error_message=msg)
            return

        data = await download_report(session, report_id, token, config)
        if not data:
            msg = f"No data for {report_name}. Skipping."
            print(msg)
            finish_report(report_row_id, "FAILED", error_message=msg)
            return

        output_path = (
            f"{config['output_base_path']}/{report_name}/{from_date}_to_{to_date}.csv"
        )
        write_to_csv(data, output_path)

        print(f"Completed: {report_name}")
        finish_report(report_row_id, "SUCCESS", output_path=output_path)
    except Exception as exc:
        msg = f"Unhandled error for {report_name}: {exc}"
        print(msg)
        finish_report(report_row_id, "FAILED", error_message=msg)


async def main():
    init_db()

    config = load_config()
    args = parse_args()

    if args.from_date and args.to_date:
        from_date = args.from_date
        to_date = args.to_date
    else:
        from_date, to_date = auto_dates()

    print(f"Running from {from_date} to {to_date}")

    job_id = start_job(from_date, to_date)

    try:
        async with aiohttp.ClientSession() as session:
            token = await get_auth_token(session, config)

            tasks = [
                process_report(
                    session,
                    report_name,
                    from_date,
                    to_date,
                    token,
                    config,
                    job_id,
                )
                for report_name in config["reports"]
            ]

            # Exception-safe parallel execution: individual task failures are
            # handled inside process_report so gather completes cleanly.
            await asyncio.gather(*tasks, return_exceptions=False)

        finish_job(job_id, "SUCCESS")
        print("\nAll reports completed.")
    except Exception as exc:
        msg = f"Job-level failure: {exc}"
        print(msg)
        finish_job(job_id, "FAILED", error_message=msg)


if __name__ == "__main__":
    asyncio.run(main())
