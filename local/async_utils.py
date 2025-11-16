import aiohttp
import asyncio
import json
import os
from pathlib import Path
from typing import Any, Callable, Awaitable, Optional

import pandas as pd


def load_config():
    # Resolve config.json from project root (one level up from this file)
    config_path = Path(__file__).resolve().parent.parent / "config.json"
    with config_path.open() as f:
        return json.load(f)


async def _with_retries(
    op: Callable[[], Awaitable[Any]],
    *,
    label: str,
    max_attempts: int = 3,
    base_delay: float = 1.0,
) -> Any:
    """
    Generic retry helper with exponential backoff.
    Retries on aiohttp / timeout errors and on 5xx responses
    signaled by raising inside `op`.
    """
    delay = base_delay
    last_exc: Optional[BaseException] = None

    for attempt in range(1, max_attempts + 1):
        try:
            return await op()
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            last_exc = exc
            if attempt == max_attempts:
                print(f"{label} failed after {attempt} attempts: {exc}")
                raise
            print(f"{label} attempt {attempt} failed: {exc}; retrying in {delay}s")
        except Exception as exc:
            # For other exceptions, do not retry unless explicitly thrown
            last_exc = exc
            print(f"{label} non-retryable error: {exc}")
            raise

        await asyncio.sleep(delay)
        delay *= 2

    if last_exc:
        raise last_exc


async def get_auth_token(session, config):
    url = config["base_url"] + config["auth_endpoint"]

    headers = {
        "Authorization": f"Bearer {config['bearer_token']}",
        "Content-Type": "application/json",
    }

    async def _op():
        async with session.post(url, headers=headers) as resp:
            if resp.status != 200:
                body = await resp.text()
                # Treat all non-200 as failures; upstream decides whether to abort job.
                raise Exception(f"Auth failed (status {resp.status}): {body}")

            data = await resp.json()
            print("Access Token Acquired.")
            return data.get("access_token")

    return await _with_retries(_op, label="get_auth_token")


async def generate_report_id(session, report_name, from_date, to_date, token, config):
    url = config["base_url"] + config["post_endpoint"]

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    body = {"report_name": report_name, "from": from_date, "to": to_date}

    async def _op():
        async with session.post(url, headers=headers, json=body) as resp:
            if resp.status != 200:
                text = await resp.text()
                # Retry on 5xx and 429; treat other 4xx as permanent.
                if resp.status == 429 or 500 <= resp.status < 600:
                    raise aiohttp.ClientError(
                        f"Server/Rate error {resp.status} for {report_name}: {text}"
                    )
                print(f"Failed report_id for {report_name}: {text}")
                return None

            data = await resp.json()
            return data.get("report_id")

    return await _with_retries(_op, label=f"generate_report_id:{report_name}")


async def download_report(session, report_id, token, config):
    url = config["base_url"] + config["get_endpoint"]

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "text/csv",
    }

    params = {"report_id": report_id}

    async def _op():
        async with session.get(url, headers=headers, params=params) as resp:
            if resp.status != 200:
                text = await resp.text()
                if resp.status == 429 or 500 <= resp.status < 600:
                    raise aiohttp.ClientError(
                        f"Server/Rate error {resp.status} for report_id={report_id}: {text}"
                    )
                print(f"GET failed for report_id={report_id}: {text}")
                return None

            # For Explore APIs returning CSV, we want the raw text.
            return await resp.text()

    return await _with_retries(_op, label=f"download_report:{report_id}")


def write_to_csv(csv_text: str, output_path: str) -> None:
    """
    Persist raw CSV text returned by the API to disk.
    The Talkdesk Explore API already returns CSV, so no transformation is needed here.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(csv_text)
    print(f"Saved → {output_path}")
