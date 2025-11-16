import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple


DB_PATH = Path("monitoring.db")


@contextmanager
def _conn():
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with _conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                from_date TEXT NOT NULL,
                to_date TEXT NOT NULL,
                status TEXT,
                error_message TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL,
                report_name TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT,
                error_message TEXT,
                output_path TEXT,
                FOREIGN KEY(job_id) REFERENCES jobs(id)
            )
            """
        )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def start_job(from_date: str, to_date: str) -> int:
    with _conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO jobs (started_at, from_date, to_date, status)
            VALUES (?, ?, ?, ?)
            """,
            (_now(), from_date, to_date, "RUNNING"),
        )
        return cur.lastrowid


def _job_report_counts(cur, job_id: int) -> Tuple[int, int, int]:
    cur.execute(
        "SELECT COUNT(*) FROM reports WHERE job_id = ?",
        (job_id,),
    )
    total = cur.fetchone()[0]

    cur.execute(
        "SELECT COUNT(*) FROM reports WHERE job_id = ? AND status = 'SUCCESS'",
        (job_id,),
    )
    success = cur.fetchone()[0]

    cur.execute(
        "SELECT COUNT(*) FROM reports WHERE job_id = ? AND status = 'FAILED'",
        (job_id,),
    )
    failed = cur.fetchone()[0]

    return total, success, failed


def finish_job(job_id: int, status: str, error_message: Optional[str] = None) -> None:
    with _conn() as conn:
        cur = conn.cursor()
        # Normalize job status to SUCCESS / PARTIAL_SUCCESS / FAILED
        total, success, failed = _job_report_counts(cur, job_id)

        if total == 0:
            norm_status = "FAILED" if status.upper() != "SUCCESS" else "SUCCESS"
        elif failed == 0 and success == total:
            norm_status = "SUCCESS"
        elif success > 0 and failed > 0:
            norm_status = "PARTIAL_SUCCESS"
        else:
            norm_status = "FAILED"

        cur.execute(
            """
            UPDATE jobs
            SET finished_at = ?, status = ?, error_message = ?
            WHERE id = ?
            """,
            (_now(), norm_status, error_message, job_id),
        )


def start_report(job_id: int, report_name: str) -> int:
    with _conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO reports (job_id, report_name, started_at, status)
            VALUES (?, ?, ?, ?)
            """,
            (job_id, report_name, _now(), "RUNNING"),
        )
        return cur.lastrowid


def finish_report(
    report_id: int,
    status: str,
    *,
    error_message: Optional[str] = None,
    output_path: Optional[str] = None,
) -> None:
    with _conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE reports
            SET finished_at = ?, status = ?, error_message = ?, output_path = ?
            WHERE id = ?
            """,
            (_now(), status, error_message, output_path, report_id),
        )
