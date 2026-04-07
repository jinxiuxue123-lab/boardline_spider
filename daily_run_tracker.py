import os
import socket
import sqlite3
from contextlib import closing
from datetime import datetime
from pathlib import Path


DB_FILE = "products.db"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _connect() -> sqlite3.Connection:
    return sqlite3.connect(DB_FILE)


def ensure_daily_run_tables() -> None:
    with closing(_connect()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_type TEXT NOT NULL,
                trigger_mode TEXT DEFAULT 'manual',
                status TEXT DEFAULT 'running',
                host TEXT,
                pid INTEGER,
                log_file TEXT,
                note TEXT,
                started_at TEXT DEFAULT CURRENT_TIMESTAMP,
                finished_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_run_steps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                step_key TEXT NOT NULL,
                step_name TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                started_at TEXT,
                finished_at TEXT,
                progress_current INTEGER DEFAULT 0,
                progress_total INTEGER DEFAULT 0,
                message TEXT,
                log_excerpt TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(run_id) REFERENCES daily_runs(id),
                UNIQUE(run_id, step_key)
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_daily_runs_status_started_at
            ON daily_runs(status, started_at)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_daily_run_steps_run_id_status
            ON daily_run_steps(run_id, status)
            """
        )
        conn.commit()


def start_run(run_type: str, trigger_mode: str = "manual", log_file: str = "", note: str = "") -> int:
    ensure_daily_run_tables()
    with closing(_connect()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO daily_runs (run_type, trigger_mode, status, host, pid, log_file, note, started_at)
            VALUES (?, ?, 'running', ?, ?, ?, ?, ?)
            """,
            (
                run_type,
                trigger_mode,
                socket.gethostname(),
                os.getpid(),
                log_file or "",
                note or "",
                _now(),
            ),
        )
        conn.commit()
        return int(cur.lastrowid)


def update_run(run_id: int, *, status: str | None = None, log_file: str | None = None, note: str | None = None) -> None:
    ensure_daily_run_tables()
    fields = []
    values = []
    if status is not None:
        fields.append("status = ?")
        values.append(status)
        if status in {"success", "failed", "stopped"}:
            fields.append("finished_at = ?")
            values.append(_now())
    if log_file is not None:
        fields.append("log_file = ?")
        values.append(log_file)
    if note is not None:
        fields.append("note = ?")
        values.append(note)
    if not fields:
        return
    values.append(int(run_id))
    with closing(_connect()) as conn:
        cur = conn.cursor()
        cur.execute(f"UPDATE daily_runs SET {', '.join(fields)} WHERE id = ?", values)
        conn.commit()


def start_step(run_id: int, step_key: str, step_name: str, message: str = "") -> None:
    ensure_daily_run_tables()
    now = _now()
    with closing(_connect()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO daily_run_steps (
                run_id, step_key, step_name, status, started_at, finished_at,
                progress_current, progress_total, message, updated_at
            )
            VALUES (?, ?, ?, 'running', ?, NULL, 0, 0, ?, ?)
            ON CONFLICT(run_id, step_key) DO UPDATE SET
                step_name = excluded.step_name,
                status = 'running',
                started_at = COALESCE(daily_run_steps.started_at, excluded.started_at),
                finished_at = NULL,
                message = excluded.message,
                updated_at = excluded.updated_at
            """,
            (int(run_id), step_key, step_name, now, message or "", now),
        )
        conn.commit()


def update_step_progress(
    run_id: int,
    step_key: str,
    *,
    current: int | None = None,
    total: int | None = None,
    message: str | None = None,
    log_excerpt: str | None = None,
) -> None:
    ensure_daily_run_tables()
    fields = ["status = 'running'", "updated_at = ?"]
    values = [_now()]
    if current is not None:
        fields.append("progress_current = ?")
        values.append(int(current))
    if total is not None:
        fields.append("progress_total = ?")
        values.append(int(total))
    if message is not None:
        fields.append("message = ?")
        values.append(message)
    if log_excerpt is not None:
        fields.append("log_excerpt = ?")
        values.append(log_excerpt)
    values.extend([int(run_id), step_key])
    with closing(_connect()) as conn:
        cur = conn.cursor()
        cur.execute(
            f"UPDATE daily_run_steps SET {', '.join(fields)} WHERE run_id = ? AND step_key = ?",
            values,
        )
        conn.commit()


def finish_step(run_id: int, step_key: str, status: str = "success", message: str = "") -> None:
    ensure_daily_run_tables()
    now = _now()
    with closing(_connect()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE daily_run_steps
            SET status = ?, finished_at = ?, message = ?, updated_at = ?
            WHERE run_id = ? AND step_key = ?
            """,
            (status, now, message or "", now, int(run_id), step_key),
        )
        conn.commit()


def get_env_run_id() -> int | None:
    raw = os.getenv("DAILY_RUN_ID", "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def get_env_step_key(default: str = "") -> str:
    return os.getenv("DAILY_RUN_STEP_KEY", default).strip()
