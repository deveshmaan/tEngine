from __future__ import annotations

import datetime as dt
import sqlite3
from pathlib import Path
from typing import Iterable

DEFAULT_MIGRATIONS_DIR = Path(__file__).with_name("schema_migrations")


def connect_db(path: str | Path) -> sqlite3.Connection:
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    for pragma in (
        "PRAGMA journal_mode=WAL;",
        "PRAGMA synchronous=NORMAL;",
        "PRAGMA busy_timeout=5000;",
        "PRAGMA foreign_keys=ON;",
    ):
        conn.execute(pragma)
    return conn


def run_migrations(conn: sqlite3.Connection, migrations_dir: Path | None = None) -> None:
    migrations_dir = migrations_dir or DEFAULT_MIGRATIONS_DIR
    migrations_dir.mkdir(parents=True, exist_ok=True)
    _ensure_tracking_table(conn)
    applied = _applied(conn)
    for sql_file in sorted(migrations_dir.glob("*.sql")):
        name = sql_file.name
        if name in applied:
            continue
        with sql_file.open("r", encoding="utf-8") as handle:
            script = handle.read()
        conn.executescript(script)
        conn.execute(
            "INSERT INTO schema_migrations(name, applied_at) VALUES (?, ?)",
            (name, dt.datetime.now(dt.timezone.utc).isoformat()),
        )
    conn.commit()


def _ensure_tracking_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations(
            name TEXT PRIMARY KEY,
            applied_at TEXT NOT NULL
        );
        """
    )
    conn.commit()


def _applied(conn: sqlite3.Connection) -> set[str]:
    cur = conn.execute("SELECT name FROM schema_migrations")
    return {row[0] for row in cur.fetchall()}


__all__ = ["connect_db", "run_migrations", "DEFAULT_MIGRATIONS_DIR"]
