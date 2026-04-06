"""
db.py

DuckDB connection management.
Opens a single read-only connection at startup and reuses it across requests.
Read-only prevents accidental writes and allows concurrent readers.
"""

from __future__ import annotations

from pathlib import Path
import duckdb

_conn: duckdb.DuckDBPyConnection | None = None

DB_PATH = Path(__file__).parent.parent / "ehr.duckdb"


def get_conn() -> duckdb.DuckDBPyConnection:
    global _conn
    if _conn is None:
        _conn = duckdb.connect(str(DB_PATH), read_only=True)
    return _conn


def close_conn() -> None:
    global _conn
    if _conn is not None:
        _conn.close()
        _conn = None
