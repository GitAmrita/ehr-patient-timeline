"""
setup_db.py

Creates a DuckDB database and registers each parsed Parquet file as a view
in the 'raw' schema. Must be run once before `dbt run`.

Re-running is safe — all views are CREATE OR REPLACE.

Usage:
    python scripts/setup_db.py
"""

import logging
from pathlib import Path

import duckdb

PROJECT_DIR = Path(__file__).parent.parent
DB_PATH = PROJECT_DIR / "ehr.duckdb"
PARSED_DIR = PROJECT_DIR / "raw" / "parsed"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# Parquet files to register — add new ones here as the pipeline grows
SOURCES = [
    "demographics_raw",
    "encounters",
    "labs",
    "notes",
    "nlp_entities",
]


def setup(db_path: Path = DB_PATH, parsed_dir: Path = PARSED_DIR) -> None:
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

    for table in SOURCES:
        parquet_path = parsed_dir / f"{table}.parquet"

        if not parquet_path.exists():
            log.warning("Skipping %s — file not found: %s", table, parquet_path)
            continue

        conn.execute(
            f"CREATE OR REPLACE VIEW raw.{table} AS "
            f"SELECT * FROM read_parquet('{parquet_path}')"
        )
        count = conn.execute(f"SELECT COUNT(*) FROM raw.{table}").fetchone()[0]
        log.info("Registered raw.%-25s — %d rows", table, count)

    conn.close()
    log.info("Database ready at %s", db_path)


if __name__ == "__main__":
    setup()
