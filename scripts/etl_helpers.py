"""ETL helper functions for DAG tasks."""

import io
import logging
import os
from pathlib import Path

import pandas as pd

from scripts.csv_helpers import DATA_DIR, load_csv
from scripts.db import get_connection, execute_sql_file

logger = logging.getLogger(__name__)

# Base path for SQL files (mounted at /opt/airflow in container)
SQL_DIR = Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow")) / "sql"


def run_sql_file(filename: str) -> None:
    """Execute a SQL file from the sql/ directory."""
    filepath = SQL_DIR / filename
    if not filepath.exists():
        raise FileNotFoundError(f"SQL file not found: {filepath}")
    with get_connection() as conn:
        execute_sql_file(conn, str(filepath))
    logger.info("Executed SQL file: %s", filename)


def load_csv_to_db(csv_path: str, table: str, schema: str | None = None) -> None:
    """
    Load a CSV file from data/ into a Postgres table using COPY.

    The table must exist and have columns matching the CSV headers.
    Uses COPY for efficient bulk load.

    Args:
        csv_path: Relative path under data/ or absolute path to CSV.
        table: Target table name.
        schema: Optional schema name (e.g. 'staging'). If None, uses public.
    """
    df = load_csv(csv_path)
    if df.empty:
        logger.warning("CSV %s is empty, skipping load", csv_path)
        return
    qualified = f'"{schema}"."{table}"' if schema else f'"{table}"'
    columns = ",".join(f'"{c}"' for c in df.columns)
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.copy_expert(
                f'COPY {qualified} ({columns}) FROM STDIN WITH (FORMAT csv)',
                buffer,
            )
    logger.info("Loaded CSV %s into %s (%d rows)", csv_path, qualified, len(df))
