"""Database connection helpers for ETL tasks."""

import logging
import os
from contextlib import contextmanager
from typing import Generator

import psycopg2
from psycopg2.extensions import connection as Connection

logger = logging.getLogger(__name__)


def get_connection_params() -> dict[str, str | int]:
    """Build connection params from environment variables."""
    return {
        "host": os.environ.get("DB_HOST", "localhost"),
        "port": int(os.environ.get("DB_PORT", "5432")),
        "database": os.environ.get("DB_DATABASE", ""),
        "user": os.environ.get("DB_USER", ""),
        "password": os.environ.get("DB_PASSWORD", ""),
        "connect_timeout": int(os.environ.get("DB_CONNECTION_TIMEOUT_MILLIS", "10000")) // 1000,
    }


@contextmanager
def get_connection() -> Generator[Connection, None, None]:
    """
    Context manager for database connections.
    Ensures proper cleanup and rollback on error.
    """
    params = get_connection_params()
    conn: Connection | None = None
    try:
        conn = psycopg2.connect(**params)
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        logger.exception("Database error: %s", e)
        raise
    finally:
        if conn:
            conn.close()


def execute_sql_file(conn: Connection, filepath: str) -> None:
    """
    Execute a SQL file against the given connection.
    Supports multiple statements separated by semicolons.
    """
    with open(filepath, encoding="utf-8") as f:
        sql = f.read()
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    with conn.cursor() as cur:
        for stmt in statements:
            if stmt:
                cur.execute(stmt)
