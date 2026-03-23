"""
Example minimal DAG: two tasks, no database required.
Replace with real ETL tasks using run_sql_file() when you have SQL and DB connection.
"""

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)


def task_start():
    """First task - just logs."""
    logger.info("Example DAG started")


def task_end():
    """Second task - just logs."""
    logger.info("Example DAG completed")


with DAG(
    dag_id="example_minimal",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["example"],
) as dag:
    start = PythonOperator(
        task_id="start",
        python_callable=task_start,
    )
    end = PythonOperator(
        task_id="end",
        python_callable=task_end,
    )

    start >> end
