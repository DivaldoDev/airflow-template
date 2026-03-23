"""
Example DAG: CSV migration pattern.
Reads CSV from data/, applies a simple transform, saves to output CSV.
For DB load, use load_csv_to_db() from scripts.etl_helpers.
"""

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.csv_helpers import load_csv, save_csv

logger = logging.getLogger(__name__)


def extract_csv():
    """Extract: load source CSV from data/."""
    df = load_csv("input/source.csv")
    return df


def transform(df):
    """Transform: example filter or reshape (idempotent)."""
    if df is None or df.empty:
        return df
    # Example: drop duplicates, filter, etc.
    df = df.drop_duplicates()
    logger.info("Transformed: %d rows", len(df))
    return df


def load_csv_task(df):
    """Load: save transformed DataFrame to output CSV."""
    if df is not None and not df.empty:
        save_csv(df, "output/transformed.csv")


def run_csv_pipeline():
    """Full pipeline: extract -> transform -> load."""
    df = extract_csv()
    df = transform(df)
    load_csv_task(df)


with DAG(
    dag_id="example_csv_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["example", "csv"],
) as dag:
    run_task = PythonOperator(
        task_id="extract_transform_load",
        python_callable=run_csv_pipeline,
    )
