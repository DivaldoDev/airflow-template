"""
Example DAG: API migration pattern.
Fetches JSON from a public API, saves to CSV in data/.
For DB load, use load_csv_to_db() from scripts.etl_helpers after fetch.
"""

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.api_helpers import fetch_json_to_csv

logger = logging.getLogger(__name__)

# Public API that returns JSON (no auth required for demo)
EXAMPLE_API_URL = "https://jsonplaceholder.typicode.com/users"


def fetch_api_to_csv():
    """
    Fetch JSON from API and save to CSV.
    Uses a public placeholder API for demonstration.
    """
    fetch_json_to_csv(
        url=EXAMPLE_API_URL,
        output_path="api_users.csv",
        record_path=None,  # Response is list at root
    )
    logger.info("API data saved to data/api_users.csv")


with DAG(
    dag_id="example_api_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["example", "api"],
) as dag:
    fetch_task = PythonOperator(
        task_id="fetch_api_to_csv",
        python_callable=fetch_api_to_csv,
    )
