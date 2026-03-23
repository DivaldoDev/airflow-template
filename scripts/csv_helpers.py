"""CSV helper functions for ETL tasks."""

import logging
import os
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# Base path for data files (mounted at /opt/airflow in container)
DATA_DIR = Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow")) / "data"


def load_csv(path: str) -> pd.DataFrame:
    """
    Load a CSV file from the data/ directory.

    Args:
        path: Relative path under data/ or absolute path to CSV file.

    Returns:
        DataFrame with CSV contents.
    """
    filepath = DATA_DIR / path if not Path(path).is_absolute() else Path(path)
    if not filepath.exists():
        raise FileNotFoundError(f"CSV file not found: {filepath}")
    df = pd.read_csv(filepath)
    logger.info("Loaded CSV: %s (%d rows)", path, len(df))
    return df


def save_csv(df: pd.DataFrame, path: str) -> None:
    """
    Save a DataFrame to a CSV file in the data/ directory.

    Args:
        df: DataFrame to persist.
        path: Relative path under data/ or absolute path for output file.
    """
    filepath = DATA_DIR / path if not Path(path).is_absolute() else Path(path)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(filepath, index=False)
    logger.info("Saved CSV: %s (%d rows)", path, len(df))
