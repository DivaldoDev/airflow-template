"""API/HTTP helper functions for ETL tasks."""

import json
import logging
import os
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from scripts.csv_helpers import DATA_DIR, save_csv

logger = logging.getLogger(__name__)


def _build_headers() -> dict[str, str]:
    """Build request headers from environment variables."""
    headers: dict[str, str] = {"Accept": "application/json"}
    token = os.environ.get("API_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def fetch_json(url: str, headers: dict[str, str] | None = None) -> dict | list:
    """
    Perform GET request and return JSON response as dict or list.

    Args:
        url: Full URL or path appended to API_BASE_URL if set.
        headers: Optional extra headers. Merged with default headers (API_TOKEN, etc.).

    Returns:
        Parsed JSON (dict or list).
    """
    base_url = os.environ.get("API_BASE_URL", "").rstrip("/")
    full_url = url if url.startswith("http") else f"{base_url}/{url.lstrip('/')}"
    req_headers = {**_build_headers(), **(headers or {})}
    resp = requests.get(full_url, headers=req_headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    logger.info("Fetched JSON from %s", full_url)
    return data


def fetch_json_to_csv(
    url: str,
    output_path: str,
    headers: dict[str, str] | None = None,
    record_path: str | None = None,
) -> None:
    """
    Perform GET request for JSON and save result to CSV.

    Args:
        url: Full URL or path appended to API_BASE_URL if set.
        output_path: Relative path under data/ for output CSV.
        headers: Optional extra headers.
        record_path: If JSON is nested (e.g. {"data": [...]}), path to list to flatten.
    """
    data: Any = fetch_json(url, headers)
    if record_path:
        for key in record_path.split("."):
            data = data[key]
    df = pd.DataFrame(data if isinstance(data, list) else [data])
    save_csv(df, output_path)
    logger.info("Saved API response to CSV: %s", output_path)
