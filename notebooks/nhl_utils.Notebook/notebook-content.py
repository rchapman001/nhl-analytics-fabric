# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# ============================================================
# NHL ANALYTICS
# SHARED FABRIC UTILITIES
#
# Reusable utilities for:
#   - NHL API access / rate limiting
#   - Bronze run paths and JSONL
#   - Spark / Delta tables
#   - Audit columns
#   - Common validation
#
# Fabric notebook requirement:
#   This notebook must be referenced/run before the pipeline
#   notebooks so its functions are available in the session.
# ============================================================

from __future__ import annotations

import json
import time
from datetime import datetime, timezone

import notebookutils
import requests
from pyspark.sql import functions as F

# ============================================================
# CONFIGURATION
# ============================================================

NHL_API_BASE_URL = "https://api-web.nhle.com/v1"
MIN_INTERVAL_SECONDS = 1.0
MAX_RETRIES = 5

_last_api_call = 0.0

# ============================================================
# NHL API
# ============================================================

def safe_get(url: str):
    """Rate-limited NHL API GET with retry handling."""
    global _last_api_call

    wait = MIN_INTERVAL_SECONDS - (time.time() - _last_api_call)
    if wait > 0:
        time.sleep(wait)

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, timeout=30)
            _last_api_call = time.time()

            if response.status_code == 429:
                retry_wait = 2 ** attempt
                print(f"Rate limited. Retrying in {retry_wait}s...")
                time.sleep(retry_wait)
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as error:
            if attempt == MAX_RETRIES - 1:
                raise

            retry_wait = 2 ** attempt
            print(
                f"Request failed: {error}. "
                f"Retrying in {retry_wait}s..."
            )
            time.sleep(retry_wait)

    raise RuntimeError(
        f"Failed to retrieve data after {MAX_RETRIES} attempts: {url}"
    )

def roster_url(team_abbrev: str) -> str:
    return f"{NHL_API_BASE_URL}/roster/{team_abbrev}/current"

def player_landing_url(player_id: int) -> str:
    return f"{NHL_API_BASE_URL}/player/{player_id}/landing"

def player_game_log_url(player_id: int) -> str:
    return f"{NHL_API_BASE_URL}/player/{player_id}/game-log/now"

# ============================================================
# BRONZE RUN / FILE UTILITIES
# ============================================================

def create_run_folder(lakehouse_path: str):
    """Return (run_id, run_path) for a unique UTC Bronze run."""
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_path = f"{lakehouse_path}/Files/runs/{run_id}"
    return run_id, run_path

def bronze_file_path(run_path: str, filename: str) -> str:
    if not run_path:
        raise ValueError("run_path cannot be empty.")
    if not filename:
        raise ValueError("filename cannot be empty.")
    return f"{run_path.rstrip('/')}/{filename}"

def read_jsonl(path: str):
    """Read a JSONL file into a Spark DataFrame."""
    if not path:
        raise ValueError("JSONL path cannot be empty.")

    print(f"Reading JSONL: {path}")
    df = spark.read.json(path)
    print(f"Loaded {df.count():,} records.")
    return df

def write_jsonl(path: str, records: list[dict]):
    """Write a list of dictionaries as JSONL."""
    if not path:
        raise ValueError("JSONL path cannot be empty.")

    payload = "".join(
        json.dumps(record, separators=(",", ":")) + "\n"
        for record in records
    )
    notebookutils.fs.put(path, payload, overwrite=True)
    print(f"Wrote JSONL: {path}")

# ============================================================
# COMMON SQL / VALIDATION UTILITIES
# ============================================================

def quote_identifier(identifier: str) -> str:
    """Safely quote a SQL identifier."""
    if not identifier:
        raise ValueError("SQL identifier cannot be empty.")

    escaped = identifier.replace("]", "]]")
    return f"[{escaped}]"


def validate_surrogate_key(
    df,
    column: str,
    table_name: str,
    description: str,
):
    """Validate that a Warehouse-generated surrogate key was populated."""
    if column not in df.columns:
        raise ValueError(
            f"{description} is missing surrogate key column "
            f"'{column}' from {table_name}."
        )

    null_count = df.filter(
        F.col(column).isNull()
    ).limit(1).count()

    if null_count > 0:
        raise ValueError(
            f"{description} contains NULL surrogate keys "
            f"for {table_name}.{column}."
        )

    return df

# ============================================================
# SPARK / DELTA UTILITIES
# ============================================================

def read_delta_table(table_name: str):
    """Read a managed Spark/Delta table."""
    if not table_name:
        raise ValueError("table_name cannot be empty.")

    print(f"Reading Delta table: {table_name}")
    df = spark.table(table_name)
    print(f"Loaded {df.count():,} rows.")
    return df

def write_delta_table(
    df,
    table_name: str,
    mode: str = "overwrite",
    overwrite_schema: bool = True,
    add_audit: bool = True,
):
    """Write a DataFrame to a managed Delta table."""
    if add_audit:
        df = add_audit_columns(df)

    writer = df.write.format("delta").mode(mode)
    if overwrite_schema:
        writer = writer.option("overwriteSchema", "true")

    row_count = df.count()
    print(f"Writing {row_count:,} rows to {table_name}...")
    writer.saveAsTable(table_name)
    print(f"✓ Wrote Delta table: {table_name}")

def add_audit_columns(df):
    """Add standard created_at / updated_at columns."""
    now = F.current_timestamp()
    return (
        df.withColumn("created_at", now)
          .withColumn("updated_at", now)
    )

def table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)

def validate_table_exists(table_name: str):
    if not table_exists(table_name):
        raise ValueError(f"Required table does not exist: {table_name}")
    print(f"✓ Table exists: {table_name}")

def require_columns(df, required_columns: list[str], label: str = "DataFrame"):
    """Raise a clear error when required DataFrame columns are missing."""
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(
            f"{label} is missing required columns: {', '.join(missing)}"
        )
    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
