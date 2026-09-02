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
# SHARED UTILITY FUNCTIONS
# ============================================================

import requests
import json
import time

from datetime import datetime, timezone


# ============================================================
# NHL API CONFIGURATION
# ============================================================

MIN_INTERVAL = 1.0
MAX_RETRIES = 5

last_call = 0


# ============================================================
# NHL API CLIENT
# ============================================================

def safe_get(url):
    """
    Makes a rate-limited request to the NHL API.

    Retries requests that return HTTP 429 or other
    request-related failures.
    """

    global last_call

    # Ensure at least MIN_INTERVAL seconds
    # between requests.
    wait = MIN_INTERVAL - (
        time.time() - last_call
    )

    if wait > 0:
        time.sleep(wait)

    for attempt in range(MAX_RETRIES):

        try:

            response = requests.get(
                url,
                timeout=30
            )

            last_call = time.time()

            # Retry if rate limited.
            if response.status_code == 429:

                retry_wait = 2 ** attempt

                print(
                    f"Rate limited. "
                    f"Retrying in {retry_wait} seconds..."
                )

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
                f"Retrying in {retry_wait} seconds..."
            )

            time.sleep(retry_wait)

    raise Exception(
        f"Failed to retrieve data after "
        f"{MAX_RETRIES} attempts: {url}"
    )


# ============================================================
# RUN FOLDER
# ============================================================

def create_run_folder(lakehouse_path):
    """
    Creates a unique UTC run folder path.

    Returns:
        tuple[str, str]:
            run_folder, run_path
    """

    run_folder = datetime.now(
        timezone.utc
    ).strftime(
        "%Y%m%d_%H%M%S"
    )

    run_path = (
        f"{lakehouse_path}/Files/runs/{run_folder}"
    )

    return run_folder, run_path


# ============================================================
# JSONL
# ============================================================

def read_jsonl(path):
    """
    Read a JSONL file from a given path using Spark.

    Args:
        path (str):
            Full path to the JSONL file.

    Returns:
        pyspark.sql.DataFrame:
            DataFrame containing the JSONL records.
    """

    print()
    print("=" * 70)
    print("READING JSONL")
    print("=" * 70)

    print()
    print(f"Path: {path}")

    if not path:
        raise ValueError(
            "JSONL path cannot be empty."
        )

    try:

        df = (
            spark.read
            .json(path)
        )

    except Exception as error:

        print()
        print("FAILED TO READ JSONL")
        print()
        print(f"Path: {path}")
        print()
        print("Error:")
        print(str(error))

        raise

    row_count = df.count()

    print()
    print(
        f"✓ Successfully loaded "
        f"{row_count:,} records"
    )

    return df


def write_jsonl(path, records):
    """
    Writes a list of dictionaries to a JSONL file
    in the Lakehouse.
    """

    jsonl_data = "\n".join(
        json.dumps(record)
        for record in records
    )

    jsonl_data += "\n"

    notebookutils.fs.put(
        path,
        jsonl_data,
        overwrite=True
    )


# ============================================================
# SPARK / DELTA UTILITIES
# ============================================================

def read_delta_table(table_name):
    """
    Read a Delta table from the attached Lakehouse.
    """

    print()
    print(f"Reading Delta table: {table_name}")

    df = spark.table(table_name)

    print(
        f"Loaded {df.count():,} rows "
        f"from {table_name}"
    )

    return df


def write_delta_table(
    df,
    table_name,
    mode="overwrite",
    overwrite_schema=True
):
    """
    Write a Spark DataFrame to a Delta table.
    """

    print()
    print("=" * 70)
    print(f"WRITING DELTA TABLE: {table_name}")
    print("=" * 70)

    row_count = df.count()

    print()
    print(f"Rows to write: {row_count:,}")

    writer = (
        df.write
        .format("delta")
        .mode(mode)
    )

    if overwrite_schema:
        writer = writer.option(
            "overwriteSchema",
            "true"
        )

    writer.saveAsTable(table_name)

    print()
    print(
        f"✓ Successfully wrote Delta table: "
        f"{table_name}"
    )


def table_exists(table_name):
    """
    Check whether a Spark table exists.
    """

    return spark.catalog.tableExists(
        table_name
    )


def validate_table_exists(table_name):
    """
    Raise an error if a required table does not exist.
    """

    if not table_exists(table_name):

        raise ValueError(
            f"Required table does not exist: "
            f"{table_name}"
        )

    print(
        f"✓ Table exists: {table_name}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
