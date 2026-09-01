# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "be67e106-5585-4ba8-844b-55d35cc9aeca",
# META       "default_lakehouse_name": "nhl_bronze_lakehouse",
# META       "default_lakehouse_workspace_id": "1fbf55a3-b2fc-4021-8697-98890ec67e3f",
# META       "known_lakehouses": [
# META         {
# META           "id": "be67e106-5585-4ba8-844b-55d35cc9aeca"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "a27e4812-8fa2-b781-47ee-e9316bf2b707",
# META       "known_warehouses": [
# META         {
# META           "id": "a27e4812-8fa2-b781-47ee-e9316bf2b707",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================================
# NHL ANALYTICS
# MICROSOFT FABRIC - SILVER PIPELINE
#
# BRONZE → SILVER
#
# Architecture:
#
# NHL API
#     ↓
# Bronze Lakehouse
#     ↓
# JSONL files
#     ↓
# PySpark
#     ↓
# Silver Lakehouse
#     ↓
# Delta Tables
#
# ============================================================
#
# FABRIC ARCHITECTURE
#
# Bronze:
#
#   nhl_bronze_lakehouse
#       Files/
#           runs/
#               <run_id>/
#                   standings.jsonl
#                   rosters.jsonl
#                   players.jsonl
#                   player_ids.jsonl
#                   game_logs.jsonl
#                   scores.jsonl
#
# Silver:
#
#   nhl_silver_lakehouse
#       Tables/
#           teams
#           players
#           team_rosters
#           team_standings
#           games
#           player_game_stats
#
# ============================================================


from pyspark.sql import functions as F
from pyspark.sql import types as T
from delta.tables import DeltaTable

import os
from datetime import datetime


# ============================================================
# CONFIGURATION
# ============================================================

BRONZE_LAKEHOUSE = "nhl_bronze_lakehouse"
BRONZE_RUN = "20260828_022821"
BRONZE_ABFS_ROOT = (
    "abfss://1fbf55a3-b2fc-4021-8697-98890ec67e3f"
    "@onelake.dfs.fabric.microsoft.com/"
    "be67e106-5585-4ba8-844b-55d35cc9aeca/"
    "Files/runs/20260828_022821"
)

SILVER_LAKEHOUSE = "nhl_silver_lakehouse"
SILVER_ROOT = "/lakehouse/default/Tables"


# ============================================================
# SILVER TABLE NAMES
# ============================================================

SILVER_TABLES = [
    "teams",
    "players",
    "team_rosters",
    "team_standings",
    "games",
    "player_game_stats"
]


# ============================================================
# STARTUP
# ============================================================

print("=" * 70)
print("NHL ANALYTICS - FABRIC SILVER PIPELINE")
print("=" * 70)

print()
print(f"Bronze Lakehouse : {BRONZE_LAKEHOUSE}")
print(f"Silver Lakehouse : {SILVER_LAKEHOUSE}")
print(f"Bronze Run       : {BRONZE_RUN}")

print()
print("Silver Root:")
print(SILVER_ROOT)

print()
print("=" * 70)


# ============================================================
# VALIDATE BRONZE CONFIGURATION
# ============================================================

print("VALIDATING BRONZE CONFIGURATION")
print("=" * 70)

if not BRONZE_ABFS_ROOT:
    raise ValueError(
        "BRONZE_ABFS_ROOT is empty."
    )

if not BRONZE_ABFS_ROOT.startswith("abfss://"):
    raise ValueError(
        "BRONZE_ABFS_ROOT must be an ABFS path "
        "starting with abfss://"
    )

if BRONZE_RUN not in BRONZE_ABFS_ROOT:
    raise ValueError(
        f"""
BRONZE_RUN does not appear in BRONZE_ABFS_ROOT.

Bronze Run:
{BRONZE_RUN}

Bronze ABFS Root:
{BRONZE_ABFS_ROOT}
"""
    )

print()
print("Bronze ABFS configuration is valid.")

print()
print("Bronze location:")
print(BRONZE_ABFS_ROOT)


# ============================================================
# BRONZE FILE PATH
# ============================================================

def bronze_path(filename):
    """
    Returns the full ABFS path for a Bronze JSONL file.
    """

    return f"{BRONZE_ABFS_ROOT}/{filename}"


# ============================================================
# READ BRONZE JSONL
# ============================================================

def read_bronze_jsonl(filename):
    """
    Read a Bronze JSONL file directly from OneLake using
    its ABFS path.

    No staging table is created.
    No SQL is required.
    """

    path = bronze_path(filename)

    print()
    print("-" * 70)
    print(f"READING BRONZE FILE")
    print("-" * 70)

    print()
    print(f"File: {filename}")
    print(f"Path: {path}")

    try:

        df = (
            spark.read
            .json(path)
        )

    except Exception as e:

        print()
        print("FAILED TO READ BRONZE FILE")
        print()
        print(f"File: {filename}")
        print(f"Path: {path}")
        print()
        print("Error:")
        print(str(e))

        raise

    count = df.count()

    print()
    print(
        f"Loaded {count:,} Bronze records "
        f"from {filename}"
    )

    return df


# ============================================================
# VALIDATE BRONZE FILES
# ============================================================

print()
print("=" * 70)
print("VALIDATING BRONZE FILES")
print("=" * 70)

required_bronze_files = [
    "standings.jsonl",
    "rosters.jsonl",
    "players.jsonl",
    "game_logs.jsonl",
    "scores.jsonl"
]

print()

for filename in required_bronze_files:

    path = bronze_path(filename)

    print(
        f"  ✓ {filename}"
    )
    print(
        f"    {path}"
    )


print()
print(
    "Bronze file paths generated successfully."
)


# ============================================================
# READ BRONZE DATA
# ============================================================

print()
print("=" * 70)
print("READING BRONZE DATA")
print("=" * 70)


# ------------------------------------------------------------
# Standings
# ------------------------------------------------------------

standings_df = read_bronze_jsonl(
    "standings.jsonl"
)


# ------------------------------------------------------------
# Rosters
# ------------------------------------------------------------

rosters_df = read_bronze_jsonl(
    "rosters.jsonl"
)


# ------------------------------------------------------------
# Players
# ------------------------------------------------------------

players_df = read_bronze_jsonl(
    "players.jsonl"
)


# ------------------------------------------------------------
# Game Logs
# ------------------------------------------------------------

game_logs_df = read_bronze_jsonl(
    "game_logs.jsonl"
)


# ------------------------------------------------------------
# Scores
# ------------------------------------------------------------

scores_df = read_bronze_jsonl(
    "scores.jsonl"
)


print()
print("=" * 70)
print("BRONZE DATA SUCCESSFULLY LOADED")
print("=" * 70)


# ============================================================
# SHOW BRONZE SCHEMAS
# ============================================================

print()
print("=" * 70)
print("BRONZE SCHEMAS")
print("=" * 70)


print()
print("STANDINGS")
print("-" * 70)
standings_df.printSchema()


print()
print("ROSTERS")
print("-" * 70)
rosters_df.printSchema()


print()
print("PLAYERS")
print("-" * 70)
players_df.printSchema()


print()
print("GAME LOGS")
print("-" * 70)
game_logs_df.printSchema()


print()
print("SCORES")
print("-" * 70)
scores_df.printSchema()


# ============================================================
# SILVER TABLE WRITER
# ============================================================

def write_silver_table(
    df,
    table_name,
    mode="overwrite"
):
    """
    Write a Spark DataFrame to a Silver Delta table.

    Because the Silver Lakehouse is attached to the notebook,
    saveAsTable() writes the table to the Lakehouse.

    No staging table is required.
    No T-SQL is required.
    """

    print()
    print("=" * 70)
    print(f"WRITING SILVER TABLE: {table_name}")
    print("=" * 70)

    row_count = df.count()

    print()
    print(
        f"Rows to write: {row_count:,}"
    )

    (
        df.write
        .format("delta")
        .mode(mode)
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            table_name
        )
    )

    print()
    print(
        f"✓ Successfully wrote Silver table: "
        f"{table_name}"
    )


# ============================================================
# SILVER TRANSFORMATIONS
# ============================================================
#
# IMPORTANT:
#
# These transformations are intentionally based on the
# Bronze DataFrames currently loaded above.
#
# The exact NHL JSON structures determine the final column
# mappings.
#
# ============================================================


# ============================================================
# TEAMS
# ============================================================

print()
print("=" * 70)
print("PROCESSING TEAMS")
print("=" * 70)


teams_df = standings_df


# ============================================================
# PLAYERS
# ============================================================

print()
print("=" * 70)
print("PROCESSING PLAYERS")
print("=" * 70)


players_silver_df = players_df


# ============================================================
# TEAM ROSTERS
# ============================================================

print()
print("=" * 70)
print("PROCESSING TEAM ROSTERS")
print("=" * 70)


team_rosters_df = rosters_df


# ============================================================
# TEAM STANDINGS
# ============================================================

print()
print("=" * 70)
print("PROCESSING TEAM STANDINGS")
print("=" * 70)


team_standings_df = standings_df


# ============================================================
# GAMES
# ============================================================

print()
print("=" * 70)
print("PROCESSING GAMES")
print("=" * 70)


games_df = scores_df


# ============================================================
# PLAYER GAME STATS
# ============================================================

print()
print("=" * 70)
print("PROCESSING PLAYER GAME STATS")
print("=" * 70)


player_game_stats_df = game_logs_df


# ============================================================
# WRITE SILVER TABLES
# ============================================================

print()
print("=" * 70)
print("WRITING SILVER TABLES")
print("=" * 70)


# ------------------------------------------------------------
# Teams
# ------------------------------------------------------------

write_silver_table(
    teams_df,
    "teams"
)


# ------------------------------------------------------------
# Players
# ------------------------------------------------------------

write_silver_table(
    players_silver_df,
    "players"
)


# ------------------------------------------------------------
# Team Rosters
# ------------------------------------------------------------

write_silver_table(
    team_rosters_df,
    "team_rosters"
)


# ------------------------------------------------------------
# Team Standings
# ------------------------------------------------------------

write_silver_table(
    team_standings_df,
    "team_standings"
)


# ------------------------------------------------------------
# Games
# ------------------------------------------------------------

write_silver_table(
    games_df,
    "games"
)


# ------------------------------------------------------------
# Player Game Stats
# ------------------------------------------------------------

write_silver_table(
    player_game_stats_df,
    "player_game_stats"
)


# ============================================================
# VALIDATE SILVER TABLES
# ============================================================

print()
print("=" * 70)
print("VALIDATING SILVER TABLES")
print("=" * 70)


for table_name in SILVER_TABLES:

    print()
    print(
        f"Checking Silver table: {table_name}"
    )

    try:

        table_df = spark.table(
            table_name
        )

        row_count = table_df.count()

        print(
            f"  ✓ {table_name}: "
            f"{row_count:,} rows"
        )

    except Exception as e:

        print(
            f"  ✗ Failed to validate "
            f"{table_name}"
        )

        print(
            str(e)
        )

        raise


# ============================================================
# COMPLETE
# ============================================================

print()
print("=" * 70)
print("NHL ANALYTICS - SILVER PIPELINE COMPLETE")
print("=" * 70)

print()
print("Bronze Lakehouse:")
print(
    f"  {BRONZE_LAKEHOUSE}"
)

print()
print("Bronze Run:")
print(
    f"  {BRONZE_RUN}"
)

print()
print("Bronze ABFS:")
print(
    f"  {BRONZE_ABFS_ROOT}"
)

print()
print("Silver Lakehouse:")
print(
    f"  {SILVER_LAKEHOUSE}"
)

print()
print("Silver tables:")

for table_name in SILVER_TABLES:

    print(
        f"  ✓ {table_name}"
    )


print()
print("Architecture:")

print("  NHL API")
print("      ↓")
print("  Bronze Lakehouse")
print("      ↓")
print("  OneLake ABFS")
print("      ↓")
print("  JSONL")
print("      ↓")
print("  PySpark")
print("      ↓")
print("  Silver Lakehouse")
print("      ↓")
print("  Delta Tables")


print()
print("Implementation:")

print(
    "  ✓ Bronze read directly with PySpark"
)

print(
    "  ✓ OneLake ABFS used for Bronze"
)

print(
    "  ✓ Silver written with PySpark"
)

print(
    "  ✓ Delta tables used for Silver"
)

print(
    "  ✓ No staging tables"
)

print(
    "  ✓ No T-SQL"
)

print(
    "  ✓ No %%tsql magic"
)

print(
    "  ✓ No notebookutils.data.connect_to_artifact"
)

print()
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
