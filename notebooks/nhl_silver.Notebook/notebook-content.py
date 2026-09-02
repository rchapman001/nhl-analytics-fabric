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

%run ./nhl_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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
#
# SHARED UTILITIES
#
# The following reusable functionality is provided by
# nhl_utils:
#
#   - read_jsonl()
#   - write_jsonl()
#   - read_delta_table()
#   - write_delta_table()
#   - table_exists()
#   - validate_table_exists()
#   - safe_get()
#   - create_run_folder()
#
# ============================================================


# ============================================================
# PYSPARK IMPORTS
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import types as T

from delta.tables import DeltaTable

import os
from datetime import datetime


# ============================================================
# CONFIGURATION
# ============================================================

# ------------------------------------------------------------
# Bronze Lakehouse
# ------------------------------------------------------------

BRONZE_LAKEHOUSE = "nhl_bronze_lakehouse"

# The specific Bronze run being processed.
BRONZE_RUN = "20260828_022821"

BRONZE_ABFS_ROOT = (
    "abfss://1fbf55a3-b2fc-4021-8697-98890ec67e3f"
    "@onelake.dfs.fabric.microsoft.com/"
    "be67e106-5585-4ba8-844b-55d35cc9aeca/"
    f"Files/runs/{BRONZE_RUN}"
)


# ------------------------------------------------------------
# Silver Lakehouse
# ------------------------------------------------------------

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
print("Bronze Root:")
print(BRONZE_ABFS_ROOT)

print()
print("Silver Root:")
print(SILVER_ROOT)

print()
print("=" * 70)


# ============================================================
# VALIDATE BRONZE CONFIGURATION
# ============================================================

print()
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
print("✓ Bronze ABFS configuration is valid.")

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

    return (
        f"{BRONZE_ABFS_ROOT}/{filename}"
    )


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


for filename in required_bronze_files:

    path = bronze_path(filename)

    print()
    print(f"  ✓ {filename}")
    print(f"    {path}")


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

standings_df = read_jsonl(
    bronze_path(
        "standings.jsonl"
    )
)


# ------------------------------------------------------------
# Rosters
# ------------------------------------------------------------

rosters_df = read_jsonl(
    bronze_path(
        "rosters.jsonl"
    )
)


# ------------------------------------------------------------
# Players
# ------------------------------------------------------------

players_df = read_jsonl(
    bronze_path(
        "players.jsonl"
    )
)


# ------------------------------------------------------------
# Game Logs
# ------------------------------------------------------------

game_logs_df = read_jsonl(
    bronze_path(
        "game_logs.jsonl"
    )
)


# ------------------------------------------------------------
# Scores
# ------------------------------------------------------------

scores_df = read_jsonl(
    bronze_path(
        "scores.jsonl"
    )
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
# SILVER TRANSFORMATIONS
# ============================================================
#
# IMPORTANT:
#
# These transformations are currently placeholders.
#
# The next implementation step is to flatten and normalize
# the NHL JSON structures into the actual Silver schemas.
#
# Silver tables should eventually contain clean relational
# structures rather than the raw Bronze JSON structures.
#
# ============================================================


# ============================================================
# TEAMS
# ============================================================

print()
print("=" * 70)
print("PROCESSING TEAMS")
print("=" * 70)


# TODO:
# Extract team-level attributes from standings JSON.
#
# Expected future structure:
#
# teams
#   team_id
#   team_abbreviation
#   team_name
#   city
#   conference
#   division
#   ...
#
# Current implementation:
# Preserve Bronze DataFrame until transformation logic
# is implemented.

teams_df = standings_df


print()
print(
    f"Teams DataFrame prepared: "
    f"{teams_df.count():,} rows"
)


# ============================================================
# PLAYERS
# ============================================================

print()
print("=" * 70)
print("PROCESSING PLAYERS")
print("=" * 70)


# TODO:
# Extract player-level attributes from player landing JSON.
#
# Expected future structure:
#
# players
#   player_id
#   first_name
#   last_name
#   position
#   shoots
#   height
#   weight
#   birth_date
#   ...
#
# Current implementation:
# Preserve Bronze DataFrame until transformation logic
# is implemented.

players_silver_df = players_df


print()
print(
    f"Players DataFrame prepared: "
    f"{players_silver_df.count():,} rows"
)


# ============================================================
# TEAM ROSTERS
# ============================================================

print()
print("=" * 70)
print("PROCESSING TEAM ROSTERS")
print("=" * 70)


# TODO:
# Flatten roster JSON into one row per player/team
# relationship.
#
# Expected future structure:
#
# team_rosters
#   team_id
#   player_id
#   position
#   roster_status
#   ...
#
# Current implementation:
# Preserve Bronze DataFrame until transformation logic
# is implemented.

team_rosters_df = rosters_df


print()
print(
    f"Team Rosters DataFrame prepared: "
    f"{team_rosters_df.count():,} rows"
)


# ============================================================
# TEAM STANDINGS
# ============================================================

print()
print("=" * 70)
print("PROCESSING TEAM STANDINGS")
print("=" * 70)


# TODO:
# Flatten standings JSON into one row per team.
#
# Expected future structure:
#
# team_standings
#   team_id
#   season
#   games_played
#   wins
#   losses
#   overtime_losses
#   points
#   goals_for
#   goals_against
#   ...
#
# Current implementation:
# Preserve Bronze DataFrame until transformation logic
# is implemented.

team_standings_df = standings_df


print()
print(
    f"Team Standings DataFrame prepared: "
    f"{team_standings_df.count():,} rows"
)


# ============================================================
# GAMES
# ============================================================

print()
print("=" * 70)
print("PROCESSING GAMES")
print("=" * 70)


# TODO:
# Flatten scores JSON into one row per game.
#
# Expected future structure:
#
# games
#   game_id
#   date
#   season
#   game_type
#   home_team_id
#   away_team_id
#   home_score
#   away_score
#   game_state
#   ...
#
# Current implementation:
# Preserve Bronze DataFrame until transformation logic
# is implemented.

games_df = scores_df


print()
print(
    f"Games DataFrame prepared: "
    f"{games_df.count():,} rows"
)


# ============================================================
# PLAYER GAME STATS
# ============================================================

print()
print("=" * 70)
print("PROCESSING PLAYER GAME STATS")
print("=" * 70)


# TODO:
# Flatten player game log JSON into one row per player/game.
#
# Expected future structure:
#
# player_game_stats
#   player_id
#   game_id
#   date
#   goals
#   assists
#   points
#   shots
#   hits
#   blocked_shots
#   penalty_minutes
#   time_on_ice
#   ...
#
# Current implementation:
# Preserve Bronze DataFrame until transformation logic
# is implemented.

player_game_stats_df = game_logs_df


print()
print(
    f"Player Game Stats DataFrame prepared: "
    f"{player_game_stats_df.count():,} rows"
)


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

write_delta_table(
    teams_df,
    "teams"
)


# ------------------------------------------------------------
# Players
# ------------------------------------------------------------

write_delta_table(
    players_silver_df,
    "players"
)


# ------------------------------------------------------------
# Team Rosters
# ------------------------------------------------------------

write_delta_table(
    team_rosters_df,
    "team_rosters"
)


# ------------------------------------------------------------
# Team Standings
# ------------------------------------------------------------

write_delta_table(
    team_standings_df,
    "team_standings"
)


# ------------------------------------------------------------
# Games
# ------------------------------------------------------------

write_delta_table(
    games_df,
    "games"
)


# ------------------------------------------------------------
# Player Game Stats
# ------------------------------------------------------------

write_delta_table(
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

        table_df = read_delta_table(
            table_name
        )

        print(
            f"  ✓ {table_name}: "
            f"{table_df.count():,} rows"
        )

    except Exception as e:

        print(
            f"  ✗ Failed to validate "
            f"{table_name}"
        )

        print()
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
    "  ✓ Shared utilities loaded from nhl_utils"
)

print(
    "  ✓ Bronze read directly with PySpark"
)

print(
    "  ✓ OneLake ABFS used for Bronze"
)

print(
    "  ✓ Silver written with shared Delta utility"
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
