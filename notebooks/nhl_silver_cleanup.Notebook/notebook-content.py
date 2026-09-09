# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "63da3f3e-aa63-453a-acff-7e3540f60e1a",
# META       "default_lakehouse_name": "nhl_silver_lakehouse",
# META       "default_lakehouse_workspace_id": "1fbf55a3-b2fc-4021-8697-98890ec67e3f",
# META       "known_lakehouses": [
# META         {
# META           "id": "63da3f3e-aa63-453a-acff-7e3540f60e1a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================================
# NHL ANALYTICS - SILVER LAKEHOUSE CLEANUP
# ============================================================
#
# Target Lakehouse:
# nhl_silver_lakehouse
#
# Drops all Silver Delta tables.
#
# WARNING:
# This permanently removes the Silver tables and their data.
#
# ============================================================


# ============================================================
# Configuration
# ============================================================

SILVER_TABLES = [
    "teams",
    "players",
    "rosters",
    "team_standings",
    "games",
    "player_game_stats"
]


# ============================================================
# Drop Silver Delta Tables
# ============================================================

for table_name in SILVER_TABLES:

    if spark.catalog.tableExists(table_name):

        spark.sql(f"DROP TABLE `{table_name}`")

        print(f"Dropped Silver Delta table: {table_name}")

    else:

        print(f"Silver Delta table does not exist: {table_name}")


# ============================================================
# Verification
# ============================================================

print()
print("=" * 60)
print("SILVER LAKEHOUSE CLEANUP VERIFICATION")
print("=" * 60)

for table_name in SILVER_TABLES:

    if spark.catalog.tableExists(table_name):
        print(f"{table_name:<25} STILL EXISTS")
    else:
        print(f"{table_name:<25} DROPPED")

print("=" * 60)
print("Silver Lakehouse cleanup complete.")
print("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
