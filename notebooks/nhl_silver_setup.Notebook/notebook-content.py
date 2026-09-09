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
# NHL ANALYTICS - SILVER LAKEHOUSE TABLE SETUP
# ============================================================
#
# Target Lakehouse:
# nhl_silver_lakehouse
#
# This script creates the normalized Silver layer Delta tables.
#
# The Silver layer is implemented as Delta tables in a Fabric
# Lakehouse rather than relational tables in a Fabric Warehouse.
#
# Tables:
#   - teams
#   - players
#   - rosters
#   - team_standings
#   - games
#   - player_game_stats
#
# ============================================================


from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    BooleanType,
    DateType,
    TimestampType,
    DecimalType
)


# ============================================================
# Configuration
# ============================================================

SILVER_LAKEHOUSE = "nhl_silver_lakehouse"


# ============================================================
# Helper
# ============================================================

def create_delta_table(table_name, schema):
    """
    Creates a Delta table in the Silver Lakehouse if it does
    not already exist.

    Existing tables are left unchanged.
    """

    if not spark.catalog.tableExists(table_name):

        empty_df = spark.createDataFrame(
            [],
            schema
        )

        (
            empty_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(table_name)
        )

        print(f"Created Delta table: {table_name}")

    else:

        print(f"Delta table already exists: {table_name}")


# ============================================================
# Teams
# ============================================================

teams_schema = StructType([

    # Keys
    StructField("team_id", IntegerType(), False),

    # Team
    StructField("place_name", StringType(), True),
    StructField("team_name", StringType(), False),
    StructField("team_common_name", StringType(), True),
    StructField("team_abbrev", StringType(), False),

    # Organization
    StructField("conference_abbrev", StringType(), True),
    StructField("conference_name", StringType(), True),
    StructField("division_abbrev", StringType(), True),
    StructField("division_name", StringType(), True),

    # Assets
    StructField("team_logo", StringType(), True),

    # Audit
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])


create_delta_table(
    "teams",
    teams_schema
)


# ============================================================
# Players
# ============================================================

players_schema = StructType([

    # Keys
    StructField("player_id", IntegerType(), False),

    # Relationships
    StructField("draft_team_id", IntegerType(), True),

    # Identity
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("player_slug", StringType(), True),

    # Current Player Information
    StructField("is_active", BooleanType(), True),
    StructField("position", StringType(), True),
    StructField("shoots_catches", StringType(), True),

    # Physical Attributes
    StructField("height_in_inches", IntegerType(), True),
    StructField("height_in_centimeters", IntegerType(), True),
    StructField("weight_in_pounds", IntegerType(), True),
    StructField("weight_in_kilograms", IntegerType(), True),

    # Birth Information
    StructField("birth_date", DateType(), True),
    StructField("birth_city", StringType(), True),
    StructField("birth_country", StringType(), True),

    # Draft Information
    StructField("draft_year", IntegerType(), True),
    StructField("draft_round", IntegerType(), True),
    StructField("draft_pick_in_round", IntegerType(), True),
    StructField("draft_overall_pick", IntegerType(), True),

    # Honors
    StructField("in_top_100_all_time", BooleanType(), True),
    StructField("in_hhof", BooleanType(), True),

    # Assets
    StructField("headshot", StringType(), True),
    StructField("hero_image", StringType(), True),

    # Audit
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])


create_delta_table(
    "players",
    players_schema
)


# ============================================================
# Rosters
# ============================================================
#
# Roster history is handled through effective dating.
#
# This matches:
#
# silver.rosters
#
# from the Airflow/Postgres implementation.
# ============================================================

rosters_schema = StructType([

    # Keys
    StructField("team_id", IntegerType(), False),
    StructField("player_id", IntegerType(), False),

    # Snapshot
    StructField("snapshot_date", DateType(), False),

    # Effective Dating
    StructField("effective_from", TimestampType(), False),
    StructField("effective_to", TimestampType(), True),

    # Roster Attributes
    StructField("sweater_number", IntegerType(), True),

    # Audit
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])


create_delta_table(
    "rosters",
    rosters_schema
)


# ============================================================
# Team Standings
# ============================================================

team_standings_schema = StructType([

    # Keys
    StructField("team_id", IntegerType(), False),
    StructField("snapshot_date", DateType(), False),

    # Context
    StructField("season_id", IntegerType(), True),
    StructField("game_type_id", IntegerType(), True),
    StructField("clinch_indicator", StringType(), True),

    # Overall Record
    StructField("games_played", IntegerType(), True),
    StructField("wins", IntegerType(), True),
    StructField("losses", IntegerType(), True),
    StructField("ot_losses", IntegerType(), True),
    StructField("ties", IntegerType(), True),
    StructField("points", IntegerType(), True),
    StructField("point_pctg", DecimalType(8, 6), True),
    StructField("win_pctg", DecimalType(8, 6), True),
    StructField("regulation_wins", IntegerType(), True),
    StructField("regulation_win_pctg", DecimalType(8, 6), True),
    StructField("regulation_plus_ot_wins", IntegerType(), True),
    StructField("regulation_plus_ot_win_pctg", DecimalType(8, 6), True),
    StructField("shootout_wins", IntegerType(), True),
    StructField("shootout_losses", IntegerType(), True),

    # Goals
    StructField("goals_for", IntegerType(), True),
    StructField("goals_against", IntegerType(), True),
    StructField("goal_differential", IntegerType(), True),
    StructField("goals_for_pctg", DecimalType(8, 6), True),
    StructField("goal_differential_pctg", DecimalType(8, 6), True),

    # Home Record
    StructField("home_games_played", IntegerType(), True),
    StructField("home_wins", IntegerType(), True),
    StructField("home_losses", IntegerType(), True),
    StructField("home_ot_losses", IntegerType(), True),
    StructField("home_ties", IntegerType(), True),
    StructField("home_points", IntegerType(), True),
    StructField("home_goals_for", IntegerType(), True),
    StructField("home_goals_against", IntegerType(), True),
    StructField("home_goal_differential", IntegerType(), True),
    StructField("home_regulation_wins", IntegerType(), True),
    StructField("home_regulation_plus_ot_wins", IntegerType(), True),

    # Road Record
    StructField("road_games_played", IntegerType(), True),
    StructField("road_wins", IntegerType(), True),
    StructField("road_losses", IntegerType(), True),
    StructField("road_ot_losses", IntegerType(), True),
    StructField("road_ties", IntegerType(), True),
    StructField("road_points", IntegerType(), True),
    StructField("road_goals_for", IntegerType(), True),
    StructField("road_goals_against", IntegerType(), True),
    StructField("road_goal_differential", IntegerType(), True),
    StructField("road_regulation_wins", IntegerType(), True),
    StructField("road_regulation_plus_ot_wins", IntegerType(), True),

    # Last 10 Games
    StructField("l10_games_played", IntegerType(), True),
    StructField("l10_wins", IntegerType(), True),
    StructField("l10_losses", IntegerType(), True),
    StructField("l10_ot_losses", IntegerType(), True),
    StructField("l10_ties", IntegerType(), True),
    StructField("l10_points", IntegerType(), True),
    StructField("l10_goals_for", IntegerType(), True),
    StructField("l10_goals_against", IntegerType(), True),
    StructField("l10_goal_differential", IntegerType(), True),
    StructField("l10_regulation_wins", IntegerType(), True),
    StructField("l10_regulation_plus_ot_wins", IntegerType(), True),

    # Rankings
    StructField("league_sequence", IntegerType(), True),
    StructField("league_home_sequence", IntegerType(), True),
    StructField("league_road_sequence", IntegerType(), True),
    StructField("league_l10_sequence", IntegerType(), True),
    StructField("conference_sequence", IntegerType(), True),
    StructField("conference_home_sequence", IntegerType(), True),
    StructField("conference_road_sequence", IntegerType(), True),
    StructField("conference_l10_sequence", IntegerType(), True),
    StructField("division_sequence", IntegerType(), True),
    StructField("division_home_sequence", IntegerType(), True),
    StructField("division_road_sequence", IntegerType(), True),
    StructField("division_l10_sequence", IntegerType(), True),
    StructField("wildcard_sequence", IntegerType(), True),
    StructField("waivers_sequence", IntegerType(), True),

    # Streak
    StructField("streak_code", StringType(), True),
    StructField("streak_count", IntegerType(), True),

    # Audit
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])


create_delta_table(
    "team_standings",
    team_standings_schema
)


# ============================================================
# Games
# ============================================================

games_schema = StructType([

    # Keys
    StructField("game_id", IntegerType(), False),

    # Game Context
    StructField("season_id", IntegerType(), False),
    StructField("game_type_id", IntegerType(), False),
    StructField("game_date", DateType(), False),
    StructField("start_time_utc", TimestampType(), True),

    # Teams
    StructField("away_team_id", IntegerType(), False),
    StructField("home_team_id", IntegerType(), False),

    # Game Results
    StructField("away_team_score", IntegerType(), True),
    StructField("home_team_score", IntegerType(), True),
    StructField("away_shots_on_goal", IntegerType(), True),
    StructField("home_shots_on_goal", IntegerType(), True),

    # Venue
    StructField("venue", StringType(), True),
    StructField("venue_time_zone", StringType(), True),

    # Audit
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])


create_delta_table(
    "games",
    games_schema
)


# ============================================================
# Player Game Stats
# ============================================================

player_game_stats_schema = StructType([

    # Keys
    StructField("player_id", IntegerType(), False),
    StructField("game_id", IntegerType(), False),

    # Scoring
    StructField("goals", IntegerType(), True),
    StructField("assists", IntegerType(), True),
    StructField("points", IntegerType(), True),
    StructField("game_winning_goals", IntegerType(), True),
    StructField("overtime_goals", IntegerType(), True),

    # Special Teams
    StructField("power_play_goals", IntegerType(), True),
    StructField("power_play_points", IntegerType(), True),
    StructField("shorthanded_goals", IntegerType(), True),
    StructField("shorthanded_points", IntegerType(), True),

    # Other Statistics
    StructField("shots", IntegerType(), True),
    StructField("plus_minus", IntegerType(), True),
    StructField("shifts", IntegerType(), True),
    StructField("pim", IntegerType(), True),
    StructField("time_on_ice", StringType(), True),

    # Audit
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])


create_delta_table(
    "player_game_stats",
    player_game_stats_schema
)


# ============================================================
# Verification
# ============================================================

print()
print("=" * 60)
print("SILVER LAKEHOUSE TABLE VERIFICATION")
print("=" * 60)

silver_tables = [
    "teams",
    "players",
    "rosters",
    "team_standings",
    "games",
    "player_game_stats"
]


for table_name in silver_tables:

    exists = spark.catalog.tableExists(table_name)

    if exists:
        count = spark.table(table_name).count()

        print(
            f"{table_name:<25} "
            f"EXISTS   "
            f"{count:,} rows"
        )

    else:

        print(
            f"{table_name:<25} "
            f"MISSING"
        )


print("=" * 60)
print("Silver Lakehouse setup complete.")
print("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
