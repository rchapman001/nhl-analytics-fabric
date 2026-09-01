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
# NHL ANALYTICS PLATFORM
# GOLD LAYER
#
# Reads from:
#   nhl_silver_warehouse
#
# Writes to:
#   nhl_gold_warehouse
#
# Tables:
#   dim_date
#   dim_team
#   dim_player
#   dim_game
#   fact_rosters
#   fact_team_standings
#   fact_player_game_stats
# ============================================================


from datetime import datetime

from pyspark.sql import functions as F


# ============================================================
# CONFIGURATION
# ============================================================

# ------------------------------------------------------------
# Warehouse Names
# ------------------------------------------------------------

SILVER_WAREHOUSE = "nhl_silver_warehouse"

GOLD_WAREHOUSE = "nhl_gold_warehouse"


# ------------------------------------------------------------
# Silver Warehouse SQL Connection
#
# Replace with your Silver Warehouse SQL connection string.
# ------------------------------------------------------------

SILVER_SQL_URL = (
    "jdbc:sqlserver://YOUR_SILVER_WAREHOUSE_ENDPOINT:1433;"
    "database=nhl_silver_warehouse;"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "hostNameInCertificate=*.datawarehouse.fabric.microsoft.com;"
    "loginTimeout=30;"
)


# ------------------------------------------------------------
# Gold Warehouse SQL Connection
#
# Replace with your Gold Warehouse SQL connection string.
# ------------------------------------------------------------

GOLD_SQL_URL = (
    "jdbc:sqlserver://YOUR_GOLD_WAREHOUSE_ENDPOINT:1433;"
    "database=nhl_gold_warehouse;"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "hostNameInCertificate=*.datawarehouse.fabric.microsoft.com;"
    "loginTimeout=30;"
)


# ============================================================
# WAREHOUSE HELPER FUNCTIONS
# ============================================================


def read_warehouse_table(
    table_name
):
    """
    Reads a table from the
    NHL Silver Warehouse.
    """

    return (
        spark.read
        .format(
            "com.microsoft.sqlserver.jdbc.spark"
        )
        .option(
            "url",
            SILVER_SQL_URL
        )
        .option(
            "dbtable",
            f"dbo.{table_name}"
        )
        .load()
    )


def write_warehouse_table(
    dataframe,
    table_name,
    mode="overwrite"
):
    """
    Writes a DataFrame to the
    NHL Gold Warehouse.
    """

    (
        dataframe.write
        .format(
            "com.microsoft.sqlserver.jdbc.spark"
        )
        .option(
            "url",
            GOLD_SQL_URL
        )
        .option(
            "dbtable",
            f"dbo.{table_name}"
        )
        .mode(
            mode
        )
        .save()
    )


# ============================================================
# LOAD SILVER WAREHOUSE TABLES
# ============================================================

print("=" * 60)
print("LOADING SILVER WAREHOUSE TABLES")
print("=" * 60)


silver_teams = (
    read_warehouse_table(
        "teams"
    )
)


silver_players = (
    read_warehouse_table(
        "players"
    )
)


silver_rosters = (
    read_warehouse_table(
        "rosters"
    )
)


silver_team_standings = (
    read_warehouse_table(
        "team_standings"
    )
)


silver_games = (
    read_warehouse_table(
        "games"
    )
)


silver_player_game_stats = (
    read_warehouse_table(
        "player_game_stats"
    )
)


print("Silver Warehouse tables loaded.")
print()


# ============================================================
# CURRENT TIMESTAMP
# ============================================================

load_timestamp = datetime.now()


# ============================================================
# DIM DATE
# ============================================================

print("Loading dim_date...")


# ------------------------------------------------------------
# Get Dates From Games
# ------------------------------------------------------------

game_dates = (
    silver_games
    .select(
        F.col(
            "game_date"
        )
        .cast(
            "date"
        )
        .alias(
            "full_date"
        )
    )
)


# ------------------------------------------------------------
# Get Dates From Rosters
# ------------------------------------------------------------

roster_dates = (
    silver_rosters
    .select(
        F.col(
            "effective_from"
        )
        .cast(
            "date"
        )
        .alias(
            "full_date"
        )
    )
)


# ------------------------------------------------------------
# Get Dates From Standings
# ------------------------------------------------------------

standings_dates = (
    silver_team_standings
    .select(
        F.col(
            "snapshot_date"
        )
        .cast(
            "date"
        )
        .alias(
            "full_date"
        )
    )
)


# ------------------------------------------------------------
# Combine All Dates
# ------------------------------------------------------------

all_dates = (
    game_dates
    .union(
        roster_dates
    )
    .union(
        standings_dates
    )
    .filter(
        F.col(
            "full_date"
        ).isNotNull()
    )
    .distinct()
)


# ------------------------------------------------------------
# Build Date Dimension
# ------------------------------------------------------------

dim_date = (
    all_dates

    .withColumn(
        "date_id",

        F.date_format(
            F.col(
                "full_date"
            ),
            "yyyyMMdd"
        ).cast(
            "int"
        )
    )

    .withColumn(
        "day",

        F.dayofmonth(
            "full_date"
        )
    )

    .withColumn(
        "day_name",

        F.date_format(
            "full_date",
            "EEEE"
        )
    )

    .withColumn(
        "day_of_week",

        F.dayofweek(
            "full_date"
        )
    )

    .withColumn(
        "week_of_year",

        F.weekofyear(
            "full_date"
        )
    )

    .withColumn(
        "month",

        F.month(
            "full_date"
        )
    )

    .withColumn(
        "month_name",

        F.date_format(
            "full_date",
            "MMMM"
        )
    )

    .withColumn(
        "quarter",

        F.quarter(
            "full_date"
        )
    )

    .withColumn(
        "year",

        F.year(
            "full_date"
        )
    )

    .withColumn(
        "is_weekend",

        F.dayofweek(
            "full_date"
        ).isin(
            [1, 7]
        )
    )

    .withColumn(
        "created_at",

        F.lit(
            load_timestamp
        ).cast(
            "timestamp"
        )
    )

    .withColumn(
        "updated_at",

        F.lit(
            load_timestamp
        ).cast(
            "timestamp"
        )
    )

    .select(
        "date_id",
        "full_date",
        "day",
        "day_name",
        "day_of_week",
        "week_of_year",
        "month",
        "month_name",
        "quarter",
        "year",
        "is_weekend",
        "created_at",
        "updated_at"
    )
)


write_warehouse_table(
    dim_date,
    "dim_date",
    "overwrite"
)


print(
    f"dim_date loaded: "
    f"{dim_date.count()} rows."
)

print()


# ============================================================
# DIM TEAM
# ============================================================

print("Loading dim_team...")


dim_team = (
    silver_teams

    .dropDuplicates(
        [
            "team_id"
        ]
    )

    .withColumn(
        "created_at",

        F.lit(
            load_timestamp
        ).cast(
            "timestamp"
        )
    )

    .withColumn(
        "updated_at",

        F.lit(
            load_timestamp
        ).cast(
            "timestamp"
        )
    )

    .select(
        "team_id",
        "place_name",
        "team_name",
        "team_common_name",
        "team_abbrev",
        "conference_abbrev",
        "conference_name",
        "division_abbrev",
        "division_name",
        "team_logo",
        "created_at",
        "updated_at"
    )
)


write_warehouse_table(
    dim_team,
    "dim_team",
    "overwrite"
)


print(
    f"dim_team loaded: "
    f"{dim_team.count()} rows."
)

print()


# ============================================================
# DIM PLAYER
# ============================================================

print("Loading dim_player...")


dim_player = (
    silver_players

    .dropDuplicates(
        [
            "player_id"
        ]
    )

    .withColumn(
        "created_at",

        F.lit(
            load_timestamp
        ).cast(
            "timestamp"
        )
    )

    .withColumn(
        "updated_at",

        F.lit(
            load_timestamp
        ).cast(
            "timestamp"
        )
    )

    .select(
        "player_id",
        "draft_team_id",
        "first_name",
        "last_name",
        "player_slug",
        "is_active",
        "position",
        "shoots_catches",
        "height_in_inches",
        "height_in_centimeters",
        "weight_in_pounds",
        "weight_in_kilograms",
        "birth_date",
        "birth_city",
        "birth_country",
        "draft_year",
        "draft_round",
        "draft_pick_in_round",
        "draft_overall_pick",
        "in_top_100_all_time",
        "in_hhof",
        "headshot",
        "hero_image",
        "created_at",
        "updated_at"
    )
)


write_warehouse_table(
    dim_player,
    "dim_player",
    "overwrite"
)


print(
    f"dim_player loaded: "
    f"{dim_player.count()} rows."
)

print()


# ============================================================
# DIM GAME
# ============================================================

print("Loading dim_game...")


# ------------------------------------------------------------
# Create Date Lookup
# ------------------------------------------------------------

date_lookup = (
    dim_date
    .select(
        "date_id",
        "full_date"
    )
)


# ------------------------------------------------------------
# Build Game Dimension
# ------------------------------------------------------------

dim_game = (
    silver_games

    .join(
        date_lookup,

        F.to_date(
            silver_games[
                "game_date"
            ]
        )
        ==
        date_lookup[
            "full_date"
        ],

        "left"
    )

    .dropDuplicates(
        [
            "game_id"
        ]
    )

    .withColumn(
        "created_at",

        F.lit(
            load_timestamp
        ).cast(
            "timestamp"
        )
    )

    .withColumn(
        "updated_at",

        F.lit(
            load_timestamp
        ).cast(
            "timestamp"
        )
    )

    .select(
        silver_games[
            "game_id"
        ],

        "date_id",

        silver_games[
            "season_id"
        ],

        silver_games[
            "game_type_id"
        ],

        silver_games[
            "game_date"
        ],

        silver_games[
            "start_time_utc"
        ],

        silver_games[
            "away_team_id"
        ],

        silver_games[
            "home_team_id"
        ],

        silver_games[
            "away_team_score"
        ],

        silver_games[
            "home_team_score"
        ],

        silver_games[
            "away_shots_on_goal"
        ],

        silver_games[
            "home_shots_on_goal"
        ],

        silver_games[
            "venue"
        ],

        silver_games[
            "venue_time_zone"
        ],

        "created_at",

        "updated_at"
    )
)


write_warehouse_table(
    dim_game,
    "dim_game",
    "overwrite"
)


print(
    f"dim_game loaded: "
    f"{dim_game.count()} rows."
)

print()


# ============================================================
# FACT ROSTERS
# ============================================================

print("Loading fact_rosters...")


fact_rosters = (
    silver_rosters

    .join(
        date_lookup,

        F.to_date(
            silver_rosters[
                "effective_from"
            ]
        )
        ==
        date_lookup[
            "full_date"
        ],

        "left"
    )

    .withColumn(
        "created_at",

        F.lit(
            load_timestamp
        ).cast(
            "timestamp"
        )
    )

    .withColumn(
        "updated_at",

        F.lit(
            load_timestamp
        ).cast(
            "timestamp"
        )
    )

    .select(
        silver_rosters[
            "team_id"
        ],

        silver_rosters[
            "player_id"
        ],

        "date_id",

        silver_rosters[
            "sweater_number"
        ],

        "created_at",

        "updated_at"
    )
)


write_warehouse_table(
    fact_rosters,
    "fact_rosters",
    "overwrite"
)


print(
    f"fact_rosters loaded: "
    f"{fact_rosters.count()} rows."
)

print()


# ============================================================
# FACT TEAM STANDINGS
# ============================================================

print(
    "Loading fact_team_standings..."
)


fact_team_standings = (
    silver_team_standings

    .join(
        date_lookup,

        F.to_date(
            silver_team_standings[
                "snapshot_date"
            ]
        )
        ==
        date_lookup[
            "full_date"
        ],

        "left"
    )

    .withColumn(
        "created_at",

        F.lit(
            load_timestamp
        ).cast(
            "timestamp"
        )
    )

    .withColumn(
        "updated_at",

        F.lit(
            load_timestamp
        ).cast(
            "timestamp"
        )
    )

    .select(
        silver_team_standings[
            "team_id"
        ],

        "date_id",

        "season_id",
        "game_type_id",
        "clinch_indicator",

        "games_played",
        "wins",
        "losses",
        "ot_losses",
        "ties",
        "points",
        "point_pctg",
        "win_pctg",
        "regulation_wins",
        "regulation_win_pctg",
        "regulation_plus_ot_wins",
        "regulation_plus_ot_win_pctg",
        "shootout_wins",
        "shootout_losses",

        "goals_for",
        "goals_against",
        "goal_differential",
        "goals_for_pctg",
        "goal_differential_pctg",

        "home_games_played",
        "home_wins",
        "home_losses",
        "home_ot_losses",
        "home_ties",
        "home_points",
        "home_goals_for",
        "home_goals_against",
        "home_goal_differential",
        "home_regulation_wins",
        "home_regulation_plus_ot_wins",

        "road_games_played",
        "road_wins",
        "road_losses",
        "road_ot_losses",
        "road_ties",
        "road_points",
        "road_goals_for",
        "road_goals_against",
        "road_goal_differential",
        "road_regulation_wins",
        "road_regulation_plus_ot_wins",

        "l10_games_played",
        "l10_wins",
        "l10_losses",
        "l10_ot_losses",
        "l10_ties",
        "l10_points",
        "l10_goals_for",
        "l10_goals_against",
        "l10_goal_differential",
        "l10_regulation_wins",
        "l10_regulation_plus_ot_wins",

        "league_sequence",
        "league_home_sequence",
        "league_road_sequence",
        "league_l10_sequence",

        "conference_sequence",
        "conference_home_sequence",
        "conference_road_sequence",
        "conference_l10_sequence",

        "division_sequence",
        "division_home_sequence",
        "division_road_sequence",
        "division_l10_sequence",

        "wildcard_sequence",
        "waivers_sequence",

        "streak_code",
        "streak_count",

        "created_at",
        "updated_at"
    )
)


write_warehouse_table(
    fact_team_standings,
    "fact_team_standings",
    "overwrite"
)


print(
    f"fact_team_standings loaded: "
    f"{fact_team_standings.count()} rows."
)

print()


# ============================================================
# FACT PLAYER GAME STATS
# ============================================================

print(
    "Loading fact_player_game_stats..."
)


# ------------------------------------------------------------
# Get Game Date Lookup
# ------------------------------------------------------------

game_date_lookup = (
    silver_games

    .select(
        "game_id",
        F.col(
            "game_date"
        )
        .cast(
            "date"
        )
        .alias(
            "game_date"
        )
    )

    .dropDuplicates(
        [
            "game_id"
        ]
    )
)


# ------------------------------------------------------------
# Build Fact Player Game Stats
# ------------------------------------------------------------

fact_player_game_stats = (
    silver_player_game_stats

    .join(
        game_date_lookup,

        "game_id",

        "left"
    )

    .join(
        date_lookup,

        F.col(
            "game_date"
        )
        ==
        date_lookup[
            "full_date"
        ],

        "left"
    )

    .filter(
        F.col(
            "date_id"
        ).isNotNull()
    )

    .withColumn(
        "created_at",

        F.lit(
            load_timestamp
        ).cast(
            "timestamp"
        )
    )

    .withColumn(
        "updated_at",

        F.lit(
            load_timestamp
        ).cast(
            "timestamp"
        )
    )

    .select(
        "player_id",
        "game_id",
        "date_id",

        "goals",
        "assists",
        "points",
        "game_winning_goals",
        "overtime_goals",

        "power_play_goals",
        "power_play_points",
        "shorthanded_goals",
        "shorthanded_points",

        "shots",
        "plus_minus",
        "shifts",
        "pim",
        "time_on_ice",

        "created_at",
        "updated_at"
    )
)


write_warehouse_table(
    fact_player_game_stats,
    "fact_player_game_stats",
    "overwrite"
)


print(
    f"fact_player_game_stats loaded: "
    f"{fact_player_game_stats.count()} rows."
)

print()


# ============================================================
# PIPELINE SUMMARY
# ============================================================

print("=" * 60)
print("NHL GOLD PIPELINE COMPLETE")
print("=" * 60)

print()

print(
    f"dim_date: "
    f"{dim_date.count()}"
)

print(
    f"dim_team: "
    f"{dim_team.count()}"
)

print(
    f"dim_player: "
    f"{dim_player.count()}"
)

print(
    f"dim_game: "
    f"{dim_game.count()}"
)

print(
    f"fact_rosters: "
    f"{fact_rosters.count()}"
)

print(
    f"fact_team_standings: "
    f"{fact_team_standings.count()}"
)

print(
    f"fact_player_game_stats: "
    f"{fact_player_game_stats.count()}"
)

print()

print(
    "Gold Warehouse load completed successfully."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
