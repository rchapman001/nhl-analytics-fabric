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
# META           "id": "be67e106-5585-4ba8-844b-55d35cc9aeca"
# META         },
# META         {
# META           "id": "63da3f3e-aa63-453a-acff-7e3540f60e1a"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
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
#           rosters
#           team_standings
#           games
#           player_game_stats
#
# ============================================================
#
# SHARED UTILITIES
#
# nhl_utils provides:
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
# NOTE:
#
# Silver Delta tables are managed directly through Spark.
#
# No Warehouse connector is required.
#
# NOTE: Run/reference nhl_utils before this notebook.
# ============================================================


# ============================================================
# PYSPARK IMPORTS
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
)
from datetime import datetime
from zoneinfo import ZoneInfo
# ============================================================
# CONFIGURATION
# ============================================================

# ------------------------------------------------------------
# Bronze Lakehouse
# ------------------------------------------------------------

BRONZE_LAKEHOUSE = "nhl_bronze_lakehouse"
BRONZE_RUN = "20260628_000000"
BRONZE_ABFS_ROOT = (
    "abfss://1fbf55a3-b2fc-4021-8697-98890ec67e3f"
    "@onelake.dfs.fabric.microsoft.com/"
    "be67e106-5585-4ba8-844b-55d35cc9aeca/"
    f"Files/runs/{BRONZE_RUN}"
)
SILVER_LAKEHOUSE = "nhl_silver_lakehouse"
# ============================================================
# SILVER TABLE NAMES
# ============================================================

SILVER_TABLES = [
    "teams",
    "players",
    "rosters",
    "team_standings",
    "games",
    "player_game_stats"
]
print("=" * 70)
print("NHL ANALYTICS - FABRIC SILVER PIPELINE")
print("=" * 70)
print()
print(f"Bronze Run       : {BRONZE_RUN}")
print()
print()
print("Silver Tables:")
for table_name in SILVER_TABLES:
    print(
        f"  {table_name}"
    )
print()
print("=" * 70)
print()
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
print("✓ Bronze ABFS configuration is valid.")
print()
print("=" * 70)
# ============================================================
# VALIDATE SILVER LAKEHOUSE CONFIGURATION
# ============================================================

print("VALIDATING SILVER LAKEHOUSE CONFIGURATION")
print("=" * 70)
if not SILVER_LAKEHOUSE:
    raise ValueError(
        "SILVER_LAKEHOUSE is empty."
    )
print()
print(
    "✓ Silver Lakehouse configuration is valid."
)
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
print()
print("=" * 70)
# ============================================================
# VALIDATE BRONZE FILES
# ============================================================

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
print()
print("=" * 70)
# ============================================================
# READ BRONZE DATA
# ============================================================

print("READING BRONZE DATA")
print("=" * 70)
standings_df = read_jsonl(
    bronze_path("standings.jsonl")
)
rosters_df = read_jsonl(
    bronze_path("rosters.jsonl")
)
players_df = read_jsonl(
    bronze_path("players.jsonl")
)
game_logs_df = read_jsonl(
    bronze_path("game_logs.jsonl")
)
scores_df = read_jsonl(
    bronze_path("scores.jsonl")
)
print()
print("=" * 70)
print("BRONZE DATA SUCCESSFULLY LOADED")
print("=" * 70)
print()
print("=" * 70)
# ============================================================
# BUILD TEAM LOOKUP
# ============================================================

print("BUILDING TEAM LOOKUP")
print("=" * 70)
team_lookup_df = (
    players_df
    .select(
        F.col("data.currentTeamAbbrev")
            .alias("team_abbrev"),
        F.col("data.currentTeamId")
            .cast("integer")
            .alias("team_id")
    )
    .filter(
        F.col("team_abbrev").isNotNull()
        &
        F.col("team_id").isNotNull()
    )
    .dropDuplicates(
        ["team_abbrev"]
    )
)
team_lookup_count = team_lookup_df.count()
print()
print(
    f"Team lookup contains "
    f"{team_lookup_count:,} teams."
)
print()
print("=" * 70)
# ============================================================
# BUILD GAME LOOKUP
# ============================================================

print("BUILDING GAME LOOKUP")
print("=" * 70)
game_lookup_df = (
    scores_df
    .select(
        F.explode(
            F.col("data.games")
        ).alias("game")
    )
    .select(
        F.col("game.id")
            .cast("integer")
            .alias("game_id")
    )
    .filter(
        F.col("game_id").isNotNull()
    )
    .dropDuplicates(
        ["game_id"]
    )
)
game_lookup_count = game_lookup_df.count()
print()
print(
    f"Game lookup contains "
    f"{game_lookup_count:,} games."
)
print()
print("=" * 70)
# ============================================================
# PROCESS TEAMS
# ============================================================

print("PROCESSING TEAMS")
print("=" * 70)
teams_source_df = (
    standings_df
    .select(
        F.explode(
            F.col("data.standings")
        ).alias("standing")
    )
)
teams_df = (
    teams_source_df
    .select(
        F.col("standing.teamAbbrev.default")
            .alias("team_abbrev"),
        F.col("standing.placeName.default")
            .alias("place_name"),
        F.col("standing.teamName.default")
            .alias("team_name"),
        F.col("standing.teamCommonName.default")
            .alias("team_common_name"),
        F.col("standing.conferenceAbbrev")
            .alias("conference_abbrev"),
        F.col("standing.conferenceName")
            .alias("conference_name"),
        F.col("standing.divisionAbbrev")
            .alias("division_abbrev"),
        F.col("standing.divisionName")
            .alias("division_name"),
        F.col("standing.teamLogo")
            .alias("team_logo")
    )
    .join(
        team_lookup_df,
        on="team_abbrev",
        how="left"
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
        "team_logo"
    )
    .filter(
        F.col("team_id").isNotNull()
    )
    .dropDuplicates(
        ["team_id"]
    )
)
teams_count = teams_df.count()
print()
print(
    f"Teams prepared: "
    f"{teams_count:,}"
)
print()
print("WRITING TEAMS")
print("-" * 70)
write_delta_table(
    teams_df,
    "teams",
    mode="overwrite"
)
print()
print("=" * 70)
# ============================================================
# PROCESS PLAYERS
# ============================================================

print("PROCESSING PLAYERS")
print("=" * 70)
players_source_df = (
    players_df
    .select(
        F.col("data").alias("player")
    )
)
players_silver_df = (
    players_source_df
    .select(
        F.col("player.playerId")
            .cast("integer")
            .alias("player_id"),
        F.col("player.draftDetails.teamAbbrev")
            .alias("draft_team_abbrev"),
        F.col("player.firstName.default")
            .alias("first_name"),
        F.col("player.lastName.default")
            .alias("last_name"),
        F.col("player.playerSlug")
            .alias("player_slug"),
        F.col("player.isActive")
            .cast("boolean")
            .alias("is_active"),
        F.col("player.position")
            .alias("position"),
        F.col("player.shootsCatches")
            .alias("shoots_catches"),
        F.col("player.heightInInches")
            .cast("integer")
            .alias("height_in_inches"),
        F.col("player.heightInCentimeters")
            .cast("integer")
            .alias("height_in_centimeters"),
        F.col("player.weightInPounds")
            .cast("integer")
            .alias("weight_in_pounds"),
        F.col("player.weightInKilograms")
            .cast("integer")
            .alias("weight_in_kilograms"),
        F.to_date(
            F.col("player.birthDate")
        ).alias("birth_date"),
        F.col("player.birthCity.default")
            .alias("birth_city"),
        F.col("player.birthCountry")
            .alias("birth_country"),
        F.col("player.draftDetails.year")
            .cast("integer")
            .alias("draft_year"),
        F.col("player.draftDetails.round")
            .cast("integer")
            .alias("draft_round"),
        F.col("player.draftDetails.pickInRound")
            .cast("integer")
            .alias("draft_pick_in_round"),
        F.col("player.draftDetails.overallPick")
            .cast("integer")
            .alias("draft_overall_pick"),
        F.coalesce(
            F.col("player.inTop100AllTime")
                .cast("boolean"),
            F.lit(False)
        ).alias(
            "in_top_100_all_time"
        ),
        F.coalesce(
            F.col("player.inHHOF")
                .cast("boolean"),
            F.lit(False)
        ).alias(
            "in_hhof"
        ),
        F.col("player.headshot")
            .alias("headshot"),
        F.col("player.heroImage")
            .alias("hero_image")
    )
    .join(
        team_lookup_df,
        F.col("draft_team_abbrev")
        ==
        F.col("team_abbrev"),
        "left"
    )
    .select(
        F.col("player_id"),
        F.col("team_id")
            .alias("draft_team_id"),
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
        "hero_image"
    )
    .filter(
        F.col("player_id").isNotNull()
    )
    .dropDuplicates(
        ["player_id"]
    )
)
players_count = players_silver_df.count()
print()
print(
    f"Players prepared: "
    f"{players_count:,}"
)
print()
print("WRITING PLAYERS")
print("-" * 70)
write_delta_table(
    players_silver_df,
    "players",
    mode="overwrite"
)
print()
print("=" * 70)
# ============================================================
# PROCESS TEAM ROSTERS
# ============================================================

print("PROCESSING TEAM ROSTERS")
print("=" * 70)
snapshot_time = (
    datetime.now(
        ZoneInfo("America/Chicago")
    )
    .replace(tzinfo=None)
)
snapshot_date = snapshot_time.date()
print()
print(
    f"Roster snapshot time: "
    f"{snapshot_time}"
)
print(
    f"Roster snapshot date: "
    f"{snapshot_date}"
)
roster_source_df = (
    rosters_df
    .select(
        F.col("team").alias("team_abbrev"),
        F.col("roster")
    )
)
forwards_df = (
    roster_source_df
    .select(
        "team_abbrev",
        F.explode_outer(
            F.col("roster.forwards")
        ).alias("player")
    )
    .select(
        F.col("team_abbrev"),
        F.col("player.id")
            .cast("integer")
            .alias("player_id"),
        F.col("player.sweaterNumber")
            .cast("integer")
            .alias("sweater_number")
    )
)
defensemen_df = (
    roster_source_df
    .select(
        "team_abbrev",
        F.explode_outer(
            F.col("roster.defensemen")
        ).alias("player")
    )
    .select(
        F.col("team_abbrev"),
        F.col("player.id")
            .cast("integer")
            .alias("player_id"),
        F.col("player.sweaterNumber")
            .cast("integer")
            .alias("sweater_number")
    )
)
goalies_df = (
    roster_source_df
    .select(
        "team_abbrev",
        F.explode_outer(
            F.col("roster.goalies")
        ).alias("player")
    )
    .select(
        F.col("team_abbrev"),
        F.col("player.id")
            .cast("integer")
            .alias("player_id"),
        F.col("player.sweaterNumber")
            .cast("integer")
            .alias("sweater_number")
    )
)
roster_players_df = (
    forwards_df
    .unionByName(
        defensemen_df
    )
    .unionByName(
        goalies_df
    )
    .filter(
        F.col("player_id").isNotNull()
    )
)
incoming_rosters_df = (
    roster_players_df
    .join(
        team_lookup_df,
        on="team_abbrev",
        how="left"
    )
    .select(
        F.col("team_id")
            .cast("integer")
            .alias("team_id"),
        F.col("player_id")
            .cast("integer")
            .alias("player_id"),
        F.lit(snapshot_date)
            .cast("date")
            .alias("snapshot_date"),
        F.col("sweater_number")
            .cast("integer")
            .alias("sweater_number")
    )
    .filter(
        F.col("team_id").isNotNull()
        &
        F.col("player_id").isNotNull()
    )
    .dropDuplicates(
        ["team_id", "player_id"]
    )
)
rosters_count = incoming_rosters_df.count()
print()
print(
    f"Roster records prepared: "
    f"{rosters_count:,}"
)
if table_exists("rosters"):
    print()
    print(
        "Existing Silver Delta rosters table found."
    )
    existing_rosters_df = read_delta_table(
        "rosters"
    )
    existing_rosters_df = (
        existing_rosters_df
        .select(
            F.col("team_id")
                .cast("integer")
                .alias("team_id"),
            F.col("player_id")
                .cast("integer")
                .alias("player_id"),
            F.col("snapshot_date")
                .cast("date")
                .alias("snapshot_date"),
            F.col("effective_from")
                .cast("timestamp")
                .alias("effective_from"),
            F.col("effective_to")
                .cast("timestamp")
                .alias("effective_to"),
            F.col("sweater_number")
                .cast("integer")
                .alias("sweater_number"),
            F.col("created_at")
                .cast("timestamp")
                .alias("created_at"),
            F.col("updated_at")
                .cast("timestamp")
                .alias("updated_at")
        )
    )
    current_active_df = (
        existing_rosters_df
        .filter(
            F.col("effective_to").isNull()
        )
    )
    roster_changes_df = (
        incoming_rosters_df.alias("incoming")
        .join(
            current_active_df.alias("current"),
            F.col("incoming.player_id")
            ==
            F.col("current.player_id"),
            "left"
        )
        .filter(
            F.col("current.player_id").isNull()
            |
            (
                F.col("incoming.team_id")
                !=
                F.col("current.team_id")
            )
            |
            ~F.col(
                "incoming.sweater_number"
            ).eqNullSafe(
                F.col(
                    "current.sweater_number"
                )
            )
        )
        .select(
            F.col("incoming.team_id")
                .alias("team_id"),
            F.col("incoming.player_id")
                .alias("player_id"),
            F.col("incoming.snapshot_date")
                .alias("snapshot_date"),
            F.col("incoming.sweater_number")
                .alias("sweater_number")
        )
    )
    changed_count = roster_changes_df.count()
    print()
    print(
        f"Roster changes detected: "
        f"{changed_count:,}"
    )
    if changed_count == 0:
        print()
        print(
            "No roster changes detected."
        )
        final_rosters_df = (
            existing_rosters_df
        )
    else:
        changed_players_df = (
            roster_changes_df
            .select(
                "player_id"
            )
            .dropDuplicates(
                ["player_id"]
            )
        )
        closed_rosters_df = (
            existing_rosters_df.alias(
                "existing"
            )
            .join(
                changed_players_df.alias(
                    "changed"
                ),
                F.col(
                    "existing.player_id"
                )
                ==
                F.col(
                    "changed.player_id"
                ),
                "left"
            )
            .withColumn(
                "effective_to",
                F.when(
                    F.col(
                        "changed.player_id"
                    ).isNotNull()
                    &
                    F.col(
                        "existing.effective_to"
                    ).isNull(),
                    F.lit(
                        snapshot_time
                    ).cast(
                        "timestamp"
                    )
                )
                .otherwise(
                    F.col(
                        "existing.effective_to"
                    )
                )
            )
            .select(
                "existing.team_id",
                "existing.player_id",
                "existing.snapshot_date",
                "effective_from",
                "effective_to",
                "existing.sweater_number",
                "existing.created_at",
                "existing.updated_at"
            )
        )
        new_roster_records_df = (
            roster_changes_df
            .withColumn(
                "effective_from",
                F.lit(
                    snapshot_time
                ).cast(
                    "timestamp"
                )
            )
            .withColumn(
                "effective_to",
                F.lit(None)
                    .cast("timestamp")
            )
            .withColumn(
                "created_at",
                F.lit(
                    snapshot_time
                ).cast(
                    "timestamp"
                )
            )
            .withColumn(
                "updated_at",
                F.lit(
                    snapshot_time
                ).cast(
                    "timestamp"
                )
            )
            .select(
                "team_id",
                "player_id",
                "snapshot_date",
                "effective_from",
                "effective_to",
                "sweater_number",
                "created_at",
                "updated_at"
            )
        )
        final_rosters_df = (
            closed_rosters_df
            .unionByName(
                new_roster_records_df
            )
        )
        print()
        print(
            f"Historical roster records: "
            f"{final_rosters_df.count():,}"
        )
    final_rosters_df = (
        final_rosters_df
        .select(
            F.col("team_id")
                .cast("integer")
                .alias("team_id"),
            F.col("player_id")
                .cast("integer")
                .alias("player_id"),
            F.col("snapshot_date")
                .cast("date")
                .alias("snapshot_date"),
            F.col("effective_from")
                .cast("timestamp")
                .alias("effective_from"),
            F.col("effective_to")
                .cast("timestamp")
                .alias("effective_to"),
            F.col("sweater_number")
                .cast("integer")
                .alias("sweater_number"),
            F.col("created_at")
                .cast("timestamp")
                .alias("created_at"),
            F.col("updated_at")
                .cast("timestamp")
                .alias("updated_at")
        )
    )
    write_delta_table(
        final_rosters_df,
        "rosters",
        mode="overwrite"
    )
    print()
    print(
        "✓ Silver roster history updated."
    )
else:
    print()
    print(
        "Silver rosters Delta table does not exist."
    )
    print(
        "Creating initial roster snapshot."
    )
    initial_rosters_df = (
        incoming_rosters_df
        .withColumn(
            "effective_from",
            F.lit(
                snapshot_time
            ).cast(
                "timestamp"
            )
        )
        .withColumn(
            "effective_to",
            F.lit(None)
                .cast("timestamp")
        )
        .withColumn(
            "created_at",
            F.lit(
                snapshot_time
            ).cast(
                "timestamp"
            )
        )
        .withColumn(
            "updated_at",
            F.lit(
                snapshot_time
            ).cast(
                "timestamp"
            )
        )
        .select(
            F.col("team_id")
                .cast("integer")
                .alias("team_id"),
            F.col("player_id")
                .cast("integer")
                .alias("player_id"),
            F.col("snapshot_date")
                .cast("date")
                .alias("snapshot_date"),
            F.col("effective_from")
                .cast("timestamp")
                .alias("effective_from"),
            F.col("effective_to")
                .cast("timestamp")
                .alias("effective_to"),
            F.col("sweater_number")
                .cast("integer")
                .alias("sweater_number"),
            F.col("created_at")
                .cast("timestamp")
                .alias("created_at"),
            F.col("updated_at")
                .cast("timestamp")
                .alias("updated_at")
        )
    )
    write_delta_table(
        initial_rosters_df,
        "rosters",
        mode="overwrite"
    )
    print()
    print(
        f"✓ Created rosters with "
        f"{rosters_count:,} records."
    )
print()
print("=" * 70)
# ============================================================
# PROCESS TEAM STANDINGS
# ============================================================

print("PROCESSING TEAM STANDINGS")
print("=" * 70)
standings_source_df = (
    standings_df
    .select(
        F.col("data").alias("data")
    )
    .select(
        F.explode(
            F.col("data.standings")
        ).alias("standing"),
        F.col(
            "data.standingsDateTimeUtc"
        ).alias(
            "standings_datetime_utc"
        )
    )
)
team_standings_df = (
    standings_source_df
    .join(
        team_lookup_df,
        F.col(
            "standing.teamAbbrev.default"
        )
        ==
        F.col("team_abbrev"),
        "left"
    )
    .select(
        F.col("team_id")
            .cast("integer")
            .alias("team_id"),
        F.to_date(
            F.col(
                "standings_datetime_utc"
            )
        ).alias("snapshot_date"),
        F.col("standing.seasonId")
            .cast("integer")
            .alias("season_id"),
        F.col("standing.gameTypeId")
            .cast("integer")
            .alias("game_type_id"),
        F.col("standing.clinchIndicator")
            .alias("clinch_indicator"),
        F.col("standing.gamesPlayed")
            .cast("integer")
            .alias("games_played"),
        F.col("standing.wins")
            .cast("integer")
            .alias("wins"),
        F.col("standing.losses")
            .cast("integer")
            .alias("losses"),
        F.col("standing.otLosses")
            .cast("integer")
            .alias("ot_losses"),
        F.col("standing.ties")
            .cast("integer")
            .alias("ties"),
        F.col("standing.points")
            .cast("integer")
            .alias("points"),
        F.col("standing.pointPctg")
            .cast("decimal(8,6)")
            .alias("point_pctg"),
        F.col("standing.winPctg")
            .cast("decimal(8,6)")
            .alias("win_pctg"),
        F.col("standing.regulationWins")
            .cast("integer")
            .alias("regulation_wins"),
        F.col("standing.regulationWinPctg")
            .cast("decimal(8,6)")
            .alias("regulation_win_pctg"),
        F.col("standing.regulationPlusOtWins")
            .cast("integer")
            .alias("regulation_plus_ot_wins"),
        F.col("standing.regulationPlusOtWinPctg")
            .cast("decimal(8,6)")
            .alias(
                "regulation_plus_ot_win_pctg"
            ),
        F.col("standing.shootoutWins")
            .cast("integer")
            .alias("shootout_wins"),
        F.col("standing.shootoutLosses")
            .cast("integer")
            .alias("shootout_losses"),
        F.col("standing.goalFor")
            .cast("integer")
            .alias("goals_for"),
        F.col("standing.goalAgainst")
            .cast("integer")
            .alias("goals_against"),
        F.col("standing.goalDifferential")
            .cast("integer")
            .alias(
                "goal_differential"
            ),
        F.col("standing.goalsForPctg")
            .cast("decimal(8,6)")
            .alias("goals_for_pctg"),
        F.col("standing.goalDifferentialPctg")
            .cast("decimal(8,6)")
            .alias(
                "goal_differential_pctg"
            ),
        F.col("standing.homeGamesPlayed")
            .cast("integer")
            .alias(
                "home_games_played"
            ),
        F.col("standing.homeWins")
            .cast("integer")
            .alias("home_wins"),
        F.col("standing.homeLosses")
            .cast("integer")
            .alias(
                "home_losses"
            ),
        F.col("standing.homeOtLosses")
            .cast("integer")
            .alias(
                "home_ot_losses"
            ),
        F.col("standing.homeTies")
            .cast("integer")
            .alias("home_ties"),
        F.col("standing.homePoints")
            .cast("integer")
            .alias("home_points"),
        F.col("standing.homeGoalsFor")
            .cast("integer")
            .alias(
                "home_goals_for"
            ),
        F.col("standing.homeGoalsAgainst")
            .cast("integer")
            .alias(
                "home_goals_against"
            ),
        F.col("standing.homeGoalDifferential")
            .cast("integer")
            .alias(
                "home_goal_differential"
            ),
        F.col("standing.homeRegulationWins")
            .cast("integer")
            .alias(
                "home_regulation_wins"
            ),
        F.col(
            "standing.homeRegulationPlusOtWins"
        )
        .cast("integer")
        .alias(
            "home_regulation_plus_ot_wins"
        ),
        F.col("standing.roadGamesPlayed")
            .cast("integer")
            .alias(
                "road_games_played"
            ),
        F.col("standing.roadWins")
            .cast("integer")
            .alias("road_wins"),
        F.col("standing.roadLosses")
            .cast("integer")
            .alias(
                "road_losses"
            ),
        F.col("standing.roadOtLosses")
            .cast("integer")
            .alias(
                "road_ot_losses"
            ),
        F.col("standing.roadTies")
            .cast("integer")
            .alias("road_ties"),
        F.col("standing.roadPoints")
            .cast("integer")
            .alias("road_points"),
        F.col("standing.roadGoalsFor")
            .cast("integer")
            .alias(
                "road_goals_for"
            ),
        F.col("standing.roadGoalsAgainst")
            .cast("integer")
            .alias(
                "road_goals_against"
            ),
        F.col("standing.roadGoalDifferential")
            .cast("integer")
            .alias(
                "road_goal_differential"
            ),
        F.col("standing.roadRegulationWins")
            .cast("integer")
            .alias(
                "road_regulation_wins"
            ),
        F.col(
            "standing.roadRegulationPlusOtWins"
        )
        .cast("integer")
        .alias(
            "road_regulation_plus_ot_wins"
        ),
        F.col("standing.l10GamesPlayed")
            .cast("integer")
            .alias(
                "l10_games_played"
            ),
        F.col("standing.l10Wins")
            .cast("integer")
            .alias("l10_wins"),
        F.col("standing.l10Losses")
            .cast("integer")
            .alias(
                "l10_losses"
            ),
        F.col("standing.l10OtLosses")
            .cast("integer")
            .alias(
                "l10_ot_losses"
            ),
        F.col("standing.l10Ties")
            .cast("integer")
            .alias("l10_ties"),
        F.col("standing.l10Points")
            .cast("integer")
            .alias(
                "l10_points"
            ),
        F.col("standing.l10GoalsFor")
            .cast("integer")
            .alias(
                "l10_goals_for"
            ),
        F.col("standing.l10GoalsAgainst")
            .cast("integer")
            .alias(
                "l10_goals_against"
            ),
        F.col("standing.l10GoalDifferential")
            .cast("integer")
            .alias(
                "l10_goal_differential"
            ),
        F.col("standing.l10RegulationWins")
            .cast("integer")
            .alias(
                "l10_regulation_wins"
            ),
        F.col(
            "standing.l10RegulationPlusOtWins"
        )
        .cast("integer")
        .alias(
            "l10_regulation_plus_ot_wins"
        ),
        F.col("standing.leagueSequence")
            .cast("integer")
            .alias(
                "league_sequence"
            ),
        F.col("standing.leagueHomeSequence")
            .cast("integer")
            .alias(
                "league_home_sequence"
            ),
        F.col("standing.leagueRoadSequence")
            .cast("integer")
            .alias(
                "league_road_sequence"
            ),
        F.col("standing.leagueL10Sequence")
            .cast("integer")
            .alias(
                "league_l10_sequence"
            ),
        F.col("standing.conferenceSequence")
            .cast("integer")
            .alias(
                "conference_sequence"
            ),
        F.col(
            "standing.conferenceHomeSequence"
        )
        .cast("integer")
        .alias(
            "conference_home_sequence"
        ),
        F.col(
            "standing.conferenceRoadSequence"
        )
        .cast("integer")
        .alias(
            "conference_road_sequence"
        ),
        F.col(
            "standing.conferenceL10Sequence"
        )
        .cast("integer")
        .alias(
            "conference_l10_sequence"
        ),
        F.col("standing.divisionSequence")
            .cast("integer")
            .alias(
                "division_sequence"
            ),
        F.col(
            "standing.divisionHomeSequence"
        )
        .cast("integer")
        .alias(
            "division_home_sequence"
        ),
        F.col(
            "standing.divisionRoadSequence"
        )
        .cast("integer")
        .alias(
            "division_road_sequence"
        ),
        F.col(
            "standing.divisionL10Sequence"
        )
        .cast("integer")
        .alias(
            "division_l10_sequence"
        ),
        F.col("standing.wildcardSequence")
            .cast("integer")
            .alias(
                "wildcard_sequence"
            ),
        F.col("standing.waiversSequence")
            .cast("integer")
            .alias(
                "waivers_sequence"
            ),
        F.col("standing.streakCode")
            .alias("streak_code"),
        F.col("standing.streakCount")
            .cast("integer")
            .alias(
                "streak_count"
            )
    )
    .filter(
        F.col("team_id").isNotNull()
        &
        F.col("snapshot_date").isNotNull()
    )
    .dropDuplicates(
        ["team_id", "snapshot_date"]
    )
)
team_standings_count = (
    team_standings_df.count()
)
print()
print(
    f"Team standings prepared: "
    f"{team_standings_count:,}"
)
print()
print("WRITING TEAM STANDINGS")
print("-" * 70)
write_delta_table(
    team_standings_df,
    "team_standings",
    mode="overwrite"
)
print()
print("=" * 70)
# ============================================================
# PROCESS GAMES
# ============================================================

print("PROCESSING GAMES")
print("=" * 70)
team_result_schema = StructType(
    [
        StructField(
            "id",
            IntegerType(),
            True
        ),
        StructField(
            "name",
            StructType(
                [
                    StructField(
                        "default",
                        StringType(),
                        True
                    )
                ]
            ),
            True
        ),
        StructField(
            "abbrev",
            StringType(),
            True
        ),
        StructField(
            "record",
            StringType(),
            True
        ),
        StructField(
            "logo",
            StringType(),
            True
        ),
        StructField(
            "score",
            IntegerType(),
            True
        ),
        StructField(
            "sog",
            IntegerType(),
            True
        )
    ]
)
games_source_df = (
    scores_df
    .select(
        F.explode(
            F.col("data.games")
        ).alias("game")
    )
)
games_normalized_df = (
    games_source_df
    .withColumn(
        "away_team",
        F.from_json(
            F.to_json(
                F.col("game.awayTeam")
            ),
            team_result_schema
        )
    )
    .withColumn(
        "home_team",
        F.from_json(
            F.to_json(
                F.col("game.homeTeam")
            ),
            team_result_schema
        )
    )
)
games_df = (
    games_normalized_df
    .select(
        F.col("game.id")
            .cast("integer")
            .alias("game_id"),
        F.col("game.season")
            .cast("integer")
            .alias("season_id"),
        F.col("game.gameType")
            .cast("integer")
            .alias("game_type_id"),
        F.to_date(
            F.col("game.gameDate")
        ).alias("game_date"),
        F.to_timestamp(
            F.col("game.startTimeUTC")
        ).alias("start_time_utc"),
        F.col("away_team.id")
            .cast("integer")
            .alias("away_team_id"),
        F.col("home_team.id")
            .cast("integer")
            .alias("home_team_id"),
        F.col("away_team.score")
            .cast("integer")
            .alias("away_team_score"),
        F.col("home_team.score")
            .cast("integer")
            .alias("home_team_score"),
        F.col("away_team.sog")
            .cast("integer")
            .alias("away_shots_on_goal"),
        F.col("home_team.sog")
            .cast("integer")
            .alias("home_shots_on_goal"),
        F.col("game.venue.default")
            .alias("venue"),
        F.col("game.venueTimezone")
            .alias("venue_time_zone")
    )
    .filter(
        F.col("game_id").isNotNull()
    )
    .dropDuplicates(
        ["game_id"]
    )
)
games_count = games_df.count()
print()
print(
    f"Games prepared: "
    f"{games_count:,}"
)
print()
print("Game schema:")
games_df.printSchema()
print()
print("Game sample:")
games_df.select(
    "game_id",
    "game_date",
    "away_team_id",
    "home_team_id",
    "away_team_score",
    "home_team_score",
    "away_shots_on_goal",
    "home_shots_on_goal"
).show(
    10,
    truncate=False
)
print()
print("WRITING GAMES")
print("-" * 70)
write_delta_table(
    games_df,
    "games",
    mode="overwrite"
)
print()
print("=" * 70)
# ============================================================
# PROCESS PLAYER GAME STATS
# ============================================================

print("PROCESSING PLAYER GAME STATS")
print("=" * 70)
player_game_log_source_df = (
    game_logs_df
    .select(
        F.col("player_id")
            .cast("integer")
            .alias("player_id"),
        F.explode(
            F.col("game_logs.gameLog")
        ).alias("game")
    )
)
player_game_stats_df = (
    player_game_log_source_df
    .select(
        F.col("player_id"),
        F.col("game.gameId")
            .cast("integer")
            .alias("game_id"),
        F.col("game.goals")
            .cast("integer")
            .alias("goals"),
        F.col("game.assists")
            .cast("integer")
            .alias("assists"),
        F.col("game.points")
            .cast("integer")
            .alias("points"),
        F.col("game.gameWinningGoals")
            .cast("integer")
            .alias(
                "game_winning_goals"
            ),
        F.col("game.otGoals")
            .cast("integer")
            .alias(
                "overtime_goals"
            ),
        F.col("game.powerPlayGoals")
            .cast("integer")
            .alias(
                "power_play_goals"
            ),
        F.col("game.powerPlayPoints")
            .cast("integer")
            .alias(
                "power_play_points"
            ),
        F.col("game.shorthandedGoals")
            .cast("integer")
            .alias(
                "shorthanded_goals"
            ),
        F.col("game.shorthandedPoints")
            .cast("integer")
            .alias(
                "shorthanded_points"
            ),
        F.col("game.shots")
            .cast("integer")
            .alias("shots"),
        F.col("game.plusMinus")
            .cast("integer")
            .alias(
                "plus_minus"
            ),
        F.col("game.shifts")
            .cast("integer")
            .alias("shifts"),
        F.col("game.pim")
            .cast("integer")
            .alias("pim"),
        F.col("game.toi")
            .alias(
                "time_on_ice"
            )
    )
    .filter(
        F.col("player_id").isNotNull()
        &
        F.col("game_id").isNotNull()
    )
)
player_game_stats_df = (
    player_game_stats_df
    .join(
        game_lookup_df,
        on="game_id",
        how="inner"
    )
    .select(
        "player_id",
        "game_id",
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
        "time_on_ice"
    )
    .dropDuplicates(
        ["player_id", "game_id"]
    )
)
player_game_stats_count = (
    player_game_stats_df.count()
)
print()
print(
    f"Player game stats prepared: "
    f"{player_game_stats_count:,}"
)
print()
print("WRITING PLAYER GAME STATS")
print("-" * 70)
write_delta_table(
    player_game_stats_df,
    "player_game_stats",
    mode="overwrite"
)
print()
print("=" * 70)
# ============================================================
# VALIDATE SILVER DELTA TABLES
# ============================================================

print("VALIDATING SILVER DELTA TABLES")
print("=" * 70)
silver_counts = {}
for table_name in SILVER_TABLES:
    print()
    print(
        f"Checking Silver Delta table: "
        f"{table_name}"
    )
    try:
        table_df = read_delta_table(
            table_name
        )
        row_count = table_df.count()
        silver_counts[
            table_name
        ] = row_count
        print(
            f"  ✓ {table_name}: "
            f"{row_count:,} rows"
        )
    except Exception as error:
        print(
            f"  ✗ Failed to validate "
            f"{table_name}"
        )
        print()
        print(
            str(error)
        )
        raise
print()
print("=" * 70)
# ============================================================
# SILVER PIPELINE SUMMARY
# ============================================================

print("SILVER PIPELINE SUMMARY")
print("=" * 70)
print()
for table_name in SILVER_TABLES:
    row_count = silver_counts[
        table_name
    ]
    print(
        f"{table_name:25} "
        f"{row_count:>12,} rows"
    )
print()
print("=" * 70)
# ============================================================
# COMPLETE
# ============================================================

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
print("  OneLake")
print("      ↓")
print("  JSONL")
print("      ↓")
print("  PySpark")
print("      ↓")
print("  Silver Lakehouse")
print("      ↓")
print("  Delta Tables")
print()
print("✓ Silver pipeline completed successfully.")
print("=" * 70)# ============================================================

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
