# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "warehouse": {
# META       "default_warehouse": "693b82e4-7ac9-42a5-b7f2-3e1e044613a6",
# META       "known_warehouses": [
# META         {
# META           "id": "693b82e4-7ac9-42a5-b7f2-3e1e044613a6",
# META           "type": "Lakewarehouse"
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

# ==========================================================
# NHL ANALYTICS
# FABRIC BRONZE PIPELINE
#
# Shared utilities:
#   notebooks/shared/nhl_utils
#
# This notebook is responsible for:
#
#   1. Ingest standings
#   2. Ingest team rosters
#   3. Extract player IDs
#   4. Ingest player landing data
#   5. Ingest player game logs
#   6. Ingest current scores
#
# Reusable functionality such as:
#
#   - NHL API requests
#   - API rate limiting
#   - Retry handling
#   - Run folder creation
#   - JSONL file writing
#
# is contained in nhl_utils.
#
# NOTE: Run/reference nhl_utils before this notebook.
#
# TESTING:
#   MAX_TEAMS controls how many team roster requests are made.
#   MAX_PLAYERS controls how many player-related requests are made.
#
#   Set either value to None to remove that limit.
# ==========================================================


# ==========================================================
# CONFIGURATION
# ==========================================================

# The Bronze Lakehouse attached to this notebook.

LAKEHOUSE_PATH = (
    "abfss://1fbf55a3-b2fc-4021-8697-98890ec67e3f@"
    "onelake.dfs.fabric.microsoft.com/"
    "be67e106-5585-4ba8-844b-55d35cc9aeca"
)


# ----------------------------------------------------------
# TESTING LIMITS
# ----------------------------------------------------------
#
# These limits are temporary and are intended to keep API
# requests small while testing the Fabric pipeline.
#
# Set to None to process everything.
#
# Example:
#
#   MAX_TEAMS = 3
#   MAX_PLAYERS = 10
#
# This results in:
#
#   1 standings request
#   3 roster requests
#   10 player requests
#   10 game log requests
#   1 scores request
#
# Total: approximately 25 API requests
# ----------------------------------------------------------

MAX_TEAMS = 3
MAX_PLAYERS = 10


# ==========================================================
# NHL API ENDPOINTS
# ==========================================================

STANDINGS_URL = (
    f"{NHL_API_BASE_URL}/standings/now"
)

SCORES_URL = (
    f"{NHL_API_BASE_URL}/score/now"
)


# ==========================================================
# CREATE RUN FOLDER
# ==========================================================

run_folder, run_path = create_run_folder(
    LAKEHOUSE_PATH
)

print("=" * 60)
print("NHL BRONZE PIPELINE")
print("=" * 60)

print(
    f"Run folder: {run_folder}"
)

print()

print(
    "Testing configuration:"
)

print(
    f"  MAX_TEAMS: "
    f"{MAX_TEAMS if MAX_TEAMS is not None else 'UNLIMITED'}"
)

print(
    f"  MAX_PLAYERS: "
    f"{MAX_PLAYERS if MAX_PLAYERS is not None else 'UNLIMITED'}"
)

print()


# ==========================================================
# 1. INGEST STANDINGS
# ==========================================================

print(
    "Starting standings ingestion..."
)

standings_data = safe_get(
    STANDINGS_URL
)

standings_records = [
    {
        "source": "standings_now",
        "data": standings_data
    }
]

standings_path = bronze_file_path(
    run_path,
    "standings.jsonl"
)

write_jsonl(
    standings_path,
    standings_records
)

print(
    "Successfully wrote standings to:"
)

print(
    standings_path
)

print()


# ==========================================================
# 2. INGEST ROSTERS
# ==========================================================

print(
    "Starting roster ingestion..."
)

teams = [
    team["teamAbbrev"]["default"]
    for team in standings_data["standings"]
]


# ----------------------------------------------------------
# Apply team testing limit
# ----------------------------------------------------------

all_teams_count = len(teams)

if MAX_TEAMS is not None:

    teams = teams[:MAX_TEAMS]


print(
    f"Teams available: "
    f"{all_teams_count}"
)

print(
    f"Teams selected for this run: "
    f"{len(teams)}"
)

print()


roster_records = []

all_player_ids = set()


for index, team in enumerate(
    teams,
    start=1
):

    print(
        f"Getting roster {index}/{len(teams)}: "
        f"{team}"
    )

    url = roster_url(
        team
    )

    roster_data = safe_get(
        url
    )

    roster_records.append(
        {
            "team": team,
            "roster": roster_data
        }
    )

    # ------------------------------------------------------
    # Extract player IDs from roster
    # ------------------------------------------------------

    for group in [
        "forwards",
        "defensemen",
        "goalies"
    ]:

        for player in roster_data.get(
            group,
            []
        ):

            player_id = player.get(
                "id"
            )

            if player_id is not None:

                all_player_ids.add(
                    player_id
                )


rosters_path = bronze_file_path(
    run_path,
    "rosters.jsonl"
)

write_jsonl(
    rosters_path,
    roster_records
)

print()

print(
    f"Successfully wrote "
    f"{len(roster_records)} rosters."
)

print()


# ==========================================================
# 3. WRITE PLAYER IDS
# ==========================================================

player_ids = sorted(
    all_player_ids
)

all_player_count = len(player_ids)


# ----------------------------------------------------------
# Apply player testing limit
# ----------------------------------------------------------

if MAX_PLAYERS is not None:

    player_ids = player_ids[:MAX_PLAYERS]


print(
    f"Players found in selected rosters: "
    f"{all_player_count}"
)

print(
    f"Players selected for this run: "
    f"{len(player_ids)}"
)

print()


player_ids_records = [
    {
        "source": "rosters",
        "player_ids": player_ids
    }
]

player_ids_path = bronze_file_path(
    run_path,
    "player_ids.jsonl"
)

write_jsonl(
    player_ids_path,
    player_ids_records
)

print(
    f"Wrote {len(player_ids)} player IDs."
)

print()


# ==========================================================
# 4. INGEST PLAYERS
# ==========================================================

print(
    "Starting player ingestion..."
)

player_records = []


for index, player_id in enumerate(
    player_ids,
    start=1
):

    print(
        f"Getting player {index}/{len(player_ids)}: "
        f"{player_id}"
    )

    url = player_landing_url(
        player_id
    )

    player_data = safe_get(
        url
    )

    player_records.append(
        {
            "player_id": player_id,
            "data": player_data
        }
    )


players_path = bronze_file_path(
    run_path,
    "players.jsonl"
)

write_jsonl(
    players_path,
    player_records
)

print()

print(
    f"Successfully wrote "
    f"{len(player_records)} players."
)

print()


# ==========================================================
# 5. INGEST PLAYER GAME LOGS
# ==========================================================

print(
    "Starting player game log ingestion..."
)

game_log_records = []


for index, player_id in enumerate(
    player_ids,
    start=1
):

    print(
        f"Getting game log "
        f"{index}/{len(player_ids)}: "
        f"{player_id}"
    )

    url = player_game_log_url(
        player_id
    )

    game_log_data = safe_get(
        url
    )

    game_log_records.append(
        {
            "player_id": player_id,
            "game_logs": game_log_data
        }
    )


game_logs_path = bronze_file_path(
    run_path,
    "game_logs.jsonl"
)

write_jsonl(
    game_logs_path,
    game_log_records
)

print()

print(
    f"Successfully wrote "
    f"{len(game_log_records)} game logs."
)

print()


# ==========================================================
# 6. INGEST SCORES
# ==========================================================

print(
    "Starting scores ingestion..."
)

scores_data = safe_get(
    SCORES_URL
)

scores_records = [
    {
        "source": "scores_now",
        "data": scores_data
    }
]

scores_path = bronze_file_path(
    run_path,
    "scores.jsonl"
)

write_jsonl(
    scores_path,
    scores_records
)

print(
    "Successfully wrote scores."
)

print()


# ==========================================================
# PIPELINE SUMMARY
# ==========================================================

print("=" * 60)

print(
    "NHL BRONZE PIPELINE COMPLETE"
)

print("=" * 60)

print(
    f"Run folder: {run_folder}"
)

print()

print(
    "Testing configuration:"
)

print(
    f"  MAX_TEAMS: "
    f"{MAX_TEAMS if MAX_TEAMS is not None else 'UNLIMITED'}"
)

print(
    f"  MAX_PLAYERS: "
    f"{MAX_PLAYERS if MAX_PLAYERS is not None else 'UNLIMITED'}"
)

print()

print(
    "Files created:"
)

print(
    "  standings.jsonl"
)

print(
    "  rosters.jsonl"
)

print(
    "  player_ids.jsonl"
)

print(
    "  players.jsonl"
)

print(
    "  game_logs.jsonl"
)

print(
    "  scores.jsonl"
)

print()

print(
    f"Total teams processed: "
    f"{len(teams)}"
)

print(
    f"Total players found in selected rosters: "
    f"{all_player_count}"
)

print(
    f"Total players processed: "
    f"{len(player_ids)}"
)

print(
    f"Players successfully loaded: "
    f"{len(player_records)}"
)

print(
    f"Game logs successfully loaded: "
    f"{len(game_log_records)}"
)

print()

print(
    "Bronze Lakehouse path:"
)

print(
    run_path
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
