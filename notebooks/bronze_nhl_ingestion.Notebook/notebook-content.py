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

import requests
import json
import time

from datetime import datetime, timezone


# ==========================================================
# CONFIGURATION
# ==========================================================

# The Bronze Lakehouse attached to this notebook.
LAKEHOUSE_PATH = (
    "abfss://1fbf55a3-b2fc-4021-8697-98890ec67e3f@"
    "onelake.dfs.fabric.microsoft.com/"
    "be67e106-5585-4ba8-844b-55d35cc9aeca"
)

# NHL API endpoints
STANDINGS_URL = "https://api-web.nhle.com/v1/standings/now"
SCORES_URL = "https://api-web.nhle.com/v1/score/now"

# Rate limiting configuration
MIN_INTERVAL = 1.0
MAX_RETRIES = 5

last_call = 0


# ==========================================================
# CREATE RUN FOLDER
# ==========================================================

run_folder = datetime.now(timezone.utc).strftime(
    "%Y%m%d_%H%M%S"
)

run_path = (
    f"{LAKEHOUSE_PATH}/Files/runs/{run_folder}"
)

print("=" * 60)
print("NHL BRONZE PIPELINE")
print("=" * 60)
print(f"Run folder: {run_folder}")
print()


# ==========================================================
# NHL API CLIENT
# ==========================================================

def safe_get(url):
    """
    Makes a rate-limited request to the NHL API.

    Retries requests that return HTTP 429.
    """

    global last_call

    # Ensure at least MIN_INTERVAL seconds between requests
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

            # Retry if rate limited
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


# ==========================================================
# WRITE JSONL FILE
# ==========================================================

def write_jsonl(path, records):
    """
    Writes a list of records to a JSONL file
    in the Bronze Lakehouse.
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


# ==========================================================
# 1. INGEST STANDINGS
# ==========================================================

print("Starting standings ingestion...")

standings_data = safe_get(
    STANDINGS_URL
)

standings_records = [
    {
        "source": "standings_now",
        "data": standings_data
    }
]

standings_path = (
    f"{run_path}/standings.jsonl"
)

write_jsonl(
    standings_path,
    standings_records
)

print(
    f"Successfully wrote standings to:"
)
print(
    standings_path
)
print()


# ==========================================================
# 2. INGEST ROSTERS
# ==========================================================

print("Starting roster ingestion...")

teams = [
    team["teamAbbrev"]["default"]
    for team in standings_data["standings"]
]

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

    url = (
        f"https://api-web.nhle.com/v1/"
        f"roster/{team}/current"
    )

    roster_data = safe_get(url)

    roster_records.append(
        {
            "team": team,
            "roster": roster_data
        }
    )

    for group in [
        "forwards",
        "defensemen",
        "goalies"
    ]:

        for player in roster_data.get(
            group,
            []
        ):

            player_id = player.get("id")

            if player_id is not None:

                all_player_ids.add(
                    player_id
                )

rosters_path = (
    f"{run_path}/rosters.jsonl"
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

player_ids_records = [
    {
        "source": "rosters",
        "player_ids": player_ids
    }
]

player_ids_path = (
    f"{run_path}/player_ids.jsonl"
)

write_jsonl(
    player_ids_path,
    player_ids_records
)

print(
    f"Found {len(player_ids)} unique players."
)

print()


# ==========================================================
# 4. INGEST PLAYERS
# ==========================================================

print("Starting player ingestion...")

player_records = []

for index, player_id in enumerate(
    player_ids,
    start=1
):

    print(
        f"Getting player {index}/{len(player_ids)}: "
        f"{player_id}"
    )

    url = (
        f"https://api-web.nhle.com/v1/"
        f"player/{player_id}/landing"
    )

    player_data = safe_get(url)

    player_records.append(
        {
            "player_id": player_id,
            "data": player_data
        }
    )

players_path = (
    f"{run_path}/players.jsonl"
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

failed_game_logs = []

for index, player_id in enumerate(
    player_ids,
    start=1
):

    print(
        f"Getting game log "
        f"{index}/{len(player_ids)}: "
        f"{player_id}"
    )

    url = (
        f"https://api-web.nhle.com/v1/"
        f"player/{player_id}/game-log/now"
    )

    try:

        game_log_data = safe_get(
            url
        )

        game_log_records.append(
            {
                "player_id": player_id,
                "game_logs": game_log_data
            }
        )

    except Exception as error:

        # Do not fail the entire Bronze pipeline
        # because one player endpoint failed.
        print(
            f"FAILED game log for "
            f"player {player_id}: {error}"
        )

        failed_game_logs.append(
            {
                "player_id": player_id,
                "error": str(error)
            }
        )

game_logs_path = (
    f"{run_path}/game_logs.jsonl"
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

if failed_game_logs:

    failed_game_logs_path = (
        f"{run_path}/failed_game_logs.jsonl"
    )

    write_jsonl(
        failed_game_logs_path,
        failed_game_logs
    )

    print(
        f"{len(failed_game_logs)} "
        f"game logs failed."
    )

print()


# ==========================================================
# 6. INGEST SCORES
# ==========================================================

print("Starting scores ingestion...")

scores_data = safe_get(
    SCORES_URL
)

scores_records = [
    {
        "source": "scores_now",
        "data": scores_data
    }
]

scores_path = (
    f"{run_path}/scores.jsonl"
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
print("NHL BRONZE PIPELINE COMPLETE")
print("=" * 60)

print(f"Run folder: {run_folder}")

print()
print("Files created:")

print(
    f"  standings.jsonl"
)
print(
    f"  rosters.jsonl"
)
print(
    f"  player_ids.jsonl"
)
print(
    f"  players.jsonl"
)
print(
    f"  game_logs.jsonl"
)
print(
    f"  scores.jsonl"
)

if failed_game_logs:

    print(
        f"  failed_game_logs.jsonl"
    )

print()
print(
    f"Total teams: {len(teams)}"
)

print(
    f"Total unique players: "
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

print(
    f"Failed game logs: "
    f"{len(failed_game_logs)}"
)

print()
print(
    "Bronze Lakehouse path:"
)

print(run_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
