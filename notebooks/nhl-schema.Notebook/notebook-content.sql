-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "a27e4812-8fa2-b781-47ee-e9316bf2b707",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "a27e4812-8fa2-b781-47ee-e9316bf2b707",
-- META           "type": "Datawarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- ============================================================
-- NHL ANALYTICS - SILVER WAREHOUSE TABLE SETUP
-- ============================================================
--
-- Run this script in:
-- nhl_silver_warehouse
--
-- This script creates the normalized Silver layer tables.
-- ============================================================


-- ============================================================
-- Teams
-- ============================================================

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables
    WHERE name = 'teams'
)
BEGIN
    CREATE TABLE teams (

        -- Business Key
        team_id INT NOT NULL,

        -- Team Information
        place_name VARCHAR(100),
        team_name VARCHAR(100),
        team_common_name VARCHAR(100),
        team_abbrev VARCHAR(10),

        -- Organization
        conference_abbrev VARCHAR(10),
        conference_name VARCHAR(100),
        division_abbrev VARCHAR(10),
        division_name VARCHAR(100),

        -- Assets
        team_logo VARCHAR(1000),

        -- Audit
        created_at DATETIME2(6),
        updated_at DATETIME2(6)
    );
END;


-- ============================================================
-- Players
-- ============================================================

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables
    WHERE name = 'players'
)
BEGIN
    CREATE TABLE players (

        -- Business Key
        player_id INT NOT NULL,

        -- Player Information
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        player_slug VARCHAR(255),

        -- Current Player Information
        is_active BIT,
        position VARCHAR(50),
        shoots_catches VARCHAR(10),

        -- Physical Attributes
        height_in_inches INT,
        height_in_centimeters INT,
        weight_in_pounds INT,
        weight_in_kilograms INT,

        -- Birth Information
        birth_date DATE,
        birth_city VARCHAR(100),
        birth_country VARCHAR(100),

        -- Draft Information
        draft_team_id INT,
        draft_year INT,
        draft_round INT,
        draft_pick_in_round INT,
        draft_overall_pick INT,

        -- Honors
        in_top_100_all_time BIT,
        in_hhof BIT,

        -- Assets
        headshot VARCHAR(1000),
        hero_image VARCHAR(1000),

        -- Audit
        created_at DATETIME2(6),
        updated_at DATETIME2(6)
    );
END;


-- ============================================================
-- Team Rosters
-- ============================================================

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables
    WHERE name = 'team_rosters'
)
BEGIN
    CREATE TABLE team_rosters (

        -- Relationships
        team_id INT NOT NULL,
        player_id INT NOT NULL,

        -- Roster Information
        sweater_number INT,
        position VARCHAR(50),

        -- Audit
        created_at DATETIME2(6),
        updated_at DATETIME2(6)
    );
END;


-- ============================================================
-- Player Roster History
-- ============================================================

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables
    WHERE name = 'player_roster_history'
)
BEGIN
    CREATE TABLE player_roster_history (

        -- Relationships
        player_id INT NOT NULL,
        team_id INT NOT NULL,

        -- Roster History
        start_date DATE NOT NULL,
        end_date DATE,

        -- Player Information at Time of Roster
        sweater_number INT,
        position VARCHAR(50),

        -- Current Record Indicator
        is_current BIT,

        -- Audit
        created_at DATETIME2(6),
        updated_at DATETIME2(6)
    );
END;


-- ============================================================
-- Team Standings
-- ============================================================

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables
    WHERE name = 'team_standings'
)
BEGIN
    CREATE TABLE team_standings (

        -- Business Key
        team_id INT NOT NULL,

        -- Snapshot Date
        standings_date DATE NOT NULL,

        -- Context
        season_id INT,
        game_type_id INT,
        clinch_indicator VARCHAR(50),

        -- Overall Record
        games_played INT,
        wins INT,
        losses INT,
        ot_losses INT,
        ties INT,
        points INT,
        point_pctg DECIMAL(8,6),
        win_pctg DECIMAL(8,6),
        regulation_wins INT,
        regulation_win_pctg DECIMAL(8,6),
        regulation_plus_ot_wins INT,
        regulation_plus_ot_win_pctg DECIMAL(8,6),
        shootout_wins INT,
        shootout_losses INT,

        -- Goals
        goals_for INT,
        goals_against INT,
        goal_differential INT,
        goals_for_pctg DECIMAL(8,6),
        goal_differential_pctg DECIMAL(8,6),

        -- Home Record
        home_games_played INT,
        home_wins INT,
        home_losses INT,
        home_ot_losses INT,
        home_ties INT,
        home_points INT,
        home_goals_for INT,
        home_goals_against INT,
        home_goal_differential INT,
        home_regulation_wins INT,
        home_regulation_plus_ot_wins INT,

        -- Road Record
        road_games_played INT,
        road_wins INT,
        road_losses INT,
        road_ot_losses INT,
        road_ties INT,
        road_points INT,
        road_goals_for INT,
        road_goals_against INT,
        road_goal_differential INT,
        road_regulation_wins INT,
        road_regulation_plus_ot_wins INT,

        -- Last 10 Games
        l10_games_played INT,
        l10_wins INT,
        l10_losses INT,
        l10_ot_losses INT,
        l10_ties INT,
        l10_points INT,
        l10_goals_for INT,
        l10_goals_against INT,
        l10_goal_differential INT,
        l10_regulation_wins INT,
        l10_regulation_plus_ot_wins INT,

        -- Rankings
        league_sequence INT,
        league_home_sequence INT,
        league_road_sequence INT,
        league_l10_sequence INT,
        conference_sequence INT,
        conference_home_sequence INT,
        conference_road_sequence INT,
        conference_l10_sequence INT,
        division_sequence INT,
        division_home_sequence INT,
        division_road_sequence INT,
        division_l10_sequence INT,
        wildcard_sequence INT,
        waivers_sequence INT,

        -- Streak
        streak_code VARCHAR(20),
        streak_count INT,

        -- Audit
        created_at DATETIME2(6),
        updated_at DATETIME2(6)
    );
END;


-- ============================================================
-- Games
-- ============================================================

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables
    WHERE name = 'games'
)
BEGIN
    CREATE TABLE games (

        -- Business Key
        game_id INT NOT NULL,

        -- Game Context
        season_id INT,
        game_type_id INT,
        game_date DATE,
        start_time_utc DATETIME2(6),

        -- Team Relationships
        away_team_id INT,
        home_team_id INT,

        -- Game Results
        away_team_score INT,
        home_team_score INT,
        away_shots_on_goal INT,
        home_shots_on_goal INT,

        -- Venue
        venue VARCHAR(255),
        venue_time_zone VARCHAR(100),

        -- Audit
        created_at DATETIME2(6),
        updated_at DATETIME2(6)
    );
END;


-- ============================================================
-- Player Game Stats
-- ============================================================

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables
    WHERE name = 'player_game_stats'
)
BEGIN
    CREATE TABLE player_game_stats (

        -- Relationships
        player_id INT NOT NULL,
        game_id INT NOT NULL,

        -- Game Date
        game_date DATE,

        -- Scoring
        goals INT,
        assists INT,
        points INT,
        game_winning_goals INT,
        overtime_goals INT,

        -- Special Teams
        power_play_goals INT,
        power_play_points INT,
        shorthanded_goals INT,
        shorthanded_points INT,

        -- Other Statistics
        shots INT,
        plus_minus INT,
        shifts INT,
        pim INT,
        time_on_ice VARCHAR(20),

        -- Audit
        created_at DATETIME2(6),
        updated_at DATETIME2(6)
    );
END;


-- ============================================================
-- Verification
-- ============================================================

SELECT
    name AS table_name
FROM sys.tables
WHERE schema_id = SCHEMA_ID('dbo')
ORDER BY name;







-- ============================================================
-- NHL ANALYTICS - GOLD WAREHOUSE TABLE SETUP
-- ============================================================
--
-- Run this script in:
-- nhl_gold_warehouse
--
-- This script creates the Gold dimensional and fact tables.
-- ============================================================


-- ============================================================
-- DIM TEAM
-- ============================================================

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables
    WHERE name = 'dim_team'
)
BEGIN
    CREATE TABLE dim_team (

        -- Business Key
        team_id INT NOT NULL,

        -- Team Information
        place_name VARCHAR(100),
        team_name VARCHAR(100),
        team_common_name VARCHAR(100),
        team_abbrev VARCHAR(10),

        -- Organization
        conference_abbrev VARCHAR(10),
        conference_name VARCHAR(100),
        division_abbrev VARCHAR(10),
        division_name VARCHAR(100),

        -- Assets
        team_logo VARCHAR(1000),

        -- Audit
        created_at DATETIME2(6),
        updated_at DATETIME2(6)
    );
END;


-- ============================================================
-- DIM PLAYER
-- ============================================================

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables
    WHERE name = 'dim_player'
)
BEGIN
    CREATE TABLE dim_player (

        -- Business Key
        player_id INT NOT NULL,

        -- Relationships
        draft_team_id INT,

        -- Player Information
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        player_slug VARCHAR(255),

        -- Current Player Information
        is_active BIT,
        position VARCHAR(50),
        shoots_catches VARCHAR(10),

        -- Physical Attributes
        height_in_inches INT,
        height_in_centimeters INT,
        weight_in_pounds INT,
        weight_in_kilograms INT,

        -- Birth Information
        birth_date DATE,
        birth_city VARCHAR(100),
        birth_country VARCHAR(100),

        -- Draft Information
        draft_year INT,
        draft_round INT,
        draft_pick_in_round INT,
        draft_overall_pick INT,

        -- Honors
        in_top_100_all_time BIT,
        in_hhof BIT,

        -- Assets
        headshot VARCHAR(1000),
        hero_image VARCHAR(1000),

        -- Audit
        created_at DATETIME2(6),
        updated_at DATETIME2(6)
    );
END;


-- ============================================================
-- DIM DATE
-- ============================================================

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables
    WHERE name = 'dim_date'
)
BEGIN
    CREATE TABLE dim_date (

        -- Date Key
        date_id INT NOT NULL,

        -- Date
        full_date DATE NOT NULL,

        -- Day
        day INT,
        day_name VARCHAR(20),
        day_of_week INT,

        -- Week
        week_of_year INT,

        -- Month
        month INT,
        month_name VARCHAR(20),

        -- Quarter
        quarter INT,

        -- Year
        year INT,

        -- Weekend Indicator
        is_weekend BIT,

        -- Audit
        created_at DATETIME2(6),
        updated_at DATETIME2(6)
    );
END;


-- ============================================================
-- DIM GAME
-- ============================================================

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables
    WHERE name = 'dim_game'
)
BEGIN
    CREATE TABLE dim_game (

        -- Business Key
        game_id INT NOT NULL,

        -- Date Relationship
        date_id INT NOT NULL,

        -- Game Context
        season_id INT,
        game_type_id INT,
        game_date DATE,
        start_time_utc DATETIME2(6),

        -- Team Relationships
        away_team_id INT,
        home_team_id INT,

        -- Game Results
        away_team_score INT,
        home_team_score INT,
        away_shots_on_goal INT,
        home_shots_on_goal INT,

        -- Venue
        venue VARCHAR(255),
        venue_time_zone VARCHAR(100),

        -- Audit
        created_at DATETIME2(6),
        updated_at DATETIME2(6)
    );
END;


-- ============================================================
-- FACT ROSTERS
-- ============================================================

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables
    WHERE name = 'fact_rosters'
)
BEGIN
    CREATE TABLE fact_rosters (

        -- Dimension Keys
        team_id INT NOT NULL,
        player_id INT NOT NULL,
        date_id INT NOT NULL,

        -- Roster Information
        sweater_number INT,

        -- Audit
        created_at DATETIME2(6),
        updated_at DATETIME2(6)
    );
END;


-- ============================================================
-- FACT TEAM STANDINGS
-- ============================================================

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables
    WHERE name = 'fact_team_standings'
)
BEGIN
    CREATE TABLE fact_team_standings (

        -- Dimension Keys
        team_id INT NOT NULL,
        date_id INT NOT NULL,

        -- Context
        season_id INT,
        game_type_id INT,
        clinch_indicator VARCHAR(50),

        -- Overall Record
        games_played INT,
        wins INT,
        losses INT,
        ot_losses INT,
        ties INT,
        points INT,
        point_pctg DECIMAL(8,6),
        win_pctg DECIMAL(8,6),
        regulation_wins INT,
        regulation_win_pctg DECIMAL(8,6),
        regulation_plus_ot_wins INT,
        regulation_plus_ot_win_pctg DECIMAL(8,6),
        shootout_wins INT,
        shootout_losses INT,

        -- Goals
        goals_for INT,
        goals_against INT,
        goal_differential INT,
        goals_for_pctg DECIMAL(8,6),
        goal_differential_pctg DECIMAL(8,6),

        -- Home Record
        home_games_played INT,
        home_wins INT,
        home_losses INT,
        home_ot_losses INT,
        home_ties INT,
        home_points INT,
        home_goals_for INT,
        home_goals_against INT,
        home_goal_differential INT,
        home_regulation_wins INT,
        home_regulation_plus_ot_wins INT,

        -- Road Record
        road_games_played INT,
        road_wins INT,
        road_losses INT,
        road_ot_losses INT,
        road_ties INT,
        road_points INT,
        road_goals_for INT,
        road_goals_against INT,
        road_goal_differential INT,
        road_regulation_wins INT,
        road_regulation_plus_ot_wins INT,

        -- Last 10 Games
        l10_games_played INT,
        l10_wins INT,
        l10_losses INT,
        l10_ot_losses INT,
        l10_ties INT,
        l10_points INT,
        l10_goals_for INT,
        l10_goals_against INT,
        l10_goal_differential INT,
        l10_regulation_wins INT,
        l10_regulation_plus_ot_wins INT,

        -- Rankings
        league_sequence INT,
        league_home_sequence INT,
        league_road_sequence INT,
        league_l10_sequence INT,
        conference_sequence INT,
        conference_home_sequence INT,
        conference_road_sequence INT,
        conference_l10_sequence INT,
        division_sequence INT,
        division_home_sequence INT,
        division_road_sequence INT,
        division_l10_sequence INT,
        wildcard_sequence INT,
        waivers_sequence INT,

        -- Streak
        streak_code VARCHAR(20),
        streak_count INT,

        -- Audit
        created_at DATETIME2(6),
        updated_at DATETIME2(6)
    );
END;


-- ============================================================
-- FACT PLAYER GAME STATS
-- ============================================================

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables
    WHERE name = 'fact_player_game_stats'
)
BEGIN
    CREATE TABLE fact_player_game_stats (

        -- Dimension Keys
        player_id INT NOT NULL,
        game_id INT NOT NULL,
        date_id INT NOT NULL,

        -- Scoring
        goals INT,
        assists INT,
        points INT,
        game_winning_goals INT,
        overtime_goals INT,

        -- Special Teams
        power_play_goals INT,
        power_play_points INT,
        shorthanded_goals INT,
        shorthanded_points INT,

        -- Other Statistics
        shots INT,
        plus_minus INT,
        shifts INT,
        pim INT,
        time_on_ice VARCHAR(20),

        -- Audit
        created_at DATETIME2(6),
        updated_at DATETIME2(6)
    );
END;


-- ============================================================
-- VERIFICATION
-- ============================================================

SELECT
    name AS table_name
FROM sys.tables
WHERE schema_id = SCHEMA_ID('dbo')
ORDER BY name;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
