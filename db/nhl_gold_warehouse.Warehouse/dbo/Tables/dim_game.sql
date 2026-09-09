CREATE TABLE [dbo].[dim_game] (

	[game_id] bigint IDENTITY NOT NULL, 
	[nhl_game_id] int NOT NULL, 
	[date_id] int NOT NULL, 
	[season_id] int NOT NULL, 
	[game_type_id] int NOT NULL, 
	[game_date] date NULL, 
	[start_time_utc] datetime2(6) NULL, 
	[away_team_id] bigint NOT NULL, 
	[home_team_id] bigint NOT NULL, 
	[away_team_score] int NULL, 
	[home_team_score] int NULL, 
	[away_shots_on_goal] int NULL, 
	[home_shots_on_goal] int NULL, 
	[venue] varchar(255) NULL, 
	[venue_time_zone] varchar(100) NULL, 
	[created_at] datetime2(6) NULL, 
	[updated_at] datetime2(6) NULL
);