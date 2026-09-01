CREATE TABLE [dbo].[games] (

	[game_id] int NOT NULL, 
	[season_id] int NULL, 
	[game_type_id] int NULL, 
	[game_date] date NULL, 
	[start_time_utc] datetime2(6) NULL, 
	[away_team_id] int NULL, 
	[home_team_id] int NULL, 
	[away_team_score] int NULL, 
	[home_team_score] int NULL, 
	[away_shots_on_goal] int NULL, 
	[home_shots_on_goal] int NULL, 
	[venue] varchar(255) NULL, 
	[venue_time_zone] varchar(100) NULL, 
	[created_at] datetime2(6) NULL, 
	[updated_at] datetime2(6) NULL
);