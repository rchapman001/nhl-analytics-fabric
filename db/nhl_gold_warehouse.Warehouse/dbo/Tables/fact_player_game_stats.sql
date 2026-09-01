CREATE TABLE [dbo].[fact_player_game_stats] (

	[player_id] int NOT NULL, 
	[game_id] int NOT NULL, 
	[date_id] int NOT NULL, 
	[goals] int NULL, 
	[assists] int NULL, 
	[points] int NULL, 
	[game_winning_goals] int NULL, 
	[overtime_goals] int NULL, 
	[power_play_goals] int NULL, 
	[power_play_points] int NULL, 
	[shorthanded_goals] int NULL, 
	[shorthanded_points] int NULL, 
	[shots] int NULL, 
	[plus_minus] int NULL, 
	[shifts] int NULL, 
	[pim] int NULL, 
	[time_on_ice] varchar(20) NULL, 
	[created_at] datetime2(6) NULL, 
	[updated_at] datetime2(6) NULL
);