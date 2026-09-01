CREATE TABLE [dbo].[fact_rosters] (

	[team_id] int NOT NULL, 
	[player_id] int NOT NULL, 
	[date_id] int NOT NULL, 
	[sweater_number] int NULL, 
	[created_at] datetime2(6) NULL, 
	[updated_at] datetime2(6) NULL
);