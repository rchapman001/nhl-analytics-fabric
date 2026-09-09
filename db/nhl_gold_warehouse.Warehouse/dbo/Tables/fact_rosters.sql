CREATE TABLE [dbo].[fact_rosters] (

	[roster_id] bigint IDENTITY NOT NULL, 
	[team_id] bigint NULL, 
	[player_id] bigint NULL, 
	[date_id] int NOT NULL, 
	[effective_from] datetime2(6) NOT NULL, 
	[effective_to] datetime2(6) NULL, 
	[sweater_number] int NULL, 
	[created_at] datetime2(6) NULL, 
	[updated_at] datetime2(6) NULL
);