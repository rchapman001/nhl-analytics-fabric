CREATE TABLE [dbo].[team_rosters] (

	[team_id] int NOT NULL, 
	[player_id] int NOT NULL, 
	[sweater_number] int NULL, 
	[position] varchar(50) NULL, 
	[created_at] datetime2(6) NULL, 
	[updated_at] datetime2(6) NULL
);