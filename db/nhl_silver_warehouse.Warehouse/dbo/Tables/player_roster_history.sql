CREATE TABLE [dbo].[player_roster_history] (

	[player_id] int NOT NULL, 
	[team_id] int NOT NULL, 
	[start_date] date NOT NULL, 
	[end_date] date NULL, 
	[sweater_number] int NULL, 
	[position] varchar(50) NULL, 
	[is_current] bit NULL, 
	[created_at] datetime2(6) NULL, 
	[updated_at] datetime2(6) NULL
);