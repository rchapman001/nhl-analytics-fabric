CREATE TABLE [dbo].[teams] (

	[team_id] int NOT NULL, 
	[place_name] varchar(100) NULL, 
	[team_name] varchar(100) NULL, 
	[team_common_name] varchar(100) NULL, 
	[team_abbrev] varchar(10) NULL, 
	[conference_abbrev] varchar(10) NULL, 
	[conference_name] varchar(100) NULL, 
	[division_abbrev] varchar(10) NULL, 
	[division_name] varchar(100) NULL, 
	[team_logo] varchar(1000) NULL, 
	[created_at] datetime2(6) NULL, 
	[updated_at] datetime2(6) NULL
);