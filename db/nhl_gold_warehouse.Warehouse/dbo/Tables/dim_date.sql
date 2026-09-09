CREATE TABLE [dbo].[dim_date] (

	[date_id] int NOT NULL, 
	[full_date] date NOT NULL, 
	[day_of_month] int NULL, 
	[day_name] varchar(20) NULL, 
	[day_of_week] int NULL, 
	[week_of_year] int NULL, 
	[month_number] int NULL, 
	[month_name] varchar(20) NULL, 
	[quarter] int NULL, 
	[year] int NULL, 
	[is_weekend] bit NULL, 
	[created_at] datetime2(6) NULL, 
	[updated_at] datetime2(6) NULL
);