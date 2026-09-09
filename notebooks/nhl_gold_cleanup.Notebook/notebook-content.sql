-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse_name": "",
-- META       "default_lakehouse_workspace_id": "",
-- META       "known_lakehouses": []
-- META     },
-- META     "warehouse": {
-- META       "default_warehouse": "67fbcfda-3717-adcf-48d4-ba74a63a0bd5",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "67fbcfda-3717-adcf-48d4-ba74a63a0bd5",
-- META           "type": "Datawarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

DROP TABLE [nhl_gold_warehouse].[dbo].[dim_team]
GO

DROP TABLE [nhl_gold_warehouse].[dbo].[dim_player]
GO

DROP TABLE [nhl_gold_warehouse].[dbo].[dim_date]
GO

DROP TABLE [nhl_gold_warehouse].[dbo].[dim_game]
GO

DROP TABLE [nhl_gold_warehouse].[dbo].[fact_rosters]
GO

DROP TABLE [nhl_gold_warehouse].[dbo].[fact_team_standings]
GO

DROP TABLE [nhl_gold_warehouse].[dbo].[fact_player_game_stats]
GO

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
