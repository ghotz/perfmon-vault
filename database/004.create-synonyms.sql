:setvar DatabaseName "PerfmonVault"
USE [$(DatabaseName)];
GO
DROP SYNONYM IF EXISTS  [dbo].[CounterData]
CREATE SYNONYM [dbo].[CounterData] FOR [staging].[CounterData];
GO
DROP SYNONYM IF EXISTS  [dbo].[CounterDetails]
CREATE SYNONYM [dbo].[CounterDetails] FOR [staging].[CounterDetails];
GO
DROP SYNONYM IF EXISTS  [dbo].[DisplayToID]
CREATE SYNONYM [dbo].[DisplayToID] FOR [staging].[DisplayToID];
GO
