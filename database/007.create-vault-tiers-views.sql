:setvar DatabaseName "PerfmonVault"
USE [$(DatabaseName)];
GO

-------------------------------------------------------------------------------
-- Unified view
-------------------------------------------------------------------------------
CREATE OR ALTER VIEW [vault].[CounterData]
AS
	SELECT 1 AS [Tier], * FROM [vault].[CounterData_Tier1]
	UNION ALL
	SELECT 2 AS [Tier], * FROM [vault].[CounterData_Tier2]
	UNION ALL
	SELECT 3 AS [Tier], * FROM [vault].[CounterData_Tier3]
;
GO

-------------------------------------------------------------------------------
-- Helper view: resolve each vault.CounterDetails row to its tier
-- Lowest tier wins; unmatched = 3
-------------------------------------------------------------------------------
CREATE OR ALTER VIEW [vault].[vCounterTierResolved]
AS
WITH Matched AS (
	SELECT
		cd.[CounterID]
	,	MIN(ct.[Tier]) AS [Tier]
	FROM	[vault].[CounterDetails] cd
	JOIN	[vault].[CounterTier] ct
		ON	cd.[MachineName]	LIKE ct.[MachineName]
		AND	cd.[ObjectName]		LIKE ct.[ObjectName]
		AND	cd.[CounterName]	LIKE ct.[CounterName]
		AND	(
				ct.[InstanceName] IS NULL			-- NULL in tier = match anything (including NULL)
			OR	ISNULL(cd.[InstanceName], '') LIKE ct.[InstanceName]
			)
	GROUP BY cd.[CounterID]
)
SELECT
	cd.[CounterID]
,	ISNULL(m.[Tier], 3) AS [Tier]		-- unmatched = Tier 3
FROM	[vault].[CounterDetails] cd
LEFT JOIN Matched m ON m.[CounterID] = cd.[CounterID];
GO
