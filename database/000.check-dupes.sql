:setvar DatabaseName "PerfmonVault"
USE [$(DatabaseName)];
GO

-------------------------------------------------------------------------------
-- Duplicate check across vault tier tables
-------------------------------------------------------------------------------

-- 1. Duplicates WITHIN each tier (same row appears twice)
SELECT 'Tier1' AS [tier], [CounterDateTime], [CounterID], COUNT(*) AS [cnt]
FROM [vault].[CounterData_Tier1]
GROUP BY [CounterDateTime], [CounterID]
HAVING COUNT(*) > 1
UNION ALL
SELECT 'Tier2', [CounterDateTime], [CounterID], COUNT(*)
FROM [vault].[CounterData_Tier2]
GROUP BY [CounterDateTime], [CounterID]
HAVING COUNT(*) > 1
UNION ALL
SELECT 'Tier3', [CounterDateTime], [CounterID], COUNT(*)
FROM [vault].[CounterData_Tier3]
GROUP BY [CounterDateTime], [CounterID]
HAVING COUNT(*) > 1
ORDER BY [tier], [CounterDateTime], [CounterID];
GO

-- 2. Duplicates ACROSS tiers (same counter in multiple tier tables)
SELECT
	t1.[CounterDateTime], t1.[CounterID], 'Tier1+Tier2' AS [overlap]
FROM [vault].[CounterData_Tier1] t1
WHERE EXISTS (
	SELECT 1 FROM [vault].[CounterData_Tier2] t2
	WHERE t2.[CounterDateTime] = t1.[CounterDateTime]
	AND   t2.[CounterID] = t1.[CounterID]
)
UNION ALL
SELECT
	t1.[CounterDateTime], t1.[CounterID], 'Tier1+Tier3'
FROM [vault].[CounterData_Tier1] t1
WHERE EXISTS (
	SELECT 1 FROM [vault].[CounterData_Tier3] t3
	WHERE t3.[CounterDateTime] = t1.[CounterDateTime]
	AND   t3.[CounterID] = t1.[CounterID]
)
UNION ALL
SELECT
	t2.[CounterDateTime], t2.[CounterID], 'Tier2+Tier3'
FROM [vault].[CounterData_Tier2] t2
WHERE EXISTS (
	SELECT 1 FROM [vault].[CounterData_Tier3] t3
	WHERE t3.[CounterDateTime] = t2.[CounterDateTime]
	AND   t3.[CounterID] = t2.[CounterID]
)
ORDER BY [overlap], [CounterDateTime], [CounterID];
GO

-- 3. Summary: same CounterID living in multiple tiers
--    (can happen after tier rule changes without rebalance)
;WITH AllCounters AS (
	SELECT DISTINCT [CounterID], 1 AS [Tier] FROM [vault].[CounterData_Tier1]
	UNION ALL
	SELECT DISTINCT [CounterID], 2 FROM [vault].[CounterData_Tier2]
	UNION ALL
	SELECT DISTINCT [CounterID], 3 FROM [vault].[CounterData_Tier3]
)
SELECT
	ac.[CounterID]
,	cd.[ObjectName]
,	cd.[CounterName]
,	cd.[InstanceName]
,	STRING_AGG(ac.[Tier], ', ') WITHIN GROUP (ORDER BY ac.[Tier]) AS [present_in_tiers]
,	COUNT(*) AS [num_tiers]
FROM AllCounters ac
JOIN [vault].[CounterDetails] cd ON cd.[CounterID] = ac.[CounterID]
GROUP BY ac.[CounterID], cd.[ObjectName], cd.[CounterName], cd.[InstanceName]
HAVING COUNT(*) > 1
ORDER BY cd.[ObjectName], cd.[CounterName];
GO