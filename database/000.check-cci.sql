:setvar DatabaseName "PerfmonVault"
USE [$(DatabaseName)];
GO
--exec sp_whoisactive
-------------------------------------------------------------------------------
-- Columnstore Rowgroup Health Dashboard
-------------------------------------------------------------------------------

-- 1. Summary per table with percentiles and fill quality
;WITH RG AS (
	SELECT
		OBJECT_SCHEMA_NAME(object_id) + '.' + OBJECT_NAME(object_id) AS [table]
	,	partition_number
	,	row_group_id
	,	state_desc
	,	total_rows
	,	deleted_rows
	,	total_rows - deleted_rows AS live_rows
	,	size_in_bytes
	,	trim_reason_desc
	,	CAST(total_rows * 100.0 / 1048576 AS decimal(5,1)) AS fill_pct
	,	PERCENT_RANK() OVER (
			PARTITION BY object_id
			ORDER BY total_rows
		) AS pct_rank
	FROM	sys.dm_db_column_store_row_group_physical_stats
	WHERE	object_id IN (
		OBJECT_ID('vault.CounterData_Tier1'),
		OBJECT_ID('vault.CounterData_Tier2'),
		OBJECT_ID('vault.CounterData_Tier3')
	)
	AND		state = 3	-- COMPRESSED only
)
SELECT
	[table]

	-- Row counts
,	COUNT(*)														AS num_rowgroups
,	SUM(live_rows)													AS total_live_rows
,	FORMAT(SUM(live_rows), 'N0')									AS total_live_rows_fmt

	-- Fill quality
,	MIN(total_rows)													AS rg_min
,	CAST(AVG(total_rows * 1.0) AS int)								AS rg_avg
,	MAX(total_rows)													AS rg_max
,	CAST(STDEV(total_rows) AS int)									AS rg_stdev

	-- Percentiles
,	MAX(CASE WHEN pct_rank <= 0.05 THEN total_rows END)				AS p5
,	MAX(CASE WHEN pct_rank <= 0.25 THEN total_rows END)				AS p25
,	MAX(CASE WHEN pct_rank <= 0.50 THEN total_rows END)				AS p50_median
,	MAX(CASE WHEN pct_rank <= 0.75 THEN total_rows END)				AS p75
,	MAX(CASE WHEN pct_rank <= 0.95 THEN total_rows END)				AS p95

	-- Fill buckets
,	SUM(CASE WHEN total_rows = 1048576 THEN 1 ELSE 0 END)			AS rg_full
,	SUM(CASE WHEN total_rows BETWEEN 900000 AND 1048575 THEN 1 ELSE 0 END)	AS rg_90pct
,	SUM(CASE WHEN total_rows BETWEEN 500000 AND  899999 THEN 1 ELSE 0 END)	AS rg_50_90
,	SUM(CASE WHEN total_rows BETWEEN 100000 AND  499999 THEN 1 ELSE 0 END)	AS rg_10_50
,	SUM(CASE WHEN total_rows < 100000 THEN 1 ELSE 0 END)			AS rg_under10

	-- Overall fill efficiency (vs perfect 1M per RG)
,	CAST(AVG(fill_pct) AS decimal(5,1))								AS avg_fill_pct
,	CAST(SUM(live_rows) * 100.0 / (COUNT(*) * 1048576) AS decimal(5,1))	AS overall_efficiency_pct

	-- Space
,	FORMAT(SUM(size_in_bytes) / 1048576, 'N0')						AS total_size_mb
,	CAST(AVG(size_in_bytes * 1.0) / 1048576 AS decimal(8,2))		AS avg_rg_size_mb

	-- Deleted rows (soft deletes from rebalance/updates)
,	SUM(deleted_rows)												AS total_deleted
,	CAST(SUM(deleted_rows) * 100.0 / NULLIF(SUM(total_rows), 0) AS decimal(5,2))	AS deleted_pct

FROM	RG
GROUP BY [table]
ORDER BY [table];
GO

-- 2. Trim reason breakdown (why are rowgroups not full?)
SELECT
	OBJECT_SCHEMA_NAME(object_id) + '.' + OBJECT_NAME(object_id) AS [table]
,	ISNULL(trim_reason_desc, 'NO_TRIM')								AS trim_reason
,	COUNT(*)														AS num_rowgroups
,	CAST(AVG(total_rows * 1.0) AS int)								AS avg_rows
,	MIN(total_rows)													AS min_rows
,	MAX(total_rows)													AS max_rows
FROM	sys.dm_db_column_store_row_group_physical_stats
WHERE	object_id IN (
	OBJECT_ID('vault.CounterData_Tier1'),
	OBJECT_ID('vault.CounterData_Tier2'),
	OBJECT_ID('vault.CounterData_Tier3')
)
AND		state = 3
GROUP BY object_id, trim_reason_desc
ORDER BY 1, num_rowgroups DESC;
GO

-- 3. Partition-level view (useful after partitioning by year)
SELECT
	OBJECT_SCHEMA_NAME(object_id) + '.' + OBJECT_NAME(object_id) AS [table]
,	partition_number
,	COUNT(*)														AS num_rowgroups
,	SUM(total_rows)													AS total_rows
,	SUM(CASE WHEN total_rows = 1048576 THEN 1 ELSE 0 END)			AS rg_full
,	CAST(AVG(total_rows * 1.0) AS int)								AS avg_rows
,	MIN(total_rows)													AS min_rows
,	SUM(deleted_rows)												AS deleted_rows
,	FORMAT(SUM(size_in_bytes) / 1048576, 'N0')						AS size_mb
FROM	sys.dm_db_column_store_row_group_physical_stats
WHERE	object_id IN (
	OBJECT_ID('vault.CounterData_Tier1'),
	OBJECT_ID('vault.CounterData_Tier2'),
	OBJECT_ID('vault.CounterData_Tier3')
)
AND		state = 3
GROUP BY object_id, partition_number
ORDER BY 1, partition_number;
GO