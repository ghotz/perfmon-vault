:setvar DatabaseName "PerfmonVault"
USE [$(DatabaseName)];
GO
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
GO
-------------------------------------------------------------------------------
-- Partition ranges, compression, and data distribution
-------------------------------------------------------------------------------
;WITH ActualRanges AS (
	SELECT
		OBJECT_ID('vault.CounterData_Tier1') AS [object_id]
	,	$PARTITION.pf_CounterData_Yearly([CounterDateTime]) AS [pn]
	,	MIN([CounterDateTime]) AS [actual_min_dt]
	,	MAX([CounterDateTime]) AS [actual_max_dt]
	FROM [vault].[CounterData_Tier1]
	GROUP BY $PARTITION.pf_CounterData_Yearly([CounterDateTime])

	UNION ALL

	SELECT
		OBJECT_ID('vault.CounterData_Tier2')
	,	$PARTITION.pf_CounterData_Yearly([CounterDateTime])
	,	MIN([CounterDateTime])
	,	MAX([CounterDateTime])
	FROM [vault].[CounterData_Tier2]
	GROUP BY $PARTITION.pf_CounterData_Yearly([CounterDateTime])

	UNION ALL

	SELECT
		OBJECT_ID('vault.CounterData_Tier3')
	,	$PARTITION.pf_CounterData_Yearly([CounterDateTime])
	,	MIN([CounterDateTime])
	,	MAX([CounterDateTime])
	FROM [vault].[CounterData_Tier3]
	GROUP BY $PARTITION.pf_CounterData_Yearly([CounterDateTime])
)
SELECT
	OBJECT_SCHEMA_NAME(p.object_id) + '.' + OBJECT_NAME(p.object_id) AS [table]
,	p.partition_number
,	CAST(prv_left.value AS date)			AS [range_from]
,	CAST(prv_right.value AS date)			AS [range_to]
,	p.data_compression_desc
,	p.rows
,	FORMAT(p.rows, 'N0')					AS [rows_fmt]
,	ar.actual_min_dt
,	ar.actual_max_dt
,	FORMAT(SUM(au.used_pages) * 8 / 1024, 'N0')	AS [used_mb]
,	FORMAT(SUM(au.total_pages) * 8 / 1024, 'N0')	AS [total_mb]

FROM	sys.partitions p
JOIN	sys.indexes i
	ON	i.object_id = p.object_id
	AND	i.index_id = p.index_id
JOIN	sys.allocation_units au
	ON	au.container_id = p.hobt_id
LEFT JOIN sys.partition_schemes ps
	ON	ps.data_space_id = i.data_space_id
LEFT JOIN sys.partition_functions pf
	ON	pf.function_id = ps.function_id
LEFT JOIN sys.partition_range_values prv_right
	ON	prv_right.function_id = pf.function_id
	AND	prv_right.boundary_id = p.partition_number
LEFT JOIN sys.partition_range_values prv_left
	ON	prv_left.function_id = pf.function_id
	AND	prv_left.boundary_id = p.partition_number - 1
LEFT JOIN ActualRanges ar
	ON	ar.object_id = p.object_id
	AND	ar.pn = p.partition_number

WHERE	p.object_id IN (
	OBJECT_ID('vault.Count3erData_Tier1'),
	OBJECT_ID('vault.CounterData_Tier2'),
	OBJECT_ID('vault.CounterData_Tier3')
)
AND		i.type = 5	-- CLUSTERED COLUMNSTORE
--AND		p.rows > 0	-- skip empty partitions

GROUP BY
	p.object_id, p.partition_number, p.rows, p.data_compression_desc
,	prv_left.value, prv_right.value
,	ar.actual_min_dt, ar.actual_max_dt

ORDER BY 1, p.partition_number;
GO

--ALTER INDEX CCI_CounterData_Tier1 ON vault.CounterData_Tier1 REORGANIZE PARTITION = 1
--ALTER INDEX CCI_CounterData_Tier2 ON vault.CounterData_Tier2 REORGANIZE PARTITION = 1
--ALTER INDEX CCI_CounterData_Tier3 ON vault.CounterData_Tier3 REORGANIZE PARTITION = 1

--ALTER INDEX CCI_CounterData_Tier1 ON vault.CounterData_Tier1 REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS=ON);
--ALTER INDEX CCI_CounterData_Tier2 ON vault.CounterData_Tier2 REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS=ON);
--ALTER INDEX CCI_CounterData_Tier3 ON vault.CounterData_Tier3 REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS=ON);

--ALTER INDEX CCI_CounterData_Tier1 ON vault.CounterData_Tier1
--REBUILD PARTITION = 1  WITH (MAXDOP = 1, ONLINE = ON, SORT_IN_TEMPDB = ON, DATA_COMPRESSION = COLUMNSTORE)
--ALTER INDEX CCI_CounterData_Tier2 ON vault.CounterData_Tier2
--REBUILD PARTITION = 1  WITH (MAXDOP = 1, ONLINE = ON, SORT_IN_TEMPDB = ON, DATA_COMPRESSION = COLUMNSTORE)
--ALTER INDEX CCI_CounterData_Tier3 ON vault.CounterData_Tier3
--REBUILD PARTITION = 1 WITH (MAXDOP = 1, ONLINE = ON, SORT_IN_TEMPDB = ON, DATA_COMPRESSION = COLUMNSTORE_ARCHIVE)

--ALTER INDEX CCI_CounterData_Tier1 ON vault.CounterData_Tier1
--REBUILD WITH (MAXDOP = 1) --, ONLINE = OFF, SORT_IN_TEMPDB = OFF, DATA_COMPRESSION = COLUMNSTORE)
--ALTER INDEX CCI_CounterData_Tier2 ON vault.CounterData_Tier2
--REBUILD WITH (MAXDOP = 1) --, ONLINE = ON, SORT_IN_TEMPDB = ON, DATA_COMPRESSION = COLUMNSTORE)
--ALTER INDEX CCI_CounterData_Tier3 ON vault.CounterData_Tier3
--REBUILD WITH (MAXDOP = 1) --, ONLINE = ON, SORT_IN_TEMPDB = ON, DATA_COMPRESSION = COLUMNSTORE_ARCHIVE)
