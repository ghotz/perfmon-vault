TRUNCATE TABLE [staging].[DisplayToID]
INSERT	[staging].[DisplayToID]
		([GUID], [RunID], [DisplayString], [LogStartTime], [LogStopTime], [NumberOfRecords], [MinutesToUTC], [TimeZoneName])
SELECT	[GUID], [RunID], [DisplayString], [LogStartTime], [LogStopTime], [NumberOfRecords], [MinutesToUTC], [TimeZoneName]
FROM	[Perfmon_nsa].[dbo].[DisplayToID]
--WHERE	[DisplayString] BETWEEN 'PRODSQL01_PAL_20190528_104914_0001' AND 'PRODSQL01_PAL_20190731_235700_0065'
WHERE	[DisplayString] BETWEEN 'PRODSQL01_PAL_20190801_235600_0066' AND 'PRODSQL01_PAL_20200229_233846_0277'
ORDER BY [DisplayString]
GO

-- duplicate keys
DROP TABLE IF EXISTS #tmp_keys
SELECT
	[CounterID]--, [MachineName], [ObjectName], [CounterName], [InstanceName], [InstanceIndex], [ParentName], [ParentObjectID]
,	MIN([CounterID]) OVER (PARTITION BY [MachineName], [ObjectName], [CounterName], [InstanceName], [InstanceIndex], [ParentName], [ParentObjectID]) AS min_counter
INTO	#tmp_keys
FROM	[Perfmon_nsa].[dbo].[CounterDetails]

ALTER TABLE #tmp_keys ADD PRIMARY KEY ([CounterID]);

TRUNCATE TABLE [staging].[CounterDetails]
SET IDENTITY_INSERT [staging].[CounterDetails] ON 
INSERT	[staging].[CounterDetails]
		([CounterID], [MachineName], [ObjectName], [CounterName], [CounterType], [DefaultScale], [InstanceName], [InstanceIndex], [ParentName], [ParentObjectID], [TimeBaseA], [TimeBaseB])
SELECT	C.[CounterID], [MachineName], [ObjectName], [CounterName], [CounterType], [DefaultScale], [InstanceName], [InstanceIndex], [ParentName], [ParentObjectID], [TimeBaseA], [TimeBaseB]
FROM	[Perfmon_nsa].[dbo].[CounterDetails] AS C
JOIN	#tmp_keys AS K
  ON	C.[CounterID] = K.[CounterID]
WHERE	K.[CounterID] = K.min_counter
SET IDENTITY_INSERT [staging].[CounterDetails] OFF 

TRUNCATE TABLE [staging].[CounterDetails]
INSERT	[staging].[CounterData]
		([GUID], [CounterID], [RecordIndex], [CounterDateTime], [CounterValue], [FirstValueA], [FirstValueB], [SecondValueA], [SecondValueB], [MultiCount])
SELECT	D.[GUID], k.min_counter AS [CounterID], [RecordIndex], [CounterDateTime], [CounterValue], [FirstValueA], [FirstValueB], [SecondValueA], [SecondValueB], [MultiCount]
FROM	[Perfmon_nsa].[dbo].[CounterData_Full] AS D
JOIN	[staging].[DisplayToID] AS I
  ON	D.[GUID] = I.[GUID]
JOIN	#tmp_keys AS K
  ON	D.[CounterID] = K.[CounterID]
WHERE	D.[CounterDateTime] BETWEEN '2019-08-01 23:56:00.493' AND '2020-02-29 23:38:47.088'

EXEC [vault].[usp_LoadFromStaging]

-- EXEC [vault].[usp_LoadFromStaging] 1

--TRUNCATE TABLE [staging].[CounterData]
--TRUNCATE TABLE [staging].[DisplayToID]
--TRUNCATE TABLE [staging].[CounterDetails]

--TRUNCATE TABLE [vault].[CounterDetails]
--TRUNCATE TABLE [vault].[DisplayToID]
--TRUNCATE TABLE [vault].[CounterData_Tier1]
--TRUNCATE TABLE [vault].[CounterData_Tier2]
--TRUNCATE TABLE [vault].[CounterData_Tier3]


--ALTER INDEX [CCI_CounterData_Tier1] ON [vault].[CounterData_Tier1] REORGANIZE  WITH ( LOB_COMPACTION = ON, COMPRESS_ALL_ROW_GROUPS = OFF );
--ALTER INDEX [CCI_CounterData_Tier2] ON [vault].[CounterData_Tier2] REORGANIZE  WITH ( LOB_COMPACTION = ON, COMPRESS_ALL_ROW_GROUPS = OFF );
--ALTER INDEX [CCI_CounterData_Tier3] ON [vault].[CounterData_Tier3] REORGANIZE  WITH ( LOB_COMPACTION = ON, COMPRESS_ALL_ROW_GROUPS = OFF );

select * from [Perfmon_nsa].dbo.CounterDetails where [ObjectName] like '%depre%'
