INSERT	[staging].[DisplayToID]
		([GUID], [RunID], [DisplayString], [LogStartTime], [LogStopTime], [NumberOfRecords], [MinutesToUTC], [TimeZoneName])
SELECT	[GUID], [RunID], [DisplayString], [LogStartTime], [LogStopTime], [NumberOfRecords], [MinutesToUTC], [TimeZoneName]
FROM	[Perfmon_nsa].[dbo].[DisplayToID];
GO

-- duplicate keys
DROP TABLE IF EXISTS #tmp_keys
SELECT
	[CounterID], [MachineName], [ObjectName], [CounterName], [InstanceName], [InstanceIndex], [ParentName], [ParentObjectID]
,	MIN([CounterID]) OVER (PARTITION BY [MachineName], [ObjectName], [CounterName], [InstanceName], [InstanceIndex], [ParentName], [ParentObjectID]) AS min_counter
INTO	#tmp_keys
FROM	[Perfmon_nsa].[dbo].[CounterDetails]

INSERT	[staging].[CounterDetails]
SELECT	[CounterID], [MachineName], [ObjectName], [CounterName], [CounterType], [DefaultScale], [InstanceName], [InstanceIndex], [ParentName], [ParentObjectID], [TimeBaseA], [TimeBaseB]
SELECT	[Perfmon_nsa].[dbo].[CounterDetails]

-- EXEC [vault].[usp_LoadFromStaging] @LoadOnlyCounters = 1
-- EXEC [vault].[usp_LoadFromStaging]

--TRUNCATE TABLE [staging].[CounterData]
--TRUNCATE TABLE [staging].[CounterDetails]
--TRUNCATE TABLE [staging].[DisplayToID]

--TRUNCATE TABLE [vault].[CounterDetails]
--TRUNCATE TABLE [vault].[DisplayToID]
--TRUNCATE TABLE [vault].[CounterData_Tier1]
--TRUNCATE TABLE [vault].[CounterData_Tier2]
--TRUNCATE TABLE [vault].[CounterData_Tier3]
