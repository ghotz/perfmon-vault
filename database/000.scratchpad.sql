:setvar DatabaseName "PerfmonVault"
:setvar SourceDatabaseName ""
USE [$(DatabaseName)];
GO

-- duplicate keys
DROP TABLE IF EXISTS #tmp_keys
SELECT
	[CounterID]--, [MachineName], [ObjectName], [CounterName], [InstanceName], [InstanceIndex], [ParentName], [ParentObjectID]
,	MIN([CounterID]) OVER (PARTITION BY [MachineName], [ObjectName], [CounterName], [InstanceName], [InstanceIndex], [ParentName], [ParentObjectID]) AS CounterID_min
INTO	#tmp_keys
FROM	[$(SourceDatabaseName)].[dbo].[CounterDetails];
ALTER TABLE #tmp_keys ADD PRIMARY KEY ([CounterID]);

WHILE NOT EXISTS(
	SELECT	*
	FROM	[vault].[DisplayToID]
	WHERE	[GUID] NOT IN (SELECT [GUID] FROM [$(SourceDatabaseName)].[dbo].[DisplayToID])
)
BEGIN
	TRUNCATE TABLE [staging].[DisplayToID];
	WITH cte AS
	(
		SELECT	[GUID], [RunID], [DisplayString], [LogStartTime], [LogStopTime], [NumberOfRecords], [MinutesToUTC], [TimeZoneName]
		,		ROW_NUMBER() OVER (ORDER BY [DisplayString]) AS rn
		FROM	[$(SourceDatabaseName)].[dbo].[DisplayToID]
		WHERE	[GUID] NOT IN (SELECT [GUID] FROM [vault].[DisplayToID])
	)
	INSERT	[staging].[DisplayToID]
	SELECT	[GUID], [RunID], [DisplayString], [LogStartTime], [LogStopTime], [NumberOfRecords], [MinutesToUTC], [TimeZoneName]
	FROM	cte
	WHERE	rn <= 30

	TRUNCATE TABLE [staging].[CounterDetails]
	SET IDENTITY_INSERT [staging].[CounterDetails] ON 
	INSERT	[staging].[CounterDetails]
			([CounterID], [MachineName], [ObjectName], [CounterName], [CounterType], [DefaultScale], [InstanceName], [InstanceIndex], [ParentName], [ParentObjectID], [TimeBaseA], [TimeBaseB])
	SELECT	C.[CounterID], [MachineName], [ObjectName], [CounterName], [CounterType], [DefaultScale], [InstanceName], [InstanceIndex], [ParentName], [ParentObjectID], [TimeBaseA], [TimeBaseB]
	FROM	[$(SourceDatabaseName)].[dbo].[CounterDetails] AS C
	JOIN	#tmp_keys AS K
	  ON	C.[CounterID] = K.[CounterID]
	WHERE	K.[CounterID] = K.CounterID_min
	SET IDENTITY_INSERT [staging].[CounterDetails] OFF 

	TRUNCATE TABLE [staging].[CounterData]
	INSERT	[staging].[CounterData]
			([GUID], [CounterID], [RecordIndex], [CounterDateTime], [CounterValue], [FirstValueA], [FirstValueB], [SecondValueA], [SecondValueB], [MultiCount])
	SELECT	D.[GUID], k.CounterID_min AS [CounterID], [RecordIndex], [CounterDateTime], [CounterValue], [FirstValueA], [FirstValueB], [SecondValueA], [SecondValueB], [MultiCount]
	FROM	[$(SourceDatabaseName)].[dbo].[CounterData_Full] AS D
	JOIN	[staging].[DisplayToID] AS I
	  ON	D.[GUID] = I.[GUID]
	JOIN	#tmp_keys AS K
	  ON	D.[CounterID] = K.[CounterID]

	EXEC [vault].[usp_LoadFromStaging] 1
END
GO


-- EXEC [vault].[usp_LoadFromStaging] 1

--TRUNCATE TABLE [staging].[CounterData]
--TRUNCATE TABLE [staging].[DisplayToID]
--TRUNCATE TABLE [staging].[CounterDetails]

--TRUNCATE TABLE [vault].[CounterDetails]
--TRUNCATE TABLE [vault].[DisplayToID]
--TRUNCATE TABLE [vault].[CounterData_Tier1]
--TRUNCATE TABLE [vault].[CounterData_Tier2]
--TRUNCATE TABLE [vault].[CounterData_Tier3]
