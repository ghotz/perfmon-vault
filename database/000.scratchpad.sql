:setvar DatabaseName "PerfmonVault"
:setvar SourceDatabaseName ""
USE [$(DatabaseName)];
GO

-------------------------------------------------------------------------------
-- General checks
-------------------------------------------------------------------------------

--SELECT	MIN([CounterDateTime]), MAX([CounterDateTime])
--FROM		[$(SourceDatabaseName)].[dbo].[CounterData]

--SELECT	MIN([CounterDateTime]), MAX([CounterDateTime])
--FROM		[staging].[CounterData]

-------------------------------------------------------------------------------
-- Import from databases with the standard schema in batches of 30 days
-- in staging tables and execute the load into the new schema
-------------------------------------------------------------------------------

-- duplicate keys
DROP TABLE IF EXISTS #tmp_keys
SELECT
	[CounterID]--, [MachineName], [ObjectName], [CounterName], [InstanceName], [InstanceIndex], [ParentName], [ParentObjectID]
,	MIN([CounterID]) OVER (PARTITION BY [MachineName], [ObjectName], [CounterName], [InstanceName], [InstanceIndex], [ParentName], [ParentObjectID]) AS CounterID_min
INTO	#tmp_keys
FROM	[$(SourceDatabaseName)].[dbo].[CounterDetails];
ALTER TABLE #tmp_keys ADD PRIMARY KEY ([CounterID]);

WHILE EXISTS(
	SELECT	*
	FROM	[$(SourceDatabaseName)].[dbo].[DisplayToID]
	WHERE	[GUID] NOT IN (SELECT [GUID] FROM [vault].[DisplayToID])
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
	FROM	[$(SourceDatabaseName)].[dbo].[CounterData] AS D
	JOIN	[staging].[DisplayToID] AS I
	  ON	D.[GUID] = I.[GUID]
	JOIN	#tmp_keys AS K
	  ON	D.[CounterID] = K.[CounterID]

	EXEC [vault].[usp_LoadFromStaging] 1
END
GO

-------------------------------------------------------------------------------
-- Common truncates (warning!)
-------------------------------------------------------------------------------

--TRUNCATE TABLE [staging].[CounterData]
--TRUNCATE TABLE [staging].[DisplayToID]
--TRUNCATE TABLE [staging].[CounterDetails]

--TRUNCATE TABLE [vault].[CounterDetails]
--TRUNCATE TABLE [vault].[DisplayToID]
--TRUNCATE TABLE [vault].[CounterData_Tier1]
--TRUNCATE TABLE [vault].[CounterData_Tier2]
--TRUNCATE TABLE [vault].[CounterData_Tier3]


-------------------------------------------------------------------------------
-- Forgot to split partition function early enough?
-- Switch out partition, split fuction, switch back in partition
-------------------------------------------------------------------------------

--CREATE TABLE			[vault].[CounterData_Tier1_2024]
--(
--	[CounterDateTime]	datetime NOT NULL
--,	[CounterID]			int NOT NULL
--,	[RecordIndex]		int NOT NULL
----,	[CounterValue]		float NOT NULL
--,	[CounterValue]		decimal(28, 0) NOT NULL
--,	[FirstValueA]		int NULL
--,	[FirstValueB]		int NULL
--,	[SecondValueA]		int NULL
--,	[SecondValueB]		int NULL
--,	[MultiCount]		int NULL
--,	[GUID]				uniqueidentifier NOT NULL

--,	INDEX CCI_CounterData_Tier1_2024
--	CLUSTERED COLUMNSTORE ORDER (CounterDateTime, CounterID)
--)
--ON [vault]
--;

--CREATE TABLE			[vault].[CounterData_Tier2_2024]
--(
--	[CounterDateTime]	datetime NOT NULL
--,	[CounterID]			int NOT NULL
--,	[RecordIndex]		int NOT NULL
----,	[CounterValue]		float NOT NULL
--,	[CounterValue]		decimal(28, 0) NOT NULL
--,	[FirstValueA]		int NULL
--,	[FirstValueB]		int NULL
--,	[SecondValueA]		int NULL
--,	[SecondValueB]		int NULL
--,	[MultiCount]		int NULL
--,	[GUID]				uniqueidentifier NOT NULL

--,	INDEX CCI_CounterData_Tier2_2024
--	CLUSTERED COLUMNSTORE ORDER (CounterDateTime, CounterID)
--)
--ON [vault]
--;

--DROP TABLE IF EXISTS [vault].[CounterData_Tier3_2024];
--CREATE TABLE			[vault].[CounterData_Tier3_2024]
--(
--	[CounterDateTime]	datetime NOT NULL
--,	[CounterID]			int NOT NULL
--,	[RecordIndex]		int NOT NULL
----,	[CounterValue]		float NOT NULL
--,	[CounterValue]		decimal(28, 0) NOT NULL
--,	[FirstValueA]		int NULL
--,	[FirstValueB]		int NULL
--,	[SecondValueA]		int NULL
--,	[SecondValueB]		int NULL
--,	[MultiCount]		int NULL
--,	[GUID]				uniqueidentifier NOT NULL

--,	INDEX CCI_CounterData_Tier3_2024
--	CLUSTERED COLUMNSTORE ORDER (CounterDateTime, CounterID)
--	 WITH (DATA_COMPRESSION = COLUMNSTORE_ARCHIVE)
--)
--ON [vault]
--;

--ALTER TABLE [vault].[CounterData_Tier1] 
--SWITCH PARTITION 5 TO [vault].[CounterData_Tier1_2024];

--ALTER TABLE [vault].[CounterData_Tier2] 
--SWITCH PARTITION 5 TO [vault].[CounterData_Tier2_2024];

--ALTER TABLE [vault].[CounterData_Tier3] 
--SWITCH PARTITION 5 TO [vault].[CounterData_Tier3_2024];

--EXEC [vault].[usp_AddYearPartition] 2025
--EXEC [vault].[usp_AddYearPartition] 2026

--select min([CounterDateTime]), max([CounterDateTime]) from [vault].[CounterData_Tier1_2024]
--select min([CounterDateTime]), max([CounterDateTime]) from [vault].[CounterData_Tier2_2024]
--select min([CounterDateTime]), max([CounterDateTime]) from [vault].[CounterData_Tier3_2024]

--ALTER TABLE [vault].[CounterData_Tier1_2024]
--ADD CHECK ([CounterDateTime] >= '2024-01-01 00:00:00' AND [CounterDateTime] < '2025-01-01 00:00:00')

--ALTER TABLE [vault].[CounterData_Tier2_2024]
--ADD CHECK ([CounterDateTime] >= '2024-01-01 00:00:00' AND [CounterDateTime] < '2025-01-01 00:00:00')

--ALTER TABLE [vault].[CounterData_Tier3_2024]
--ADD CHECK ([CounterDateTime] >= '2024-01-01 00:00:00' AND [CounterDateTime] < '2025-01-01 00:00:00')

--ALTER TABLE [vault].[CounterData_Tier1_2024]
--SWITCH TO [vault].[CounterData_Tier1] PARTITION 5;

--ALTER TABLE [vault].[CounterData_Tier2_2024]
--SWITCH TO [vault].[CounterData_Tier2] PARTITION 5;

--ALTER TABLE [vault].[CounterData_Tier3_2024]
--SWITCH TO [vault].[CounterData_Tier3] PARTITION 5;

--DROP TABLE IF EXISTS [vault].[CounterData_Tier1_2024];
--DROP TABLE IF EXISTS [vault].[CounterData_Tier2_2024];
--DROP TABLE IF EXISTS [vault].[CounterData_Tier3_2024];

-------------------------------------------------------------------------------
-- Other general queries/commands
-------------------------------------------------------------------------------
--BACKUP DATABASE [PerfmonVault] TO DISK = 'F:\Backups\SQL Server\PerfmonVault_telesan.bak' WITH INIT, CHECKSUM, COMPRESSION (ALGORITHM = ZSTD )
--exec sp_whoisactive --@get_plans = 1


--SELECT	COUNT(*)
--FROM	[vault].[CounterData]
--WHERE	CounterId IN (SELECT CounterId FROM [vault].[CounterDetails] WHERE [ObjectName] LIKE '%depre%')

--SELECT	*
--FROM	[staging].[DisplayToID]
--WHERE	GUID IN (SELECT GUID FROM [vault].[DisplayToID])

SELECT LEFT(DisplayString, 11), MAX(DisplayString) FROM [vault].[DisplayToID] GROUP BY LEFT(DisplayString, 11) order by 1, 2

--DELETE	[vault].[CounterData_Tier1]
--WHERE	GUID IN (SELECT GUID FROM [staging].[DisplayToID])

--DELETE	[vault].[CounterData_Tier2]
--WHERE	GUID IN (SELECT GUID FROM [staging].[DisplayToID])

--DELETE	[vault].[CounterData_Tier3]
--WHERE	GUID IN (SELECT GUID FROM [staging].[DisplayToID])