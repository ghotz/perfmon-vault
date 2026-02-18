:setvar DatabaseName "PerfmonVault"
USE [$(DatabaseName)];
GO

-------------------------------------------------------------------------------
-- Counters Data Tier 1: hot — standard columnstore, ~50-55 counters
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS	[vault].[CounterData_Tier1];
CREATE TABLE			[vault].[CounterData_Tier1]
(
	[CounterDateTime]	datetime NOT NULL
,	[CounterID]			int NOT NULL
,	[RecordIndex]		int NOT NULL
--,	[CounterValue]		float NOT NULL
,	[CounterValue]		decimal(28, 0) NOT NULL
,	[FirstValueA]		int NULL
,	[FirstValueB]		int NULL
,	[SecondValueA]		int NULL
,	[SecondValueB]		int NULL
,	[MultiCount]		int NULL
,	[GUID]				uniqueidentifier NOT NULL

--,	CONSTRAINT	pk_CounterData_Tier1
--	PRIMARY KEY	([CounterID], [RecordIndex])

--,	CONSTRAINT	ak_CounterData_Tier1
--	UNIQUE		([CounterID], [CounterDateTime])

,	INDEX CCI_CounterData_Tier1
	CLUSTERED COLUMNSTORE ORDER (CounterDateTime, CounterID)
);

-------------------------------------------------------------------------------
-- Counters Data Tier 2: warm — standard columnstore, remaining SQL/OS counters
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS	[vault].[CounterData_Tier2];
CREATE TABLE			[vault].[CounterData_Tier2]
(
	[CounterDateTime]	datetime NOT NULL
,	[CounterID]			int NOT NULL
,	[RecordIndex]		int NOT NULL
--,	[CounterValue]		float NOT NULL
,	[CounterValue]		decimal(28, 0) NOT NULL
,	[FirstValueA]		int NULL
,	[FirstValueB]		int NULL
,	[SecondValueA]		int NULL
,	[SecondValueB]		int NULL
,	[MultiCount]		int NULL
,	[GUID]				uniqueidentifier NOT NULL

--,	CONSTRAINT	pk_CounterData_Tier2
--	PRIMARY KEY	([CounterID], [RecordIndex])

--,	CONSTRAINT	ak_CounterData_Tier2
--	UNIQUE		([CounterID], [CounterDateTime])

,	INDEX CCI_CounterData_Tier2
	CLUSTERED COLUMNSTORE ORDER (CounterDateTime, CounterID)
);

-------------------------------------------------------------------------------
-- Counters Data Tier 3: cold — archive compression, everything else
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS	[vault].[CounterData_Tier3];
CREATE TABLE			[vault].[CounterData_Tier3]
(
	[CounterDateTime]	datetime NOT NULL
,	[CounterID]			int NOT NULL
,	[RecordIndex]		int NOT NULL
--,	[CounterValue]		float NOT NULL
,	[CounterValue]		decimal(28, 0) NOT NULL
,	[FirstValueA]		int NULL
,	[FirstValueB]		int NULL
,	[SecondValueA]		int NULL
,	[SecondValueB]		int NULL
,	[MultiCount]		int NULL
,	[GUID]				uniqueidentifier NOT NULL

--,	CONSTRAINT	pk_CounterData_Tier3
--	PRIMARY KEY	([CounterID], [RecordIndex])

--,	CONSTRAINT	ak_CounterData_Tier3
--	UNIQUE		([CounterID], [CounterDateTime])

,	INDEX CCI_CounterData_Tier3
	CLUSTERED COLUMNSTORE ORDER (CounterDateTime, CounterID)
);

-------------------------------------------------------------------------------
-- Counter details
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS	[vault].[CounterDetails];
CREATE TABLE			[vault].[CounterDetails]
(
	[CounterID]			int IDENTITY(1, 1) NOT NULL
,	[MachineName]		varchar(1024) NOT NULL
,	[ObjectName]		varchar(1024) NOT NULL
,	[CounterName]		varchar(1024) NOT NULL
,	[CounterType]		int NOT NULL
,	[DefaultScale]		int NOT NULL
,	[InstanceName]		varchar(1024) NULL
,	[InstanceIndex]		int NULL
,	[ParentName]		varchar(1024) NULL
,	[ParentObjectID]	int NULL
,	[TimeBaseA]			int NULL
,	[TimeBaseB]			int NULL

,	CONSTRAINT	pk_CounterDetails
	PRIMARY KEY	([CounterID])

,	CONSTRAINT	ak_CounterDetails
	UNIQUE		([MachineName], [ObjectName], [CounterName], [InstanceName], [InstanceIndex], [ParentName], [ParentObjectID])
);

DROP TABLE IF EXISTS	[vault].[DisplayToID];
CREATE TABLE			[vault].[DisplayToID]
(
	[GUID]				uniqueidentifier NOT NULL
,	[RunID]				int NULL
,	[DisplayString]		varchar(1024) NOT NULL
,	[LogStartTime]		datetime NOT NULL
,	[LogStopTime]		datetime NOT NULL
,	[NumberOfRecords]	int NULL
,	[MinutesToUTC]		int NULL
,	[TimeZoneName]		char(32) NULL

,	CONSTRAINT	pk_DisplayToID
	PRIMARY KEY	([GUID])

,	CONSTRAINT	ak_DisplayToID_DisplayString	-- passing the filename, we try to avoid loading the same file multiple times
	UNIQUE		([DisplayString])
);
GO

-------------------------------------------------------------------------------
-- CounterTier: pattern-based tier classification
-- Uses LIKE wildcards (%) so new volumes/databases/instances are auto-classified
-- Priority: lowest Tier wins (if a counter matches Tier 1 and Tier 2, it's Tier 1)
-- Unmatched counters default to Tier 3
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS [vault].[CounterTier];
CREATE TABLE [vault].[CounterTier]
(
	[TierID]		int IDENTITY(1,1) NOT NULL
,	[Tier]			tinyint NOT NULL
,	[MachineName]	varchar(1024) NOT NULL DEFAULT '%'
,	[ObjectName]	varchar(1024) NOT NULL
,	[CounterName]	varchar(1024) NOT NULL
,	[InstanceName]	varchar(1024) NULL		-- NULL = match both NULL and any value

,	CONSTRAINT	pk_CounterTier
	PRIMARY KEY	([TierID])

,	CONSTRAINT	ck_CounterTier_Tier
	CHECK		([Tier] IN (1, 2, 3))
);
GO
