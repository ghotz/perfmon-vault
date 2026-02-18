:setvar DatabaseName "PerfmonVault"
USE [$(DatabaseName)];
GO

DROP TABLE IF EXISTS	[vault].[CounterData];
CREATE TABLE			[vault].[CounterData]
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

--,	CONSTRAINT	pk_CounterData
--	PRIMARY KEY	([CounterID], [RecordIndex])

--,	CONSTRAINT	ak_CounterData
--	UNIQUE		([CounterID], [CounterDateTime])

,	INDEX CCI_CounterData
	CLUSTERED COLUMNSTORE ORDER (CounterDateTime, CounterID)
);

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
