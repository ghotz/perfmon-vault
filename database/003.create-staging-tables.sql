:setvar DatabaseName "PerfmonVault"
USE [$(DatabaseName)];
GO

DROP TABLE IF EXISTS	[staging].[CounterData];
CREATE TABLE	[staging].[CounterData] (
	[GUID]				uniqueidentifier NOT NULL
,	[CounterID]			int NOT NULL
,	[RecordIndex]		int NOT NULL
,	[CounterDateTime]	char(24) NOT NULL
,	[CounterValue]		float NOT NULL
,	[FirstValueA]		int NULL
,	[FirstValueB]		int NULL
,	[SecondValueA]		int NULL
,	[SecondValueB]		int NULL
,	[MultiCount]		int NULL

,	CONSTRAINT	pk_CounterData
	PRIMARY KEY	([GUID], [CounterID], [RecordIndex])
	WITH (DATA_COMPRESSION = PAGE)
);

DROP TABLE IF EXISTS	[staging].[CounterDetails];
CREATE TABLE	[staging].[CounterDetails](
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
	WITH (DATA_COMPRESSION = PAGE)
);

DROP TABLE IF EXISTS	[staging].[DisplayToID];
CREATE TABLE	[staging].[DisplayToID](
	[GUID]				uniqueidentifier NOT NULL
,	[RunID]				int NULL
,	[DisplayString]		varchar(1024) NOT NULL
,	[LogStartTime]		char(24) NULL
,	[LogStopTime]		char(24) NULL
,	[NumberOfRecords]	int NULL
,	[MinutesToUTC]		int NULL
,	[TimeZoneName]		char(32) NULL

,	CONSTRAINT	pk_DisplayToID
	PRIMARY KEY	([GUID])
	WITH (DATA_COMPRESSION = PAGE)
,	CONSTRAINT	ak_DisplayToID	-- passing the filename, we try to avoid loading the same file multiple times
	UNIQUE ([DisplayString])
	WITH (DATA_COMPRESSION = PAGE)
)
GO
