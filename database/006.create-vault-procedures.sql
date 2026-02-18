:setvar DatabaseName "PerfmonVault"
USE [$(DatabaseName)];
GO

-------------------------------------------------------------------------------
-- usp_LoadFromStaging
--
-- Loads data from [staging] into [vault]:
--   1. Identifies new BLGs (by GUID) not yet in vault
--   2. Merges new counter definitions into vault.CounterDetails (deduplicated)
--   3. Inserts counter data with remapped CounterIDs
--   4. Marks BLGs as loaded by inserting into vault.DisplayToID
--   5. Truncates staging tables
--
-- char(24) → datetime conversion handles the null terminator (0x00)
-- injected by relog via LEFT(field, 23).
--
-- CounterData insert uses TABLOCKX for bulk load path into columnstore,
-- ORDER BY matching the CCI order, and MAXDOP 1 for optimal dictionary quality.
-------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [vault].[usp_LoadFromStaging]
	@TruncateStaging bit = 0	-- set to 1 to auto-truncate staging after successful load
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	DECLARE @msg varchar(500);
	DECLARE @rows int;
	DECLARE @start datetime = GETDATE();

	---------------------------------------------------------------------------
	-- 0. Identify new BLGs not yet loaded into vault
	---------------------------------------------------------------------------
	DECLARE @NewGUIDs TABLE ([GUID] uniqueidentifier PRIMARY KEY);

	INSERT INTO @NewGUIDs ([GUID])
	SELECT	s.[GUID]
	FROM	[staging].[DisplayToID] s
	WHERE	NOT EXISTS (
		SELECT	1
		FROM	[vault].[DisplayToID] v
		WHERE	v.[GUID] = s.[GUID]
	);

	SET @rows = @@ROWCOUNT;
	SET @msg = CONCAT('New BLGs to load: ', @rows);
	RAISERROR('%s', 0, 1, @msg) WITH NOWAIT;

	IF @rows = 0
	BEGIN
		RAISERROR('No new data to load. Exiting.', 0, 1) WITH NOWAIT;
		RETURN 0;
	END

	---------------------------------------------------------------------------
	-- 1. Merge counter definitions (deduplicated by business key)
	--    staging may have multiple CounterIDs for the same logical counter
	--    (one per BLG); vault.CounterDetails keeps a single canonical row.
	--    NULL-safe comparison via ISNULL with sentinel values.
	---------------------------------------------------------------------------
	RAISERROR('Merging counter definitions...', 0, 1) WITH NOWAIT;

	;WITH StagingCounters AS (
		SELECT DISTINCT
			sc.[MachineName]
		,	sc.[ObjectName]
		,	sc.[CounterName]
		,	sc.[CounterType]
		,	sc.[DefaultScale]
		,	sc.[InstanceName]
		,	sc.[InstanceIndex]
		,	sc.[ParentName]
		,	sc.[ParentObjectID]
		,	sc.[TimeBaseA]
		,	sc.[TimeBaseB]
		FROM	[staging].[CounterDetails] sc
		WHERE	EXISTS (
			SELECT	1
			FROM	[staging].[CounterData] sd
			JOIN	@NewGUIDs ng ON ng.[GUID] = sd.[GUID]
			WHERE	sd.[CounterID] = sc.[CounterID]
		)
	)
	MERGE [vault].[CounterDetails] AS tgt
	USING StagingCounters AS src
	ON	tgt.[MachineName]	= src.[MachineName]
	AND	tgt.[ObjectName]	= src.[ObjectName]
	AND	tgt.[CounterName]	= src.[CounterName]
	AND	ISNULL(tgt.[InstanceName],		'')	= ISNULL(src.[InstanceName],	'')
	AND	ISNULL(tgt.[InstanceIndex],		-1)	= ISNULL(src.[InstanceIndex],	-1)
	AND	ISNULL(tgt.[ParentName],		'')	= ISNULL(src.[ParentName],		'')
	AND	ISNULL(tgt.[ParentObjectID],	-1)	= ISNULL(src.[ParentObjectID],	-1)
	WHEN NOT MATCHED THEN
		INSERT (
			[MachineName],	[ObjectName],	[CounterName],	[CounterType],	[DefaultScale]
		,	[InstanceName],	[InstanceIndex]
		,	[ParentName],	[ParentObjectID]
		,	[TimeBaseA],	[TimeBaseB]
		)
		VALUES (
			src.[MachineName],	src.[ObjectName],	src.[CounterName],	src.[CounterType],	src.[DefaultScale]
		,	src.[InstanceName],	src.[InstanceIndex]
		,	src.[ParentName],	src.[ParentObjectID]
		,	src.[TimeBaseA],	src.[TimeBaseB]
		);

	SET @rows = @@ROWCOUNT;
	SET @msg = CONCAT('New counter definitions added: ', @rows);
	RAISERROR('%s', 0, 1, @msg) WITH NOWAIT;

	---------------------------------------------------------------------------
	-- 2. Insert counter data with remapped CounterIDs
	--    staging.CounterID → vault.CounterID via business key join
	--    char(24) → datetime via LEFT(,23) to strip null terminator
	---------------------------------------------------------------------------
	RAISERROR('Loading counter data...', 0, 1) WITH NOWAIT;

	INSERT INTO [vault].[CounterData] WITH (TABLOCKX)
	(
		[CounterDateTime]
	,	[CounterID]
	,	[RecordIndex]
	,	[CounterValue]
	,	[FirstValueA]
	,	[FirstValueB]
	,	[SecondValueA]
	,	[SecondValueB]
	,	[MultiCount]
	,	[GUID]
	)
	SELECT
		CONVERT(datetime, LEFT(sd.[CounterDateTime], 23), 121)
	,	vc.[CounterID]
	,	sd.[RecordIndex]
	,	CAST(sd.[CounterValue] AS decimal(28,0)) AS [CounterValue]
	,	sd.[FirstValueA]
	,	sd.[FirstValueB]
	,	sd.[SecondValueA]
	,	sd.[SecondValueB]
	,	sd.[MultiCount]
	,	sd.[GUID]
	FROM	[staging].[CounterData] sd
	JOIN	@NewGUIDs ng
		ON	ng.[GUID] = sd.[GUID]
	JOIN	[staging].[CounterDetails] sc
		ON	sc.[CounterID] = sd.[CounterID]
	JOIN	[vault].[CounterDetails] vc
		ON	vc.[MachineName]	= sc.[MachineName]
		AND	vc.[ObjectName]		= sc.[ObjectName]
		AND	vc.[CounterName]	= sc.[CounterName]
		AND	ISNULL(vc.[InstanceName],		'')	= ISNULL(sc.[InstanceName],		'')
		AND	ISNULL(vc.[InstanceIndex],		-1)	= ISNULL(sc.[InstanceIndex],	-1)
		AND	ISNULL(vc.[ParentName],			'')	= ISNULL(sc.[ParentName],		'')
		AND	ISNULL(vc.[ParentObjectID],		-1)	= ISNULL(sc.[ParentObjectID],	-1)
	ORDER BY
		CONVERT(datetime, LEFT(sd.[CounterDateTime], 23), 121)
	,	vc.[CounterID]
	OPTION (MAXDOP 1);

	SET @rows = @@ROWCOUNT;
	SET @msg = CONCAT('Counter data rows loaded: ', @rows);
	RAISERROR('%s', 0, 1, @msg) WITH NOWAIT;

	---------------------------------------------------------------------------
	-- 3. Mark BLGs as loaded by inserting into vault.DisplayToID
	--    Done AFTER CounterData so that a failed load can be re-run:
	--    if CounterData fails, the GUID is not in vault.DisplayToID,
	--    and next execution will pick it up again.
	---------------------------------------------------------------------------
	INSERT INTO [vault].[DisplayToID]
	(
		[GUID]
	,	[RunID]
	,	[DisplayString]
	,	[LogStartTime]
	,	[LogStopTime]
	,	[NumberOfRecords]
	,	[MinutesToUTC]
	,	[TimeZoneName]
	)
	SELECT
		s.[GUID]
	,	s.[RunID]
	,	s.[DisplayString]
	,	CONVERT(datetime, LEFT(s.[LogStartTime], 23), 121)
	,	CONVERT(datetime, LEFT(s.[LogStopTime], 23), 121)
	,	s.[NumberOfRecords]
	,	s.[MinutesToUTC]
	,	s.[TimeZoneName]
	FROM	[staging].[DisplayToID] s
	JOIN	@NewGUIDs ng
		ON	ng.[GUID] = s.[GUID];

	SET @rows = @@ROWCOUNT;
	SET @msg = CONCAT('BLGs marked as loaded: ', @rows);
	RAISERROR('%s', 0, 1, @msg) WITH NOWAIT;

	---------------------------------------------------------------------------
	-- 4. Truncate staging (optional)
	---------------------------------------------------------------------------
	IF @TruncateStaging = 1
	BEGIN
		TRUNCATE TABLE [staging].[CounterData];
		TRUNCATE TABLE [staging].[CounterDetails];
		TRUNCATE TABLE [staging].[DisplayToID];
		RAISERROR('Staging tables truncated.', 0, 1) WITH NOWAIT;
	END

	---------------------------------------------------------------------------
	-- Done
	---------------------------------------------------------------------------
	SET @msg = CONCAT('Load complete. Duration: ',
		DATEDIFF(SECOND, @start, GETDATE()), ' seconds.');
	RAISERROR('%s', 0, 1, @msg) WITH NOWAIT;

	RETURN 0;
END
GO
