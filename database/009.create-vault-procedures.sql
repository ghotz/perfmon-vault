:setvar DatabaseName "PerfmonVault"
USE [$(DatabaseName)];
GO

-------------------------------------------------------------------------------
-- usp_LoadFromStaging
--
-- Loads data from [staging] into [vault] with tier-based routing:
--   1. Identifies new BLGs (by GUID) not yet in vault
--   2. Merges new counter definitions into vault.CounterDetails
--   3. Inserts counter data into T1/T2/T3 based on CounterTier patterns
--   4. Marks BLGs as loaded in vault.DisplayToID
--   5. Optionally truncates staging
--
-- char(24) → datetime: LEFT(,23) strips the null terminator from relog
-- CounterID remapping: staging.CounterID → vault.CounterID via business key
-- Tier resolution: vault.vCounterTierResolved (lowest matching tier wins)
-------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [vault].[usp_LoadFromStaging]
	@TruncateStaging bit = 0
,	@LoadOnlyCounters bit = 0
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	DECLARE @msg varchar(500);
	DECLARE @rows int;
	DECLARE @start datetime = GETDATE();

	---------------------------------------------------------------------------
	-- 0. Identify new BLGs
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
	-- 1. Merge counter definitions
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

	IF @LoadOnlyCounters = 1 RETURN;

	---------------------------------------------------------------------------
	-- 2. Prepare tier resolution into a temp table for performance
	--    (avoids re-evaluating the LIKE patterns per row)
	---------------------------------------------------------------------------
	DROP TABLE IF EXISTS #TierMap;

	SELECT	vc.[CounterID]	AS [VaultCounterID]
	,		t.[Tier]
	INTO	#TierMap
	FROM	[vault].[vCounterTierResolved] t
	JOIN	[vault].[CounterDetails] vc ON vc.[CounterID] = t.[CounterID];

	CREATE UNIQUE CLUSTERED INDEX uci_TierMap ON #TierMap ([VaultCounterID]);

	---------------------------------------------------------------------------
	-- 3. Prepare staging-to-vault CounterID mapping into a temp table
	---------------------------------------------------------------------------
	DROP TABLE IF EXISTS #CounterMap;

	SELECT	sc.[CounterID]	AS [StagingCounterID]
	,		vc.[CounterID]	AS [VaultCounterID]
	INTO	#CounterMap
	FROM	[staging].[CounterDetails] sc
	JOIN	[vault].[CounterDetails] vc
		ON	vc.[MachineName]	= sc.[MachineName]
		AND	vc.[ObjectName]		= sc.[ObjectName]
		AND	vc.[CounterName]	= sc.[CounterName]
		AND	ISNULL(vc.[InstanceName],		'')	= ISNULL(sc.[InstanceName],		'')
		AND	ISNULL(vc.[InstanceIndex],		-1)	= ISNULL(sc.[InstanceIndex],	-1)
		AND	ISNULL(vc.[ParentName],			'')	= ISNULL(sc.[ParentName],		'')
		AND	ISNULL(vc.[ParentObjectID],		-1)	= ISNULL(sc.[ParentObjectID],	-1);

	CREATE UNIQUE CLUSTERED INDEX uci_CounterMap ON #CounterMap ([StagingCounterID]);

	---------------------------------------------------------------------------
	-- 4. Insert counter data — Tier 1
	---------------------------------------------------------------------------
	RAISERROR('Loading Tier 1 (hot)...', 0, 1) WITH NOWAIT;

	INSERT INTO [vault].[CounterData_Tier1] WITH (TABLOCKX)
	(
		[CounterDateTime],	[CounterID],	[RecordIndex],	[CounterValue]
	,	[FirstValueA],		[FirstValueB]
	,	[SecondValueA],		[SecondValueB]
	,	[MultiCount],		[GUID]
	)
	SELECT
		CONVERT(datetime, LEFT(sd.[CounterDateTime], 23), 121)
	,	cm.[VaultCounterID]
	,	sd.[RecordIndex]
	,	CAST(sd.[CounterValue] AS decimal(28,0))
	,	sd.[FirstValueA],	sd.[FirstValueB]
	,	sd.[SecondValueA],	sd.[SecondValueB]
	,	sd.[MultiCount]
	,	sd.[GUID]
	FROM	[staging].[CounterData] sd
	JOIN	@NewGUIDs ng	ON ng.[GUID] = sd.[GUID]
	JOIN	#CounterMap cm	ON cm.[StagingCounterID] = sd.[CounterID]
	JOIN	#TierMap tm		ON tm.[VaultCounterID] = cm.[VaultCounterID]
	WHERE	tm.[Tier] = 1
	ORDER BY
		CONVERT(datetime, LEFT(sd.[CounterDateTime], 23), 121)
	,	cm.[VaultCounterID]
	OPTION (MAXDOP 1);

	SET @rows = @@ROWCOUNT;
	SET @msg = CONCAT('  Tier 1 rows: ', @rows);
	RAISERROR('%s', 0, 1, @msg) WITH NOWAIT;

	---------------------------------------------------------------------------
	-- 5. Insert counter data — Tier 2
	---------------------------------------------------------------------------
	RAISERROR('Loading Tier 2 (warm)...', 0, 1) WITH NOWAIT;

	INSERT INTO [vault].[CounterData_Tier2] WITH (TABLOCKX)
	(
		[CounterDateTime],	[CounterID],	[RecordIndex],	[CounterValue]
	,	[FirstValueA],		[FirstValueB]
	,	[SecondValueA],		[SecondValueB]
	,	[MultiCount],		[GUID]
	)
	SELECT
		CONVERT(datetime, LEFT(sd.[CounterDateTime], 23), 121)
	,	cm.[VaultCounterID]
	,	sd.[RecordIndex]
	,	CAST(sd.[CounterValue] AS decimal(28,0))
	,	sd.[FirstValueA],	sd.[FirstValueB]
	,	sd.[SecondValueA],	sd.[SecondValueB]
	,	sd.[MultiCount]
	,	sd.[GUID]
	FROM	[staging].[CounterData] sd
	JOIN	@NewGUIDs ng	ON ng.[GUID] = sd.[GUID]
	JOIN	#CounterMap cm	ON cm.[StagingCounterID] = sd.[CounterID]
	JOIN	#TierMap tm		ON tm.[VaultCounterID] = cm.[VaultCounterID]
	WHERE	tm.[Tier] = 2
	ORDER BY
		CONVERT(datetime, LEFT(sd.[CounterDateTime], 23), 121)
	,	cm.[VaultCounterID]
	OPTION (MAXDOP 1);

	SET @rows = @@ROWCOUNT;
	SET @msg = CONCAT('  Tier 2 rows: ', @rows);
	RAISERROR('%s', 0, 1, @msg) WITH NOWAIT;

	---------------------------------------------------------------------------
	-- 6. Insert counter data — Tier 3
	---------------------------------------------------------------------------
	RAISERROR('Loading Tier 3 (cold/archive)...', 0, 1) WITH NOWAIT;

	INSERT INTO [vault].[CounterData_Tier3] WITH (TABLOCKX)
	(
		[CounterDateTime],	[CounterID],	[RecordIndex],	[CounterValue]
	,	[FirstValueA],		[FirstValueB]
	,	[SecondValueA],		[SecondValueB]
	,	[MultiCount],		[GUID]
	)
	SELECT
		CONVERT(datetime, LEFT(sd.[CounterDateTime], 23), 121)
	,	cm.[VaultCounterID]
	,	sd.[RecordIndex]
	,	CAST(sd.[CounterValue] AS decimal(28,0))
	,	sd.[FirstValueA],	sd.[FirstValueB]
	,	sd.[SecondValueA],	sd.[SecondValueB]
	,	sd.[MultiCount]
	,	sd.[GUID]
	FROM	[staging].[CounterData] sd
	JOIN	@NewGUIDs ng	ON ng.[GUID] = sd.[GUID]
	JOIN	#CounterMap cm	ON cm.[StagingCounterID] = sd.[CounterID]
	JOIN	#TierMap tm		ON tm.[VaultCounterID] = cm.[VaultCounterID]
	WHERE	tm.[Tier] = 3
	ORDER BY
		CONVERT(datetime, LEFT(sd.[CounterDateTime], 23), 121)
	,	cm.[VaultCounterID]
	OPTION (MAXDOP 1);

	SET @rows = @@ROWCOUNT;
	SET @msg = CONCAT('  Tier 3 rows: ', @rows);
	RAISERROR('%s', 0, 1, @msg) WITH NOWAIT;

	---------------------------------------------------------------------------
	-- 7. Mark BLGs as loaded
	---------------------------------------------------------------------------
	INSERT INTO [vault].[DisplayToID]
	(
		[GUID],			[RunID],		[DisplayString]
	,	[LogStartTime],	[LogStopTime]
	,	[NumberOfRecords]
	,	[MinutesToUTC],	[TimeZoneName]
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
	JOIN	@NewGUIDs ng ON ng.[GUID] = s.[GUID];

	SET @rows = @@ROWCOUNT;
	SET @msg = CONCAT('BLGs marked as loaded: ', @rows);
	RAISERROR('%s', 0, 1, @msg) WITH NOWAIT;

	---------------------------------------------------------------------------
	-- 8. Cleanup
	---------------------------------------------------------------------------
	DROP TABLE IF EXISTS #TierMap;
	DROP TABLE IF EXISTS #CounterMap;

	IF @TruncateStaging = 1
	BEGIN
		TRUNCATE TABLE [staging].[CounterData];
		TRUNCATE TABLE [staging].[CounterDetails];
		TRUNCATE TABLE [staging].[DisplayToID];
		RAISERROR('Staging tables truncated.', 0, 1) WITH NOWAIT;
	END

	SET @msg = CONCAT('Load complete. Duration: ',
		DATEDIFF(SECOND, @start, GETDATE()), ' seconds.');
	RAISERROR('%s', 0, 1, @msg) WITH NOWAIT;

	RETURN 0;
END
GO

