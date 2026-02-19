:setvar DatabaseName "PerfmonVault"
USE [$(DatabaseName)];
GO

-------------------------------------------------------------------------------
-- usp_RebalanceTiers
--
-- Moves counter data between tier tables when CounterTier rules change.
--
-- Flow:
--   1. Detects misplaced data (CounterID in wrong tier table)
--   2. Moves data to the correct tier table
--   3. Deletes from the source tier table
--
-- Run this after modifying CounterTier rules.
--
-- Parameters:
--   @DryRun    = 1 (default): report only, no data movement
--              = 0: actually move the data
-------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [vault].[usp_RebalanceTiers]
	@DryRun bit = 1
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	DECLARE @msg varchar(500);
	DECLARE @rows int;
	DECLARE @start datetime = GETDATE();

	---------------------------------------------------------------------------
	-- 1. Build current tier assignments
	---------------------------------------------------------------------------
	DROP TABLE IF EXISTS #TierMap;

	SELECT	[CounterID], [Tier]
	INTO	#TierMap
	FROM	[vault].[vCounterTierResolved];

	CREATE UNIQUE CLUSTERED INDEX uci_TierMap ON #TierMap ([CounterID]);

	---------------------------------------------------------------------------
	-- 2. Detect misplaced data
	---------------------------------------------------------------------------
	DROP TABLE IF EXISTS #Misplaced;

	;WITH CurrentPlacement AS (
		SELECT [CounterID], 1 AS [CurrentTier] FROM [vault].[CounterData_T1] GROUP BY [CounterID]
		UNION ALL
		SELECT [CounterID], 2 AS [CurrentTier] FROM [vault].[CounterData_T2] GROUP BY [CounterID]
		UNION ALL
		SELECT [CounterID], 3 AS [CurrentTier] FROM [vault].[CounterData_T3] GROUP BY [CounterID]
	)
	SELECT
		cp.[CounterID]
	,	cp.[CurrentTier]
	,	ISNULL(tm.[Tier], 3) AS [TargetTier]
	INTO	#Misplaced
	FROM	CurrentPlacement cp
	LEFT JOIN #TierMap tm ON tm.[CounterID] = cp.[CounterID]
	WHERE	cp.[CurrentTier] <> ISNULL(tm.[Tier], 3);

	SET @rows = @@ROWCOUNT;

	IF @rows = 0
	BEGIN
		RAISERROR('All data is in the correct tier. Nothing to do.', 0, 1) WITH NOWAIT;
		RETURN 0;
	END

	---------------------------------------------------------------------------
	-- 3. Report
	---------------------------------------------------------------------------
	SET @msg = CONCAT('Counters to rebalance: ', @rows);
	RAISERROR('%s', 0, 1, @msg) WITH NOWAIT;

	-- Summary by movement direction
	SELECT
		m.[CurrentTier]
	,	m.[TargetTier]
	,	COUNT(DISTINCT m.[CounterID])	AS [NumCounters]
	,	SUM(cnt.[NumRows])				AS [EstimatedRows]
	FROM	#Misplaced m
	CROSS APPLY (
		SELECT COUNT_BIG(*) AS [NumRows]
		FROM [vault].[CounterData_T1] WHERE [CounterID] = m.[CounterID] AND m.[CurrentTier] = 1
		UNION ALL
		SELECT COUNT_BIG(*) FROM [vault].[CounterData_T2] WHERE [CounterID] = m.[CounterID] AND m.[CurrentTier] = 2
		UNION ALL
		SELECT COUNT_BIG(*) FROM [vault].[CounterData_T3] WHERE [CounterID] = m.[CounterID] AND m.[CurrentTier] = 3
	) cnt
	GROUP BY m.[CurrentTier], m.[TargetTier]
	ORDER BY m.[CurrentTier], m.[TargetTier];

	IF @DryRun = 1
	BEGIN
		RAISERROR('DRY RUN: no data moved. Run with @DryRun = 0 to execute.', 0, 1) WITH NOWAIT;

		-- Detail: which counters would move
		SELECT
			m.[CounterID]
		,	m.[CurrentTier]
		,	m.[TargetTier]
		,	cd.[ObjectName]
		,	cd.[CounterName]
		,	cd.[InstanceName]
		FROM	#Misplaced m
		JOIN	[vault].[CounterDetails] cd ON cd.[CounterID] = m.[CounterID]
		ORDER BY m.[CurrentTier], m.[TargetTier], cd.[ObjectName], cd.[CounterName];

		RETURN 0;
	END

	---------------------------------------------------------------------------
	-- 4. Move data: T1 → T2
	---------------------------------------------------------------------------
	IF EXISTS (SELECT 1 FROM #Misplaced WHERE [CurrentTier] = 1 AND [TargetTier] = 2)
	BEGIN
		RAISERROR('Moving T1 → T2...', 0, 1) WITH NOWAIT;

		INSERT INTO [vault].[CounterData_T2] WITH (TABLOCKX)
		([CounterDateTime], [CounterID], [RecordIndex], [CounterValue],
		 [FirstValueA], [FirstValueB], [SecondValueA], [SecondValueB],
		 [MultiCount], [GUID])
		SELECT
			[CounterDateTime], [CounterID], [RecordIndex], [CounterValue],
			[FirstValueA], [FirstValueB], [SecondValueA], [SecondValueB],
			[MultiCount], [GUID]
		FROM	[vault].[CounterData_T1] src
		WHERE	EXISTS (
			SELECT 1 FROM #Misplaced m
			WHERE m.[CounterID] = src.[CounterID]
			AND m.[CurrentTier] = 1 AND m.[TargetTier] = 2
		)
		ORDER BY src.[CounterDateTime], src.[CounterID]
		OPTION (MAXDOP 1);

		SET @rows = @@ROWCOUNT;
		SET @msg = CONCAT('  Inserted into T2: ', @rows);
		RAISERROR('%s', 0, 1, @msg) WITH NOWAIT;

		DELETE src
		FROM	[vault].[CounterData_T1] src
		WHERE	EXISTS (
			SELECT 1 FROM #Misplaced m
			WHERE m.[CounterID] = src.[CounterID]
			AND m.[CurrentTier] = 1 AND m.[TargetTier] = 2
		);

		SET @msg = CONCAT('  Deleted from T1: ', @@ROWCOUNT);
		RAISERROR('%s', 0, 1, @msg) WITH NOWAIT;
	END

	---------------------------------------------------------------------------
	-- 5. Move data: T1 → T3
	---------------------------------------------------------------------------
	IF EXISTS (SELECT 1 FROM #Misplaced WHERE [CurrentTier] = 1 AND [TargetTier] = 3)
	BEGIN
		RAISERROR('Moving T1 → T3...', 0, 1) WITH NOWAIT;

		INSERT INTO [vault].[CounterData_T3] WITH (TABLOCKX)
		([CounterDateTime], [CounterID], [RecordIndex], [CounterValue],
		 [FirstValueA], [FirstValueB], [SecondValueA], [SecondValueB],
		 [MultiCount], [GUID])
		SELECT
			[CounterDateTime], [CounterID], [RecordIndex], [CounterValue],
			[FirstValueA], [FirstValueB], [SecondValueA], [SecondValueB],
			[MultiCount], [GUID]
		FROM	[vault].[CounterData_T1] src
		WHERE	EXISTS (
			SELECT 1 FROM #Misplaced m
			WHERE m.[CounterID] = src.[CounterID]
			AND m.[CurrentTier] = 1 AND m.[TargetTier] = 3
		)
		ORDER BY src.[CounterDateTime], src.[CounterID]
		OPTION (MAXDOP 1);

		SET @rows = @@ROWCOUNT;
		SET @msg = CONCAT('  Inserted into T3: ', @rows);
		RAISERROR('%s', 0, 1, @msg) WITH NOWAIT;

		DELETE src
		FROM	[vault].[CounterData_T1] src
		WHERE	EXISTS (
			SELECT 1 FROM #Misplaced m
			WHERE m.[CounterID] = src.[CounterID]
			AND m.[CurrentTier] = 1 AND m.[TargetTier] = 3
		);

		SET @msg = CONCAT('  Deleted from T1: ', @@ROWCOUNT);
		RAISERROR('%s', 0, 1, @msg) WITH NOWAIT;
	END

	---------------------------------------------------------------------------
	-- 6. Move data: T2 → T1
	---------------------------------------------------------------------------
	IF EXISTS (SELECT 1 FROM #Misplaced WHERE [CurrentTier] = 2 AND [TargetTier] = 1)
	BEGIN
		RAISERROR('Moving T2 → T1...', 0, 1) WITH NOWAIT;

		INSERT INTO [vault].[CounterData_T1] WITH (TABLOCKX)
		([CounterDateTime], [CounterID], [RecordIndex], [CounterValue],
		 [FirstValueA], [FirstValueB], [SecondValueA], [SecondValueB],
		 [MultiCount], [GUID])
		SELECT
			[CounterDateTime], [CounterID], [RecordIndex], [CounterValue],
			[FirstValueA], [FirstValueB], [SecondValueA], [SecondValueB],
			[MultiCount], [GUID]
		FROM	[vault].[CounterData_T2] src
		WHERE	EXISTS (
			SELECT 1 FROM #Misplaced m
			WHERE m.[CounterID] = src.[CounterID]
			AND m.[CurrentTier] = 2 AND m.[TargetTier] = 1
		)
		ORDER BY src.[CounterDateTime], src.[CounterID]
		OPTION (MAXDOP 1);

		SET @rows = @@ROWCOUNT;
		SET @msg = CONCAT('  Inserted into T1: ', @rows);
		RAISERROR('%s', 0, 1, @msg) WITH NOWAIT;

		DELETE src
		FROM	[vault].[CounterData_T2] src
		WHERE	EXISTS (
			SELECT 1 FROM #Misplaced m
			WHERE m.[CounterID] = src.[CounterID]
			AND m.[CurrentTier] = 2 AND m.[TargetTier] = 1
		);

		SET @msg = CONCAT('  Deleted from T2: ', @@ROWCOUNT);
		RAISERROR('%s', 0, 1, @msg) WITH NOWAIT;
	END

	---------------------------------------------------------------------------
	-- 7. Move data: T2 → T3
	---------------------------------------------------------------------------
	IF EXISTS (SELECT 1 FROM #Misplaced WHERE [CurrentTier] = 2 AND [TargetTier] = 3)
	BEGIN
		RAISERROR('Moving T2 → T3...', 0, 1) WITH NOWAIT;

		INSERT INTO [vault].[CounterData_T3] WITH (TABLOCKX)
		([CounterDateTime], [CounterID], [RecordIndex], [CounterValue],
		 [FirstValueA], [FirstValueB], [SecondValueA], [SecondValueB],
		 [MultiCount], [GUID])
		SELECT
			[CounterDateTime], [CounterID], [RecordIndex], [CounterValue],
			[FirstValueA], [FirstValueB], [SecondValueA], [SecondValueB],
			[MultiCount], [GUID]
		FROM	[vault].[CounterData_T2] src
		WHERE	EXISTS (
			SELECT 1 FROM #Misplaced m
			WHERE m.[CounterID] = src.[CounterID]
			AND m.[CurrentTier] = 2 AND m.[TargetTier] = 3
		)
		ORDER BY src.[CounterDateTime], src.[CounterID]
		OPTION (MAXDOP 1);

		SET @rows = @@ROWCOUNT;
		SET @msg = CONCAT('  Inserted into T3: ', @rows);
		RAISERROR('%s', 0, 1, @msg) WITH NOWAIT;

		DELETE src
		FROM	[vault].[CounterData_T2] src
		WHERE	EXISTS (
			SELECT 1 FROM #Misplaced m
			WHERE m.[CounterID] = src.[CounterID]
			AND m.[CurrentTier] = 2 AND m.[TargetTier] = 3
		);

		SET @msg = CONCAT('  Deleted from T2: ', @@ROWCOUNT);
		RAISERROR('%s', 0, 1, @msg) WITH NOWAIT;
	END

	---------------------------------------------------------------------------
	-- 8. Move data: T3 → T1
	---------------------------------------------------------------------------
	IF EXISTS (SELECT 1 FROM #Misplaced WHERE [CurrentTier] = 3 AND [TargetTier] = 1)
	BEGIN
		RAISERROR('Moving T3 → T1...', 0, 1) WITH NOWAIT;

		INSERT INTO [vault].[CounterData_T1] WITH (TABLOCKX)
		([CounterDateTime], [CounterID], [RecordIndex], [CounterValue],
		 [FirstValueA], [FirstValueB], [SecondValueA], [SecondValueB],
		 [MultiCount], [GUID])
		SELECT
			[CounterDateTime], [CounterID], [RecordIndex], [CounterValue],
			[FirstValueA], [FirstValueB], [SecondValueA], [SecondValueB],
			[MultiCount], [GUID]
		FROM	[vault].[CounterData_T3] src
		WHERE	EXISTS (
			SELECT 1 FROM #Misplaced m
			WHERE m.[CounterID] = src.[CounterID]
			AND m.[CurrentTier] = 3 AND m.[TargetTier] = 1
		)
		ORDER BY src.[CounterDateTime], src.[CounterID]
		OPTION (MAXDOP 1);

		SET @rows = @@ROWCOUNT;
		SET @msg = CONCAT('  Inserted into T1: ', @rows);
		RAISERROR('%s', 0, 1, @msg) WITH NOWAIT;

		DELETE src
		FROM	[vault].[CounterData_T3] src
		WHERE	EXISTS (
			SELECT 1 FROM #Misplaced m
			WHERE m.[CounterID] = src.[CounterID]
			AND m.[CurrentTier] = 3 AND m.[TargetTier] = 1
		);

		SET @msg = CONCAT('  Deleted from T3: ', @@ROWCOUNT);
		RAISERROR('%s', 0, 1, @msg) WITH NOWAIT;
	END

	---------------------------------------------------------------------------
	-- 9. Move data: T3 → T2
	---------------------------------------------------------------------------
	IF EXISTS (SELECT 1 FROM #Misplaced WHERE [CurrentTier] = 3 AND [TargetTier] = 2)
	BEGIN
		RAISERROR('Moving T3 → T2...', 0, 1) WITH NOWAIT;

		INSERT INTO [vault].[CounterData_T2] WITH (TABLOCKX)
		([CounterDateTime], [CounterID], [RecordIndex], [CounterValue],
		 [FirstValueA], [FirstValueB], [SecondValueA], [SecondValueB],
		 [MultiCount], [GUID])
		SELECT
			[CounterDateTime], [CounterID], [RecordIndex], [CounterValue],
			[FirstValueA], [FirstValueB], [SecondValueA], [SecondValueB],
			[MultiCount], [GUID]
		FROM	[vault].[CounterData_T3] src
		WHERE	EXISTS (
			SELECT 1 FROM #Misplaced m
			WHERE m.[CounterID] = src.[CounterID]
			AND m.[CurrentTier] = 3 AND m.[TargetTier] = 2
		)
		ORDER BY src.[CounterDateTime], src.[CounterID]
		OPTION (MAXDOP 1);

		SET @rows = @@ROWCOUNT;
		SET @msg = CONCAT('  Inserted into T2: ', @rows);
		RAISERROR('%s', 0, 1, @msg) WITH NOWAIT;

		DELETE src
		FROM	[vault].[CounterData_T3] src
		WHERE	EXISTS (
			SELECT 1 FROM #Misplaced m
			WHERE m.[CounterID] = src.[CounterID]
			AND m.[CurrentTier] = 3 AND m.[TargetTier] = 2
		);

		SET @msg = CONCAT('  Deleted from T3: ', @@ROWCOUNT);
		RAISERROR('%s', 0, 1, @msg) WITH NOWAIT;
	END

	---------------------------------------------------------------------------
	-- 10. Move data to Tier 0 (excluded) — just delete
	---------------------------------------------------------------------------
	IF EXISTS (SELECT 1 FROM #Misplaced WHERE [TargetTier] = 0)
	BEGIN
		RAISERROR('Purging Tier 0 (excluded) counters...', 0, 1) WITH NOWAIT;

		DELETE src
		FROM [vault].[CounterData_T1] src
		WHERE EXISTS (
			SELECT 1 FROM #Misplaced m
			WHERE m.[CounterID] = src.[CounterID] AND m.[CurrentTier] = 1 AND m.[TargetTier] = 0
		);

		DELETE src
		FROM [vault].[CounterData_T2] src
		WHERE EXISTS (
			SELECT 1 FROM #Misplaced m
			WHERE m.[CounterID] = src.[CounterID] AND m.[CurrentTier] = 2 AND m.[TargetTier] = 0
		);

		DELETE src
		FROM [vault].[CounterData_T3] src
		WHERE EXISTS (
			SELECT 1 FROM #Misplaced m
			WHERE m.[CounterID] = src.[CounterID] AND m.[CurrentTier] = 3 AND m.[TargetTier] = 0
		);

		RAISERROR('  Tier 0 purge complete.', 0, 1) WITH NOWAIT;
	END

	---------------------------------------------------------------------------
	-- 11. Cleanup & summary
	---------------------------------------------------------------------------
	DROP TABLE IF EXISTS #TierMap;
	DROP TABLE IF EXISTS #Misplaced;

	SET @msg = CONCAT('Rebalance complete. Duration: ',
		DATEDIFF(SECOND, @start, GETDATE()), ' seconds.');
	RAISERROR('%s', 0, 1, @msg) WITH NOWAIT;

	RAISERROR('Consider running REORGANIZE on affected tier tables to consolidate rowgroups.', 0, 1) WITH NOWAIT;

	RETURN 0;
END
GO
