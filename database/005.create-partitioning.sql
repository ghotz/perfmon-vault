:setvar DatabaseName "PerfmonVault"
:setvar StartYear "2024"
:setvar NumYears "2"
:setvar FilegroupName "VAULT"
USE [$(DatabaseName)];
GO

-------------------------------------------------------------------------------
-- Dynamic partition function and scheme for yearly partitioning
-- Boundaries: 2022-01-01, 2023-01-01, ..., (StartYear + NumYears - 1)-01-01
-- LEFT range: each partition holds [year-01-01, next-year-01-01)
--
-- Example with StartYear=2022, NumYears=10:
--   Partition  1: < 2022-01-01          (overflow left)
--   Partition  2: >= 2022 and < 2023
--   Partition  3: >= 2023 and < 2024
--   ...
--   Partition 11: >= 2031 and < 2032
--   Partition 12: >= 2032              (overflow right — not created, RIGHT range)
--
-- Using RANGE RIGHT so boundary value is the FIRST value in the partition.
-------------------------------------------------------------------------------

-- 1. Build the boundary values dynamically
DECLARE @StartYear	int = $(StartYear);
DECLARE @NumYears	int = $(NumYears);
DECLARE @sql		nvarchar(max);
DECLARE @boundaries	nvarchar(max) = N'';
DECLARE @fglist		nvarchar(max) = N'';
DECLARE @i			int = 0;

WHILE @i < @NumYears
BEGIN
	IF @i > 0
		SET @boundaries += N', ';

	SET @boundaries += N'''' + CAST(DATEFROMPARTS(@StartYear + @i, 1, 1) AS nvarchar(10)) + N'''';
	SET @i += 1;
END

-- NumYears boundaries = NumYears + 1 partitions
-- All mapped to the same filegroup for now
SET @i = 0;
WHILE @i <= @NumYears
BEGIN
	IF @i > 0
		SET @fglist += N', ';

	SET @fglist += N'[$(FilegroupName)]';
	SET @i += 1;
END

-- 2. Drop existing objects if any (clean re-run)
IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = 'ps_CounterData_Yearly')
BEGIN
	-- Can't drop scheme if tables are using it — tables must be dropped first
	RAISERROR('Partition scheme ps_CounterData_Yearly already exists. Drop dependent tables first.', 16, 1);
	RETURN;
END

IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'pf_CounterData_Yearly')
BEGIN
	DROP PARTITION FUNCTION [pf_CounterData_Yearly];
END

-- 3. Create partition function (RANGE RIGHT on datetime)
SET @sql = N'
CREATE PARTITION FUNCTION [pf_CounterData_Yearly] (datetime)
AS RANGE RIGHT FOR VALUES (' + @boundaries + N');';

RAISERROR('Creating partition function...', 0, 1) WITH NOWAIT;
EXEC sp_executesql @sql;

-- 4. Create partition scheme
SET @sql = N'
CREATE PARTITION SCHEME [ps_CounterData_Yearly]
AS PARTITION [pf_CounterData_Yearly]
TO (' + @fglist + N');';

RAISERROR('Creating partition scheme...', 0, 1) WITH NOWAIT;
EXEC sp_executesql @sql;

-- 5. Verify
SELECT
	pf.name					AS partition_function
,	prv.boundary_id
,	CAST(prv.value AS date)	AS boundary_value
,	ps.name					AS partition_scheme
FROM	sys.partition_functions pf
JOIN	sys.partition_range_values prv	ON prv.function_id = pf.function_id
JOIN	sys.partition_schemes ps		ON ps.function_id = pf.function_id
WHERE	pf.name = 'pf_CounterData_Yearly'
ORDER BY prv.boundary_id;
GO

-------------------------------------------------------------------------------
-- Helper: add a new year partition (run annually before new data arrives)
-------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [vault].[usp_AddYearPartition]
	@Year int
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @boundary datetime = DATEFROMPARTS(@Year, 1, 1);

	-- Check if boundary already exists
	IF EXISTS (
		SELECT	1
		FROM	sys.partition_functions pf
		JOIN	sys.partition_range_values prv ON prv.function_id = pf.function_id
		WHERE	pf.name = 'pf_CounterData_Yearly'
		AND		prv.value = @boundary
	)
	BEGIN
		RAISERROR('Partition boundary for year %d already exists.', 0, 1, @Year) WITH NOWAIT;
		RETURN 0;
	END

	-- Extend scheme to use VAULT filegroup for the new partition
	ALTER PARTITION SCHEME [ps_CounterData_Yearly]
	NEXT USED [VAULT];

	-- Split the last partition
	ALTER PARTITION FUNCTION [pf_CounterData_Yearly]()
	SPLIT RANGE (@boundary);

	DECLARE @msg varchar(200) = CONCAT('Added partition for year ', @Year);
	RAISERROR('%s', 0, 1, @msg) WITH NOWAIT;

	RETURN 0;
END
GO
