

CREATE TABLE [dbo].[CounterData_LogSet] (
    LogSetID    int IDENTITY(1,1) PRIMARY KEY,
    OriginalGUID uniqueidentifier NOT NULL UNIQUE
);

-- Tabella fact ottimizzata per columnstore
CREATE TABLE [dbo].[CounterData_Final] (
    LogSetID        int             NOT NULL,  -- FK → LogSet, era GUID
    CounterID       int             NOT NULL,
    RecordIndex     int             NOT NULL,
    CounterDate     date            NOT NULL,
    CounterTime     time(3)         NOT NULL,
    CounterValue    decimal(20,0)   NOT NULL,
    --CounterValue    float   NOT NULL,
    FirstValueA     int             NULL,
    FirstValueB     int             NULL,
    SecondValueA    int             NULL,
    SecondValueB    int             NULL,
    MultiCount      int             NULL
);

-- Ordered Clustered Columnstore (SQL Server 2025)
CREATE CLUSTERED COLUMNSTORE INDEX CCI_CounterData_Final
ON [dbo].[CounterData_Final]
ORDER (LogSetID, CounterID, CounterTime);
GO

-- Tabella fact ottimizzata per columnstore
CREATE TABLE [dbo].[CounterData_Final2] (
    LogSetID        int             NOT NULL,  -- FK → LogSet, era GUID
    CounterID       int             NOT NULL,
    RecordIndex     int             NOT NULL,
    CounterDate     date            NOT NULL,
    CounterTime     time(3)         NOT NULL,
    CounterValue    decimal(20,0)   NOT NULL,
    --CounterValue    float   NOT NULL,
    FirstValueA     int             NULL,
    FirstValueB     int             NULL,
    SecondValueA    int             NULL,
    SecondValueB    int             NULL,
    MultiCount      int             NULL
);

-- Ordered Clustered Columnstore (SQL Server 2025)
CREATE CLUSTERED COLUMNSTORE INDEX CCI_CounterData_Final2
ON [dbo].[CounterData_Final2]
ORDER (CounterDate, CounterTime, CounterID, LogSetID);
GO



INSERT INTO CounterData_LogSet (OriginalGUID)
SELECT  [GUID]
FROM    [dbo].[DisplayToID] AS L1
WHERE NOT EXISTS (
    SELECT  *
    FROM    CounterData_LogSet AS L2
    WHERE   L2.OriginalGUID = L1.[GUID]
)
GROUP BY [LogStartTime], [GUID]
ORDER BY [LogStartTime]
GO

TRUNCATE TABLE CounterData_Final;
TRUNCATE TABLE CounterData_Final2;

DECLARE @start date = '2020-06-01', @end date = '2020-06-01', @batch date;

--SELECT @start = MIN(CAST(LEFT(CounterDateTime, 10) AS date)),
--       @end   = MAX(CAST(LEFT(CounterDateTime, 10) AS date))
--FROM CounterData_Full;

SET @batch = DATEFROMPARTS(YEAR(@start), MONTH(@start), 1);

WHILE @batch <= @end
BEGIN
    DECLARE @next date = DATEADD(MONTH, 1, @batch);
    
    DECLARE @msg varchar(200);

    SET @msg = CONVERT(char(7), @batch, 120);
    RAISERROR('Caricamento %s ...', 0, 1, @msg) WITH NOWAIT;

    INSERT INTO CounterData_Final WITH (TABLOCKX)
        (LogSetID, CounterID, RecordIndex, CounterDate, CounterTime, CounterValue,
         FirstValueA, FirstValueB, SecondValueA, SecondValueB, MultiCount)
    SELECT
        l.LogSetID,
        s.CounterID,
        s.RecordIndex,
        CAST(LEFT(s.CounterDateTime, 10) AS date),
        CONVERT(time(3), SUBSTRING(s.CounterDateTime, 12, 12)),
        CAST(s.CounterValue AS decimal(28,0)),
        --s.CounterValue,
        s.FirstValueA, s.FirstValueB,
        s.SecondValueA, s.SecondValueB,
        s.MultiCount
    FROM CounterData_Full s
    JOIN CounterData_LogSet l ON l.OriginalGUID = s.[GUID]
    WHERE s.CounterDateTime >= CONVERT(char(10), @batch, 120)
      AND s.CounterDateTime < CONVERT(char(10), @next, 120)
    ORDER BY l.LogSetID, s.CounterID, CONVERT(time(3), SUBSTRING(s.CounterDateTime, 12, 12))
             
    OPTION (MAXDOP 1);

    INSERT INTO CounterData_Final2 WITH (TABLOCKX)
        (LogSetID, CounterID, RecordIndex, CounterDate, CounterTime, CounterValue,
         FirstValueA, FirstValueB, SecondValueA, SecondValueB, MultiCount)
    SELECT
        l.LogSetID,
        s.CounterID,
        s.RecordIndex,
        CAST(LEFT(s.CounterDateTime, 10) AS date),
        CONVERT(time(3), SUBSTRING(s.CounterDateTime, 12, 12)),
        CAST(s.CounterValue AS decimal(28,0)),
        --s.CounterValue,
        s.FirstValueA, s.FirstValueB,
        s.SecondValueA, s.SecondValueB,
        s.MultiCount
    FROM CounterData_Full s
    JOIN CounterData_LogSet l ON l.OriginalGUID = s.[GUID]
    WHERE s.CounterDateTime >= CONVERT(char(10), @batch, 120)
      AND s.CounterDateTime < CONVERT(char(10), @next, 120)
    ORDER BY CONVERT(datetime2(3), LEFT(s.CounterDateTime, 23), 121), 
             s.CounterID, l.LogSetID
    OPTION (MAXDOP 1);


    SET @msg = CONCAT('  completato ', CONVERT(char(7), @batch, 120), ' - ', @@ROWCOUNT, ' righe');
    RAISERROR('%s', 0, 1, @msg) WITH NOWAIT;
    
    SET @batch = @next;
END


--TRUNCATE TABLE [staging].[CounterData]
--TRUNCATE TABLE [staging].[CounterDetails]
--TRUNCATE TABLE [staging].[DisplayToID]

SELECT   COUNT(*)
FROM    [vault].[CounterDetails]
WHERE   [ObjectName] like '%sql%'
--GROUP BY [MachineName], [ObjectName], [CounterName]

sele