:setvar DatabaseName "PerfmonVault"
:setvar DataFilesDir "E:\SQLServer\MSSQL17.SQL2025\MSSQL\DATA"
:setvar LogFilesDir "E:\SQLServer\MSSQL17.SQL2025\MSSQL\DATA"

-------------------------------------------------------------------------------
-- CReate default database
-------------------------------------------------------------------------------
USE [master];
GO
IF EXISTS (SELECT * FROM sys.databases WHERE [name] = N'$(DatabaseName)')
BEGIN
    ALTER DATABASE [$(DatabaseName)] SET OFFLINE WITH ROLLBACK IMMEDIATE;
    ALTER DATABASE [$(DatabaseName)] SET ONLINE WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [$(DatabaseName)];
END
GO

CREATE DATABASE [$(DatabaseName)]
 ON  PRIMARY 
    ( NAME = N'$(DatabaseName)'
    , FILENAME = N'$(DataFilesDir)\$(DatabaseName).mdf'
    , SIZE = 64MB , MAXSIZE = 256MB , FILEGROWTH = 64MB )
, FILEGROUP [VAULT]  DEFAULT
    ( NAME = N'$(DatabaseName)_vault'
    , FILENAME = N'$(DataFilesDir)\$(DatabaseName)_vault.ndf'
    , SIZE = 64MB , MAXSIZE = UNLIMITED, FILEGROWTH = 64MB)
, FILEGROUP [STAGING] 
    ( NAME = N'$(DatabaseName)_staging'
    , FILENAME = N'$(DataFilesDir)\$(DatabaseName)_staging.ndf'
    , SIZE = 64MB , MAXSIZE = UNLIMITED, FILEGROWTH = 64MB )
 LOG ON 
    ( NAME = N'$(DatabaseName)_log'
    , FILENAME = N'$(LogFilesDir)\$(DatabaseName)_log.ldf'
    , SIZE = 64 , MAXSIZE = 2048GB , FILEGROWTH = 64MB
    );
GO
ALTER DATABASE [$(DatabaseName)] SET RECOVERY SIMPLE;
ALTER DATABASE [$(DatabaseName)] SET TRUSTWORTHY OFF;
ALTER DATABASE [$(DatabaseName)] SET PAGE_VERIFY CHECKSUM;
ALTER DATABASE [$(DatabaseName)] SET TARGET_RECOVERY_TIME = 60 SECONDS;
ALTER DATABASE [$(DatabaseName)] SET DELAYED_DURABILITY = DISABLED;
ALTER DATABASE [$(DatabaseName)] SET ACCELERATED_DATABASE_RECOVERY = ON
ALTER DATABASE [$(DatabaseName)] SET OPTIMIZED_LOCKING = ON
--ALTER DATABASE [$(DatabaseName)] SET QUERY_STORE = ON
--ALTER DATABASE [$(DatabaseName)] SET QUERY_STORE (OPERATION_MODE = READ_WRITE, CLEANUP_POLICY = (STALE_QUERY_THRESHOLD_DAYS = 30), DATA_FLUSH_INTERVAL_SECONDS = 900, INTERVAL_LENGTH_MINUTES = 60, MAX_STORAGE_SIZE_MB = 1000, QUERY_CAPTURE_MODE = AUTO, SIZE_BASED_CLEANUP_MODE = AUTO, MAX_PLANS_PER_QUERY = 200, WAIT_STATS_CAPTURE_MODE = ON)
GO
USE [$(DatabaseName)];
EXEC sp_changedbowner 'sa';
GO
