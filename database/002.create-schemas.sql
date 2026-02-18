:setvar DatabaseName "PerfmonVault"
USE [$(DatabaseName)];
GO
DROP SCHEMA IF EXISTS  [staging]
GO
CREATE SCHEMA [staging];
GO
DROP SCHEMA IF EXISTS  [vault]
GO
CREATE SCHEMA [vault];
GO