IF NOT EXISTS (SELECT * FROM sys.databases WHERE [name] = N'PerfmonVault_$(Customer)')
    CREATE DATABASE [PerfmonVault_$(Customer)];
