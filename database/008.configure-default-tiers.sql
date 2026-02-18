:setvar DatabaseName "PerfmonVault"
USE [$(DatabaseName)];
GO

-------------------------------------------------------------------------------
-- Tier 0: counters to exclude
-------------------------------------------------------------------------------
INSERT INTO [vault].[CounterTier] ([Tier], [ObjectName], [CounterName], [InstanceName])
VALUES
    (0, '%:Deprecated Features', '%', '%')			  -- Always forget to remove from PAL template
,   (0, '%:Buffer Manager',     'Extension%', '%')    -- Bugged extended pool counters, same as above but corrupts other memory counters
;

-------------------------------------------------------------------------------
-- Tier 1: ~50-55 counters for resource trend analysis
-------------------------------------------------------------------------------

-- CPU
INSERT INTO [vault].[CounterTier] ([Tier], [ObjectName], [CounterName], [InstanceName])
VALUES
	(1, 'Processor',							'% Processor Time',			'_Total')
,	(1, 'Processor',							'% Privileged Time',		'_Total')
,	(1, '%:SQL Statistics',						'SQL Compilations/sec',		NULL)
,	(1, '%:SQL Statistics',						'SQL Re-Compilations/sec',	NULL)
;

-- RAM / Buffer Pool
INSERT INTO [vault].[CounterTier] ([Tier], [ObjectName], [CounterName], [InstanceName])
VALUES
	(1, '%:Buffer Manager',						'Page life expectancy',		NULL)
,	(1, '%:Buffer Manager',						'Buffer cache hit ratio',	NULL)
,	(1, '%:Buffer Manager',						'Free list stalls/sec',		NULL)
,	(1, '%:Buffer Manager',						'Lazy writes/sec',			NULL)
,	(1, '%:Buffer Manager',						'Checkpoint pages/sec',		NULL)
,	(1, '%:Buffer Manager',						'Page reads/sec',			NULL)
,	(1, '%:Buffer Manager',						'Page writes/sec',			NULL)
,	(1, '%:Memory Manager',						'Total Server Memory (KB)',	NULL)
,	(1, '%:Memory Manager',						'Target Server Memory (KB)',NULL)
,	(1, 'Memory',								'Available MBytes',			NULL)
;

-- Workload
INSERT INTO [vault].[CounterTier] ([Tier], [ObjectName], [CounterName], [InstanceName])
VALUES
	(1, '%:SQL Statistics',						'Batch Requests/sec',		NULL)
,	(1, '%:General Statistics',					'User Connections',			NULL)
,	(1, '%:General Statistics',					'Processes blocked',		NULL)
;

-- I/O per volume (% on InstanceName to catch all volumes)
INSERT INTO [vault].[CounterTier] ([Tier], [ObjectName], [CounterName], [InstanceName])
VALUES
	(1, 'PhysicalDisk',						'Avg. Disk sec/Read',		'%')
,	(1, 'PhysicalDisk',						'Avg. Disk sec/Write',		'%')
,	(1, 'PhysicalDisk',						'Disk Reads/sec',			'%')
,	(1, 'PhysicalDisk',						'Disk Writes/sec',			'%')
,	(1, 'PhysicalDisk',						'Avg. Disk Bytes/Read',		'%')
,	(1, 'PhysicalDisk',						'Avg. Disk Bytes/Write',	'%')
;

-- Wait Stats
INSERT INTO [vault].[CounterTier] ([Tier], [ObjectName], [CounterName], [InstanceName])
VALUES
	(1, '%:Wait Statistics',					'Lock waits',					'Average wait time (ms)')
,	(1, '%:Wait Statistics',					'Page IO latch waits',			'Average wait time (ms)')
,	(1, '%:Wait Statistics',					'Page latch waits',				'Average wait time (ms)')
,	(1, '%:Wait Statistics',					'Memory grant queue waits',		'Average wait time (ms)')
,	(1, '%:Wait Statistics',					'Network IO waits',				'Average wait time (ms)')
;

-- Network
INSERT INTO [vault].[CounterTier] ([Tier], [ObjectName], [CounterName], [InstanceName])
VALUES
	(1, 'Network Interface',					'Bytes Total/sec',			'%')
,	(1, 'Network Interface',					'Output Queue Length',		'%')
;

-------------------------------------------------------------------------------
-- Tier 2: all remaining SQL Server counters
-------------------------------------------------------------------------------
INSERT INTO [vault].[CounterTier] ([Tier], [ObjectName], [CounterName], [InstanceName])
VALUES
	(2, '%:Access Methods',						'%',						'%')
,	(2, '%:Buffer Manager',						'%',						'%')
,	(2, '%:Catalog Metadata',					'%',						'%')
,	(2, '%:CLR',								'%',						'%')
,	(2, '%:Cursor Manager%',					'%',						'%')
,	(2, '%:Database Mirroring',					'%',						'%')
,	(2, '%:Database Replica',					'%',						'%')
,	(2, '%:Databases',							'%',						'%')
,	(2, '%:Deprecated Features',				'%',						'%')
,	(2, '%:Exec Statistics',					'%',						'%')
,	(2, '%:General Statistics',					'%',						'%')
,	(2, '%:Latches',							'%',						'%')
,	(2, '%:Locks',								'%',						'%')
,	(2, '%:Memory Manager',						'%',						'%')
,	(2, '%:Plan Cache',							'%',						'%')
,	(2, '%:SQL Errors',							'%',						'%')
,	(2, '%:SQL Statistics',						'%',						'%')
,	(2, '%:Transactions',						'%',						'%')
,	(2, '%:Wait Statistics',					'%',						'%')
,	(2, '%:Workload Group Stats',				'%',						'%')
,	(2, '%:Batch Resp Statistics',				'%',						'%')
,	(2, '%:Availability Replica',				'%',						'%')
,	(2, '%:Resource Pool Stats',				'%',						'%')
,	(2, '%:Columnstore',						'%',						'%')
,	(2, '%:Advanced Analytics',					'%',						'%')
,	(2, '%:Query Store',						'%',						'%')
;

-- Tier 2: remaining OS counters
INSERT INTO [vault].[CounterTier] ([Tier], [ObjectName], [CounterName], [InstanceName])
VALUES
	(2, 'PhysicalDisk',						'%',						'%')
,	(2, 'LogicalDisk',							'%',						'%')
,	(2, 'Processor',							'%',						'%')
,	(2, 'Memory',								'%',						'%')
,	(2, 'Network Interface',					'%',						'%')
,	(2, 'Paging File',							'%',						'%')
,	(2, 'System',								'%',						'%')
;

-- Everything else is Tier 3 by default (no rules needed)
GO


-------------------------------------------------------------------------------
-- Diagnostic: check tier distribution
-------------------------------------------------------------------------------
SELECT Tier, COUNT(*) AS NumCounters
FROM vault.vCounterTierResolved
GROUP BY Tier
ORDER BY Tier;

-- Drill-down per tier
SELECT t.Tier, cd.ObjectName, cd.CounterName, cd.InstanceName
FROM vault.vCounterTierResolved t
JOIN vault.CounterDetails cd ON cd.CounterID = t.CounterID
ORDER BY t.Tier, cd.ObjectName, cd.CounterName;
GO
