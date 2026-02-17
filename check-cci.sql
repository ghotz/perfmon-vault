select N'dbo.CounterData_Final' as db, min(total_rows) as total_rows_min, avg(total_rows) as total_rows_avg, max(total_rows) as total_rows_max, sum(total_rows) AS total_rows_sum, sum(case when total_rows = 1048576 then 1 else 0 end) as num_segments_with_max_rows
from sys.dm_db_column_store_row_group_physical_stats
where object_id = OBJECT_ID('dbo.CounterData_Final')
union all
select N'dbo.CounterData_Final2' as db, min(total_rows) as total_rows_min, avg(total_rows) as total_rows_avg, max(total_rows) as total_rows_max, sum(total_rows) AS total_rows_sum, sum(case when total_rows = 1048576 then 1 else 0 end) as num_segments_with_max_rows
from sys.dm_db_column_store_row_group_physical_stats
where object_id = OBJECT_ID('dbo.CounterData_Final2')
union all
select N'dbo.CounterData_Full' as db, min(total_rows) as total_rows_min, avg(total_rows) as total_rows_avg, max(total_rows) as total_rows_max, sum(total_rows) AS total_rows_sum, sum(case when total_rows = 1048576 then 1 else 0 end) as num_segments_with_max_rows
from sys.dm_db_column_store_row_group_physical_stats
where object_id = OBJECT_ID('dbo.CounterData_Full')

SELECT 
    N'dbo.CounterData_Final' as db, c.name AS column_name,
    COUNT(*) AS num_segments,
    SUM(CASE WHEN d.on_disk_size > 10 * 1024 * 1024 THEN 1 ELSE 0 END) AS big_dicts,
    MAX(d.on_disk_size) / 1024 / 1024 AS max_dict_mb,
    MAX(d.entry_count) AS max_entries
FROM sys.column_store_dictionaries d
JOIN sys.partitions p ON d.hobt_id = p.hobt_id
JOIN sys.columns c ON d.column_id = c.column_id 
    AND c.object_id = p.object_id
WHERE p.object_id = OBJECT_ID('CounterData_Final')
GROUP BY c.name
union all
SELECT 
    N'dbo.CounterData_Final2' as db, c.name AS column_name,
    COUNT(*) AS num_segments,
    SUM(CASE WHEN d.on_disk_size > 10 * 1024 * 1024 THEN 1 ELSE 0 END) AS big_dicts,
    MAX(d.on_disk_size) / 1024 / 1024 AS max_dict_mb,
    MAX(d.entry_count) AS max_entries
FROM sys.column_store_dictionaries d
JOIN sys.partitions p ON d.hobt_id = p.hobt_id
JOIN sys.columns c ON d.column_id = c.column_id 
    AND c.object_id = p.object_id
WHERE p.object_id = OBJECT_ID('CounterData_Final2')
GROUP BY c.name
union all
SELECT 
    N'dbo.CounterData_Full' as db, c.name AS column_name,
    COUNT(*) AS num_segments,
    SUM(CASE WHEN d.on_disk_size > 10 * 1024 * 1024 THEN 1 ELSE 0 END) AS big_dicts,
    MAX(d.on_disk_size) / 1024 / 1024 AS max_dict_mb,
    MAX(d.entry_count) AS max_entries
FROM sys.column_store_dictionaries d
JOIN sys.partitions p ON d.hobt_id = p.hobt_id
JOIN sys.columns c ON d.column_id = c.column_id 
    AND c.object_id = p.object_id
WHERE p.object_id = OBJECT_ID('CounterData_Full')
GROUP BY c.name
ORDER BY column_name, db DESC;