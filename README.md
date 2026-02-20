# PerfMon Vault

A SQL Server 2025 data warehouse for long-term storage and analysis of Windows Performance Monitor (PerfMon) counter data, optimized for **3.5+ billion rows** using tiered columnstore indexes with yearly partitioning.

The system ingests `.BLG` files via `relog`, stages data with type conversions, and routes counters into three storage tiers based on pattern-matched classification rules. The architecture was designed through extensive empirical testing of columnstore compression behavior, dictionary pressure patterns, and rowgroup fill quality.

> ⚠️ **AI-assisted project:** this repository was built with the help of AI as a coding assistant. Treat it as a starting point, review the scripts, and test in a non-production environment before you run it anywhere important.

> **Coming soon:** Reporting layer and dashboards.

---

## Architecture Overview

```
 .BLG / .ZIP / .RAR files
        │
        ▼
 ┌──────────────┐    relog.exe (ODBC DSN)   ┌───────────────────────────┐
 │  PowerShell  │────────────────────────►  │ dbo.* synonyms            │
 │  import      │                           │ (relog expects dbo tables)│
 │  script      │                           └──────────────┬────────────┘
 └──────────────┘                                          │
                                                           ▼
                                                ┌─────────────────────┐
                                                │  [staging] schema   │
                                                │  CounterData        │
                                                │  CounterDetails     │
                                                │  DisplayToID        │
                                                └────────┬────────────┘
                                                         │
                                              usp_LoadFromStaging
                                           (MERGE + tier routing)
                                                         │
                                    ┌────────────────────┼────────────────────┐
                                    ▼                    ▼                    ▼
                            ┌──────────┐          ┌──────────┐          ┌──────────┐
                            │  Tier 1  │          │  Tier 2  │          │  Tier 3  │
                            │   HOT    │          │  WARM    │          │   COLD   │
                            │ standard │          │ standard │          │ ARCHIVE  │
                            │ CCI order│          │ CCI order│          │ CCI order│
                            │(DT, CID) │          │(DT, CID) │          │(CID, DT) │
                            └──────────┘          └──────────┘          └──────────┘
                                    │                    │                    │
                                    └────────────────────┼────────────────────┘
                                                         ▼
                                                ┌──────────────────┐
                                                │ vault.CounterData│  (unified view)
                                                │    UNION ALL     │
                                                └──────────────────┘
```

### Key Components

| Component | Description |
|---|---|
| **PowerShell import script** | Handles `.BLG`, `.ZIP`, `.RAR` files. Extracts archives, loads via `relog` with progress tracking, moves processed files to archive directory. |
| **Staging schema** | Receives raw `relog` output (`CounterData`, `CounterDetails`, `DisplayToID`) with `char(24)` datetime and `float` counter values. `dbo.*` synonyms are used so `relog` can write to its expected table names while the data lands in `[staging]`. |
| **Vault schema** | Optimized storage. `CounterDetails` + `DisplayToID` dimensions, `CounterTier` classification table, and three tiered fact tables with yearly partitioning. `usp_LoadFromStaging` deduplicates and merges counters from staging. |
| **CounterTier** | Pattern-based tier classification using `LIKE` wildcards on `ObjectName`, `CounterName`, `InstanceName`. New volumes, databases, or instances are auto-classified without manual intervention. |
| **vCounterTierResolved** | View that resolves each `CounterID` to its tier. `MIN(Tier)` wins when multiple rules match. Unmatched counters default to Tier 3. Tier 0 = excluded from loading. |
| **usp_LoadFromStaging** | ETL procedure from `[staging]` into the three tiers: counter definition merge + tier-routed INSERT (with `MAXDOP 1` and `ORDER BY` matching the CCI order). Supports optional staging truncation (`@TruncateStaging`), validation bypass (`@SkipValidation`), and a counters-only run (`@LoadOnlyCounters`). |
| **usp_RebalanceTiers** | Moves data between tier tables after classification rule changes. Supports dry-run mode. Handles Tier 0 purge. (NOT TESTED YET!) |
| **usp_AddYearPartition** | Extends the partition function with a new yearly boundary. Run annually before new data arrives. |

### Tier Strategy

| Tier | Purpose | Counters | CCI Order | Compression | Rationale |
|---|---|---|---|---|---|
| **0** | Excluded | Deprecated Features, buggy counters | — | — | Pattern-matched exclusion, never loaded |
| **1** | Hot / dashboards | ~55 (CPU, RAM, PLE, I/O, waits, network) | `(CounterDateTime, CounterID)` | COLUMNSTORE | Optimized for time-range scans on dashboard queries |
| **2** | Warm / SQL+OS | ~600 (all SQL Server objects, PhysicalDisk, Processor, Memory, Network) | `(CounterDateTime, CounterID)` | COLUMNSTORE | Broad SQL Server monitoring |
| **3** | Cold / archive | ~2000+ (process-level, everything else) | `(CounterID, CounterDateTime)` | COLUMNSTORE_ARCHIVE | Inverted key order eliminates dictionary pressure; rarely queried |

The inverted CCI key order on Tier 3 is critical: with `(CounterID, CounterDateTime)`, each rowgroup contains data from a single counter, producing homogeneous dictionaries. See [Findings](#findings) below.

### Single server vs multi-server consolidation

PerfMon Vault was initially tuned around collecting counters from a **single monitored server**, but it can also consolidate **multiple servers** in the same vault: `vault.CounterDetails` includes `MachineName`, and `CounterID` is unique per `(MachineName, ObjectName, CounterName, InstanceName, ...)`.

When you add more machines, you effectively multiply the number of distinct `CounterID`s per time slice. With columnstore tables ordered by `(CounterDateTime, CounterID)`, that increases counter diversity inside each rowgroup and can push dictionary pressure back up.

Practical guidance (based on observed thresholds on the reference dataset):

- **Tier 3:** already uses `(CounterID, CounterDateTime)` and is the “scale-out friendly” layout.
- **Tier 2:** start watching dictionary health as you approach **3–4 machines**. If `database/999.check-cci.sql` starts showing large dictionaries (for example `CounterValue` dictionaries consistently above ~5 MB) and rising `DICTIONARY_SIZE` trims, evaluate switching Tier 2 to `(CounterID, CounterDateTime)` like Tier 3. The tradeoff is weaker time-based segment elimination, which is usually acceptable for investigative (non-dashboard) queries.
- **Tier 1:** **do not** invert the key order. Tier 1 exists to make “last N hours/days” dashboard queries fast via time-based segment elimination. If Tier 1 starts suffering at higher scale (order of magnitude: **20–30 machines**), prefer splitting Tier 1 by machine (separate hot tables or per-machine vaults) or adding a `MachineID` column and using `(MachineID, CounterDateTime, CounterID)` instead of flipping the time-first order.


### Partitioning

All three tier tables are partitioned yearly on `CounterDateTime` using `RANGE RIGHT`:

- Partition function and scheme are generated dynamically via `:setvar StartYear` and `:setvar NumYears`
- All partitions map to a single filegroup (`VAULT`) for simplicity
- Enables per-partition `REBUILD` with `COLUMNSTORE_ARCHIVE` for aging warm data
- Enables per-partition maintenance without touching recent data

---

## Files

### Database build scripts (`/database`)

> Tip: these scripts use `:setvar` and are meant to be executed in **SQLCMD mode** (SSMS) or via `sqlcmd`.

| File | Description |
|---|---|
| `database/000.build-default.sql` | Convenience SQLCMD “build all” script that `:r`-includes the numbered scripts in order (note: currently uses local absolute paths). |
| `database/000.scratchpad.sql` | Scratchpad / one-off migration experiments (not part of the standard build). |
| `database/001.create-database.sql` | Creates the `PerfmonVault` database, file(s), and the default `VAULT` filegroup (SQLCMD variables for paths). |
| `database/002.create-schemas.sql` | Creates the `[staging]` and `[vault]` schemas. |
| `database/003.create-staging-tables.sql` | Creates staging tables for `relog` landing (`staging.CounterData`, `staging.CounterDetails`, `staging.DisplayToID`) with PAGE compression. |
| `database/004.create-synonyms.sql` | Creates `dbo.CounterData`, `dbo.CounterDetails`, `dbo.DisplayToID` synonyms pointing to `[staging]` tables (so `relog` can write using dbo names). |
| `database/005.create-partitioning.sql` | Creates yearly partition function/scheme (`pf_/ps_CounterData_Yearly`) and `vault.usp_AddYearPartition`. |
| `database/006.create-vault-tables.sql` | Creates vault dimensions (`CounterDetails`, `DisplayToID`), tier rules table (`CounterTier`), and tiered fact tables (`CounterData_Tier1/2/3`) with CCIs and yearly partitioning. |
| `database/007.create-vault-tiers-views.sql` | Creates `vault.CounterData` unified view and `vault.vCounterTierResolved` tier resolution view. |
| `database/008.configure-default-tiers.sql` | Seeds default Tier 0/1/2 rules into `vault.CounterTier` (pattern-based). |
| `database/009.create-vault-procedures.sql` | Creates `vault.usp_LoadFromStaging` (ETL from staging into tiers). |
| `database/010.create-rebalance-procedure.sql` | Creates `vault.usp_RebalanceTiers` (moves data between tiers after rule changes). |

### Diagnostics (`/database`)

| File | Description |
|---|---|
| `database/999.check-cci.sql` | Columnstore health dashboard: rowgroup fill percentiles, trim reasons, per-partition stats, and dictionary pressure indicators. |
| `database/999.partition-info.sql` | Partition boundaries, compression settings, and actual data ranges per partition (per tier). |
| `database/999.check-dupes.sql` | Duplicate detection within and across tier tables. |

### PowerShell (`/powershell`)

| File | Description |
|---|---|
| `powershell/import-perfmon.ps1` | Imports `.blg` (and `.zip`/`.rar` containing `.blg`) using `relog.exe` + an ODBC DSN; archives loaded files under a `Loaded` folder. |


---

## Findings

The architecture decisions were driven by empirical testing on a dataset of **3.6 billion rows** (~5 years of PerfMon data, ~3,600 counters sampled every minute).

### Data Type Selection

| Column | relog default | Vault type | Reason |
|---|---|---|---|
| `GUID` | `uniqueidentifier` (16B, random) | Kept as-is | With temporal CCI ordering, only 1 GUID value per rowgroup → perfect RLE compression, negligible cost |
| `CounterDateTime` | `char(24)` with null terminator | `datetime` | VertiPaq decomposes internally; no need to split date/time. `LEFT(field, 23)` strips the 0x00 terminator from relog |
| `CounterValue` | `float` (IEEE 754) | `decimal(28,0)` on T1/T2, `decimal(28,0)` on T3 | Better dictionary compression for integer-heavy data; float creates more distinct values due to IEEE representation |

**Key insight:** Splitting `datetime` into separate `date` + `time` columns was tested but unnecessary — SQL Server's columnstore handles `datetime` efficiently via internal decomposition.

### Dictionary Pressure — The Core Problem

With all ~3,600 counters in a single table, `CounterValue` dictionaries grew to **10-11 MB** (approaching the 16 MB hard limit), causing rowgroup truncation at 813K rows instead of the optimal 1,048,576.

| Configuration | Avg rows/RG | Full RGs | CounterValue max dict |
|---|---|---|---|
| Single table, `float`, unordered CCI | 945K | 61% | 4 MB, 1011 segments |
| Single table, `decimal(28,0)`, ordered CCI | 1,007K | 70% | 10 MB, 3 segments |
| **Tiered (final)** | **990K–1,030K** | **88–98%** | **0-2 MB, 0 big dicts** |

The paradox: `decimal` initially *worsened* dictionary pressure vs `float` because the columnstore engine applies dictionary encoding more aggressively to integers. The solution was tiering — reducing counter diversity per table.

### Tiering Impact

Splitting counters into three tables by query frequency dramatically reduced dictionary cardinality per rowgroup:

| Metric | Single table (3,600 counters) | Tier 1 (55 ctr) | Tier 2 (600 ctr) | Tier 3 (2,000+ ctr) |
|---|---|---|---|---|
| CounterValue dict size | 10 MB | **0.7 MB** | **0.3 MB** | **2 MB** |
| FirstValueA dict size | 2 MB | — | 1 MB | 2 MB |
| Big dicts (>10 MB) | Yes | **0** | **0** | **0** |

### CCI Key Order — Tier 3 Breakthrough

For Tier 3, inverting the CCI order from `(CounterDateTime, CounterID)` to `(CounterID, CounterDateTime)` was the single biggest improvement:

| Tier 3 Config | Efficiency | Space | Rationale |
|---|---|---|---|
| `(DateTime, CID)` + `decimal` + ARCHIVE | 94.2% | 528 MB | Counters mixed per RG → large dicts |
| `(DateTime, CID)` + `float` + ARCHIVE | 91.0% | 522 MB | Float worsened dict when mixed |
| `(CID, DateTime)` + `float` + ARCHIVE | 98.0% | 1,447 MB | Homogeneous RGs but float hurts ARCHIVE |
| **`(CID, DateTime)` + `decimal` + ARCHIVE** | **98.4%** | **1,145 MB** | **Best quality + best compression** |

With `(CounterID, CounterDateTime)`, each rowgroup contains ~728 days of a single counter. Dictionaries are small and homogeneous. The tradeoff: segment elimination on time range is less effective, but Tier 3 is "cold" data — rarely queried.

This order is *not* used on Tier 1 and Tier 2, where time-range queries for dashboards need `CounterDateTime` first for segment elimination.

### MAXDOP 1 — Still Necessary in SQL Server 2025

Research confirmed that SQL Server 2025 did **not** change parallel INSERT behavior for columnstore:

- Each thread builds rowgroups independently with local dictionaries
- No global sort across threads — round-robin distribution can break data locality
- SQL Server 2025 improved sort quality for `CREATE INDEX` / `REBUILD` (using tempdb), but **not** for `INSERT...SELECT`

`OPTION (MAXDOP 1)` remains the correct choice for optimal dictionary consolidation during data loading.

### Final Production Results

**3.6 billion rows across three tiers:**

| Tier | Rows | Rowgroups | Efficiency | Space | Avg RG size | Big Dicts |
|---|---|---|---|---|---|---|
| Tier 1 (hot) | 73M | 79 | 88.4% | 774 MB | 9.8 MB | **0** |
| Tier 2 (warm) | 1.19B | 1,170 | 97.3% | 4.3 GB | 3.7 MB | **0** |
| Tier 3 (cold) | 2.33B | 2,258 | 98.3% | 9.4 GB | 4.1 MB | **0** |
| **Total** | **3.59B** | **3,507** | **~97%** | **14.5 GB** | — | **0** |

**14.5 GB** for 3.6 billion rows = **~4 bytes/row** average (10 columns per row).

Tier 1 at 88.4% is a structural limitation: ~55 counters produce only ~500K rows per weekly batch, resulting in partial rowgroups. A `REBUILD` does not improve this — the data distribution is inherently sparse. This is acceptable for a 774 MB hot table.

### Trim Reason Distribution

| Tier | NO_TRIM (full) | DICTIONARY_SIZE | BULKLOAD | REORG | RESIDUAL |
|---|---|---|---|---|---|
| Tier 1 | 50 | 1 | 11 | 16 | 1 |
| Tier 2 | 1,058 | 82 | 17 | 11 | 2 |
| Tier 3 | 1,972 | 253 | 18 | 15 | 0 |

DICTIONARY_SIZE trims on Tier 3 (253 rowgroups) are within acceptable bounds — max dictionary size is 2 MB, far from the 16 MB limit. These occur on high-cardinality columns like `FirstValueA` and `SecondValueA`, not on `CounterValue`.

---

## Usage

### Initial Setup

You can either run the scripts manually in order (SQLCMD mode), or use `database/000.build-default.sql` as a wrapper.

```sql
-- Execute in order (SQLCMD mode):
--   database/001 → database/010
--
-- Then seed default tiers:
--   database/008.configure-default-tiers.sql
--
-- Finally, create procedures:
--   database/009.create-vault-procedures.sql
--   database/010.create-rebalance-procedure.sql
```


### Loading Data

```powershell
# Import BLG files (supports .blg, .zip, .rar)
.\import-perfmon.ps1 -SourcePath "C:\PerfmonData" -ArchivePath "C:\PerfmonArchive"
```

```sql
-- Fine-tune tier classification (counters only, no data)
EXEC vault.usp_LoadFromStaging @LoadOnlyCounters = 1;

-- Verify tier assignments
SELECT Tier, COUNT(*) AS NumCounters
FROM vault.vCounterTierResolved
GROUP BY Tier ORDER BY Tier;

-- Full load
EXEC vault.usp_LoadFromStaging @TruncateStaging = 1;
```

**`vault.usp_LoadFromStaging` parameters**

- `@TruncateStaging` (bit, default 0): if 1, truncates staging tables at the end of the run (keeps `[staging]` small).
- `@SkipValidation` (bit, default 0): if 1, skips validation checks (use only if you know staging is clean).
- `@LoadOnlyCounters` (bit, default 0): if 1, loads/merges only counter metadata and tier classification, without moving fact rows.

### Rebalancing After Rule Changes

```sql
-- Preview what would move
EXEC vault.usp_RebalanceTiers @DryRun = 1;

-- Execute
EXEC vault.usp_RebalanceTiers @DryRun = 0;

-- Consolidate rowgroups after rebalance
ALTER INDEX CCI_CounterData_Tier2 ON vault.CounterData_Tier2
REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS = ON);
```

### Annual Maintenance

```sql
-- Add partition for next year
EXEC vault.usp_AddYearPartition @Year = 2033;

-- Archive old partitions on Tier 2
ALTER INDEX CCI_CounterData_Tier2 ON vault.CounterData_Tier2
REBUILD PARTITION = 2  -- year 2022
WITH (DATA_COMPRESSION = COLUMNSTORE_ARCHIVE, ONLINE = ON, MAXDOP = 1);
```

---

## Requirements

- SQL Server 2025 (Enterprise Edition recommended for online REBUILD and COLUMNSTORE_ARCHIVE)
- PowerShell 5.1+
- `relog.exe` (included with Windows)
- Optional: 7-Zip or WinRAR for archive extraction