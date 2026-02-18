<#
.SYNOPSIS
    import-perfmon: imports PerfMon .blg files into SQL via relog.exe and an ODBC DSN.

.DESCRIPTION
    Enumerates *.blg files in a directory and runs relog.exe for each file, outputting to:
    "SQL:<DSN>!<BaseName>"

.PARAMETER Path
    Directory that contains the .blg files.
    Default: "."

.PARAMETER Dsn
    ODBC DSN name used by relog.exe.
    Default: "PerfmonVault"

.EXAMPLE
    .\import-perfmon.ps1
    # Uses current directory and DSN "PerfmonVault"

.EXAMPLE
    .\import-perfmon.ps1 -Path "D:\PerfLogs" -Dsn "MyDsn"
#>

[CmdletBinding()]
param(
    [Parameter()]
    [ValidateNotNullOrEmpty()]
    [string]$Path = '.',

    [Parameter()]
    [ValidateNotNullOrEmpty()]
    [string]$Dsn  = 'PerfmonVault'
)
# original one-liner dir *.blg | % { relog.exe "$_" -f SQL -o "SQL:PerfmonVault!$($_.BaseName)" }

$ResolvedPath = (Resolve-Path -LiteralPath $Path -ErrorAction Stop).Path

Get-ChildItem -LiteralPath $ResolvedPath -Filter '*.blg' -File |
    ForEach-Object {
        relog.exe $_.FullName -f SQL -o "SQL:$Dsn!$($_.BaseName)"
    }
pause