<#
.SYNOPSIS
    import-perfmon: imports PerfMon .blg files into SQL via relog.exe and an ODBC DSN.

.DESCRIPTION
    Processes files in the input directory:
    - .BLG files: loaded directly via relog, then moved to archive
    - .ZIP files: extracted, BLG loaded, BLG deleted, ZIP moved to archive
    - .RAR files: extracted (requires unrar/7z), BLG loaded, BLG deleted, RAR moved to archive

    Archive directory is created automatically under the input path as "Loaded".

.PARAMETER Path
    Directory that contains the .blg/.zip/.rar files.
    Default: "."

.PARAMETER Dsn
    ODBC DSN name used by relog.exe.
    Default: "PerfmonVault"

.PARAMETER ArchiveDir
    Name of the subdirectory for already-loaded files.
    Default: "Loaded"

.EXAMPLE
    .\import-perfmon.ps1
    # Uses current directory, DSN "PerfmonVault", archive in .\Loaded

.EXAMPLE
    .\import-perfmon.ps1 -Path "D:\PerfLogs" -Dsn "MyDsn" -ArchiveDir "Done"
#>

[CmdletBinding()]
param(
    [Parameter()]
    [ValidateNotNullOrEmpty()]
    [string]$Path = '.',

    [Parameter()]
    [ValidateNotNullOrEmpty()]
    [string]$Dsn  = 'PerfmonVault',

    [Parameter()]
    [ValidateNotNullOrEmpty()]
    [string]$ArchiveDir = 'Loaded'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ---------------------------------------------------------------------------
# Resolve paths
# ---------------------------------------------------------------------------
$ResolvedPath = (Resolve-Path -LiteralPath $Path).Path
$ArchivePath  = Join-Path -Path $ResolvedPath -ChildPath $ArchiveDir

if (-not (Test-Path -LiteralPath $ArchivePath)) {
    New-Item -Path $ArchivePath -ItemType Directory | Out-Null
    Write-Host "Created archive directory: $ArchivePath" -ForegroundColor Cyan
}

# ---------------------------------------------------------------------------
# Helper: load a BLG file via relog
# Returns $true on success, $false on failure
# ---------------------------------------------------------------------------
function Invoke-Relog {
    param([string]$BlgPath)

    $baseName = [System.IO.Path]::GetFileNameWithoutExtension($BlgPath)
    Write-Host "  Loading: $baseName ..." -ForegroundColor Yellow

    $p = Start-Process -FilePath 'relog.exe' `
        -ArgumentList "`"$BlgPath`"", '-f', 'SQL', '-o', "`"SQL:${Dsn}!${baseName}`"" `
        -NoNewWindow -Wait -PassThru

    if ($p.ExitCode -eq 0) {
        Write-Host "  OK" -ForegroundColor Green
        return $true
    }
    else {
        Write-Host "  FAILED (exit code $($p.ExitCode))" -ForegroundColor Red
        return $false
    }
}

# ---------------------------------------------------------------------------
# Helper: extract a single BLG from a ZIP file
# Returns the extracted BLG path, or $null on failure
# ---------------------------------------------------------------------------
function Expand-ZipBlg {
    param([string]$ZipPath)

    $expectedBlg = [System.IO.Path]::ChangeExtension($ZipPath, '.blg')
    $expectedName = [System.IO.Path]::GetFileName($expectedBlg)
    $destDir = [System.IO.Path]::GetDirectoryName($ZipPath)

    try {
        Expand-Archive -LiteralPath $ZipPath -DestinationPath $destDir -Force
    }
    catch {
        Write-Host "  Failed to extract ZIP: $_" -ForegroundColor Red
        return $null
    }

    # Check if expected BLG exists; if not, search for any BLG extracted
    if (Test-Path -LiteralPath $expectedBlg) {
        return $expectedBlg
    }

    $found = Get-ChildItem -LiteralPath $destDir -Filter '*.blg' -File |
        Where-Object { $_.LastWriteTime -ge (Get-Item -LiteralPath $ZipPath).LastWriteTime.AddMinutes(-1) } |
        Select-Object -First 1

    if ($found) {
        return $found.FullName
    }

    Write-Host "  No BLG found after extracting: $ZipPath" -ForegroundColor Red
    return $null
}

# ---------------------------------------------------------------------------
# Helper: extract a single BLG from a RAR file
# Tries 7z first, then unrar
# Returns the extracted BLG path, or $null on failure
# ---------------------------------------------------------------------------
function Expand-RarBlg {
    param([string]$RarPath)

    $expectedBlg = [System.IO.Path]::ChangeExtension($RarPath, '.blg')
    $destDir = [System.IO.Path]::GetDirectoryName($RarPath)

    # Try 7z
    $sevenZip = Get-Command '7z' -ErrorAction SilentlyContinue
    if (-not $sevenZip) {
        $sevenZip = Get-Command '7z.exe' -ErrorAction SilentlyContinue
    }
    # Common install paths
    if (-not $sevenZip) {
        @(
            "${env:ProgramFiles}\7-Zip\7z.exe",
            "${env:ProgramFiles(x86)}\7-Zip\7z.exe"
        ) | ForEach-Object {
            if (Test-Path $_) { $sevenZip = $_ }
        }
    }

    # Try unrar
    $unrar = Get-Command 'unrar' -ErrorAction SilentlyContinue
    if (-not $unrar) {
        $unrar = Get-Command 'unrar.exe' -ErrorAction SilentlyContinue
    }

    if ($sevenZip) {
        $exe = if ($sevenZip -is [System.Management.Automation.ApplicationInfo]) { $sevenZip.Source } else { $sevenZip }
        & $exe x -y "-o${destDir}" $RarPath '*.blg' 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Write-Host "  7z extraction failed (exit code $LASTEXITCODE)" -ForegroundColor Red
            return $null
        }
    }
    elseif ($unrar) {
        $exe = if ($unrar -is [System.Management.Automation.ApplicationInfo]) { $unrar.Source } else { $unrar }
        & $exe e -y -o+ $RarPath '*.blg' $destDir 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Write-Host "  unrar extraction failed (exit code $LASTEXITCODE)" -ForegroundColor Red
            return $null
        }
    }
    else {
        Write-Host "  Cannot extract RAR: neither 7z nor unrar found in PATH" -ForegroundColor Red
        return $null
    }

    if (Test-Path -LiteralPath $expectedBlg) {
        return $expectedBlg
    }

    $found = Get-ChildItem -LiteralPath $destDir -Filter '*.blg' -File |
        Where-Object { $_.LastWriteTime -ge (Get-Item -LiteralPath $RarPath).LastWriteTime.AddMinutes(-1) } |
        Select-Object -First 1

    if ($found) {
        return $found.FullName
    }

    Write-Host "  No BLG found after extracting: $RarPath" -ForegroundColor Red
    return $null
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
$stats = @{ OK = 0; Failed = 0; Skipped = 0 }

# --- Process .BLG files directly ---
Get-ChildItem -LiteralPath $ResolvedPath -Filter '*.blg' -File | ForEach-Object {
    Write-Host "BLG: $($_.Name)" -ForegroundColor Cyan

    if (Invoke-Relog -BlgPath $_.FullName) {
        Move-Item -LiteralPath $_.FullName -Destination $ArchivePath -Force
        $stats.OK++
    }
    else {
        $stats.Failed++
    }
}

# --- Process .ZIP files ---
Get-ChildItem -LiteralPath $ResolvedPath -Filter '*.zip' -File | ForEach-Object {
    Write-Host "ZIP: $($_.Name)" -ForegroundColor Cyan

    $blgPath = Expand-ZipBlg -ZipPath $_.FullName
    if (-not $blgPath) {
        $stats.Failed++
        return  # next file in ForEach-Object
    }

    if (Invoke-Relog -BlgPath $blgPath) {
        Remove-Item -LiteralPath $blgPath -Force
        Move-Item -LiteralPath $_.FullName -Destination $ArchivePath -Force
        $stats.OK++
    }
    else {
        Remove-Item -LiteralPath $blgPath -Force -ErrorAction SilentlyContinue
        $stats.Failed++
    }
}

# --- Process .RAR files ---
Get-ChildItem -LiteralPath $ResolvedPath -Filter '*.rar' -File | ForEach-Object {
    Write-Host "RAR: $($_.Name)" -ForegroundColor Cyan

    $blgPath = Expand-RarBlg -RarPath $_.FullName
    if (-not $blgPath) {
        $stats.Failed++
        return  # next file in ForEach-Object
    }

    if (Invoke-Relog -BlgPath $blgPath) {
        Remove-Item -LiteralPath $blgPath -Force
        Move-Item -LiteralPath $_.FullName -Destination $ArchivePath -Force
        $stats.OK++
    }
    else {
        Remove-Item -LiteralPath $blgPath -Force -ErrorAction SilentlyContinue
        $stats.Failed++
    }
}

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "------------------------------" -ForegroundColor White
Write-Host "  Loaded:  $($stats.OK)"       -ForegroundColor Green
Write-Host "  Failed:  $($stats.Failed)"    -ForegroundColor Red
Write-Host "------------------------------" -ForegroundColor White

pause
