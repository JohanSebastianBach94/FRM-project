# PowerShell wrapper to run FAO FPI fetcher and capture logs
# Usage (PowerShell):
#   Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass; .\scripts\run_faostat_fetch.ps1

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
Push-Location $scriptDir\..\

$logDir = Join-Path $PWD 'outputs'
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }
$logFile = Join-Path $logDir ('faostat_fetch_' + (Get-Date -Format 'yyyyMMdd_HHmmss') + '.log')

Write-Host "Running FAO FPI fetcher; logging to $logFile"
# Run with Python in the current environment
python .\scripts\fetch_fao_fpi.py 2>&1 | Tee-Object -FilePath $logFile

# On completion, show summary of files written (if any)
$targetDir = Join-Path $PWD 'data_repository\raw\faostat'
if (Test-Path $targetDir) {
    Write-Host "Contents of $targetDir:" 
    Get-ChildItem -Path $targetDir | Select-Object Name,Length,LastWriteTime | Format-Table
} else {
    Write-Host "No faostat directory found at $targetDir"
}

Pop-Location
Write-Host "Done."