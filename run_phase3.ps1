param(
  [string]$StartStep = "0",
  [string]$EndStep = "11"
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$py = Join-Path $root ".conda\python.exe"
if (-not (Test-Path $py)) {
  $py = "python"
}

& $py (Join-Path $root "SRESS TEST PIPELINE\run_phase3_pipeline.py") --start-step $StartStep --end-step $EndStep
