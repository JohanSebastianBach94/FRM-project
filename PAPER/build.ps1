# ── build.ps1 ──────────────────────────────────────────────────────────────────
# Compile main.tex with MiKTeX's latexmk.
# MiKTeX auto-installs missing packages on first run (may take a few minutes).
#
# Run:   .\build.ps1          → full build (pdflatex + biber + 2× pdflatex)
# Run:   .\build.ps1 -Quick   → single pdflatex pass (skip biber, fast preview)
#
# Output: main.pdf (same folder)
# ──────────────────────────────────────────────────────────────────────────────
param([switch]$Quick)

$ErrorActionPreference = "Stop"
$script_dir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $script_dir

$bin = "$env:LOCALAPPDATA\Programs\MiKTeX\miktex\bin\x64"
$pdflatex = "$bin\pdflatex.exe"
$biber    = "$bin\biber.exe"
$latexmk  = "$bin\latexmk.exe"

if (-not (Test-Path $pdflatex)) {
    Write-Error "MiKTeX not found at $bin. Re-run: winget install MiKTeX.MiKTeX"
    exit 1
}

# Satisfy MiKTeX's update-check requirement (needed by biber; safe to run always)
& "$bin\mpm.exe" --update-db 2>$null

if ($Quick) {
    Write-Host "Quick pass (pdflatex only) ..." -ForegroundColor Cyan
    & $pdflatex --enable-installer -interaction=nonstopmode -synctex=1 main.tex
} else {
    Write-Host "Full build: pdflatex → biber → pdflatex × 2 ..." -ForegroundColor Cyan
    # Pass 1 — generate .bcf for biber
    & $pdflatex --enable-installer -interaction=nonstopmode -synctex=1 main.tex
    # Pass 2 — process bibliography
    & $biber main
    # Pass 3+4 — resolve references
    & $pdflatex --enable-installer -interaction=nonstopmode -synctex=1 main.tex
    & $pdflatex --enable-installer -interaction=nonstopmode -synctex=1 main.tex
}

if ($LASTEXITCODE -eq 0) {
    Write-Host "`nDone.  Output: $script_dir\main.pdf" -ForegroundColor Green
} else {
    Write-Host "`nBuild had errors — check main.log for details." -ForegroundColor Red
    exit 1
}

