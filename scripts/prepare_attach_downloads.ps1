<#
Prepare attach downloads for IMF, ECB and BIS.

Usage (run on a machine with working Internet):
  1. Open PowerShell on a machine that can reach the providers.
  2. Run this script. By default it writes files into C:\tmp\frm_attach (change $AttachDir).
  3. Copy the resulting folder to the project machine and run:
       python .\scripts\extend_fetch_structural_data.py --attach C:\path\to\frm_attach

This script conservatively attempts the same endpoints the fetcher uses. Some endpoints
may need manual tuning if the provider URL patterns have changed.
#>

param(
    [string]$AttachDir = 'C:\tmp\frm_attach'
)

Write-Host "Creating attach directory: $AttachDir"
New-Item -ItemType Directory -Path $AttachDir -Force | Out-Null

# Helper to download safely (catch errors and continue)
function SafeDownload {
    param($url, $out)
    try {
        Invoke-WebRequest -Uri $url -OutFile $out -UseBasicParsing -ErrorAction Stop
        Write-Host "  fetched $url -> $out"
    } catch {
        Write-Warning "Failed to download $url : $($_.Exception.Message)"
    }
}

# World Bank (example: general government gross debt % GDP)
$wbIndicator = 'GC.DOD.TOTL.GD.ZS'
$countries = @('FRA','DEU','ITA','ESP','USA','GBR','CHE')
foreach ($c in $countries) {
    $url = "https://api.worldbank.org/v2/country/$c/indicator/$wbIndicator?format=json&per_page=2000"
    $out = Join-Path $AttachDir "wb_${wbIndicator}_${c}.json"
    Write-Host "Downloading World Bank $c -> $out"
    SafeDownload $url $out
}

# IMF (CompactData pattern)
$imfBase = 'https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData'
$imfSeries = @(
    @{ dataset='IFS'; key='USA.NGDP_R'; fname='imf_USA_NGDP_R.json' },
    @{ dataset='IFS'; key='ITA.NGDP_R'; fname='imf_ITA_NGDP_R.json' }
)
foreach ($s in $imfSeries) {
    $url = "$imfBase/$($s.dataset)/$($s.key)"
    $out = Join-Path $AttachDir $s.fname
    Write-Host "Downloading IMF $($s.key) -> $out"
    SafeDownload $url $out
}

# ECB BSI sample (SDMX JSON via SDW REST)
$ecbHosts = @('https://sdw-wsrest.ecb.europa.eu/service','https://sdw.ecb.europa.eu/service')
$ecbSeries = @(
    @{ dataset='BSI'; key='M.U2.N.A.A20.A.1.U2.3000.Z01.E'; fname='ecb_M_U2_N_A_A20_A_1_U2_3000_Z01_E.json' },
    @{ dataset='BSI'; key='M.U2.N.A.A20.A.1.U2.1000.Z01.E'; fname='ecb_M_U2_N_A_A20_A_1_U2_1000_Z01_E.json' }
)
foreach ($s in $ecbSeries) {
    foreach ($host in $ecbHosts) {
        $url = "$host/data/$($s.dataset)/$($s.key)?detail=dataonly&startPeriod=2018-01&format=sdmx-json"
        $out = Join-Path $AttachDir $s.fname
    Write-Host "Attempting ECB $($s.key) via $host -> $out"
    SafeDownload $url $out
    }
}

# BIS candidate downloads
$bisCandidates = @(
    @{ url='https://stats.bis.org/api/views/LBS_D_PUB/CSV?downloadfilename=LBS_D_PUB.csv'; fname='bis_lbs_d_pub.csv' },
    @{ url='https://stats.bis.org/api/views/CDIS_D_PUB/CSV?downloadfilename=CDIS_D_PUB.csv'; fname='bis_cdis_d_pub.csv' }
)
foreach ($b in $bisCandidates) {
    $out = Join-Path $AttachDir $b.fname
    Write-Host "Downloading BIS candidate: $($b.url) -> $out"
    SafeDownload $b.url $out
    # also try conservative fallbacks
    $base = $b.url.Split('?')[0]
    $alts = @($base, $base + '/csv', $base + '/CSV')
    foreach ($a in $alts) {
        $altOut = Join-Path $AttachDir ([IO.Path]::GetFileName($a) + '_' + $b.fname)
    Write-Host "Trying alternative $a -> $altOut"
    SafeDownload $a $altOut
    }
}

Write-Host "Attach folder prepared at: $AttachDir"
Write-Host "Copy this folder to the project machine and run: python .\scripts\extend_fetch_structural_data.py --attach $AttachDir"
