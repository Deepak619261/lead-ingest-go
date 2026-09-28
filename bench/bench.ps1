# Benchmark matrix: .\bench\bench.ps1 [-Quick] [-SkipLegacy]   (run from the repo root)
param([switch]$Quick, [switch]$SkipLegacy)
$ErrorActionPreference = "Continue"  # native tools log to stderr; we check $LASTEXITCODE instead
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
$bin = Join-Path $root "bin"
$out = Join-Path $root "bench\results"
New-Item -ItemType Directory -Force $out | Out-Null
$csv = Join-Path $out "results.csv"
"phase,mode,rows,workers,shards,batch,durationSec,rowsPerSec,inserted,failed,retries,sinkP50Ms,sinkP99Ms,lookups,peakHeapMB,peakMemMB,cpuSec,cpuPct,numGC,gcPauseMs" | Set-Content $csv

go build -o $bin ./cmd/... ; if ($LASTEXITCODE) { throw "build failed" }

# Realistic lead API: 30ms fixed + 50us/lead per bulk call, 1ms per single-row lookup, 8 requests served at once.
$engineArgs = @("-addr", ":8090", "-latency", "30ms", "-per-lead", "50us", "-lookup-latency", "1ms", "-capacity", "8")
function Start-Engine($extra) {
    Get-Process mockengine -ErrorAction SilentlyContinue | Stop-Process -Force
    $p = Start-Process -FilePath "$bin\mockengine.exe" -ArgumentList ($engineArgs + $extra) -PassThru -WindowStyle Hidden
    Start-Sleep -Milliseconds 700
    return $p
}

function Get-Data($rows) {
    $f = "testdata\leads_$rows.csv"
    if (-not (Test-Path $f)) { & "$bin\gencsv.exe" -n $rows -out $f 2>$null }
    return $f
}

function Run-One($phase, $mode, $rows, $workers, $shards, $batch) {
    Invoke-RestMethod -Method Post "http://localhost:8090/reset" | Out-Null
    $file = Get-Data $rows
    $json = Join-Path $out "$phase-$mode-$rows-w$workers-s$shards-b$batch.json"
    & "$bin\ingest.exe" run -file $file -mode $mode -sink mock -workers $workers -shards $shards -batch $batch -log error -report-json $json | Out-Null
    $r = Get-Content $json | ConvertFrom-Json
    $ins = if ($r.outcomes.inserted) { $r.outcomes.inserted } else { 0 }
    $fail = if ($r.outcomes.failed) { $r.outcomes.failed } else { 0 }
    $line = "$phase,$mode,$rows,$workers,$shards,$batch,{0:N3},{1:N0},$ins,$fail,$($r.retries),$($r.sinkP50Ms),$($r.sinkP99Ms),$($r.lookups),{2:N1},{3:N1},{4:N2},{5:N0},$($r.numGC),{6:N1}" -f `
        $r.durationSec, $r.rowsPerSec, $r.peakHeapMB, $r.peakMemMB, $r.cpuSeconds, $r.cpuPercentOfOneCore, $r.gcPauseTotalMs
    $line = $line -replace '(?<=\d),(?=\d{3}(\D|$))', ''
    Add-Content $csv $line
    Write-Host ("{0,-12} {1,-8} rows={2,-7} w={3,-3} s={4,-3} b={5,-5} {6,8:N2}s {7,10:N0} rows/s  heap {8:N1}MB" -f `
        $phase, $mode, $rows, $workers, $shards, $batch, $r.durationSec, $r.rowsPerSec, $r.peakHeapMB)
}

$engine = Start-Engine @()
try {
    $big = if ($Quick) { 50000 } else { 150000 }

    Write-Host "== 1. concurrency sweep (workers = shards = N)"
    foreach ($n in 1, 2, 4, 8, 16, 32) { Run-One "concurrency" "pipeline" $big $n $n 500 }

    Write-Host "== 2. workers only (shards fixed at 8): CPU side"
    foreach ($n in 1, 2, 4, 8, 16) { Run-One "workers" "pipeline" $big $n 8 500 }

    Write-Host "== 3. batch size sweep (N = 8)"
    foreach ($b in 100, 250, 500, 1000, 2000) { Run-One "batch" "pipeline" $big 8 8 $b }

    Write-Host "== 4. dataset size (N = 8, batch 500)"
    $sizes = if ($Quick) { 10000, 50000 } else { 10000, 50000, 100000, 150000, 500000 }
    foreach ($rows in $sizes) { Run-One "rows" "pipeline" $rows 8 8 500 }

    if (-not $SkipLegacy) {
        Write-Host "== 5. legacy per-row importer model (8 sequential lookups/row, 500-row chunks)"
        foreach ($rows in 10000, 50000) { Run-One "legacy" "legacy" $rows 1 1 500 }
    }

    Write-Host "== 6. downstream rate limit (429 above 8 in-flight) vs concurrency"
    $engine = Start-Engine @("-max-inflight", "8", "-retry-after", "1s")
    foreach ($n in 4, 8, 16, 32) { Run-One "ratelimit" "pipeline" $big $n $n 500 }
}
finally {
    Get-Process mockengine -ErrorAction SilentlyContinue | Stop-Process -Force
}
Write-Host "results: $csv"
