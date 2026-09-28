# Postgres benchmark: $env:LEADINGEST_PG_URL="postgres://..."; .\bench\bench-pg.ps1
$ErrorActionPreference = "Continue"
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
$bin = Join-Path $root "bin"
$out = Join-Path $root "bench\results"
New-Item -ItemType Directory -Force $out | Out-Null
$csv = Join-Path $out "results-pg.csv"
if (-not $env:LEADINGEST_PG_URL) { throw "set LEADINGEST_PG_URL" }
"phase,mode,rows,workers,shards,batch,durationSec,rowsPerSec,inserted,updated,failed,sinkP50Ms,sinkP99Ms,lookups,lookupP50Ms,peakHeapMB,peakMemMB,cpuSec,cpuPct,numGC,gcPauseMs" | Set-Content $csv
go build -o $bin ./cmd/... ; if ($LASTEXITCODE) { throw "build failed" }

function Get-Data($rows) {
    $f = "testdata\leads_$rows.csv"
    if (-not (Test-Path $f)) { & "$bin\gencsv.exe" -n $rows -out $f 2>&1 | Out-Null }
    return $f
}

function Run-One($phase, $mode, $rows, $workers, $shards, $batch) {
    $json = Join-Path $out "pg-$phase-$mode-$rows-w$workers-s$shards-b$batch.json"
    & "$bin\ingest.exe" run -file (Get-Data $rows) -mode $mode -sink pg -pg-reset -workers $workers -shards $shards -batch $batch -lookups 8 -log error -report-json $json | Out-Null
    $r = Get-Content $json | ConvertFrom-Json
    $o = $r.outcomes
    $vals = @($phase, $mode, $rows, $workers, $shards, $batch, [math]::Round($r.durationSec, 3), [math]::Round($r.rowsPerSec),
        [int]$o.inserted, [int]$o.updated, [int]$o.failed, $r.sinkP50Ms, $r.sinkP99Ms, $r.lookups, $r.lookupP50Ms,
        [math]::Round($r.peakHeapMB, 1), [math]::Round($r.peakMemMB, 1), [math]::Round($r.cpuSeconds, 2), [math]::Round($r.cpuPercentOfOneCore), $r.numGC, [math]::Round($r.gcPauseTotalMs, 1))
    Add-Content $csv ($vals -join ",")
    Write-Host ("{0,-12} {1,-8} rows={2,-7} w={3,-3} s={4,-3} b={5,-5} {6,8:N2}s {7,10:N0} rows/s  p99 {8}ms heap {9:N1}MB" -f `
        $phase, $mode, $rows, $workers, $shards, $batch, $r.durationSec, $r.rowsPerSec, $r.sinkP99Ms, $r.peakHeapMB)
}

Write-Host "== pg writers sweep (150k, batch 500)"
foreach ($s in 1, 2, 4, 8, 16, 32) { Run-One "pg-shards" "pipeline" 150000 4 $s 500 }
Write-Host "== pg batch sweep (150k, 8 writers)"
foreach ($b in 100, 250, 500, 1000, 2000) { Run-One "pg-batch" "pipeline" 150000 4 8 $b }
Write-Host "== pg legacy model (8 real single-row queries per row)"
Run-One "pg-legacy" "legacy" 10000 1 1 500
Write-Host "results: $csv"
