# Failure-handling demos: .\bench\failures.ps1   (run from the repo root)
$ErrorActionPreference = "Continue"
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
$bin = Join-Path $root "bin"
go build -o $bin ./cmd/... ; if ($LASTEXITCODE) { throw "build failed" }
$data = "testdata\leads_20000.csv"
if (-not (Test-Path $data)) { & "$bin\gencsv.exe" -n 20000 -out $data 2>&1 | Out-Null }

function Engine($extra) {
    Get-Process mockengine -ErrorAction SilentlyContinue | Stop-Process -Force
    Start-Process "$bin\mockengine.exe" -ArgumentList (@("-addr", ":8091", "-latency", "10ms") + $extra) -WindowStyle Hidden | Out-Null
    Start-Sleep -Milliseconds 600
}
function Ingest($title, $extra) {
    Write-Host "`n=== $title" -ForegroundColor Cyan
    & "$bin\ingest.exe" run -file $data -engine-url http://localhost:8091 -workers 4 -shards 4 -log warn @extra 2>&1 |
        Select-String -Pattern "rows read|inserted|failed|invalid|malformed|duplicate|retries|ERROR|level=(WARN|ERROR)" | ForEach-Object { "  $_" }
    Write-Host "  exit code: $LASTEXITCODE"
    $s = Invoke-RestMethod http://localhost:8091/stats
    Write-Host "  engine: records=$($s.records) 429s=$($s.rejected429) 500s=$($s.injected500) hung=$($s.injectedSlow)"
}

Engine @()
Ingest "1. malformed CSV rows + invalid leads: rejected individually, import continues" @("-rejects", "testdata\rejects.csv")
Write-Host "  rejects.csv sample:"; Get-Content testdata\rejects.csv -TotalCount 6 | ForEach-Object { "    $_" }

Engine @("-p500", "0.2")
Ingest "2. API 500 on 20% of requests: retried with backoff, nothing lost" @()

Engine @("-p429", "0.2", "-retry-after", "1s")
Ingest "3. API 429 on 20% of requests: Retry-After honoured" @()

Engine @("-pslow", "0.05", "-slow-for", "30s")
Ingest "4. API timeout (5% of requests hang 30s, client timeout 1s): retried" @("-timeout", "1s")

Engine @("-token", "secret")
Ingest "5. wrong credentials (401): FATAL, stops immediately" @("-token", "wrong")

Engine @("-p500", "1")
Ingest "6. engine completely down: stops after 5 consecutive failed batches, checkpoint not advanced" @("-retries", "2", "-checkpoint", "testdata\down.ckpt")
Remove-Item testdata\down.ckpt -ErrorAction SilentlyContinue

Get-Process mockengine -ErrorAction SilentlyContinue | Stop-Process -Force
Write-Host "`nCancellation, graceful shutdown and resume are covered by: go test ./cmd/ingest/ ./internal/pipeline/ -v -run 'Cancel|Shutdown|Resume'"
