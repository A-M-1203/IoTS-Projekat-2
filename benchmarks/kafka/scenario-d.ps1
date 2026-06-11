param(
    [ValidateSet("0", "1", "all")]
    [string]$Acks = "all",
    [int]$DurationSeconds = 60
)

$ErrorActionPreference = "Stop"
$resultsDir = Join-Path $PSScriptRoot "..\..\results\kafka"
New-Item -ItemType Directory -Force -Path $resultsDir | Out-Null

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$outputFile = Join-Path $resultsDir "scenario-d_acks${Acks}_${timestamp}.txt"

Write-Host "=== Scenario D: Real-Time Alerting (Kafka) ==="

docker compose --profile kafka stop data-ingestion-service 2>$null
docker compose --profile kafka rm -f data-ingestion-service 2>$null

$env:INJECT_CRITICAL = "true"
$env:KAFKA_ACKS = $Acks
$env:DEVICE_COUNT = "10"
$env:PUBLISH_INTERVAL_MS = "200"
$env:BROKER_TYPE = "kafka"

docker compose --profile kafka up -d data-ingestion-service
Start-Sleep -Seconds 20

$logStart = Get-Date
Start-Sleep -Seconds $DurationSeconds

$logs = docker logs iot-analytics --since "$($logStart.ToString('yyyy-MM-ddTHH:mm:ss'))" 2>&1
$e2eLines = $logs | Select-String "E2E latency"
$alertLines = $logs | Select-String "CRITICAL ALERT"

$latencies = @()
foreach ($line in $e2eLines) {
    if ($line -match "latency=([\d.]+)ms") {
        $latencies += [double]$Matches[1]
    }
}

$sorted = $latencies | Sort-Object
$p50 = if ($sorted.Count -gt 0) { $sorted[[int]($sorted.Count * 0.5)] } else { $null }
$p95 = if ($sorted.Count -gt 0) { $sorted[[int]($sorted.Count * 0.95)] } else { $null }
$p99 = if ($sorted.Count -gt 0) { $sorted[[int]($sorted.Count * 0.99)] } else { $null }

$analyticsMetrics = Invoke-RestMethod -Uri "http://localhost:8000/metrics"

$summary = @"

=== SCENARIO D SUMMARY (Kafka) ===
Acks:               $Acks
Duration:           ${DurationSeconds}s
Critical alerts:    $($alertLines.Count)
E2E samples:        $($latencies.Count)
E2E p50:            $p50 ms
E2E p95:            $p95 ms
E2E p99:            $p99 ms
Analytics p95:      $($analyticsMetrics.e2eLatencyP95Ms) ms
Alerts triggered:   $($analyticsMetrics.alertsTriggered)
"@

Write-Host $summary
$summary | Out-File -FilePath $outputFile -Encoding utf8
