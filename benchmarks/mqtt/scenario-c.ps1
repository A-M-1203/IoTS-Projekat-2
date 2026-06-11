param(
    [ValidateSet(0, 1, 2)]
    [int]$Qos = 1
)

$ErrorActionPreference = "Stop"
$resultsDir = Join-Path $PSScriptRoot "..\..\results\mqtt"
New-Item -ItemType Directory -Force -Path $resultsDir | Out-Null

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$outputFile = Join-Path $resultsDir "scenario-c_qos${Qos}_${timestamp}.txt"

Write-Host "=== Scenario C: Burst Event Load (MQTT) ==="

function Get-StoragePending {
    $m = Invoke-RestMethod -Uri "http://localhost:3000/metrics" -ErrorAction SilentlyContinue
    return @{
        Received = $m.messagesReceived
        Stored = $m.messagesStored
        Pending = $m.pendingBatch
        DbCount = $m.dbRowCount
    }
}

$results = @()

# Phase 1: Baseline 50 msg/s (~5 clients, 100ms interval)
Write-Host "Phase 1: Baseline ~50 msg/s for 10s..."
$phase1Start = Get-Date
docker run --rm --network iot-network emqx/emqtt-bench pub `
    -h mosquitto -p 1883 -t iot/agriculture/readings -c 5 -I 100 -q $Qos 2>&1 | Out-Null
$results += "Phase1 (50 msg/s): completed at $(Get-Date -Format 'o')"

# Phase 2: Burst 5000 msg/s (~500 clients, 100ms interval for ~10s)
Write-Host "Phase 2: Burst ~5000 msg/s for 10s..."
$burstStart = Get-Date
$burstMetrics = Get-StoragePending
docker run --rm --network iot-network emqx/emqtt-bench pub `
    -h mosquitto -p 1883 -t iot/agriculture/readings -c 500 -I 100 -q $Qos 2>&1 | Out-Null
$burstEnd = Get-Date
$burstAfter = Get-StoragePending
$results += "Phase2 (5000 msg/s burst): started=$($burstStart.ToString('o')) ended=$($burstEnd.ToString('o'))"
$results += "  Pending batch at burst end: $($burstAfter.Pending)"
$results += "  Backlog (received-stored): $($burstAfter.Received - $burstAfter.Stored)"

# Phase 3: Recovery monitoring
Write-Host "Phase 3: Monitoring recovery..."
$recoveryStart = Get-Date
$recoveryTime = $null
for ($i = 0; $i -lt 60; $i++) {
    Start-Sleep -Seconds 2
    docker exec iot-data-storage wget -qO- --post-data="" http://localhost:3000/flush 2>$null
    $m = Get-StoragePending
    $backlog = $m.Received - $m.Stored
    Write-Host "  t=${i}x2s: backlog=$backlog pending=$($m.Pending)"
    if ($backlog -le 10 -and $m.Pending -eq 0 -and -not $recoveryTime) {
        $recoveryTime = (Get-Date) - $recoveryStart
        $results += "Recovery time: $($recoveryTime.TotalSeconds)s"
        break
    }
}

if (-not $recoveryTime) {
    $results += "Recovery time: >120s (not fully recovered)"
}

# Phase 4: Return to baseline
Write-Host "Phase 4: Return to baseline ~50 msg/s..."
docker run --rm --network iot-network emqx/emqtt-bench pub `
    -h mosquitto -p 1883 -t iot/agriculture/readings -c 5 -I 100 -q $Qos 2>&1 | Out-Null

$summary = @"

=== SCENARIO C SUMMARY (MQTT) ===
QoS: $Qos
$($results -join "`n")
"@
Write-Host $summary
$summary | Out-File -FilePath $outputFile -Encoding utf8
Write-Host "Results saved to $outputFile"
