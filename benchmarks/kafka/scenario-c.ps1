param(
    [ValidateSet("0", "1", "all")]
    [string]$Acks = "all"
)

$ErrorActionPreference = "Stop"
$resultsDir = Join-Path $PSScriptRoot "..\..\results\kafka"
New-Item -ItemType Directory -Force -Path $resultsDir | Out-Null

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$outputFile = Join-Path $resultsDir "scenario-c_acks${Acks}_${timestamp}.txt"

Write-Host "=== Scenario C: Burst Event Load (Kafka) ==="

function Get-ConsumerLag {
    $output = docker exec iot-kafka /opt/kafka/bin/kafka-consumer-groups.sh `
        --bootstrap-server localhost:9092 `
        --describe --group storage-group 2>&1
    $lagLines = $output | Select-String "iot-agriculture"
    $totalLag = 0
    foreach ($line in $lagLines) {
        if ($line -match "\s+(\d+)\s*$") {
            $totalLag += [int]$Matches[1]
        }
    }
    return @{ TotalLag = $totalLag; Raw = $output }
}

$results = @()

# Phase 1: Baseline 50 msg/s
Write-Host "Phase 1: Baseline 50 msg/s for 10s..."
docker exec iot-kafka /opt/kafka/bin/kafka-producer-perf-test.sh `
    --topic iot-agriculture-readings --num-records 500 --record-size 512 `
    --throughput 50 --producer-props "bootstrap.servers=localhost:9092,acks=$Acks" 2>&1 | Out-Null
$results += "Phase1 (50 msg/s): completed"

# Phase 2: Burst 5000 msg/s
Write-Host "Phase 2: Burst 5000 msg/s..."
$burstStart = Get-Date
docker exec iot-kafka /opt/kafka/bin/kafka-producer-perf-test.sh `
    --topic iot-agriculture-readings --num-records 50000 --record-size 512 `
    --throughput 5000 --producer-props "bootstrap.servers=localhost:9092,acks=$Acks" 2>&1 | Out-Null
$burstEnd = Get-Date
$lagAtBurst = Get-ConsumerLag
$results += "Phase2 burst: $($burstStart.ToString('o')) - $($burstEnd.ToString('o'))"
$results += "  Consumer lag at burst end: $($lagAtBurst.TotalLag)"

# Phase 3: Recovery monitoring
Write-Host "Phase 3: Monitoring consumer lag recovery..."
$recoveryStart = Get-Date
$recoveryTime = $null
for ($i = 0; $i -lt 60; $i++) {
    Start-Sleep -Seconds 2
    docker exec iot-data-storage wget -qO- --post-data="" http://localhost:3000/flush 2>$null
    $lag = Get-ConsumerLag
    Write-Host "  t=${i}x2s: consumer_lag=$($lag.TotalLag)"
    if ($lag.TotalLag -eq 0 -and -not $recoveryTime) {
        $recoveryTime = (Get-Date) - $recoveryStart
        $results += "Recovery time (lag=0): $($recoveryTime.TotalSeconds)s"
        break
    }
}

if (-not $recoveryTime) {
    $results += "Recovery time: >120s"
}

# Phase 4: Baseline
Write-Host "Phase 4: Return to baseline..."
docker exec iot-kafka /opt/kafka/bin/kafka-producer-perf-test.sh `
    --topic iot-agriculture-readings --num-records 500 --record-size 512 `
    --throughput 50 --producer-props "bootstrap.servers=localhost:9092,acks=$Acks" 2>&1 | Out-Null

$summary = @"

=== SCENARIO C SUMMARY (Kafka) ===
Acks: $Acks
$($results -join "`n")
"@
Write-Host $summary
$summary | Out-File -FilePath $outputFile -Encoding utf8
