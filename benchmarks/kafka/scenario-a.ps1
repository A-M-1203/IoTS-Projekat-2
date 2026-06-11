param(
    [ValidateSet(100, 1000, 10000)]
    [int]$Records = 100000,
    [ValidateSet("0", "1", "all")]
    [string]$Acks = "1",
    [int]$RecordSize = 512,
    [int]$Threads = 1
)

$ErrorActionPreference = "Stop"
$resultsDir = Join-Path $PSScriptRoot "..\..\results\kafka"
New-Item -ItemType Directory -Force -Path $resultsDir | Out-Null

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$outputFile = Join-Path $resultsDir "scenario-a_${Records}records_acks${Acks}_${timestamp}.txt"

Write-Host "=== Scenario A: Massive Sensor Ingestion (Kafka) ==="
Write-Host "Records: $Records | acks: $Acks | record-size: $RecordSize | threads: $Threads"

$dbCountBefore = docker exec iot-postgres psql -U iotuser -d iotdb -t -c "SELECT COUNT(*) FROM sensor_readings;"
$dbCountBefore = [int]($dbCountBefore -replace "\s", "")

$metricsJob = Start-Job -ScriptBlock {
    param($script, $dur, $out)
    & $script -DurationSeconds $dur -IntervalSeconds 5 -OutputFile $out
} -ArgumentList (Join-Path $PSScriptRoot "..\common\collect-metrics.ps1"), 90, (Join-Path $resultsDir "metrics_a_${timestamp}.csv")

Write-Host "Running kafka-producer-perf-test..."
$benchOutput = docker exec iot-kafka /opt/kafka/bin/kafka-producer-perf-test.sh `
    --topic iot-agriculture-readings `
    --num-records $Records `
    --record-size $RecordSize `
    --throughput -1 `
    --producer-props "bootstrap.servers=localhost:9092,acks=$Acks" 2>&1

$benchOutput | Out-File -FilePath $outputFile -Encoding utf8
Write-Host $benchOutput

Start-Sleep -Seconds 10
docker exec iot-data-storage wget -qO- --post-data="" http://localhost:3000/flush 2>$null
Start-Sleep -Seconds 5

$dbCountAfter = docker exec iot-postgres psql -U iotuser -d iotdb -t -c "SELECT COUNT(*) FROM sensor_readings;"
$dbCountAfter = [int]($dbCountAfter -replace "\s", "")

$storageMetrics = Invoke-RestMethod -Uri "http://localhost:3000/metrics" -ErrorAction SilentlyContinue
$storedDelta = $dbCountAfter - $dbCountBefore
$lostPct = if ($Records -gt 0) { [math]::Round((1 - $storedDelta / $Records) * 100, 2) } else { 0 }

# Parse throughput from bench output
$throughputLine = $benchOutput | Select-String "records sent" | Select-Object -Last 1
$throughput = if ($throughputLine) { $throughputLine.ToString() } else { "N/A" }

$summary = @"

=== SCENARIO A SUMMARY (Kafka) ===
Records:           $Records
Acks:              $Acks
Record size:       $RecordSize bytes
Throughput:        $throughput
DB before:         $dbCountBefore
DB after:          $dbCountAfter
Stored (delta):    $storedDelta
Lost estimate:     $lostPct%
Storage received:  $($storageMetrics.messagesReceived)
Storage stored:    $($storageMetrics.messagesStored)
"@

Write-Host $summary
$summary | Out-File -FilePath $outputFile -Append -Encoding utf8

Wait-Job $metricsJob -Timeout 120 | Out-Null
Remove-Job $metricsJob -Force -ErrorAction SilentlyContinue

Write-Host "Results saved to $outputFile"
