param(
    [ValidateSet("0", "1", "all")]
    [string]$Acks = "all"
)

$ErrorActionPreference = "Stop"
$resultsDir = Join-Path $PSScriptRoot "..\..\results\kafka"
New-Item -ItemType Directory -Force -Path $resultsDir | Out-Null

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$outputFile = Join-Path $resultsDir "scenario-b_acks${Acks}_${timestamp}.txt"

Write-Host "=== Scenario B: Edge Connectivity Failures (Kafka) ==="

$dbBefore = docker exec iot-postgres psql -U iotuser -d iotdb -t -c "SELECT COUNT(*) FROM sensor_readings;"

Write-Host "Starting moderate producer load..."
$benchJob = Start-Job -ScriptBlock {
    docker exec iot-kafka /opt/kafka/bin/kafka-producer-perf-test.sh `
        --topic iot-agriculture-readings `
        --num-records 50000 `
        --record-size 512 `
        --throughput 500 `
        --producer-props "bootstrap.servers=localhost:9092,acks=$using:Acks" 2>&1
}

Start-Sleep -Seconds 10

Write-Host "Disconnecting data-ingestion-service for 30 seconds..."
docker network disconnect iot-network iot-data-ingestion 2>$null
$disconnectTime = Get-Date

Start-Sleep -Seconds 30

Write-Host "Reconnecting data-ingestion-service..."
docker network connect iot-network iot-data-ingestion
$reconnectTime = Get-Date

Start-Sleep -Seconds 15

$consumerLag = docker exec iot-kafka /opt/kafka/bin/kafka-consumer-groups.sh `
    --bootstrap-server localhost:9092 `
    --describe --group storage-group 2>&1

Wait-Job $benchJob -Timeout 180 | Out-Null
Receive-Job $benchJob | Out-Null
Remove-Job $benchJob -Force -ErrorAction SilentlyContinue

$dbAfter = docker exec iot-postgres psql -U iotuser -d iotdb -t -c "SELECT COUNT(*) FROM sensor_readings;"
$storageMetrics = Invoke-RestMethod -Uri "http://localhost:3000/metrics"

$summary = @"

=== SCENARIO B SUMMARY (Kafka) ===
Acks:               $Acks
Disconnect at:      $($disconnectTime.ToString("o"))
Reconnect at:       $($reconnectTime.ToString("o"))
Outage duration:    30s
DB rows before:     $($dbBefore.Trim())
DB rows after:      $($dbAfter.Trim())
Storage received:   $($storageMetrics.messagesReceived)
Storage stored:     $($storageMetrics.messagesStored)
Consumer lag:
$consumerLag
Recovery note:      Kafka retains messages; consumers resume from committed offset
"@

Write-Host $summary
$summary | Out-File -FilePath $outputFile -Encoding utf8
