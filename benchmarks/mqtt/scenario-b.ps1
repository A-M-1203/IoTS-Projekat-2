param(
    [ValidateSet(0, 1, 2)]
    [int]$Qos = 1,
    [int]$DurationSeconds = 60
)

$ErrorActionPreference = "Stop"
$resultsDir = Join-Path $PSScriptRoot "..\..\results\mqtt"
New-Item -ItemType Directory -Force -Path $resultsDir | Out-Null

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$outputFile = Join-Path $resultsDir "scenario-b_qos${Qos}_${timestamp}.txt"

Write-Host "=== Scenario B: Edge Connectivity Failures (MQTT) ==="
Write-Host "QoS: $Qos | Disconnect duration: 30s"

$dbBefore = docker exec iot-postgres psql -U iotuser -d iotdb -t -c "SELECT COUNT(*) FROM sensor_readings;"
$ingBefore = (Invoke-RestMethod -Uri "http://localhost:8080/metrics").messagesPublished

Write-Host "Starting moderate load (100 clients)..."
$benchJob = Start-Job -ScriptBlock {
    docker run --rm --network iot-network emqx/emqtt-bench pub `
        -h mosquitto -p 1883 -t iot/agriculture/readings `
        -c 100 -I 50 -q $using:Qos
}

Start-Sleep -Seconds 10

Write-Host "Disconnecting data-ingestion-service from network for 30 seconds..."
docker network disconnect iot-network iot-data-ingestion 2>$null
$disconnectTime = Get-Date

Start-Sleep -Seconds 30

Write-Host "Reconnecting data-ingestion-service..."
docker network connect iot-network iot-data-ingestion
$reconnectTime = Get-Date

Start-Sleep -Seconds 20

Wait-Job $benchJob -Timeout 120 | Out-Null
Receive-Job $benchJob | Out-Null
Remove-Job $benchJob -Force -ErrorAction SilentlyContinue

$dbAfter = docker exec iot-postgres psql -U iotuser -d iotdb -t -c "SELECT COUNT(*) FROM sensor_readings;"
$ingAfter = (Invoke-RestMethod -Uri "http://localhost:8080/metrics").messagesPublished
$storageMetrics = Invoke-RestMethod -Uri "http://localhost:3000/metrics"

$summary = @"

=== SCENARIO B SUMMARY (MQTT) ===
QoS:                    $Qos
Disconnect at:          $($disconnectTime.ToString("o"))
Reconnect at:           $($reconnectTime.ToString("o"))
Outage duration:        30s
DB rows before:         $($dbBefore.Trim())
DB rows after:          $($dbAfter.Trim())
Ingestion published:    $($ingAfter - $ingBefore) (during test)
Storage received:       $($storageMetrics.messagesReceived)
Storage stored:         $($storageMetrics.messagesStored)
Recovery note:          QoS 0 = message loss expected; QoS 1/2 = redelivery after reconnect
"@

Write-Host $summary
$summary | Out-File -FilePath $outputFile -Encoding utf8
Write-Host "Results saved to $outputFile"
