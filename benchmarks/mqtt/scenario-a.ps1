param(
    [ValidateSet(100, 1000, 10000)]
    [int]$Clients = 100,
    [ValidateSet(0, 1, 2)]
    [int]$Qos = 1,
    [int]$IntervalMs = 10,
    [int]$DurationSeconds = 30,
    [string]$Topic = "iot/agriculture/readings"
)

$ErrorActionPreference = "Stop"
$resultsDir = Join-Path $PSScriptRoot "..\..\results\mqtt"
New-Item -ItemType Directory -Force -Path $resultsDir | Out-Null

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$outputFile = Join-Path $resultsDir "scenario-a_${Clients}clients_qos${Qos}_${timestamp}.txt"

Write-Host "=== Scenario A: Massive Sensor Ingestion (MQTT) ==="
Write-Host "Clients: $Clients | QoS: $Qos | Interval: ${IntervalMs}ms | Duration: ${DurationSeconds}s"

# Enable batch mode on storage
Write-Host "Enabling batch mode on storage service..."
docker exec iot-data-storage sh -c "echo batch enabled" 2>$null

$metricsJob = Start-Job -ScriptBlock {
    param($script, $dur, $out)
    & $script -DurationSeconds $dur -IntervalSeconds 5 -OutputFile $out
} -ArgumentList (Join-Path $PSScriptRoot "..\common\collect-metrics.ps1"), ($DurationSeconds + 10), (Join-Path $resultsDir "metrics_a_${timestamp}.csv")

$dbCountBefore = docker exec iot-postgres psql -U iotuser -d iotdb -t -c "SELECT COUNT(*) FROM sensor_readings;" 2>$null
$dbCountBefore = [int]($dbCountBefore -replace "\s", "")

$messageCount = [int]($DurationSeconds * 1000 / $IntervalMs)

Write-Host "Running emqtt-bench ($messageCount msgs/client)..."
$benchOutput = docker run --rm --network iot-network emqx/emqtt-bench pub `
    -h mosquitto -p 1883 -t $Topic `
    -c $Clients -I $IntervalMs -q $Qos -n $messageCount 2>&1 | Tee-Object -Variable benchResult

$benchOutput | Out-File -FilePath $outputFile -Encoding utf8

Start-Sleep -Seconds 5
docker exec iot-data-storage wget -qO- --post-data="" http://localhost:3000/flush 2>$null

Start-Sleep -Seconds 3
$dbCountAfter = docker exec iot-postgres psql -U iotuser -d iotdb -t -c "SELECT COUNT(*) FROM sensor_readings;" 2>$null
$dbCountAfter = [int]($dbCountAfter -replace "\s", "")

$storageMetrics = Invoke-RestMethod -Uri "http://localhost:3000/metrics" -ErrorAction SilentlyContinue

$expectedMessages = $Clients * ($DurationSeconds * 1000 / $IntervalMs)
$storedDelta = $dbCountAfter - $dbCountBefore
$lostPct = if ($expectedMessages -gt 0) { [math]::Round((1 - $storedDelta / $expectedMessages) * 100, 2) } else { 0 }

$summary = @"

=== SCENARIO A SUMMARY (MQTT) ===
Clients:           $Clients
QoS:               $Qos
Interval:          ${IntervalMs}ms
Expected (~):      $expectedMessages messages
DB before:         $dbCountBefore
DB after:          $dbCountAfter
Stored (delta):    $storedDelta
Lost estimate:     $lostPct%
Storage received:  $($storageMetrics.messagesReceived)
Storage stored:    $($storageMetrics.messagesStored)
Storage failed:    $($storageMetrics.messagesFailed)
"@

Write-Host $summary
$summary | Out-File -FilePath $outputFile -Append -Encoding utf8

Wait-Job $metricsJob -Timeout ($DurationSeconds + 30) | Out-Null
Receive-Job $metricsJob | Out-Null
Remove-Job $metricsJob -Force -ErrorAction SilentlyContinue

Write-Host "Results saved to $outputFile"
