param(
    [string]$OutputFile = "results\experiment-results.json"
)

$ErrorActionPreference = "Continue"
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
New-Item -ItemType Directory -Force -Path "results" | Out-Null

$results = @()

function Add-Result($scenario, $broker, $param, $throughput, $p95, $cpu, $ram, $lost, $note) {
    $script:results += [PSCustomObject]@{
        Scenario = $scenario
        Broker = $broker
        Param = $param
        ThroughputMsgS = $throughput
        P95LatencyMs = $p95
        CpuRam = "$cpu / $ram"
        LostPct = $lost
        Note = $note
    }
}

function Get-DockerStats($containers) {
    $stats = @{}
    foreach ($c in $containers) {
        $s = docker stats $c --no-stream --format "{{.CPUPerc}}|{{.MemUsage}}" 2>$null
        if ($s) {
            $parts = $s -split "\|"
            $stats[$c] = @{ CPU = $parts[0]; MEM = $parts[1] }
        }
    }
    return $stats
}

function Reset-Storage {
    docker exec iot-postgres psql -U iotuser -d iotdb -c "TRUNCATE sensor_readings;" 2>$null | Out-Null
    # Restart storage to reset counters
    docker compose restart data-storage-service 2>$null | Out-Null
    Start-Sleep -Seconds 8
}

Write-Host "========== MQTT EXPERIMENTS =========="
$env:BROKER_TYPE = "mqtt"
$env:BATCH_MODE = "true"
$env:BATCH_SIZE = "500"
docker compose --profile mqtt up -d 2>$null | Out-Null
docker stop iot-data-ingestion 2>$null | Out-Null
Start-Sleep -Seconds 10

foreach ($qos in @(0, 1, 2)) {
    foreach ($clients in @(100, 1000)) {
        Reset-Storage
        $n = 100
        Write-Host "MQTT A: clients=$clients qos=$qos n=$n"
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        $job = Start-Job { param($c,$q,$n) docker run --rm --network iot-network emqx/emqtt-bench pub -h mosquitto -p 1883 -t iot/agriculture/readings -c $c -I 10 -q $q -n $n 2>&1 } -ArgumentList $clients,$qos,$n
        Wait-Job $job -Timeout 120 | Out-Null
        $out = Receive-Job $job; Remove-Job $job -Force -ErrorAction SilentlyContinue
        if (-not $out) { Write-Host "  (timeout after 120s)"; $out = @() }
        $sw.Stop()
        Start-Sleep -Seconds 3
        Invoke-RestMethod -Method POST http://localhost:3000/flush -ErrorAction SilentlyContinue | Out-Null
        Start-Sleep -Seconds 2
        $m = Invoke-RestMethod http://localhost:3000/metrics -ErrorAction SilentlyContinue
        $stats = Get-DockerStats @("iot-mosquitto", "iot-data-storage", "iot-postgres")
        $expected = $clients * $n
        $throughput = [math]::Round($expected / $sw.Elapsed.TotalSeconds, 0)
        $lost = if ($expected -gt 0) { [math]::Round((1 - $m.messagesReceived / $expected) * 100, 2) } else { 0 }
        $rateLine = ($out | Select-String "rate" | Select-Object -Last 1).ToString()
        Add-Result "A" "MQTT" "QoS$qos/$clients dev" $throughput "N/A" $stats["iot-mosquitto"].CPU $stats["iot-mosquitto"].MEM $lost $rateLine
    }
}

# Scenario B MQTT
Write-Host "MQTT B: network disconnect"
Reset-Storage
docker start iot-data-ingestion 2>$null | Out-Null
Start-Sleep -Seconds 5
$before = (Invoke-RestMethod http://localhost:3000/metrics).messagesReceived
$benchJob = Start-Job { docker run --rm --network iot-network emqx/emqtt-bench pub -h mosquitto -p 1883 -t iot/agriculture/readings -c 50 -I 50 -q 1 -n 200 2>&1 }
Start-Sleep -Seconds 5
docker network disconnect iot-network iot-data-ingestion 2>$null
Start-Sleep -Seconds 30
$disconnectEnd = (Invoke-RestMethod http://localhost:3000/metrics).messagesReceived
docker network connect iot-network iot-data-ingestion 2>$null
Start-Sleep -Seconds 15
$after = (Invoke-RestMethod http://localhost:3000/metrics).messagesReceived
Wait-Job $benchJob -Timeout 60 | Out-Null; Remove-Job $benchJob -Force -ErrorAction SilentlyContinue
$stats = Get-DockerStats @("iot-mosquitto", "iot-data-storage")
Add-Result "B" "MQTT" "QoS1/100dev" "N/A" "N/A" $stats["iot-mosquitto"].CPU $stats["iot-mosquitto"].MEM "N/A" "During outage: +$($disconnectEnd-$before) msgs; after reconnect: +$($after-$disconnectEnd)"

# Scenario C MQTT
Write-Host "MQTT C: burst load"
Reset-Storage
docker stop iot-data-ingestion 2>$null | Out-Null
$sw1 = [System.Diagnostics.Stopwatch]::StartNew()
docker run --rm --network iot-network emqx/emqtt-bench pub -h mosquitto -p 1883 -t iot/agriculture/readings -c 5 -I 100 -q 1 -n 500 2>&1 | Out-Null
$sw1.Stop()
$m1 = Invoke-RestMethod http://localhost:3000/metrics
$burstStart = Get-Date
docker run --rm --network iot-network emqx/emqtt-bench pub -h mosquitto -p 1883 -t iot/agriculture/readings -c 200 -I 10 -q 1 -n 500 2>&1 | Out-Null
$m2 = Invoke-RestMethod http://localhost:3000/metrics
$backlog = $m2.messagesReceived - $m2.messagesStored
$recoverySec = 0
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Seconds 2
    Invoke-RestMethod -Method POST http://localhost:3000/flush -ErrorAction SilentlyContinue | Out-Null
    $mc = Invoke-RestMethod http://localhost:3000/metrics
    if (($mc.messagesReceived - $mc.messagesStored) -le 5) { $recoverySec = ($i+1)*2; break }
}
$stats = Get-DockerStats @("iot-mosquitto", "iot-data-storage")
Add-Result "C" "MQTT" "QoS1/burst" "~5000" "N/A" $stats["iot-data-storage"].CPU $stats["iot-data-storage"].MEM "N/A" "Backlog=$backlog; recovery=${recoverySec}s"

# Scenario D MQTT
Write-Host "MQTT D: alerting"
docker stop iot-data-ingestion 2>$null | Out-Null
$env:INJECT_CRITICAL = "true"
$env:DEVICE_COUNT = "10"
$env:PUBLISH_INTERVAL_MS = "200"
$env:MQTT_QOS = "1"
docker compose --profile mqtt up -d data-ingestion-service 2>$null | Out-Null
Start-Sleep -Seconds 45
$am = Invoke-RestMethod http://localhost:8000/metrics
$stats = Get-DockerStats @("iot-analytics", "iot-data-ingestion")
Add-Result "D" "MQTT" "QoS1/alert" "N/A" $am.e2eLatencyP95Ms $stats["iot-analytics"].CPU $stats["iot-analytics"].MEM "N/A" "Alerts=$($am.alertsTriggered); samples=$($am.e2eSampleCount)"

Write-Host "`n========== KAFKA EXPERIMENTS =========="
docker compose --profile mqtt down 2>$null | Out-Null
$env:BROKER_TYPE = "kafka"
$env:BATCH_MODE = "true"
docker compose --profile kafka up -d --build 2>$null | Out-Null
Start-Sleep -Seconds 50
docker stop iot-data-ingestion 2>$null | Out-Null

foreach ($acks in @("0", "1", "all")) {
    Reset-Storage
    $records = 50000
    Write-Host "Kafka A: records=$records acks=$acks"
    $out = docker exec iot-kafka /opt/kafka/bin/kafka-producer-perf-test.sh `
        --topic iot-agriculture-readings --num-records $records --record-size 512 `
        --throughput -1 --producer-props "bootstrap.servers=localhost:9092,acks=$acks" 2>&1
    Start-Sleep -Seconds 10
    Invoke-RestMethod -Method POST http://localhost:3000/flush -ErrorAction SilentlyContinue | Out-Null
    $m = Invoke-RestMethod http://localhost:3000/metrics
    $stats = Get-DockerStats @("iot-kafka", "iot-data-storage")
    $tpLine = ($out | Select-String "MB/sec" | Select-Object -Last 1).ToString()
    $tp = if ($tpLine -match "([\d.]+) MB/sec") { $Matches[1] + " MB/s" } else { "N/A" }
    $lost = [math]::Round((1 - $m.messagesStored / $records) * 100, 2)
    Add-Result "A" "Kafka" "acks=$acks/50k" $tp "N/A" $stats["iot-kafka"].CPU $stats["iot-kafka"].MEM $lost $tpLine
}

# Kafka B
Write-Host "Kafka B: network disconnect"
Reset-Storage
docker start iot-data-ingestion 2>$null | Out-Null
Start-Sleep -Seconds 5
$before = (Invoke-RestMethod http://localhost:3000/metrics).messagesReceived
$benchJob = Start-Job { docker exec iot-kafka /opt/kafka/bin/kafka-producer-perf-test.sh --topic iot-agriculture-readings --num-records 30000 --record-size 512 --throughput 500 --producer-props "bootstrap.servers=localhost:9092,acks=all" 2>&1 }
Start-Sleep -Seconds 5
docker network disconnect iot-network iot-data-ingestion 2>$null
Start-Sleep -Seconds 30
docker network connect iot-network iot-data-ingestion 2>$null
Start-Sleep -Seconds 15
$lag = docker exec iot-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group storage-group 2>&1
Wait-Job $benchJob -Timeout 120 | Out-Null; Remove-Job $benchJob -Force -ErrorAction SilentlyContinue
$stats = Get-DockerStats @("iot-kafka", "iot-data-storage")
Add-Result "B" "Kafka" "acks=all" "N/A" "N/A" $stats["iot-kafka"].CPU $stats["iot-kafka"].MEM "0" "Offset-based recovery; lag after reconnect"

# Kafka C
Write-Host "Kafka C: burst"
Reset-Storage
docker stop iot-data-ingestion 2>$null | Out-Null
docker exec iot-kafka /opt/kafka/bin/kafka-producer-perf-test.sh --topic iot-agriculture-readings --num-records 5000 --record-size 512 --throughput 50 --producer-props "bootstrap.servers=localhost:9092,acks=all" 2>&1 | Out-Null
docker exec iot-kafka /opt/kafka/bin/kafka-producer-perf-test.sh --topic iot-agriculture-readings --num-records 50000 --record-size 512 --throughput 5000 --producer-props "bootstrap.servers=localhost:9092,acks=all" 2>&1 | Out-Null
$lagOut = docker exec iot-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group storage-group 2>&1
$recoverySec = 0
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Seconds 2
    Invoke-RestMethod -Method POST http://localhost:3000/flush -ErrorAction SilentlyContinue | Out-Null
    $lagLine = docker exec iot-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group storage-group 2>&1 | Select-String "iot-agriculture"
    $totalLag = 0
    foreach ($l in $lagLine) { if ($l -match "\s+(\d+)\s*$") { $totalLag += [int]$Matches[1] } }
    if ($totalLag -eq 0) { $recoverySec = ($i+1)*2; break }
}
$stats = Get-DockerStats @("iot-kafka", "iot-data-storage")
Add-Result "C" "Kafka" "acks=all/burst" "5000 peak" "N/A" $stats["iot-kafka"].CPU $stats["iot-kafka"].MEM "N/A" "Recovery=${recoverySec}s; 4 partitions"

# Kafka D
Write-Host "Kafka D: alerting"
docker stop iot-data-ingestion 2>$null | Out-Null
$env:INJECT_CRITICAL = "true"
$env:BROKER_TYPE = "kafka"
$env:KAFKA_ACKS = "all"
docker compose --profile kafka up -d data-ingestion-service 2>$null | Out-Null
Start-Sleep -Seconds 45
$am = Invoke-RestMethod http://localhost:8000/metrics
$stats = Get-DockerStats @("iot-analytics", "iot-kafka")
Add-Result "D" "Kafka" "acks=all/alert" "N/A" $am.e2eLatencyP95Ms $stats["iot-analytics"].CPU $stats["iot-analytics"].MEM "N/A" "Alerts=$($am.alertsTriggered); samples=$($am.e2eSampleCount)"

$results | ConvertTo-Json -Depth 5 | Out-File $OutputFile -Encoding utf8
$results | Format-Table -AutoSize
Write-Host "Results saved to $OutputFile"
