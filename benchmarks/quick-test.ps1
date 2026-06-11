# Brza verzija eksperimenata (manji broj klijenata/zapisa, ~5 min)

$ErrorActionPreference = "Continue"
Set-Location (Split-Path $PSScriptRoot -Parent)
New-Item -ItemType Directory -Force -Path "results" | Out-Null

Write-Host "Brzi MQTT test (10 klijenata, QoS 1)..."
docker stop iot-data-ingestion 2>$null | Out-Null
$sw = [System.Diagnostics.Stopwatch]::StartNew()
docker run --rm --network iot-network emqx/emqtt-bench pub `
    -h mosquitto -p 1883 -t iot/agriculture/readings -c 10 -I 10 -q 1 -n 100 2>&1 | Select-Object -Last 2
$sw.Stop()
Write-Host "MQTT throughput: ~$([math]::Round(1000/$sw.Elapsed.TotalSeconds)) msg/s ($($sw.Elapsed.TotalSeconds)s)"
docker stats --no-stream iot-mosquitto --format "Mosquitto CPU={{.CPUPerc}} MEM={{.MemUsage}}"

Write-Host "`nBrzi Kafka test (10000 zapisa, acks=all)..."
$out = docker exec iot-kafka /opt/kafka/bin/kafka-producer-perf-test.sh `
    --topic iot-agriculture-readings --num-records 10000 --record-size 512 `
    --throughput -1 --producer-props "bootstrap.servers=localhost:9092,acks=all" 2>&1
$out | Select-Object -Last 3
docker stats --no-stream iot-kafka --format "Kafka CPU={{.CPUPerc}} MEM={{.MemUsage}}"

Write-Host "`nStorage metrike:"
Invoke-RestMethod http://localhost:3000/metrics | ConvertTo-Json

Write-Host "`nZa kompletne eksperimente pokrenite: .\benchmarks\run-experiments.ps1"
