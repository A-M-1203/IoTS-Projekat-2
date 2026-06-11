param(
    [ValidateSet("mqtt", "kafka")]
    [string]$Profile = "mqtt"
)

$ErrorActionPreference = "Stop"
$root = Join-Path $PSScriptRoot "..\.."
Set-Location $root

Write-Host "=== Running all benchmarks for profile: $Profile ==="

if ($Profile -eq "mqtt") {
    $env:BROKER_TYPE = "mqtt"
    $env:BATCH_MODE = "true"
    $env:BATCH_SIZE = "500"
    docker compose --profile mqtt down -v 2>$null
    docker compose --profile mqtt up -d --build
    Write-Host "Waiting for services to start..."
    Start-Sleep -Seconds 30

    foreach ($qos in @(0, 1, 2)) {
        foreach ($clients in @(100, 1000)) {
            Write-Host "`n--- MQTT Scenario A: $clients clients, QoS $qos ---"
            & (Join-Path $PSScriptRoot "mqtt\scenario-a.ps1") -Clients $clients -Qos $qos -DurationSeconds 20
        }
    }

    Write-Host "`n--- MQTT Scenario B ---"
    & (Join-Path $PSScriptRoot "mqtt\scenario-b.ps1") -Qos 1

    Write-Host "`n--- MQTT Scenario C ---"
    & (Join-Path $PSScriptRoot "mqtt\scenario-c.ps1") -Qos 1

    Write-Host "`n--- MQTT Scenario D ---"
    & (Join-Path $PSScriptRoot "mqtt\scenario-d.ps1") -Qos 1 -DurationSeconds 30

} else {
    $env:BROKER_TYPE = "kafka"
    $env:BATCH_MODE = "true"
    $env:BATCH_SIZE = "500"
    docker compose --profile kafka down -v 2>$null
    docker compose --profile kafka up -d --build
    Write-Host "Waiting for Kafka and services to start..."
    Start-Sleep -Seconds 45

    foreach ($acks in @("0", "1", "all")) {
        foreach ($records in @(100000, 500000)) {
            Write-Host "`n--- Kafka Scenario A: $records records, acks=$acks ---"
            & (Join-Path $PSScriptRoot "kafka\scenario-a.ps1") -Records $records -Acks $acks
        }
    }

    Write-Host "`n--- Kafka Scenario B ---"
    & (Join-Path $PSScriptRoot "kafka\scenario-b.ps1") -Acks "all"

    Write-Host "`n--- Kafka Scenario C ---"
    & (Join-Path $PSScriptRoot "kafka\scenario-c.ps1") -Acks "all"

    Write-Host "`n--- Kafka Scenario D ---"
    & (Join-Path $PSScriptRoot "kafka\scenario-d.ps1") -Acks "all" -DurationSeconds 30
}

Write-Host "`n=== All benchmarks complete for $Profile ==="
