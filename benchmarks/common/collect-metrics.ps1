param(
    [int]$DurationSeconds = 60,
    [int]$IntervalSeconds = 5,
    [string]$OutputFile = "metrics.csv"
)

$containers = @(
    "iot-postgres",
    "iot-mosquitto",
    "iot-kafka",
    "iot-data-storage",
    "iot-analytics",
    "iot-data-ingestion"
)

$header = "timestamp,container,cpu_percent,mem_usage,mem_limit,net_io,block_io"
$header | Out-File -FilePath $OutputFile -Encoding utf8

$endTime = (Get-Date).AddSeconds($DurationSeconds)
Write-Host "Collecting docker stats for $DurationSeconds seconds -> $OutputFile"

while ((Get-Date) -lt $endTime) {
    $timestamp = (Get-Date).ToString("o")
    foreach ($name in $containers) {
        $running = docker inspect -f "{{.State.Running}}" $name 2>$null
        if ($running -ne "true") { continue }

        $stats = docker stats $name --no-stream --format "{{.CPUPerc}},{{.MemUsage}},{{.NetIO}},{{.BlockIO}}" 2>$null
        if ($stats) {
            $parts = $stats -split ","
            $cpu = $parts[0] -replace "%", ""
            $memParts = ($parts[1] -split " / ")
            $memUsage = $memParts[0]
            $memLimit = if ($memParts.Count -gt 1) { $memParts[1] } else { "N/A" }
            $netIo = $parts[2]
            $blockIo = $parts[3]
            "$timestamp,$name,$cpu,$memUsage,$memLimit,$netIo,$blockIo" | Out-File -FilePath $OutputFile -Append -Encoding utf8
        }
    }
    Start-Sleep -Seconds $IntervalSeconds
}

Write-Host "Metrics saved to $OutputFile"

# Fetch service metrics
Write-Host "`n--- Storage Metrics ---"
try {
    Invoke-RestMethod -Uri "http://localhost:3000/metrics" | ConvertTo-Json -Depth 5
} catch {
    Write-Host "Storage metrics unavailable: $_"
}

Write-Host "`n--- Analytics Metrics ---"
try {
    Invoke-RestMethod -Uri "http://localhost:8000/metrics" | ConvertTo-Json -Depth 5
} catch {
    Write-Host "Analytics metrics unavailable: $_"
}

Write-Host "`n--- Ingestion Metrics ---"
try {
    Invoke-RestMethod -Uri "http://localhost:8080/metrics" | ConvertTo-Json -Depth 5
} catch {
    Write-Host "Ingestion metrics unavailable: $_"
}
