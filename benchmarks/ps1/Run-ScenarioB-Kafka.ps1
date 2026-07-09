param(
    [string]$BashPath = "bash"
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$Script = Join-Path $Root "benchmarks\kafka\scenario-b\run.sh"

Write-Host "Running Scenario B (Kafka) via $Script"
& $BashPath $Script
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
