param(
    [string]$BashPath = "bash"
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$Script = Join-Path $Root "benchmarks\kafka\scenario-d\run.sh"

Write-Host "Running Scenario D (Kafka) via $Script"
& $BashPath $Script
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
