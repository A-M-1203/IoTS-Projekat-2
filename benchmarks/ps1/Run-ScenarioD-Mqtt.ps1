param(
    [string]$BashPath = "bash"
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$Script = Join-Path $Root "benchmarks\mqtt\scenario-d\run.sh"

Write-Host "Running Scenario D (MQTT) via $Script"
& $BashPath $Script
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
