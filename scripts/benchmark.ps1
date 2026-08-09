[CmdletBinding()]
param(
    [switch]$OpenReport
)

$ErrorActionPreference = "Stop"

Write-Host "Running the Criterion in-memory store benchmarks."
& cargo bench --bench store
if ($LASTEXITCODE -ne 0) {
    throw "Criterion benchmark failed with exit code $LASTEXITCODE."
}

$report = Join-Path $PSScriptRoot "..\target\criterion\report\index.html"
if ($OpenReport -and (Test-Path -LiteralPath $report)) {
    Invoke-Item $report
}
