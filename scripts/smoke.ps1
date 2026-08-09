[CmdletBinding()]
param(
    [string]$CliPath = ".\target\release\forgekv-cli.exe"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $CliPath)) {
    throw "ForgeKV CLI not found at '$CliPath'. Build it before running this manual smoke test."
}

& $CliPath ping
& $CliPath set smoke:key smoke-value
& $CliPath get smoke:key
& $CliPath exists smoke:key
& $CliPath setex smoke:session 60 session-value
& $CliPath ttl smoke:session
& $CliPath del smoke:key
& $CliPath stats
