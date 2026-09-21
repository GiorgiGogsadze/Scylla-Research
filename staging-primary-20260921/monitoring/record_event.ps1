param(
    [Parameter(Mandatory = $true)][string]$Output,
    [Parameter(Mandatory = $true)]
    [ValidateSet('join_command_start','join_command_end','node4_started',
                 'decommission_command_start','decommission_command_end',
                 'node4_absent','phase_start','phase_end')]
    [string]$Event,
    [string]$Phase = ''
)

$ErrorActionPreference = 'Stop'
$parent = Split-Path -Parent $Output
if ($parent) { New-Item -ItemType Directory -Path $parent -Force | Out-Null }
$record = [ordered]@{
    event = $Event
    phase = $Phase
    host_utc = [DateTimeOffset]::UtcNow.ToString('o')
    node4_container_started_utc = $null
}
if ($Event -eq 'node4_started') {
    $started = & docker inspect r-scylla4 --format '{{.State.StartedAt}}'
    if ($LASTEXITCODE -ne 0) { throw 'docker inspect r-scylla4 failed' }
    $record.node4_container_started_utc = $started.Trim()
}
($record | ConvertTo-Json -Compress) | Add-Content -Path $Output -Encoding utf8
Write-Host "$Event at $($record.host_utc)"
