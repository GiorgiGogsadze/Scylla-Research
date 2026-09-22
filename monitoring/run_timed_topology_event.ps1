<#
Wait for the first measured CSV request and start a join or graceful departure
120 seconds later. Run in a separate PowerShell process BEFORE the measured
workload. Does not stop node 4 after decommissioning.
#>
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('scaleout', 'scalein')]
    [string]$Phase,

    [Parameter(Mandatory = $true)]
    [string]$LifecycleDir,

    [string]$ProjectRoot = '.',
    [string]$ComposeProject = 'primaryb1tablets',
    [int]$TargetOffsetSeconds = 120
)

$ErrorActionPreference = 'Stop'
if ($TargetOffsetSeconds -le 0) { throw 'TargetOffsetSeconds must be positive' }
$ProjectRoot = (Resolve-Path $ProjectRoot).Path
$LifecycleDir = (Resolve-Path $LifecycleDir).Path
$csv = Join-Path $LifecycleDir "$Phase\measured.csv"
$trigger = Join-Path $LifecycleDir $(if ($Phase -eq 'scaleout') { 'join.trigger' } else { 'departure.trigger' })
$events = Join-Path $LifecycleDir 'events.jsonl'
$eventHelper = Join-Path $ProjectRoot 'monitoring\record_event.ps1'
if (-not (Test-Path $eventHelper -PathType Leaf)) { throw "Missing event helper: $eventHelper" }
if (Test-Path $csv) { throw "Measured CSV already exists: $csv" }
if (Test-Path $trigger) { throw "Event trigger already exists: $trigger" }

$originalIds = @(
    foreach ($node in 1..3) {
        $id = & docker inspect "r-scylla$node" --format '{{.Id}}'
        if ($LASTEXITCODE -ne 0) { throw "Node $node inspect failed" }
        $id.Trim()
    }
)
if ($Phase -eq 'scaleout') {
    $fourth = & docker ps -a --filter 'name=^/r-scylla4$' --format '{{.Names}}'
    if ($LASTEXITCODE -ne 0 -or $fourth) { throw 'Node 4 already exists or Docker query failed' }
} else {
    $fourthRunning = & docker inspect r-scylla4 --format '{{.State.Running}}'
    if ($LASTEXITCODE -ne 0 -or $fourthRunning.Trim() -ne 'true') {
        throw 'Node 4 must be running before scale-in'
    }
}

Write-Host 'RESULTS'
Write-Host "event_controller_armed=True phase=$Phase waiting_for_first_measured_request=True"
do {
    Start-Sleep -Milliseconds 400
    if (Test-Path $csv) {
        $lines = @(Get-Content -Path $csv -TotalCount 2)
    } else {
        $lines = @()
    }
} until ($lines.Count -ge 2)

$firstRow = @($lines | ConvertFrom-Csv)[0]
if ([int]$firstRow.index -ne 0) { throw 'First saved request is not index 0' }
$firstRequest = [datetimeoffset]::Parse($firstRow.timestamp_utc)
$target = $firstRequest.AddSeconds($TargetOffsetSeconds)
Write-Host "first_request=$($firstRequest.ToString('o')) target_event=$($target.ToString('o'))"
if ([datetimeoffset]::UtcNow -ge $target) { throw 'Event target already passed before controller was ready' }
while ([datetimeoffset]::UtcNow -lt $target) { Start-Sleep -Milliseconds 150 }

if ($Phase -eq 'scaleout') {
    & $eventHelper -Output $events -Event join_command_start -Phase $Phase
    if (-not $?) { throw 'Join start marker failed' }
    New-Item -ItemType File -Path $trigger -ErrorAction Stop | Out-Null
    & docker compose -p $ComposeProject `
        -f (Join-Path $ProjectRoot 'docker\scylla-research-nodes.yml') `
        -f (Join-Path $ProjectRoot 'docker\scylla-exp2-node4.yml') `
        up -d --no-deps --no-recreate r-scylla4
    if ($LASTEXITCODE -ne 0) { throw 'Node 4 join command failed' }
    & $eventHelper -Output $events -Event join_command_end -Phase $Phase
    & $eventHelper -Output $events -Event node4_started -Phase $Phase
} else {
    & $eventHelper -Output $events -Event decommission_command_start -Phase $Phase
    if (-not $?) { throw 'Decommission start marker failed' }
    New-Item -ItemType File -Path $trigger -ErrorAction Stop | Out-Null
    & docker exec r-scylla4 nodetool decommission
    if ($LASTEXITCODE -ne 0) { throw 'Node 4 decommission failed; do not stop it' }
    & $eventHelper -Output $events -Event decommission_command_end -Phase $Phase
}
if (-not $?) { throw 'Topology event end marker failed' }

foreach ($node in 1..3) {
    $id = & docker inspect "r-scylla$node" --format '{{.Id}}'
    if ($LASTEXITCODE -ne 0 -or $id.Trim() -ne $originalIds[$node - 1]) {
        throw "Original node $node container identity changed"
    }
}
Write-Host "RESULTS event_command_completed=True phase=$Phase original_three_container_ids_preserved=True"
