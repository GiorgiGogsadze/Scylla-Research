param(
    [Parameter(Mandatory = $true)]
    [string]$Output,
    [Parameter(Mandatory = $true)]
    [string]$DepartureTriggerFile
)

$ErrorActionPreference = 'Stop'
$parent = Split-Path -Parent $Output
if ($parent) {
    New-Item -ItemType Directory -Path $parent -Force | Out-Null
}
if (Test-Path $Output) {
    throw "Output already exists: $Output"
}

$sample = 0
$fourthSeen = $false
$fourthDeparted = $false
while ($true) {
    $timestamp = [DateTimeOffset]::UtcNow.ToString('o')
    $running = @(& docker ps --format '{{.Names}}')
    if ($LASTEXITCODE -ne 0) {
        throw "docker ps failed with exit code $LASTEXITCODE"
    }
    $nodes = @($running | Where-Object { $_ -match '^r-scylla[1-4]$' } | Sort-Object)
    $required = @('r-scylla1', 'r-scylla2', 'r-scylla3')
    foreach ($node in $required) {
        if ($nodes -notcontains $node) {
            throw "Required node is not running: $node"
        }
    }
    if ($nodes -contains 'r-scylla4') {
        if ($fourthDeparted) {
            throw 'Node 4 reappeared after its recorded departure'
        }
        $fourthSeen = $true
    } elseif ($fourthSeen -and -not $fourthDeparted) {
        if (-not (Test-Path $DepartureTriggerFile)) {
            throw 'Node 4 disappeared before the departure trigger was created'
        }
        $fourthDeparted = $true
        Write-Host "Node 4 departed from running-container set at $timestamp"
    }

    $lines = @(& docker stats --no-stream --format '{{json .}}' @nodes)
    if ($LASTEXITCODE -ne 0) {
        throw "docker stats failed with exit code $LASTEXITCODE"
    }
    if ($lines.Count -ne $nodes.Count) {
        throw "Expected $($nodes.Count) docker stats rows; got $($lines.Count)"
    }

    $memLine = @(& docker exec r-scylla1 sh -c 'grep ^MemAvailable: /proc/meminfo')
    if ($LASTEXITCODE -ne 0 -or $memLine.Count -ne 1 -or
        $memLine[0] -notmatch '^MemAvailable:\s+(\d+)\s+kB') {
        throw 'Could not read Docker VM MemAvailable from node 1'
    }
    $availableKiB = [long]$Matches[1]
    $finished = [DateTimeOffset]::UtcNow.ToString('o')
    $records = @(
        foreach ($line in $lines) {
            $record = $line | ConvertFrom-Json
            if ($nodes -notcontains $record.Name) {
                throw "Unexpected stats node: $($record.Name)"
            }
            [pscustomobject]@{
                timestamp_utc = $timestamp
                sample_end_utc = $finished
                sample        = $sample
                node          = $record.Name
                node_count    = $nodes.Count
                cpu_percent   = $record.CPUPerc
                memory_usage  = $record.MemUsage
                network_io    = $record.NetIO
                block_io      = $record.BlockIO
                vm_mem_available_kib = $availableKiB
            }
        }
    )
    if (@($records | Select-Object -ExpandProperty node -Unique).Count -ne $nodes.Count) {
        throw "Duplicate or missing docker stats node in sample $sample"
    }
    $records | Export-Csv -Path $Output -NoTypeInformation -Append
    $sample++
    Start-Sleep -Seconds 1
}
