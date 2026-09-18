param(
    [Parameter(Mandatory = $true)]
    [string]$Output
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

    $lines = @(& docker stats --no-stream --format '{{json .}}' @nodes)
    if ($LASTEXITCODE -ne 0) {
        throw "docker stats failed with exit code $LASTEXITCODE"
    }
    if ($lines.Count -ne $nodes.Count) {
        throw "Expected $($nodes.Count) docker stats rows; got $($lines.Count)"
    }

    $records = @(
        foreach ($line in $lines) {
            $record = $line | ConvertFrom-Json
            if ($nodes -notcontains $record.Name) {
                throw "Unexpected stats node: $($record.Name)"
            }
            [pscustomobject]@{
                timestamp_utc = $timestamp
                sample        = $sample
                node          = $record.Name
                node_count    = $nodes.Count
                cpu_percent   = $record.CPUPerc
                memory_usage  = $record.MemUsage
                network_io    = $record.NetIO
                block_io      = $record.BlockIO
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
