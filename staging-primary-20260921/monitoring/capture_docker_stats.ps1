param(
    [Parameter(Mandatory = $true)]
    [string]$Output,

    [string[]]$Nodes = @('r-scylla1', 'r-scylla2', 'r-scylla3')
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
    $lines = & docker stats --no-stream --format '{{json .}}' @Nodes
    if ($LASTEXITCODE -ne 0) {
        throw "docker stats failed with exit code $LASTEXITCODE"
    }

    foreach ($line in $lines) {
        $record = $line | ConvertFrom-Json
        [pscustomobject]@{
            timestamp_utc = $timestamp
            sample         = $sample
            node           = $record.Name
            cpu_percent    = $record.CPUPerc
            memory_usage   = $record.MemUsage
            network_io     = $record.NetIO
            block_io       = $record.BlockIO
        } | Export-Csv -Path $Output -NoTypeInformation -Append
    }

    $sample++
    Start-Sleep -Seconds 1
}
