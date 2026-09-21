param(
    [Parameter(Mandatory = $true)][string]$OutputDirectory,
    [Parameter(Mandatory = $true)][string]$Keyspace,
    [double]$IntervalSeconds = 2
)

$ErrorActionPreference = 'Stop'
if ($IntervalSeconds -le 0) { throw 'IntervalSeconds must be positive' }
if ($Keyspace -notmatch '^[a-z][a-z0-9_]*$') { throw 'Invalid keyspace' }
if (Test-Path $OutputDirectory) { throw "Output directory already exists: $OutputDirectory" }
New-Item -ItemType Directory -Path $OutputDirectory | Out-Null
$indexPath = Join-Path $OutputDirectory 'index.csv'
$sample = 0
while ($true) {
    $started = [DateTimeOffset]::UtcNow
    $ring = @(& docker exec r-scylla1 nodetool ring $Keyspace 2>&1)
    if ($LASTEXITCODE -ne 0) { throw "nodetool ring failed at sample $sample`: $($ring -join ' ')" }
    $name = 'ring_{0:D6}.txt' -f $sample
    $path = Join-Path $OutputDirectory $name
    $ring | Set-Content -Path $path -Encoding utf8
    $finished = [DateTimeOffset]::UtcNow
    $addresses = @($ring | Where-Object { $_ -match '^\s*172\.29\.0\.1[1-4]\s+' })
    $record = [pscustomobject]@{
        sample = $sample
        start_utc = $started.ToString('o')
        end_utc = $finished.ToString('o')
        file = $name
        address_rows = $addresses.Count
        sha256 = (Get-FileHash -Path $path -Algorithm SHA256).Hash
    }
    $record | Export-Csv -Path $indexPath -NoTypeInformation -Append
    if ($sample -eq 0 -or $sample % 30 -eq 0) {
        Write-Host "Ring sample $sample at $($started.ToString('o')); address rows=$($addresses.Count)"
    }
    $sample++
    $remaining = $IntervalSeconds - ($finished - $started).TotalSeconds
    if ($remaining -gt 0) { Start-Sleep -Milliseconds ([int]($remaining * 1000)) }
}
