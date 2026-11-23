<#
  Create .env from .env.example with strong random secrets (the Windows twin of what bootstrap_vm.sh does).
  Never overwrites an existing file.   Usage: powershell -NoProfile -File scripts\init_env.ps1
#>
param([string]$Path = "")
$ErrorActionPreference = "Stop"
$root = Split-Path $PSScriptRoot -Parent
if (-not $Path) { $Path = Join-Path $root ".env" }
if (Test-Path $Path) {
    Write-Host "$Path already exists; leaving it alone."
    exit 0
}

function New-Secret([int]$Bytes = 24) {
    $buf = New-Object byte[] $Bytes
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    $rng.GetBytes($buf)
    $rng.Dispose()
    return (($buf | ForEach-Object { $_.ToString("x2") }) -join "")
}

$text = Get-Content (Join-Path $root ".env.example") -Raw
$text = $text -replace '(?m)^S3_SECRET_KEY=.*$', ("S3_SECRET_KEY=" + (New-Secret))
$text = $text -replace '(?m)^GRAFANA_ADMIN_PASSWORD=.*$', ("GRAFANA_ADMIN_PASSWORD=" + (New-Secret))
# A shared PC has less headroom than a dedicated VM: two wikis were measured at about 5.5 GB of RAM.
$text = $text -replace '(?m)^WIKIS=.*$', 'WIKIS=enwiki,dewiki'
[System.IO.File]::WriteAllText($Path, $text, (New-Object System.Text.UTF8Encoding($false)))
Write-Host "Created $Path with generated passwords."
Write-Host "Next: edit USER_AGENT so it includes your contact email (Wikimedia asks automated clients for one)."
