<#
  Publish the dashboard from a machine GitHub cannot reach (no VM needed).

  Takes the latest snapshot from the running stack, adds it to the static site, and force-pushes the
  result as a single commit to the gh-pages branch, so repository history never grows.
  In GitHub: Settings > Pages > Source: "Deploy from a branch", branch gh-pages, folder / (root).

  Usage:   powershell -NoProfile -File scripts\publish_local.ps1
  Options: -RemoteUrl <url>   push somewhere other than 'origin'
           -DataDir <folder>  use *.json from a folder instead of the running airflow container
#>
param(
    [string]$RemoteUrl = "",
    [string]$Branch = "gh-pages",
    [string]$DataDir = "",
    [int]$MaxLagMinutes = 45
)
$ErrorActionPreference = "Continue"
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root

function Assert-Ok([string]$what) {
    if ($LASTEXITCODE -ne 0) { throw "$what failed (exit code $LASTEXITCODE)" }
}

if (-not $RemoteUrl) {
    $RemoteUrl = (git config --get remote.origin.url)
    if (-not $RemoteUrl) { throw "No git remote named origin. Add one or pass -RemoteUrl." }
}

$work = Join-Path ([System.IO.Path]::GetTempPath()) ("lakewire-publish-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $work | Out-Null
try {
    # The site itself, without any stale local data folder.
    Copy-Item -Path (Join-Path $root "site\*") -Destination $work -Recurse -Exclude "data"
    $dest = Join-Path $work "data"
    New-Item -ItemType Directory -Path $dest -Force | Out-Null

    if ($DataDir) {
        Copy-Item -Path (Join-Path $DataDir "*.json") -Destination $dest
    } else {
        docker compose cp "airflow:/data/exports/." $dest
        Assert-Ok "copying the snapshot from the airflow container (is the stack running?)"
    }

    $healthFile = Join-Path $dest "health.json"
    if (-not (Test-Path $healthFile)) { throw "health.json is missing; the hourly marts job has not produced a snapshot yet." }
    $lag = (Get-Content $healthFile -Raw | ConvertFrom-Json).lag_seconds
    if ($null -eq $lag) {
        Write-Warning "The snapshot has no event timestamp yet."
    } elseif ($lag -gt ($MaxLagMinutes * 60)) {
        Write-Warning ("Data is {0:N0} minutes old (limit {1}). Publishing anyway; the dashboard will show it as stale." -f ($lag / 60), $MaxLagMinutes)
    }

    New-Item -ItemType File -Path (Join-Path $work ".nojekyll") -Force | Out-Null
    git -C $work init -q -b $Branch;                                   Assert-Ok "git init"
    git -C $work add -A;                                               Assert-Ok "git add"
    git -C $work -c user.name="lakewire-publisher" -c user.email="publisher@users.noreply.github.com" `
        commit -q -m ("Publish dashboard snapshot " + (Get-Date).ToUniversalTime().ToString("u"));  Assert-Ok "git commit"
    git -C $work push -q --force $RemoteUrl "${Branch}:${Branch}";     Assert-Ok "git push"
    Write-Host "Published to branch '$Branch'."
}
finally {
    Remove-Item -Recurse -Force $work -ErrorAction SilentlyContinue
}
