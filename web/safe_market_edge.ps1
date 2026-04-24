param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("init", "apply", "rollback", "list")]
    [string]$Action,

    [string]$BackupFile = ""
)

$ErrorActionPreference = "Stop"

$Target = "web/market_edge_engine.py"
$Baseline = "web/market_edge_engine.py.remote_try3"
$Work = "web/market_edge_engine.work.py"
$BackupDir = "web/.backups"

function Ensure-Paths {
    if (!(Test-Path $Baseline)) { throw "Missing baseline: $Baseline" }
    if (!(Test-Path $Target)) { throw "Missing target: $Target" }
    if (!(Test-Path $BackupDir)) { New-Item -ItemType Directory -Path $BackupDir | Out-Null }
}

function Compile-Or-Throw([string]$FilePath) {
    python -m py_compile $FilePath | Out-Host
    if ($LASTEXITCODE -ne 0) { throw "Compile failed: $FilePath" }
}

Ensure-Paths

switch ($Action) {
    "init" {
        Copy-Item $Baseline $Work -Force
        Compile-Or-Throw $Work
        Write-Output "INIT_OK -> $Work (from $Baseline)"
    }

    "apply" {
        if (!(Test-Path $Work)) { throw "Missing work file: $Work. Run init first." }

        # 1) compile work first
        Compile-Or-Throw $Work

        # 2) backup current target
        $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
        $backup = Join-Path $BackupDir "market_edge_engine.$stamp.py"
        Copy-Item $Target $backup -Force

        # 3) atomic replace target
        $tmp = "$Target.tmp"
        Copy-Item $Work $tmp -Force
        Move-Item $tmp $Target -Force

        # 4) compile target
        Compile-Or-Throw $Target
        Write-Output "APPLY_OK -> $Target"
        Write-Output "BACKUP -> $backup"
    }

    "list" {
        Get-ChildItem $BackupDir -Filter "market_edge_engine.*.py" |
            Sort-Object LastWriteTime -Descending |
            Select-Object LastWriteTime, FullName
    }

    "rollback" {
        if ([string]::IsNullOrWhiteSpace($BackupFile)) {
            throw "Provide -BackupFile with full path from list action."
        }
        if (!(Test-Path $BackupFile)) { throw "Backup not found: $BackupFile" }
        Copy-Item $BackupFile $Target -Force
        Compile-Or-Throw $Target
        Write-Output "ROLLBACK_OK -> $Target"
    }
}

