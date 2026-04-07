param(
    [int]$WarmupSeconds = 12
)

$ErrorActionPreference = "Stop"

Write-Host "Paper run with paper risk + portfolio checks"

if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
    throw "cargo not found in PATH. Install Rust toolchain before running paper validation."
}
if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    throw "go not found in PATH. Install Go toolchain before running paper validation."
}

$env:MODE = "paper"

Push-Location "$PSScriptRoot\..\rust-core"
$trader = Start-Process -FilePath "cargo" -ArgumentList "run -p trader" -PassThru -NoNewWindow
Pop-Location

try {
    Start-Sleep -Seconds $WarmupSeconds

    Push-Location "$PSScriptRoot\..\go-services"
    go run ./cmd/dry-run
    Pop-Location

    Write-Host "Paper validation completed. Check Redis trader state for portfolio_* fields."
}
finally {
    if ($null -ne $trader -and -not $trader.HasExited) {
        Stop-Process -Id $trader.Id -Force
    }
}
