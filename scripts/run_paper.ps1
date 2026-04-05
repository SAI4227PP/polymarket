param()
Write-Host "Paper run"
Set-Location "$PSScriptRoot\..\rust-core"
cargo run -p trader
Set-Location "$PSScriptRoot\..\go-services"
go run ./cmd/dry-run
