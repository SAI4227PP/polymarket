param()
Write-Host "Running Rust and Go checks"
Set-Location "$PSScriptRoot\..\rust-core"
cargo check
Set-Location "$PSScriptRoot\..\go-services"
go test ./...
