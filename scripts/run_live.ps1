param()
Write-Host "Live run"
Set-Location "$PSScriptRoot\..\go-services"
go run ./cmd/live-run
