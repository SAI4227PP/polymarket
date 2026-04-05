.PHONY: rust-run go-dry go-live fmt docker-build docker-up docker-down

rust-run:
	cd rust-core && cargo run -p trader

go-dry:
	cd go-services && go run ./cmd/dry-run

go-live:
	cd go-services && go run ./cmd/live-run

fmt:
	cd rust-core && cargo fmt
	cd go-services && go fmt ./...

docker-build:
	docker compose build

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down
