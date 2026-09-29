.PHONY: test test-race vet fmt build run tidy

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	go fmt ./...

build:
	go build -o bin/distrikv ./cmd/server

run:
	go run ./cmd/server

tidy:
	go mod tidy
