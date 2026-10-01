.PHONY: test test-race vet fmt lint build run tidy proto

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	go fmt ./...

# lint checks without rewriting: fail if any file needs gofmt, then vet.
lint:
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt -l reported (run 'make fmt'):"; \
		echo "$$unformatted"; \
		exit 1; \
	fi
	go vet ./...

proto:
	protoc \
		--go_out=. --go_opt=module=distrikv \
		--go-grpc_out=. --go-grpc_opt=module=distrikv \
		proto/kv.proto proto/raft.proto

build:
	go build -o bin/distrikv ./cmd/server

run:
	go run ./cmd/server

tidy:
	go mod tidy
