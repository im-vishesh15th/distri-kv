.PHONY: test test-race vet fmt lint build run tidy proto demo-up demo-down demo-monitoring e2e e2e-control

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

# --- product demo (needs Docker) ------------------------------------------
# demo-up: 3 nodes + gateway, creates a tenant + API key, prints a curl example.
demo-up:
	./scripts/demo_up.sh

# demo-monitoring: adds Prometheus + Grafana (http://localhost:3000).
demo-monitoring:
	docker compose --profile monitoring up -d --wait

# demo-down: stop everything and DELETE all data volumes.
demo-down:
	docker compose --profile monitoring down -v

# e2e: end-to-end product checks against a running stack (run demo-up first).
e2e:
	./scripts/e2e_signup.sh

# e2e-control: signup -> login -> keys -> data plane, through the control API.
e2e-control:
	./scripts/e2e_control.sh
