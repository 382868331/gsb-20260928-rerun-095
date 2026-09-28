.PHONY: test fmt run-v1 run-v2 run-gateway

fmt:
	gofmt -w cmd internal

test:
	go test -race ./...

run-v1:
	go run ./cmd/local-service -addr 127.0.0.1:18081 -name v1

run-v2:
	go run ./cmd/local-service -addr 127.0.0.1:18082 -name v2

run-gateway:
	go run ./cmd/gateway -config configs/gateway.json
