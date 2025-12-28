GO ?= go
BUF ?= buf
GOLANGCI_LINT ?= golangci-lint
GORELEASER ?= goreleaser
PROTOC_GEN_GO ?= protoc-gen-go
PROTOC_GEN_GO_GRPC ?= protoc-gen-go-grpc
GOBIN ?= $(shell $(GO) env GOPATH)/bin
PROFILE ?= small
TARGETS ?= all
SCENARIO ?= base

.PHONY: fmt lint test test-integration proto tidy release release-snapshot proto-tools bench bench-ddl bench-up bench-down benchmark benchmark-profile

fmt:
	$(GO) fmt ./...

lint:
	$(GOLANGCI_LINT) run ./...

test:
	$(GO) test ./...

test-integration:
	$(GO) test ./tests/...

proto: proto-tools
	PATH="$(GOBIN):$$PATH" $(BUF) generate

proto-tools:
	GOBIN="$(GOBIN)" $(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	GOBIN="$(GOBIN)" $(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

tidy:
	GOFLAGS='-tags=tools' $(GO) mod tidy

release:
	$(GORELEASER) release --clean

release-snapshot:
	$(GORELEASER) release --snapshot --clean

bench-up:
	docker compose -f bench/docker-compose.yml up -d

bench-down:
	docker compose -f bench/docker-compose.yml down

bench: bench-up
	$(GO) run ./cmd/wallaby-bench -profile $(PROFILE) -targets $(TARGETS) -scenario $(SCENARIO)

bench-ddl:
	$(MAKE) bench SCENARIO=ddl

benchmark:
	./bench/benchmark.sh

benchmark-profile:
	ENABLE_PROFILES=1 PROFILE_FORMAT=both ./bench/benchmark.sh
