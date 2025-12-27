GO ?= go
BUF ?= buf
GOLANGCI_LINT ?= golangci-lint
GORELEASER ?= goreleaser

.PHONY: fmt lint test test-integration proto tidy release release-snapshot

fmt:
	$(GO) fmt ./...

lint:
	$(GOLANGCI_LINT) run ./...

test:
	$(GO) test ./...

test-integration:
	$(GO) test ./tests/...

proto:
	$(BUF) generate

tidy:
	GOFLAGS='-tags=tools' $(GO) mod tidy

release:
	$(GORELEASER) release --clean

release-snapshot:
	$(GORELEASER) release --snapshot --clean
