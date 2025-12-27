GO ?= go
BUF ?= buf
GOLANGCI_LINT ?= golangci-lint

.PHONY: fmt lint test test-integration proto tidy

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
