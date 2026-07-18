GO ?= go
BUF ?= buf
GOLANGCI_LINT ?= golangci-lint
STATICCHECK_VERSION ?= v0.6.1
GOVULNCHECK_VERSION ?= v1.1.4
STATICCHECK_CMD ?= $(GOENV) $(GO) run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION)
GOVULNCHECK_CMD ?= $(GOENV) $(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)
GORELEASER ?= goreleaser
PROTOC_GEN_GO ?= protoc-gen-go
PROTOC_GEN_GO_GRPC ?= protoc-gen-go-grpc
PROTOC_GEN_GO_VERSION ?= v1.36.11
PROTOC_GEN_GO_GRPC_VERSION ?= v1.6.0
GOMARKDOC_VERSION ?= v1.1.0
PROTOC_GEN_DOC_VERSION ?= v1.5.1
UV ?= uv
UV_VERSION ?= 0.11.29
GOBIN ?= $(shell $(GO) env GOPATH)/bin
CACHE_DIR ?= .cache
GOMODCACHE ?= $(abspath $(CACHE_DIR)/gomod)
GOCACHE ?= $(abspath $(CACHE_DIR)/gocache)
GOLANGCI_LINT_CACHE ?= $(abspath $(CACHE_DIR)/golangci-lint)
GOENV ?= GOMODCACHE="$(GOMODCACHE)" GOCACHE="$(GOCACHE)"
PROFILE ?= small
TARGETS ?= all
SCENARIO ?= base
TLC ?= tlc2.TLC
TLA_MODULE ?= specs/CDCFlow.tla
TLA_CONFIG ?= specs/CDCFlow.cfg
TLA_TOOLS_TAG ?= v1.7.4
TLA_TOOLS_URL ?= https://github.com/tlaplus/tlaplus/releases/download/$(TLA_TOOLS_TAG)/tla2tools.jar
TLA_TOOLS_DIR ?= $(abspath $(CACHE_DIR)/tla)
TLA_TOOLS_JAR ?= $(TLA_TOOLS_DIR)/tla2tools.jar
TLA_TOOLS_SHA256_FILE ?= scripts/tla2tools-$(TLA_TOOLS_TAG).sha256
TLC_JAVA_OPTS ?= -XX:+UseParallelGC
TLC_ARGS ?=
SKIP_TLA_CHECKS ?= false
TLC_COVERAGE_DIR ?= specs/coverage
TLA_COVERAGE_MIN ?= 1
TLA_COVERAGE_IGNORE ?=
TRACE_CASES ?= 1000
TRACE_SEED ?= 1
TRACE_MAX_BATCHES ?= 10
TRACE_MAX_RECORDS ?= 3
RAPID_CHECKS ?= 100
RAPID_PACKAGES ?= ./pkg/stream ./pkg/wire ./internal/ddl ./internal/registry ./internal/workflow ./connectors/sources/postgres
SPEC_LINT_VERBOSE ?=
SPEC_LINT_VERBOSE_MODE ?= checks

export GO_TEST_TIMEOUT

GO_TEST_VERBOSE ?= 1
GO_TEST_VERBOSE_FLAG ?=

.PHONY: fmt lint staticcheck vulncheck lint-full test test-rapid test-integration test-integration-ci test-integration-kind test-e2e test-k8s-kind proto generate generate-check tidy release release-snapshot proto-tools tla-tools bench bench-ddl bench-up bench-down benchmark benchmark-profile benchstat check check-coverage check-tla tla tla-single tla-flow tla-state tla-fanout tla-liveness tla-witness tla-coverage tla-coverage-check trace-suite trace-suite-large spec-manifest spec-verify spec-lint spec-sync
.PHONY: proto-lint proto-breaking docs-tools docs-generate docs-verify docs-verify-selftest docs-build docs-preview docs-links docs-check release-tag-check

# Integration test harness knobs.
# Set defaults in the caller env or override in CI to tune test behavior.
.PHONY: check-lite check-integration-core check-integration-full
IT_KIND ?=
IT_KEEP ?= 0
IT_KIND_CLUSTER ?=
IT_KIND_NODE_IMAGE ?=
IT_SERVICE_READY_TIMEOUT_SECONDS ?= 240
IT_RUN_FILTER ?=
IT_COUNT ?=
IT_PACKAGE_PARALLELISM ?= 1
IT_EXPECTED_HARNESS_PARTICIPANTS ?= $(IT_PACKAGE_PARALLELISM)
INTEGRATION_PACKAGE ?= ./tests/...

fmt:
	$(GO) fmt ./...

lint:
	GOMODCACHE="$(GOMODCACHE)" GOCACHE="$(GOCACHE)" GOLANGCI_LINT_CACHE="$(GOLANGCI_LINT_CACHE)" $(GOLANGCI_LINT) run ./...

staticcheck:
	$(STATICCHECK_CMD) ./...

vulncheck:
	$(GOVULNCHECK_CMD) ./...

lint-full: lint staticcheck vulncheck proto-lint proto-breaking

check: spec-verify spec-sync spec-lint check-tla
	$(GOENV) $(GO) test ./...

check-tla:
	SKIP_TLA_CHECKS="$(SKIP_TLA_CHECKS)" MAKE="$(MAKE)" ./scripts/check-tla.sh

check-lite: spec-sync spec-lint
	$(GOENV) $(GO) test ./cmd/wallaby-admin ./pkg/... ./internal/... ./connectors/... ./tests/...

check-coverage: spec-sync tla-coverage tla-coverage-check trace-suite test-e2e

test:
	$(GOENV) GO="$(GO)" GO_TEST_VERBOSE="$(GO_TEST_VERBOSE)" GO_TEST_VERBOSE_FLAG="$(GO_TEST_VERBOSE_FLAG)" ./scripts/test-go.sh

test-rapid:
	$(GOENV) GO="$(GO)" RAPID_PACKAGES="$(RAPID_PACKAGES)" RAPID_CHECKS="$(RAPID_CHECKS)" ./scripts/test-rapid.sh

test-integration:
	$(GOENV) GO="$(GO)" GO_TEST_VERBOSE="$(GO_TEST_VERBOSE)" GO_TEST_VERBOSE_FLAG="$(GO_TEST_VERBOSE_FLAG)" IT_KIND="$(IT_KIND)" IT_KEEP="$(IT_KEEP)" IT_KIND_CLUSTER="$(IT_KIND_CLUSTER)" IT_KIND_NODE_IMAGE="$(IT_KIND_NODE_IMAGE)" IT_SERVICE_READY_TIMEOUT_SECONDS="$(IT_SERVICE_READY_TIMEOUT_SECONDS)" IT_RUN_FILTER="$(IT_RUN_FILTER)" IT_COUNT="$(IT_COUNT)" IT_PACKAGE_PARALLELISM="$(IT_PACKAGE_PARALLELISM)" IT_EXPECTED_HARNESS_PARTICIPANTS="$(IT_EXPECTED_HARNESS_PARTICIPANTS)" INTEGRATION_PACKAGE="$(INTEGRATION_PACKAGE)" ./scripts/test-integration.sh

test-integration-ci: test-integration

test-integration-kind: IT_RUN_FILTER="^TestKubernetesDispatcher"
test-integration-kind: IT_COUNT=1
test-integration-kind: test-integration

test-e2e: IT_RUN_FILTER="^TestPostgresToPostgresE2E"
test-e2e: IT_COUNT=1
test-e2e: test-integration

test-k8s-kind: IT_RUN_FILTER="^TestKubernetesDispatcher"
test-k8s-kind: IT_COUNT=1
test-k8s-kind: test-integration

check-integration-core: test-integration

check-integration-full: test-integration test-e2e

proto: proto-tools
	rm -rf gen/go
	mkdir -p gen/go
	PATH="$(GOBIN):$$PATH" $(BUF) generate

generate: proto

generate-check: generate
	./scripts/generate-check.sh

proto-lint:
	$(BUF) lint

proto-breaking:
	./scripts/proto-breaking.sh "$(BUF)"

proto-tools:
	GOBIN="$(GOBIN)" $(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	GOBIN="$(GOBIN)" $(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

docs-tools:
	@command -v $(UV) >/dev/null 2>&1 || { echo "uv $(UV_VERSION) is required: https://docs.astral.sh/uv/" >&2; exit 1; }
	GOBIN="$(GOBIN)" $(GO) install github.com/princjef/gomarkdoc/cmd/gomarkdoc@$(GOMARKDOC_VERSION)
	GOBIN="$(GOBIN)" $(GO) install github.com/pseudomuto/protoc-gen-doc/cmd/protoc-gen-doc@$(PROTOC_GEN_DOC_VERSION)
	$(UV) sync --frozen

docs-generate: docs-tools
	PATH="$(GOBIN):$$PATH" GOBIN="$(GOBIN)" ./scripts/docs-generate.sh

# Nonmutating drift verification: generate into a temporary tree, then compare.
docs-verify: docs-tools
	./scripts/docs-verify.sh

docs-verify-selftest:
	./scripts/docs-verify-selftest.sh

docs-build: docs-generate
	$(UV) run --frozen mkdocs build --strict --clean

docs-preview: docs-generate
	$(UV) run --frozen mkdocs serve

docs-links: docs-build
	$(UV) run --frozen python ./scripts/docs-links.py site

# Developer convenience target: intentionally refresh generated docs in-tree,
# then perform the strict build and local site link check.
docs-check: docs-generate
	$(UV) run --frozen mkdocs build --strict --clean
	$(UV) run --frozen python ./scripts/docs-links.py site

spec-manifest:
	$(GOENV) $(GO) run ./cmd/wallaby-spec-manifest --out specs/coverage.json --dir specs

spec-verify:
	$(GOENV) GO="$(GO)" ./scripts/spec-verify.sh

spec-lint:
	$(GOENV) GO="$(GO)" GOBIN="$(GOBIN)" SPEC_LINT_VERBOSE_MODE="$(SPEC_LINT_VERBOSE_MODE)" SPEC_LINT_VERBOSE="$(SPEC_LINT_VERBOSE)" ./scripts/spec-lint.sh

spec-sync:
	$(GOENV) $(GO) run ./cmd/wallaby-spec-sync --spec-dir specs --manifest-dir specs

tla-tools:
	./scripts/tla-tools.sh "$(TLA_TOOLS_URL)" "$(TLA_TOOLS_DIR)" "$(TLA_TOOLS_JAR)" "$(GOBIN)" "$(TLA_TOOLS_SHA256_FILE)"

tidy:
	GOFLAGS='-tags=tools' $(GO) mod tidy

release-tag-check:
	./scripts/validate-release-tag-selftest.sh

release:
	$(GORELEASER) release --clean

release-snapshot:
	$(GORELEASER) release --snapshot --clean

bench-up:
	docker compose -f bench/docker-compose.yml up -d

bench-down:
	docker compose -f bench/docker-compose.yml down

bench: bench-up
	$(GOENV) $(GO) run ./cmd/wallaby-bench -profile $(PROFILE) -targets $(TARGETS) -scenario $(SCENARIO)

bench-ddl:
	$(MAKE) bench SCENARIO=ddl

benchmark:
	./bench/benchmark.sh

benchmark-profile:
	ENABLE_PROFILES=1 PROFILE_FORMAT=both ./bench/benchmark.sh

benchstat:
	$(GOENV) $(GO) run ./cmd/wallaby-bench-summary -dir "$(BASELINE)" -format benchstat -latest=false -output "$(BASELINE)/benchstat.txt"
	$(GOENV) $(GO) run ./cmd/wallaby-bench-summary -dir "$(CANDIDATE)" -format benchstat -latest=false -output "$(CANDIDATE)/benchstat.txt"
	$(GOENV) $(GO) run golang.org/x/perf/cmd/benchstat@latest "$(BASELINE)/benchstat.txt" "$(CANDIDATE)/benchstat.txt"

tla: tla-flow tla-state tla-fanout tla-liveness tla-witness

tla-single:
	PATH="$(GOBIN):$$PATH" JAVA_TOOL_OPTIONS="$(TLC_JAVA_OPTS)" $(TLC) $(TLC_ARGS) -config "$(TLA_CONFIG)" "$(TLA_MODULE)"

tla-flow:
	$(MAKE) tla-single TLA_MODULE=specs/CDCFlow.tla TLA_CONFIG=specs/CDCFlow.cfg

tla-state:
	$(MAKE) tla-single TLA_MODULE=specs/FlowStateMachine.tla TLA_CONFIG=specs/FlowStateMachine.cfg

tla-fanout:
	$(MAKE) tla-single TLA_MODULE=specs/CDCFlowFanout.tla TLA_CONFIG=specs/CDCFlowFanout.cfg

tla-liveness:
	$(MAKE) tla-single TLA_MODULE=specs/CDCFlow.tla TLA_CONFIG=specs/CDCFlowLiveness.cfg

tla-witness:
	$(MAKE) tla-single TLA_MODULE=specs/CDCFlow.tla TLA_CONFIG=specs/CDCFlowWitness.cfg

tla-coverage:
	@mkdir -p "$(TLC_COVERAGE_DIR)"
	PATH="$(GOBIN):$$PATH" JAVA_TOOL_OPTIONS="$(TLC_JAVA_OPTS)" $(TLC) -coverage 1 -config "specs/CDCFlow.cfg" "specs/CDCFlow.tla" > "$(TLC_COVERAGE_DIR)/CDCFlow.txt" 2>&1
	PATH="$(GOBIN):$$PATH" JAVA_TOOL_OPTIONS="$(TLC_JAVA_OPTS)" $(TLC) -coverage 1 -config "specs/FlowStateMachine.cfg" "specs/FlowStateMachine.tla" > "$(TLC_COVERAGE_DIR)/FlowStateMachine.txt" 2>&1
	PATH="$(GOBIN):$$PATH" JAVA_TOOL_OPTIONS="$(TLC_JAVA_OPTS)" $(TLC) -coverage 1 -config "specs/CDCFlowFanout.cfg" "specs/CDCFlowFanout.tla" > "$(TLC_COVERAGE_DIR)/CDCFlowFanout.txt" 2>&1

tla-coverage-check:
	$(GOENV) $(GO) run ./cmd/wallaby-tla-coverage --dir "$(TLC_COVERAGE_DIR)" --min "$(TLA_COVERAGE_MIN)" --ignore "$(TLA_COVERAGE_IGNORE)" --json "$(TLC_COVERAGE_DIR)/report.json"

trace-suite:
	TRACE_CASES=$(TRACE_CASES) TRACE_SEED=$(TRACE_SEED) TRACE_MAX_BATCHES=$(TRACE_MAX_BATCHES) TRACE_MAX_RECORDS=$(TRACE_MAX_RECORDS) $(GOENV) $(GO) test ./pkg/stream -run TestTraceSuite -count=1

trace-suite-large:
	TRACE_CASES=20000 TRACE_SEED=123 TRACE_MAX_BATCHES=12 TRACE_MAX_RECORDS=5 $(GOENV) $(GO) test ./pkg/stream -run TestTraceSuite -count=1
