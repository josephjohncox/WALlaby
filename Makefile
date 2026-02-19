GO ?= go
BUF ?= buf
GOLANGCI_LINT ?= golangci-lint
STATICCHECK_VERSION ?= latest
GOVULNCHECK_VERSION ?= latest
STATICCHECK_CMD ?= $(GOENV) $(GO) run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION)
GOVULNCHECK_CMD ?= $(GOENV) $(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)
GORELEASER ?= goreleaser
PROTOC_GEN_GO ?= protoc-gen-go
PROTOC_GEN_GO_GRPC ?= protoc-gen-go-grpc
PROTOC_GEN_GO_VERSION ?= v1.36.11
PROTOC_GEN_GO_GRPC_VERSION ?= v1.6.0
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
TLA_TOOLS_TAG ?= latest
TLA_TOOLS_URL ?= https://github.com/tlaplus/tlaplus/releases/$(TLA_TOOLS_TAG)/download/tla2tools.jar
TLA_TOOLS_DIR ?= $(abspath $(CACHE_DIR)/tla)
TLA_TOOLS_JAR ?= $(TLA_TOOLS_DIR)/tla2tools.jar
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
GO_TEST_VERBOSE_FLAG := $(if $(filter 1,$(GO_TEST_VERBOSE)),-v,)

.PHONY: fmt lint staticcheck vulncheck lint-full test test-rapid test-integration test-integration-ci test-integration-kind test-e2e test-k8s-kind proto tidy release release-snapshot proto-tools tla-tools bench bench-ddl bench-up bench-down benchmark benchmark-profile benchstat check check-coverage check-tla tla tla-single tla-flow tla-state tla-fanout tla-liveness tla-witness tla-coverage tla-coverage-check trace-suite trace-suite-large spec-manifest spec-verify spec-lint spec-sync
.PHONY: proto-lint proto-breaking

# Integration test harness knobs.
# Set defaults in the caller env or override in CI to tune test behavior.
.PHONY: check-lite check-integration-core check-integration-full
IT_KIND ?= $(if $(WALLABY_TEST_K8S_KIND),$(WALLABY_TEST_K8S_KIND),1)
IT_KEEP ?= 0
IT_KIND_CLUSTER ?= $(if $(KIND_CLUSTER),$(KIND_CLUSTER),wallaby-test)
IT_KIND_NODE_IMAGE ?= $(if $(KIND_NODE_IMAGE),$(KIND_NODE_IMAGE),kindest/node:v1.35.0)
IT_SERVICE_READY_TIMEOUT_SECONDS ?= 240
IT_RUN_FILTER ?=
IT_COUNT ?=
IT_PACKAGE_PARALLELISM ?= 1
IT_EXPECTED_HARNESS_PARTICIPANTS ?= $(IT_PACKAGE_PARALLELISM)
INTEGRATION_PACKAGE ?= ./tests/...
INTEGRATION_FLAGS = \
	-it-kind="$(IT_KIND)" \
	-it-keep="$(IT_KEEP)" \
	-it-k8s-kind-cluster="$(IT_KIND_CLUSTER)" \
	-it-k8s-kind-node-image="$(IT_KIND_NODE_IMAGE)" \
	-it-expected-harness-participants="$(IT_EXPECTED_HARNESS_PARTICIPANTS)"
INTEGRATION_RUN_FILTER = $(if $(strip $(IT_RUN_FILTER)),-run $(IT_RUN_FILTER),)
INTEGRATION_COUNT = $(if $(strip $(IT_COUNT)),-count=$(IT_COUNT),)
INTEGRATION_TEST_CMD = \
	$(GOENV) \
	IT_VERBOSE="$(GO_TEST_VERBOSE)" \
	WALLABY_IT_VERBOSE="$(GO_TEST_VERBOSE)" \
	WALLABY_IT_SERVICE_READY_TIMEOUT_SECONDS="$(IT_SERVICE_READY_TIMEOUT_SECONDS)" \
	$(GO) test -p $(IT_PACKAGE_PARALLELISM) $(GO_TEST_VERBOSE_FLAG) $(INTEGRATION_PACKAGE) $(INTEGRATION_FLAGS) $(INTEGRATION_RUN_FILTER) $(INTEGRATION_COUNT)

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
	@if [ "$(SKIP_TLA_CHECKS)" = "1" ] || [ "$(SKIP_TLA_CHECKS)" = "true" ] || [ "$(SKIP_TLA_CHECKS)" = "yes" ]; then \
		echo "Skipping TLC checks (SKIP_TLA_CHECKS=$(SKIP_TLA_CHECKS)); set to false/0 to run them."; \
	else \
		$(MAKE) tla; \
	fi

check-lite: spec-sync spec-lint
	$(GOENV) $(GO) test ./cmd/wallaby-admin ./pkg/... ./internal/... ./connectors/... ./tests/...

check-coverage: spec-sync tla-coverage tla-coverage-check trace-suite test-e2e

test:
	$(GOENV) $(GO) test $(GO_TEST_VERBOSE_FLAG) ./...

test-rapid:
	@rapid_pkgs="$(RAPID_PACKAGES)"; \
	$(GOENV) $(GO) test $$rapid_pkgs -args -rapid.checks="$(RAPID_CHECKS)"; \
	skip_regex='^github.com/josephjohncox/wallaby/(pkg/stream|pkg/wire|internal/ddl|internal/registry|internal/workflow|connectors/sources/postgres)(/.*)?$$'; \
	non_rapid_pkgs=$$($(GOENV) $(GO) list ./... | grep -Ev "$$skip_regex" || true); \
	if [ -n "$$non_rapid_pkgs" ]; then \
		$(GOENV) $(GO) test $$non_rapid_pkgs; \
	fi

test-integration:
	$(INTEGRATION_TEST_CMD)

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
	PATH="$(GOBIN):$$PATH" $(BUF) generate

proto-lint:
	$(BUF) lint

proto-breaking:
	@set -e; \
	against="$${BUF_BREAKING_AGAINST:-}"; \
	if [ -z "$$against" ]; then \
		ref="$${BUF_BREAKING_REF:-}"; \
		if [ -z "$$ref" ]; then \
			if [ -n "$$GITHUB_BASE_SHA" ]; then \
				ref="$$GITHUB_BASE_SHA"; \
			elif git show-ref --verify --quiet refs/remotes/origin/main; then \
				ref="refs/remotes/origin/main"; \
			elif git show-ref --verify --quiet refs/heads/main; then \
				ref="refs/heads/main"; \
			elif git show-ref --verify --quiet refs/remotes/origin/HEAD; then \
				ref="refs/remotes/origin/HEAD"; \
			else \
				ref="$$(git rev-parse --verify HEAD~1 2>/dev/null || git rev-parse --verify HEAD)"; \
			fi; \
		fi; \
		against=".git#ref=$$ref"; \
	fi; \
	echo "buf breaking --against '$$against'"; \
	$(BUF) breaking --against "$$against"

proto-tools:
	GOBIN="$(GOBIN)" $(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	GOBIN="$(GOBIN)" $(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

spec-manifest:
	$(GOENV) $(GO) run ./cmd/wallaby-spec-manifest --out specs/coverage.json --dir specs

spec-verify: spec-manifest
	@echo "Verifying spec coverage manifests match generator output"
	@ls -1 specs/coverage*.json
	@if ! git diff --exit-code -- specs/coverage*.json; then \
		echo "spec manifests are out of date; run make spec-manifest"; \
		git diff -- specs/coverage*.json; \
		exit 1; \
	fi

spec-lint:
	@mkdir -p "$(GOBIN)"
	$(GOENV) $(GO) build -o "$(GOBIN)/wallaby-speccheck" ./cmd/wallaby-speccheck
	$(GOENV) $(GO) vet -vettool="$(GOBIN)/wallaby-speccheck" -specaction.verbose-mode="$(SPEC_LINT_VERBOSE_MODE)" $(if $(SPEC_LINT_VERBOSE),-specaction.verbose="$(SPEC_LINT_VERBOSE)",) ./...

spec-sync:
	$(GOENV) $(GO) run ./cmd/wallaby-spec-sync --spec-dir specs --manifest-dir specs

tla-tools:
	@mkdir -p "$(TLA_TOOLS_DIR)" "$(GOBIN)"
	@if command -v curl >/dev/null 2>&1; then \
		curl -fsSL "$(TLA_TOOLS_URL)" -o "$(TLA_TOOLS_JAR)"; \
	elif command -v wget >/dev/null 2>&1; then \
		wget -qO "$(TLA_TOOLS_JAR)" "$(TLA_TOOLS_URL)"; \
	else \
		echo "curl or wget is required to download tla2tools.jar"; \
		exit 1; \
	fi
	@chmod 0644 "$(TLA_TOOLS_JAR)"
	@printf '%s\n' '#!/bin/sh' 'exec java -cp "$(TLA_TOOLS_JAR)" tlc2.TLC "$$@"' > "$(GOBIN)/tlc2.TLC"
	@printf '%s\n' '#!/bin/sh' 'exec java -cp "$(TLA_TOOLS_JAR)" pcal.trans "$$@"' > "$(GOBIN)/pcal"
	@chmod +x "$(GOBIN)/tlc2.TLC" "$(GOBIN)/pcal"

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
