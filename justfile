set shell := ["bash", "-euo", "pipefail", "-c"]

# Tool selection. Every value can be overridden through the environment.
go := env_var_or_default("GO", "go")
buf := env_var_or_default("BUF", "buf")
golangci_lint := env_var_or_default("GOLANGCI_LINT", "golangci-lint")
staticcheck_version := env_var_or_default("STATICCHECK_VERSION", "v0.6.1")
govulncheck_version := env_var_or_default("GOVULNCHECK_VERSION", "v1.1.4")
goreleaser := env_var_or_default("GORELEASER", "goreleaser")
protoc_gen_go_version := env_var_or_default("PROTOC_GEN_GO_VERSION", "v1.36.11")
protoc_gen_go_grpc_version := env_var_or_default("PROTOC_GEN_GO_GRPC_VERSION", "v1.6.0")
gomarkdoc_version := env_var_or_default("GOMARKDOC_VERSION", "v1.1.0")
protoc_gen_doc_version := env_var_or_default("PROTOC_GEN_DOC_VERSION", "v1.5.1")
uv := env_var_or_default("UV", "uv")
uv_version := env_var_or_default("UV_VERSION", "0.11.29")
root := justfile_directory()
cache_dir := env_var_or_default("CACHE_DIR", ".cache")
gomodcache := env_var_or_default("GOMODCACHE", root + "/" + cache_dir + "/gomod")
gocache := env_var_or_default("GOCACHE", root + "/" + cache_dir + "/gocache")
golangci_lint_cache := env_var_or_default("GOLANGCI_LINT_CACHE", root + "/" + cache_dir + "/golangci-lint")
gobin := env_var_or_default("GOBIN", `go env GOPATH` + "/bin")

# Benchmark inputs.
profile := env_var_or_default("PROFILE", "small")
targets := env_var_or_default("TARGETS", "all")
scenario := env_var_or_default("SCENARIO", "base")
baseline := env_var_or_default("BASELINE", "")
candidate := env_var_or_default("CANDIDATE", "")

# TLA+ and trace inputs.
tlc := env_var_or_default("TLC", "tlc2.TLC")
tla_module := env_var_or_default("TLA_MODULE", "specs/CDCFlow.tla")
tla_config := env_var_or_default("TLA_CONFIG", "specs/CDCFlow.cfg")
tla_tools_tag := env_var_or_default("TLA_TOOLS_TAG", "v1.7.4")
tla_tools_url := env_var_or_default("TLA_TOOLS_URL", "https://github.com/tlaplus/tlaplus/releases/download/" + tla_tools_tag + "/tla2tools.jar")
tla_tools_dir := env_var_or_default("TLA_TOOLS_DIR", root + "/" + cache_dir + "/tla")
tla_tools_jar := env_var_or_default("TLA_TOOLS_JAR", tla_tools_dir + "/tla2tools.jar")
tla_tools_sha256_file := env_var_or_default("TLA_TOOLS_SHA256_FILE", "scripts/tla2tools-" + tla_tools_tag + ".sha256")
tlc_java_opts := env_var_or_default("TLC_JAVA_OPTS", "-XX:+UseParallelGC")
tlc_args := env_var_or_default("TLC_ARGS", "")
skip_tla_checks := env_var_or_default("SKIP_TLA_CHECKS", "false")
tlc_coverage_dir := env_var_or_default("TLC_COVERAGE_DIR", "specs/coverage")
tla_coverage_min := env_var_or_default("TLA_COVERAGE_MIN", "1")
tla_coverage_ignore := env_var_or_default("TLA_COVERAGE_IGNORE", "")
trace_cases := env_var_or_default("TRACE_CASES", "1000")
trace_seed := env_var_or_default("TRACE_SEED", "1")
trace_max_batches := env_var_or_default("TRACE_MAX_BATCHES", "10")
trace_max_records := env_var_or_default("TRACE_MAX_RECORDS", "3")
rapid_checks := env_var_or_default("RAPID_CHECKS", "100")
rapid_packages := env_var_or_default("RAPID_PACKAGES", "./pkg/stream ./pkg/wire ./internal/artifactlog ./internal/ddl ./internal/registry ./internal/schema ./internal/workflow ./connectors/sources/postgres ./connectors/destinations/postgres")
spec_lint_verbose := env_var_or_default("SPEC_LINT_VERBOSE", "")
spec_lint_verbose_mode := env_var_or_default("SPEC_LINT_VERBOSE_MODE", "checks")

# Test harness inputs.
go_test_timeout := env_var_or_default("GO_TEST_TIMEOUT", "")
go_test_verbose := env_var_or_default("GO_TEST_VERBOSE", "1")
go_test_verbose_flag := env_var_or_default("GO_TEST_VERBOSE_FLAG", "")
it_kind := env_var_or_default("IT_KIND", "")
it_keep := env_var_or_default("IT_KEEP", "0")
it_kind_cluster := env_var_or_default("IT_KIND_CLUSTER", "")
it_kind_node_image := env_var_or_default("IT_KIND_NODE_IMAGE", "")
it_service_ready_timeout_seconds := env_var_or_default("IT_SERVICE_READY_TIMEOUT_SECONDS", "240")
it_run_filter := env_var_or_default("IT_RUN_FILTER", "")
it_count := env_var_or_default("IT_COUNT", "")
it_package_parallelism := env_var_or_default("IT_PACKAGE_PARALLELISM", "1")
it_expected_harness_participants := env_var_or_default("IT_EXPECTED_HARNESS_PARTICIPANTS", it_package_parallelism)
integration_package := env_var_or_default("INTEGRATION_PACKAGE", "./tests/...")
integration_worker_binary := root + "/" + cache_dir + "/wallaby-worker-integration"

# Fuzz inputs. The smoke recipe is deterministic (seed corpora only); the bounded
# recipe drives each target for a fixed wall-clock budget.
snowflake_fuzz_time := env_var_or_default("SNOWFLAKE_FUZZ_TIME", "30s")
snowflake_fuzz_targets := env_var_or_default("SNOWFLAKE_FUZZ_TARGETS", "FuzzManagedSchemaContractHash FuzzNormalizeManagedSourceType FuzzManagedRecordKey FuzzBuildManagedSnowflakeOperationSQLSafety")
snowflake_fuzz_rapid_checks := env_var_or_default("SNOWFLAKE_FUZZ_RAPID_CHECKS", "1000")

# List available recipes.
default:
    @just --list

# Format all Go packages.
fmt:
    {{ go }} fmt ./...

# Verify Go formatting without modifying the worktree.
fmt-check:
    unformatted="$(gofmt -l $(find . -type f -name '*.go' -not -path './.git/*' -not -path './.cache/*'))"; test -z "$unformatted" || { printf 'unformatted Go files:\n%s\n' "$unformatted" >&2; exit 1; }

# Run golangci-lint.
lint:
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" GOLANGCI_LINT_CACHE="{{ golangci_lint_cache }}" {{ golangci_lint }} run ./...

# Run staticcheck at the repository-pinned version.
staticcheck:
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" {{ go }} run honnef.co/go/tools/cmd/staticcheck@{{ staticcheck_version }} ./...

# Scan reachable Go code for known vulnerabilities.
vulncheck:
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" {{ go }} run golang.org/x/vuln/cmd/govulncheck@{{ govulncheck_version }} ./...

lint-full: lint staticcheck vulncheck proto-lint proto-breaking

check: spec-verify spec-sync spec-lint check-tla
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" {{ go }} test ./...

check-tla:
    SKIP_TLA_CHECKS="{{ skip_tla_checks }}" JUST="just" ./scripts/check-tla.sh

check-lite: spec-sync spec-lint
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" {{ go }} test ./cmd/wallaby-admin ./pkg/... ./internal/... ./connectors/... ./tests/...

check-coverage: spec-sync tla-coverage tla-coverage-check trace-suite test-e2e

test:
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" GO="{{ go }}" GO_TEST_TIMEOUT="{{ go_test_timeout }}" GO_TEST_VERBOSE="{{ go_test_verbose }}" GO_TEST_VERBOSE_FLAG="{{ go_test_verbose_flag }}" ./scripts/test-go.sh

test-rapid:
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" GO="{{ go }}" RAPID_PACKAGES="{{ rapid_packages }}" RAPID_CHECKS="{{ rapid_checks }}" ./scripts/test-rapid.sh

test-integration:
    mkdir -p "$(dirname '{{ integration_worker_binary }}')"
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" {{ go }} build -o "{{ integration_worker_binary }}" ./cmd/wallaby-worker
    WALLABY_WORKER_BINARY="${WALLABY_WORKER_BINARY:-{{ integration_worker_binary }}}" GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" GO="{{ go }}" GO_TEST_TIMEOUT="{{ go_test_timeout }}" GO_TEST_VERBOSE="{{ go_test_verbose }}" GO_TEST_VERBOSE_FLAG="{{ go_test_verbose_flag }}" IT_KIND="{{ it_kind }}" IT_KEEP="{{ it_keep }}" IT_KIND_CLUSTER="{{ it_kind_cluster }}" IT_KIND_NODE_IMAGE="{{ it_kind_node_image }}" IT_SERVICE_READY_TIMEOUT_SECONDS="{{ it_service_ready_timeout_seconds }}" IT_RUN_FILTER="{{ it_run_filter }}" IT_COUNT="{{ it_count }}" IT_PACKAGE_PARALLELISM="{{ it_package_parallelism }}" IT_EXPECTED_HARNESS_PARTICIPANTS="{{ it_expected_harness_participants }}" INTEGRATION_PACKAGE="{{ integration_package }}" ./scripts/test-integration.sh

test-integration-ci: test-integration

# Durable-core unit and contract gate. Real-service evidence runs separately.
test-durable-pr:
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" {{ go }} test -count=1 ./internal/authority ./internal/controlplane ./internal/controlstore ./internal/delivery ./internal/bootstrap ./internal/artifactlog ./internal/workflow ./internal/checkpoint ./internal/registry ./internal/replication ./internal/runner ./pkg/connector ./pkg/stream ./connectors/sources/postgres ./connectors/destinations/postgres ./connectors/destinations/clickhouse ./connectors/destinations/iceberg

# Race detector coverage for the durable mutation and execution surfaces.
test-durable-race:
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" {{ go }} test -race -count=1 ./internal/authority ./internal/controlplane ./internal/controlstore ./internal/delivery ./internal/bootstrap ./internal/artifactlog ./internal/workflow ./internal/checkpoint ./internal/registry ./internal/replication ./internal/runner ./pkg/connector ./pkg/stream ./connectors/sources/postgres ./connectors/destinations/postgres ./connectors/destinations/iceberg

# Required live PostgreSQL/MinIO durability profiles. The harness provisions
# services, so these named tests must run rather than skip.
test-durable-integration:
    WALLABY_WORKER_BINARY="{{ integration_worker_binary }}" IT_REQUIRED_TESTS='TestWallabyWorkerProcessKillRecovery,TestAuthorityProtocolGateRejectsStaleBinarySession,TestPostgresGenerationFenceRejectsStaleCommit,TestPostgresRunFenceValidationSerializesTakeover,TestPostgresCheckpointGenerationFenceRejectsStaleCommit,TestPostgresFlowIDReuseDoesNotRestoreOldState,TestPostgresDestinationRevisionIsImmutable,TestPostgresAckOnlyCheckpointHasIntentAndReceipt,TestPostgresCommitBeforeReceiptReconciles,TestPostgresManagedDriverMarkerReconciles,TestPostgresTargetReplayConvergesIncludingMetadata,TestPostgresTargetPreservesSameKeyOperationOrderIntegration,TestLogicalSlotExportedSnapshotContract,TestBootstrapConcurrentWritesBoundary,TestManagedTerminalStopHardCrashSlotBeforePersist,TestManagedTerminalStopHardCrashPublicationBeforePublish,TestBootstrapRecoveryFailpoints,TestManagedBootstrapWorkerWiringConcurrentBoundary,TestManagedBootstrapSnapshotBatchCommitBeforeReceiptRecovery,TestManagedBootstrapPublicationReceiptBeforeHandoffRecovery,TestManagedBootstrapHandoffBeforeCDCOpenRecovery,TestManagedBootstrapLiveAdmissionMatrix,TestCanonicalPublicationFingerprintLive,TestManagedTerminalStopOwnershipLive,TestRegistryAndDDLReceiptsRejectStaleTakeover,TestCanonicalArtifactS3AdmissionRequiresEnabledVersioning,TestCanonicalArtifactPublicationRecovery,TestCanonicalArtifactPublicationFailureBoundaries,TestCanonicalArtifactBarrierOnlyDelivery,TestCanonicalArtifactStalePublisherCannotCommit,TestCanonicalArtifactBackpressureAndRootedRetention,TestCanonicalArtifactOrphanMarkSweepCrashRecovery,TestCanonicalArtifactGCDoesNotClaimActiveUpload,TestCanonicalArtifactGCTakeoverDoesNotOrphanInFlightPut,TestCanonicalArtifactPublisherGCClaimRevalidation,TestWallabyWorkerMaterializedPublicationRecovery,TestPostgresToPostgresManagedRecoveryContract' IT_RUN_FILTER='^(TestWallabyWorkerProcessKillRecovery|TestAuthorityProtocolGateRejectsStaleBinarySession|TestPostgresGenerationFenceRejectsStaleCommit|TestPostgresRunFenceValidationSerializesTakeover|TestPostgresCheckpointGenerationFenceRejectsStaleCommit|TestPostgresFlowIDReuseDoesNotRestoreOldState|TestPostgresDestinationRevisionIsImmutable|TestPostgresAckOnlyCheckpointHasIntentAndReceipt|TestPostgresCommitBeforeReceiptReconciles|TestPostgresManagedDriverMarkerReconciles|TestPostgresTargetReplayConvergesIncludingMetadata|TestPostgresTargetPreservesSameKeyOperationOrderIntegration|TestLogicalSlotExportedSnapshotContract|TestBootstrapConcurrentWritesBoundary|TestManagedTerminalStopHardCrashSlotBeforePersist|TestManagedTerminalStopHardCrashPublicationBeforePublish|TestBootstrapRecoveryFailpoints|TestManagedBootstrapWorkerWiringConcurrentBoundary|TestManagedBootstrapSnapshotBatchCommitBeforeReceiptRecovery|TestManagedBootstrapPublicationReceiptBeforeHandoffRecovery|TestManagedBootstrapHandoffBeforeCDCOpenRecovery|TestManagedBootstrapLiveAdmissionMatrix|TestCanonicalPublicationFingerprintLive|TestManagedTerminalStopOwnershipLive|TestRegistryAndDDLReceiptsRejectStaleTakeover|TestCanonicalArtifactS3AdmissionRequiresEnabledVersioning|TestCanonicalArtifactPublicationRecovery|TestCanonicalArtifactPublicationFailureBoundaries|TestCanonicalArtifactBarrierOnlyDelivery|TestCanonicalArtifactStalePublisherCannotCommit|TestCanonicalArtifactBackpressureAndRootedRetention|TestCanonicalArtifactOrphanMarkSweepCrashRecovery|TestCanonicalArtifactGCDoesNotClaimActiveUpload|TestCanonicalArtifactGCTakeoverDoesNotOrphanInFlightPut|TestCanonicalArtifactPublisherGCClaimRevalidation|TestWallabyWorkerMaterializedPublicationRecovery|TestPostgresToPostgresManagedRecoveryContract)$' INTEGRATION_PACKAGE='./tests' just test-integration

# Live DBOS evidence for the production managed-bootstrap wiring.
test-durable-dbos-integration:
    IT_REQUIRED_TESTS='TestDBOSManagedBootstrapProductionWiring' IT_RUN_FILTER='^TestDBOSManagedBootstrapProductionWiring$' INTEGRATION_PACKAGE='./tests/integration' just test-integration

# Checkpoint-1 requires both direct worker and in-process DBOS evidence.
# Use distinct per-run harness identities so the sequential phases cannot reuse
# port-forward state from infrastructure torn down by the preceding phase.
test-checkpoint1-integration:
    #!/usr/bin/env bash
    set -euo pipefail
    harness_base="${IT_KIND_CLUSTER:-wallaby-test}-checkpoint1-$$"
    cleanup() {
        status=$?
        if [[ "${IT_KEEP:-0}" != "1" ]]; then
            if command -v kind >/dev/null 2>&1; then
                kind delete cluster --name "${harness_base}-direct" || true
                kind delete cluster --name "${harness_base}-dbos" || true
            fi
            rm -f "${TMPDIR:-/tmp}/wallaby-it-integration-harness.state"
        fi
        return "$status"
    }
    trap cleanup EXIT
    IT_KIND_CLUSTER="${harness_base}-direct" just test-durable-integration
    IT_KIND_CLUSTER="${harness_base}-dbos" just test-durable-dbos-integration

# Checkpoint-2 named PostgreSQL profile gate. CI runs this recipe against every
# admitted PostgreSQL major. JSON verification makes missing or skipped evidence
# fail instead of letting a stale -run expression pass vacuously.
test-checkpoint2-postgres-profile:
    #!/usr/bin/env bash
    set -euo pipefail
    test -n "${TEST_PG_DSN:-}" || { echo 'TEST_PG_DSN is required' >&2; exit 2; }
    mkdir -p "$(dirname '{{ integration_worker_binary }}')"
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" {{ go }} build -o "{{ integration_worker_binary }}" ./cmd/wallaby-worker
    results=$(mktemp)
    trap 'rm -f "${results}"' EXIT
    required=
    required+='TestManagedProfileCannotBypassManagedAdmission'
    required+=',TestPostgresManagedProfileVersionContract'
    required+=',TestPostgresManagedStreamedSubtransactionAbort'
    required+=',TestPostgresManagedProfileTargetAdmission'
    required+=',TestPostgresManagedProfileDestinationSchemaEvolution'
    required+=',TestPostgresManagedProfileDDLTargetMapping'
    required+=',TestPostgresManagedProfileDDLCommitReconciliation'
    required+=',TestPostgresManagedProfileUpgradeMigrations'
    required+=',TestPostgresManagedFullTransactionPreservesFragmentsAndMarker'
    required+=',TestPostgresManagedTransactionCommitBeforeReceiptReconciles'
    required+=',TestPostgresManagedOverlappingTakeoverAdoptsConcurrentCommit'
    required+=',TestPostgresManagedDeliveryRetryAndRetention'
    required+=',TestManagedDeliveryRetentionRunsDuringLongLivedFlow'
    required+=',TestPostgresAuthorizedSourceFlushRejectsStaleWorker'
    required+=',TestPostgresToPostgresManagedRecoveryContract'
    required+=',TestManagedBootstrapWorkerWiringConcurrentBoundary'
    required+=',TestPostgresManagedProfileSourceSchemaEvolutionAfterRestart'
    required+=',TestWallabyWorkerProcessKillRecovery'
    required+=',TestPostgresManagedProfilePoolExhaustion'
    required+=',TestPostgresManagedTargetReceiptRollingCompatibility'
    required+=',TestPostgresManagedProfileMetrics'
    filter="^($(printf '%s' "${required}" | tr ',' '|'))$"
    set +e
    WALLABY_WORKER_BINARY="{{ integration_worker_binary }}" GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" \
      {{ go }} test -p 1 -count=1 -json \
      ./tests ./internal/runner ./internal/replication ./connectors/destinations/postgres ./internal/telemetry \
      -run "${filter}" >"${results}"
    test_rc=$?
    set -e
    cat "${results}"
    test "${test_rc}" -eq 0
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" {{ go }} run ./scripts/verify-go-test-json.go \
      -results "${results}" -required "${required}"

# Checkpoint-3 exact PostgreSQL 16 + ClickHouse/Keeper 25.12.1.649 append profile. The harness
# provisions both processes and verified native TLS. Required-test validation
# rejects skipped or missing evidence.
test-clickhouse-managed-profile:
    #!/usr/bin/env bash
    set -euo pipefail
    harness_cluster="${IT_KIND_CLUSTER:-wallaby-test}-clickhouse-profile-$$"
    cleanup() {
        status=$?
        if [[ "${IT_KEEP:-0}" != "1" ]] && command -v kind >/dev/null 2>&1; then
            kind delete cluster --name "${harness_cluster}" || true
        fi
        return "$status"
    }
    trap cleanup EXIT
    required=
    required+='TestClickHouseManagedProfileVersionMatrix'
    required+=',TestClickHouseManagedProfileAdmission'
    required+=',TestClickHouseManagedProfileCommitBeforeReceipt'
    required+=',TestClickHouseManagedProfileDedupWindowEviction'
    required+=',TestClickHouseManagedProfileOrderingAndConcurrency'
    required+=',TestClickHouseManagedProfileKeyChangesAndTombstones'
    required+=',TestClickHouseManagedProfileSchemaEvolutionAndTypes'
    required+=',TestPostgresToClickHouseManagedProfileRecoveryContract'
    required+=',TestClickHouseManagedProfileBoundedLoad'
    required+=',TestClickHouseManagedProfileTLS'
    required+=',TestClickHouseManagedProfileProcessKillRecovery'
    required+=',TestClickHouseManagedProfileKeeperFailureRecovery'
    required+=',TestClickHouseManagedProfileBackpressure'
    filter="^($(printf '%s' "${required}" | tr ',' '|'))$"
    IT_KIND_CLUSTER="${harness_cluster}" IT_REQUIRED_TESTS="${required}" IT_RUN_FILTER="${filter}" INTEGRATION_PACKAGE='./tests' just test-integration
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" {{ go }} test -count=1 \
      ./internal/telemetry -run '^TestClickHouseManagedProfileTelemetry$'

# Local/emulated Iceberg REST live gate. The ordinary package conformance tests
# use httptest and always run in test-durable-pr; this recipe targets a real REST
# fixture and warehouse already exported into the environment by an operator or
# by the integration harness.
test-iceberg-rest:
    test -n "${WALLABY_TEST_ICEBERG_REST_URI:-}" || { echo 'WALLABY_TEST_ICEBERG_REST_URI is required' >&2; exit 2; }
    test -n "${WALLABY_TEST_ICEBERG_WAREHOUSE:-}" || { echo 'WALLABY_TEST_ICEBERG_WAREHOUSE is required' >&2; exit 2; }
    test -n "${WALLABY_TEST_ICEBERG_NAMESPACE:-}" || { echo 'WALLABY_TEST_ICEBERG_NAMESPACE is required' >&2; exit 2; }
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" {{ go }} test -count=1 ./tests -run '^TestIcebergRESTLiveAppendProjection$'

# Checkpoint-5 mandatory gate. The integration harness provisions a real Apache
# Iceberg REST catalog backed by MinIO and exports the connection env, so the
# live append/projection/readback test must RUN rather than skip. JSON
# verification fails the gate on a missing or skipped result.
test-checkpoint5-iceberg-integration:
    #!/usr/bin/env bash
    set -euo pipefail
    harness_cluster="${IT_KIND_CLUSTER:-wallaby-test}-iceberg-rest-$$"
    cleanup() {
        status=$?
        if [[ "${IT_KEEP:-0}" != "1" ]] && command -v kind >/dev/null 2>&1; then
            kind delete cluster --name "${harness_cluster}" || true
        fi
        rm -f "${TMPDIR:-/tmp}/wallaby-it-integration-harness.state"
        return "$status"
    }
    trap cleanup EXIT
    required='TestIcebergRESTLiveAppendProjection,TestIcebergRESTLiveSchemaEvolutionRename'
    filter="^(TestIcebergRESTLiveAppendProjection|TestIcebergRESTLiveSchemaEvolutionRename)$"
    IT_KIND_CLUSTER="${harness_cluster}" IT_REQUIRED_TESTS="${required}" IT_RUN_FILTER="${filter}" INTEGRATION_PACKAGE='./tests' just test-integration

# Opt-in AWS S3 Tables gate. This creates a uniquely named experimental table,
# configures maintenance through the S3 Tables API, appends, and reconciles it.
test-s3tables-live:
    test "${WALLABY_TEST_S3TABLES:-}" = "1" || { echo 'WALLABY_TEST_S3TABLES=1 is required' >&2; exit 2; }
    test -n "${WALLABY_TEST_S3TABLES_REGION:-}" || { echo 'WALLABY_TEST_S3TABLES_REGION is required' >&2; exit 2; }
    test -n "${WALLABY_TEST_S3TABLES_WAREHOUSE:-}" || { echo 'WALLABY_TEST_S3TABLES_WAREHOUSE is required' >&2; exit 2; }
    test -n "${WALLABY_TEST_S3TABLES_TABLE_BUCKET_ARN:-}" || { echo 'WALLABY_TEST_S3TABLES_TABLE_BUCKET_ARN is required' >&2; exit 2; }
    test -n "${WALLABY_TEST_S3TABLES_EXPECTED_ROLE_ARN:-}" || { echo 'WALLABY_TEST_S3TABLES_EXPECTED_ROLE_ARN is required' >&2; exit 2; }
    test -n "${WALLABY_TEST_S3TABLES_NAMESPACE:-}" || { echo 'WALLABY_TEST_S3TABLES_NAMESPACE is required' >&2; exit 2; }
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" {{ go }} test -count=1 ./tests -run '^TestS3TablesLiveAppendProjection$'

# Constrained Snowflake SQL profile evidence. This intentionally fails until
# every named live cell exists and passes on a reviewed real Snowflake account
# with hybrid-table support. Fakesnow cannot satisfy this recipe.
test-snowflake-managed-profile:
    #!/usr/bin/env bash
    set -euo pipefail
    test "${WALLABY_TEST_SNOWFLAKE_MANAGED:-}" = "1" || { echo 'WALLABY_TEST_SNOWFLAKE_MANAGED=1 is required' >&2; exit 2; }
    test -n "${WALLABY_TEST_SNOWFLAKE_DSN:-}" || { echo 'WALLABY_TEST_SNOWFLAKE_DSN is required' >&2; exit 2; }
    test -n "${WALLABY_TEST_SNOWFLAKE_PROVISION_DSN:-}" || { echo 'WALLABY_TEST_SNOWFLAKE_PROVISION_DSN is required' >&2; exit 2; }
    test -n "${WALLABY_TEST_SNOWFLAKE_VERSION:-}" || { echo 'WALLABY_TEST_SNOWFLAKE_VERSION is required; the gate may not self-pin CURRENT_VERSION()' >&2; exit 2; }
    test -n "${WALLABY_TEST_SNOWFLAKE_REGION:-}" || { echo 'WALLABY_TEST_SNOWFLAKE_REGION is required' >&2; exit 2; }
    test -n "${WALLABY_TEST_SNOWFLAKE_OWNER_ROLE:-}" || { echo 'WALLABY_TEST_SNOWFLAKE_OWNER_ROLE is required' >&2; exit 2; }
    test -n "${TEST_PG_DSN:-}" || { echo 'TEST_PG_DSN is required for generation-fenced recovery evidence' >&2; exit 2; }
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" {{ go }} test -count=1 \
      ./connectors/destinations/snowflake ./internal/runner ./pkg/connector ./pkg/stream ./internal/telemetry
    results=$(mktemp)
    trap 'rm -f "${results}"' EXIT
    required=
    required+='TestSnowflakeManagedProfilePostgresSourceCatalog'
    required+=',TestSnowflakeManagedProfileFencedCleanSourceCut'
    required+=',TestSnowflakeManagedProfileLiveAdmission'
    required+=',TestSnowflakeManagedProfileReviewedDeploymentCell'
    required+=',TestSnowflakeManagedProfileRoleIsolation'
    required+=',TestSnowflakeManagedProfileTaskIsolation'
    required+=',TestSnowflakeManagedProfileAmbiguousCommit'
    required+=',TestSnowflakeManagedProfileCommitTransportLossAndDetachedTakeover'
    required+=',TestSnowflakeManagedProfileOrderedFragmentsAndTypes'
    required+=',TestSnowflakeManagedProfileSchemaReconciliation'
    required+=',TestSnowflakeManagedProfileProcessKillRecovery'
    required+=',TestSnowflakeManagedProfileWorkerSIGKILLRecovery'
    required+=',TestSnowflakeManagedProfileNetworkFaultMatrix'
    required+=',TestSnowflakeManagedProfileCancellationAndPoolSafety'
    required+=',TestSnowflakeManagedProfileBoundedLoadAndBackpressure'
    required+=',TestSnowflakeManagedProfileSecretRedaction'
    required+=',TestSnowflakeManagedProfileCleanup'
    required+=',TestPostgresToSnowflakeManagedProfileRecoveryContract'
    filter="^($(printf '%s' "${required}" | tr ',' '|'))$"
    set +e
    WALLABY_TEST_SNOWFLAKE_MANAGED_DIRECT=1 GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" \
      {{ go }} test -p 1 -count=1 -json ./tests ./connectors/sources/postgres -run "${filter}" >"${results}"
    test_rc=$?
    set -e
    cat "${results}"
    test "${test_rc}" -eq 0
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" {{ go }} run ./scripts/verify-go-test-json.go \
      -results "${results}" -required "${required}"

# Deterministic fuzz smoke for the constrained Snowflake SQL planner. It runs
# every managed fuzz target's seed corpus (no -fuzz, so no randomness and no
# network) plus the bounded rapid SQL-injection-safety and hash-determinism
# properties. Safe for pull-request CI and the non-credential suite.
fuzz-smoke:
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" {{ go }} test -count=1 \
      -run '^(Fuzz|TestManagedSnowflake.*Property)$' ./connectors/destinations/snowflake \
      -args -rapid.checks={{ snowflake_fuzz_rapid_checks }}

# Bounded coverage-guided fuzzing for the Snowflake SQL planner. Each target runs
# for SNOWFLAKE_FUZZ_TIME; a crasher is written under testdata/fuzz and fails the
# recipe. This is intentionally separate from the deterministic smoke.
fuzz-managed-snowflake:
    #!/usr/bin/env bash
    set -euo pipefail
    for target in {{ snowflake_fuzz_targets }}; do
      echo "fuzzing ${target} for {{ snowflake_fuzz_time }}"
      GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" {{ go }} test -run '^$' \
        -fuzz="^${target}$" -fuzztime={{ snowflake_fuzz_time }} ./connectors/destinations/snowflake
    done

# Cross-service promotion gate. The Snowflake catalog-linked database must
# already be read-only and linked to the same S3 Tables bucket/namespace.
test-s3tables-snowflake-live:
    test "${WALLABY_TEST_S3TABLES_SNOWFLAKE:-}" = "1" || { echo 'WALLABY_TEST_S3TABLES_SNOWFLAKE=1 is required' >&2; exit 2; }
    test -n "${WALLABY_TEST_S3TABLES_REGION:-}" || { echo 'WALLABY_TEST_S3TABLES_REGION is required' >&2; exit 2; }
    test -n "${WALLABY_TEST_S3TABLES_WAREHOUSE:-}" || { echo 'WALLABY_TEST_S3TABLES_WAREHOUSE is required' >&2; exit 2; }
    test -n "${WALLABY_TEST_S3TABLES_TABLE_BUCKET_ARN:-}" || { echo 'WALLABY_TEST_S3TABLES_TABLE_BUCKET_ARN is required' >&2; exit 2; }
    test -n "${WALLABY_TEST_S3TABLES_EXPECTED_ROLE_ARN:-}" || { echo 'WALLABY_TEST_S3TABLES_EXPECTED_ROLE_ARN is required' >&2; exit 2; }
    test -n "${WALLABY_TEST_S3TABLES_NAMESPACE:-}" || { echo 'WALLABY_TEST_S3TABLES_NAMESPACE is required' >&2; exit 2; }
    test -n "${WALLABY_TEST_SNOWFLAKE_DSN:-}" || { echo 'WALLABY_TEST_SNOWFLAKE_DSN is required' >&2; exit 2; }
    test -n "${WALLABY_TEST_SNOWFLAKE_LINKED_DATABASE:-}" || { echo 'WALLABY_TEST_SNOWFLAKE_LINKED_DATABASE is required' >&2; exit 2; }
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" {{ go }} test -count=1 ./tests -run '^TestS3TablesSnowflakeCatalogLinkedReadback$'

# Nightly increases property checks and repeats worker bootstrap/fencing plus
# DBOS bootstrap evidence. IT_REQUIRED_TESTS makes every named test no-skip.
test-durable-nightly:
    RAPID_CHECKS=20000 just test-rapid
    IT_COUNT=10 just test-durable-integration
    IT_COUNT=10 just test-durable-dbos-integration

test-integration-kind:
    IT_RUN_FILTER="^TestKubernetesDispatcher" IT_COUNT=1 just test-integration

test-e2e:
    IT_RUN_FILTER="^TestPostgresToPostgresE2E" IT_COUNT=1 just test-integration

test-k8s-kind:
    IT_RUN_FILTER="^TestKubernetesDispatcher" IT_COUNT=1 just test-integration

check-integration-core: test-integration

check-integration-full: test-integration test-e2e

proto: proto-tools
    rm -rf gen/go
    mkdir -p gen/go
    PATH="{{ gobin }}:$PATH" {{ buf }} generate

generate: proto

generate-check: generate
    ./scripts/generate-check.sh

proto-lint:
    {{ buf }} lint

proto-breaking:
    ./scripts/proto-breaking.sh "{{ buf }}"

proto-tools:
    GOBIN="{{ gobin }}" {{ go }} install google.golang.org/protobuf/cmd/protoc-gen-go@{{ protoc_gen_go_version }}
    GOBIN="{{ gobin }}" {{ go }} install google.golang.org/grpc/cmd/protoc-gen-go-grpc@{{ protoc_gen_go_grpc_version }}

docs-tools:
    @command -v {{ uv }} >/dev/null 2>&1 || { echo "uv {{ uv_version }} is required: https://docs.astral.sh/uv/" >&2; exit 1; }
    GOBIN="{{ gobin }}" {{ go }} install github.com/princjef/gomarkdoc/cmd/gomarkdoc@{{ gomarkdoc_version }}
    GOBIN="{{ gobin }}" {{ go }} install github.com/pseudomuto/protoc-gen-doc/cmd/protoc-gen-doc@{{ protoc_gen_doc_version }}
    {{ uv }} sync --frozen

docs-generate: docs-tools
    PATH="{{ gobin }}:$PATH" GOBIN="{{ gobin }}" ./scripts/docs-generate.sh

docs-verify: docs-tools
    ./scripts/docs-verify.sh

docs-verify-selftest:
    ./scripts/docs-verify-selftest.sh

docs-build: docs-generate
    {{ uv }} run --frozen mkdocs build --strict --clean

docs-preview: docs-generate
    {{ uv }} run --frozen mkdocs serve

docs-links: docs-build
    {{ uv }} run --frozen python ./scripts/docs-links.py site

docs-check: docs-generate
    {{ uv }} run --frozen mkdocs build --strict --clean
    {{ uv }} run --frozen python ./scripts/docs-links.py site

spec-manifest:
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" {{ go }} run ./cmd/wallaby-spec-manifest --out specs/coverage.json --dir specs

spec-verify:
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" GO="{{ go }}" ./scripts/spec-verify.sh

spec-lint:
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" GO="{{ go }}" GOBIN="{{ gobin }}" SPEC_LINT_VERBOSE_MODE="{{ spec_lint_verbose_mode }}" SPEC_LINT_VERBOSE="{{ spec_lint_verbose }}" ./scripts/spec-lint.sh

spec-sync:
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" {{ go }} run ./cmd/wallaby-spec-sync --spec-dir specs --manifest-dir specs

tla-tools:
    ./scripts/tla-tools.sh "{{ tla_tools_url }}" "{{ tla_tools_dir }}" "{{ tla_tools_jar }}" "{{ gobin }}" "{{ tla_tools_sha256_file }}"

tidy:
    GOFLAGS='-tags=tools' {{ go }} mod tidy

release-tag-check:
    ./scripts/validate-release-tag-selftest.sh

release:
    {{ goreleaser }} release --clean

release-snapshot:
    {{ goreleaser }} release --snapshot --clean

bench-up:
    docker compose -f bench/docker-compose.yml up -d

bench-down:
    docker compose -f bench/docker-compose.yml down

bench: bench-up
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" {{ go }} run ./cmd/wallaby-bench -profile "{{ profile }}" -targets "{{ targets }}" -scenario "{{ scenario }}"

bench-ddl:
    SCENARIO=ddl just bench

benchmark:
    ./bench/benchmark.sh

benchmark-profile:
    ENABLE_PROFILES=1 PROFILE_FORMAT=both ./bench/benchmark.sh

benchstat:
    test -n "{{ baseline }}" && test -n "{{ candidate }}" || { echo "BASELINE and CANDIDATE are required" >&2; exit 2; }
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" {{ go }} run ./cmd/wallaby-bench-summary -dir "{{ baseline }}" -format benchstat -latest=false -output "{{ baseline }}/benchstat.txt"
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" {{ go }} run ./cmd/wallaby-bench-summary -dir "{{ candidate }}" -format benchstat -latest=false -output "{{ candidate }}/benchstat.txt"
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" {{ go }} run golang.org/x/perf/cmd/benchstat@latest "{{ baseline }}/benchstat.txt" "{{ candidate }}/benchstat.txt"

tla: tla-flow tla-state tla-fanout tla-ddl-execution tla-lifecycle-generation tla-snapshot-transition tla-managed-durability tla-managed-postgres-delivery tla-liveness tla-witness

tla-single:
    PATH="{{ gobin }}:$PATH" JAVA_TOOL_OPTIONS="{{ tlc_java_opts }}" {{ tlc }} {{ tlc_args }} -config "{{ tla_config }}" "{{ tla_module }}"

tla-flow:
    TLA_MODULE=specs/CDCFlow.tla TLA_CONFIG=specs/CDCFlow.cfg just tla-single

tla-state:
    TLA_MODULE=specs/FlowStateMachine.tla TLA_CONFIG=specs/FlowStateMachine.cfg just tla-single

tla-fanout:
    TLA_MODULE=specs/CDCFlowFanout.tla TLA_CONFIG=specs/CDCFlowFanout.cfg just tla-single

tla-ddl-execution:
    TLA_MODULE=specs/DDLExecution.tla TLA_CONFIG=specs/DDLExecution.cfg just tla-single

tla-lifecycle-generation:
    TLA_MODULE=specs/LifecycleGeneration.tla TLA_CONFIG=specs/LifecycleGeneration.cfg just tla-single

tla-snapshot-transition:
    TLA_MODULE=specs/SnapshotTransition.tla TLA_CONFIG=specs/SnapshotTransition.cfg just tla-single

tla-managed-durability:
    TLA_MODULE=specs/ManagedDurability.tla TLA_CONFIG=specs/ManagedDurability.cfg just tla-single

tla-managed-postgres-delivery:
    TLA_MODULE=specs/ManagedPostgresDelivery.tla TLA_CONFIG=specs/ManagedPostgresDelivery.cfg just tla-single

tla-liveness:
    TLA_MODULE=specs/CDCFlow.tla TLA_CONFIG=specs/CDCFlowLiveness.cfg just tla-single

tla-witness:
    TLA_MODULE=specs/CDCFlow.tla TLA_CONFIG=specs/CDCFlowWitness.cfg just tla-single

tla-coverage:
    mkdir -p "{{ tlc_coverage_dir }}"
    PATH="{{ gobin }}:$PATH" JAVA_TOOL_OPTIONS="{{ tlc_java_opts }}" {{ tlc }} -coverage 1 -config specs/CDCFlow.cfg specs/CDCFlow.tla > "{{ tlc_coverage_dir }}/CDCFlow.txt" 2>&1
    PATH="{{ gobin }}:$PATH" JAVA_TOOL_OPTIONS="{{ tlc_java_opts }}" {{ tlc }} -coverage 1 -config specs/FlowStateMachine.cfg specs/FlowStateMachine.tla > "{{ tlc_coverage_dir }}/FlowStateMachine.txt" 2>&1
    PATH="{{ gobin }}:$PATH" JAVA_TOOL_OPTIONS="{{ tlc_java_opts }}" {{ tlc }} -coverage 1 -config specs/CDCFlowFanout.cfg specs/CDCFlowFanout.tla > "{{ tlc_coverage_dir }}/CDCFlowFanout.txt" 2>&1
    PATH="{{ gobin }}:$PATH" JAVA_TOOL_OPTIONS="{{ tlc_java_opts }}" {{ tlc }} -coverage 1 -config specs/DDLExecution.cfg specs/DDLExecution.tla > "{{ tlc_coverage_dir }}/DDLExecution.txt" 2>&1
    PATH="{{ gobin }}:$PATH" JAVA_TOOL_OPTIONS="{{ tlc_java_opts }}" {{ tlc }} -coverage 1 -config specs/LifecycleGeneration.cfg specs/LifecycleGeneration.tla > "{{ tlc_coverage_dir }}/LifecycleGeneration.txt" 2>&1
    PATH="{{ gobin }}:$PATH" JAVA_TOOL_OPTIONS="{{ tlc_java_opts }}" {{ tlc }} -coverage 1 -config specs/SnapshotTransition.cfg specs/SnapshotTransition.tla > "{{ tlc_coverage_dir }}/SnapshotTransition.txt" 2>&1
    PATH="{{ gobin }}:$PATH" JAVA_TOOL_OPTIONS="{{ tlc_java_opts }}" {{ tlc }} -coverage 1 -config specs/ManagedPostgresDelivery.cfg specs/ManagedPostgresDelivery.tla > "{{ tlc_coverage_dir }}/ManagedPostgresDelivery.txt" 2>&1

tla-coverage-check:
    GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" {{ go }} run ./cmd/wallaby-tla-coverage --dir "{{ tlc_coverage_dir }}" --min "{{ tla_coverage_min }}" --ignore "{{ tla_coverage_ignore }}" --json "{{ tlc_coverage_dir }}/report.json"

trace-suite:
    TRACE_CASES="{{ trace_cases }}" TRACE_SEED="{{ trace_seed }}" TRACE_MAX_BATCHES="{{ trace_max_batches }}" TRACE_MAX_RECORDS="{{ trace_max_records }}" GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" {{ go }} test ./pkg/stream -run TestTraceSuite -count=1

trace-suite-large:
    TRACE_CASES=20000 TRACE_SEED=123 TRACE_MAX_BATCHES=12 TRACE_MAX_RECORDS=5 GOMODCACHE="{{ gomodcache }}" GOCACHE="{{ gocache }}" {{ go }} test ./pkg/stream -run TestTraceSuite -count=1
