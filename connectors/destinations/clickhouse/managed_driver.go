package clickhouse

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	chclient "github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

var (
	_ connector.ManagedTransactionDestination = (*Destination)(nil)
	_ connector.ManagedTransactionPreparer    = (*Destination)(nil)
)

// errManagedReplicaLost marks the loss-class endpoint failure that recovery-only
// admission exists for: the endpoint no longer has Keeper-backed metadata for a
// managed table, which is what storage loss looks like. It deliberately does not
// cover static contract violations (a missing FINAL view, a wrong deduplication
// window, a bad table definition): those are operator misconfigurations that
// must fail closed instead of silently degrading the destination.
var errManagedReplicaLost = errors.New("managed ClickHouse replica metadata is lost")

const (
	managedDeploymentKeeper      = "self-managed-keeper"
	managedMinDedupWindow        = uint64(1000)
	managedMinDedupWindowSeconds = uint64(3600)
)

// ManagedHooks exposes deterministic post-commit boundaries to real-service
// tests. Production callers leave every hook nil.
type ManagedHooks struct {
	AfterFragment func(fragmentOrdinal uint64) error
	AfterReceipt  func() error
}

// SetManagedHooks installs deterministic failure injection for managed profile
// tests. Hooks run only after ClickHouse has acknowledged the named insert.
func (d *Destination) SetManagedHooks(hooks ManagedHooks) {
	d.managedHooks = hooks
}

type managedConfig struct {
	database            string
	changelogTable      string
	receiptsTable       string
	finalView           string
	deployment          string
	keeperPathPrefix    string
	keeperAddress       string
	replicaDSN          string
	replicaNames        []string
	insertQuorum        uint64
	maxActiveParts      uint64
	maxTransactionRows  int
	maxTransactionBytes int64
	maxFragments        int
	maxRowsPerBatch     int
	maxBatchBytes       int64
}

func (c managedConfig) planLimits() managedPlanLimits {
	return managedPlanLimits{
		maxFragments: c.maxFragments, maxRows: c.maxTransactionRows, maxBytes: c.maxTransactionBytes,
		maxRowsPerInsert: c.maxRowsPerBatch, maxBytesPerInsert: c.maxBatchBytes,
	}
}

func managedWriteSettings(insertQuorum uint64, deduplicationToken string) chclient.Settings {
	settings := chclient.Settings{
		"async_insert":           uint64(0),
		"wait_for_async_insert":  uint64(1),
		"insert_deduplicate":     uint64(1),
		"insert_quorum":          insertQuorum,
		"insert_quorum_parallel": uint64(1),
	}
	if deduplicationToken != "" {
		settings["insert_deduplication_token"] = deduplicationToken
	}
	return settings
}

type managedTableContract struct {
	columns        map[string]string
	sortingKey     string
	keeperPath     string
	replicaNames   map[string]struct{}
	maxActiveParts uint64
}

type managedTableDefinition struct {
	engine       string
	engineFull   string
	createSQL    string
	sortingKey   string
	primaryKey   string
	partitionKey string
	columns      map[string]string
	columnKinds  map[string]string
}

func (d *Destination) openManaged(ctx context.Context, dsn string, spec connector.Spec) error {
	cfg, err := managedConfigFromSpec(spec)
	if err != nil {
		return err
	}
	options, err := chclient.ParseDSN(dsn)
	if err != nil {
		return fmt.Errorf("parse managed ClickHouse DSN: %w", err)
	}
	if options.Protocol != chclient.Native {
		return errors.New("managed ClickHouse append profile requires the native protocol")
	}
	if options.TLS != nil && options.TLS.InsecureSkipVerify {
		return errors.New("managed ClickHouse append profile rejects TLS skip_verify")
	}
	if err := configureManagedTLS(options, spec.Options); err != nil {
		return err
	}
	if options.Settings == nil {
		options.Settings = chclient.Settings{}
	}
	for name, value := range managedWriteSettings(cfg.insertQuorum, "") {
		options.Settings[name] = value
	}
	options.Compression = &chclient.Compression{Method: chclient.CompressionLZ4}
	if options.MaxOpenConns == 0 {
		options.MaxOpenConns = 4
	}
	if options.MaxOpenConns < 1 || options.MaxOpenConns > 32 {
		return fmt.Errorf("managed ClickHouse max_open_conns must be between 1 and 32, got %d", options.MaxOpenConns)
	}

	profile := connector.PostgresToClickHouseAppendV1Profile()

	replicaOptions, err := chclient.ParseDSN(cfg.replicaDSN)
	if err != nil {
		return fmt.Errorf("parse managed ClickHouse replica DSN: %w", err)
	}
	if replicaOptions.Protocol != chclient.Native {
		return errors.New("managed ClickHouse replica requires the native protocol")
	}
	if err := configureManagedTLS(replicaOptions, spec.Options); err != nil {
		return fmt.Errorf("configure managed ClickHouse replica TLS: %w", err)
	}
	if replicaServerName := strings.TrimSpace(spec.Options["managed_replica_tls_server_name"]); replicaServerName != "" {
		replicaOptions.TLS.ServerName = replicaServerName
	}
	if strings.Join(options.Addr, ",") == strings.Join(replicaOptions.Addr, ",") {
		return errors.New("managed ClickHouse primary and replica DSNs must use distinct endpoints")
	}
	if replicaOptions.Settings == nil {
		replicaOptions.Settings = chclient.Settings{}
	}
	for name, value := range managedWriteSettings(cfg.insertQuorum, "") {
		replicaOptions.Settings[name] = value
	}
	replicaOptions.Compression = &chclient.Compression{Method: chclient.CompressionLZ4}
	replicaOptions.MaxOpenConns = 2

	conn, version, primaryErr := openManagedEndpoint(ctx, options, profile, "primary")
	replicaConn, replicaVersion, replicaErr := openManagedEndpoint(ctx, replicaOptions, profile, "replica")
	opened := false
	defer func() {
		if opened {
			return
		}
		if conn != nil {
			_ = conn.Close()
		}
		if replicaConn != nil {
			_ = replicaConn.Close()
		}
	}()

	d.managedConn = conn
	d.managedReplicaConn = replicaConn
	d.managedOptions = options
	d.managedConfig = cfg
	d.managedRecoveryOnly = false
	validationErrs := []error{primaryErr, replicaErr}
	lossObserved := primaryErr != nil || replicaErr != nil
	if primaryErr == nil && replicaErr == nil {
		d.managedVersion = version
		if err := d.validateManagedTarget(ctx, true, 0, 0); err == nil {
			opened = true
			return nil
		} else {
			if errors.Is(err, errManagedReplicaLost) {
				lossObserved = true
			}
			validationErrs = append(validationErrs, fmt.Errorf("healthy two-replica admission: %w", err))
		}
	}

	for _, survivor := range []struct {
		conn            chdriver.Conn
		endpointErr     error
		version         string
		expectedReplica string
	}{
		{conn: conn, endpointErr: primaryErr, version: version, expectedReplica: cfg.replicaNames[0]},
		{conn: replicaConn, endpointErr: replicaErr, version: replicaVersion, expectedReplica: cfg.replicaNames[1]},
	} {
		if survivor.endpointErr != nil || survivor.conn == nil {
			continue
		}
		// Recovery-only admission exists to survive a lost peer, never to downgrade
		// around a live but non-compliant one. Proceed only when some endpoint failed
		// in a loss class: an endpoint that would not open at all, or one whose
		// Keeper-backed table metadata is gone. A static contract violation on either
		// endpoint (for example a dropped FINAL view) is an operator misconfiguration
		// and must keep the authoritative two-endpoint admission error.
		if !lossObserved {
			validationErrs = append(validationErrs, fmt.Errorf("replica %s admission refused: no endpoint reported lost replica metadata, so recovery-only admission does not apply", survivor.expectedReplica))
			continue
		}
		d.managedVersion = survivor.version
		if err := d.validateManagedConnectionTarget(ctx, survivor.conn, survivor.expectedReplica, true, true, true, 0, 0); err != nil {
			validationErrs = append(validationErrs, fmt.Errorf("recovery-only replica %s admission: %w", survivor.expectedReplica, err))
			continue
		}
		d.managedVersion = survivor.version
		d.managedRecoveryOnly = true
		opened = true
		return nil
	}

	d.managedConn = nil
	d.managedReplicaConn = nil
	d.managedOptions = nil
	d.managedVersion = ""
	return errors.Join(append([]error{errors.New("managed ClickHouse has no admissible healthy or recovery-only endpoint")}, validationErrs...)...)
}

func openManagedEndpoint(ctx context.Context, options *chclient.Options, profile connector.ManagedProfileContract, endpoint string) (chdriver.Conn, string, error) {
	conn, err := chclient.Open(options)
	if err != nil {
		return nil, "", fmt.Errorf("open managed ClickHouse %s: %w", endpoint, err)
	}
	if err := conn.Ping(ctx); err != nil {
		return conn, "", fmt.Errorf("ping managed ClickHouse %s: %w", endpoint, err)
	}
	var version string
	if err := conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		return conn, "", fmt.Errorf("read managed ClickHouse %s version: %w", endpoint, err)
	}
	if !profile.SupportsClickHouseVersion(version) {
		return conn, version, fmt.Errorf("managed profile %s does not admit ClickHouse %s %s", profile.Name, endpoint, version)
	}
	return conn, version, nil
}

func configureManagedTLS(options *chclient.Options, specOptions map[string]string) error {
	if options.TLS == nil {
		return errors.New("managed ClickHouse append profile requires verified native TLS with secure=true")
	}
	if options.TLS.InsecureSkipVerify {
		return errors.New("managed ClickHouse append profile rejects TLS skip_verify")
	}
	caFile := strings.TrimSpace(specOptions["tls_ca_file"])
	serverName := strings.TrimSpace(specOptions["tls_server_name"])
	certFile := strings.TrimSpace(specOptions["tls_cert_file"])
	keyFile := strings.TrimSpace(specOptions["tls_key_file"])
	if caFile == "" && serverName == "" && certFile == "" && keyFile == "" {
		options.TLS.MinVersion = tls.VersionTLS12
		return nil
	}
	tlsConfig := options.TLS.Clone()
	tlsConfig.MinVersion = tls.VersionTLS12
	if serverName != "" {
		tlsConfig.ServerName = serverName
	}
	if caFile != "" {
		// #nosec G304 -- the operator-supplied CA path is a required connector option.
		contents, err := os.ReadFile(caFile)
		if err != nil {
			return fmt.Errorf("read managed ClickHouse TLS CA: %w", err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(contents) {
			return errors.New("managed ClickHouse TLS CA contains no certificates")
		}
		tlsConfig.RootCAs = roots
	}
	if (certFile == "") != (keyFile == "") {
		return errors.New("managed ClickHouse TLS client certificate and key must be configured together")
	}
	if certFile != "" {
		certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return fmt.Errorf("load managed ClickHouse TLS client identity: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	options.TLS = tlsConfig
	return nil
}

func managedConfigFromSpec(spec connector.Spec) (managedConfig, error) {
	options := spec.Options
	cfg := managedConfig{
		database:         strings.TrimSpace(options["managed_database"]),
		changelogTable:   strings.TrimSpace(options["managed_changelog_table"]),
		receiptsTable:    strings.TrimSpace(options["managed_receipts_table"]),
		finalView:        strings.TrimSpace(options["managed_final_view"]),
		deployment:       strings.ToLower(strings.TrimSpace(options["managed_deployment"])),
		keeperPathPrefix: strings.TrimSuffix(strings.TrimSpace(options["managed_keeper_path_prefix"]), "/"),
		keeperAddress:    strings.TrimSpace(options["managed_keeper_address"]),
		replicaDSN:       strings.TrimSpace(options["managed_replica_dsn"]),
		insertQuorum:     2, maxActiveParts: 180,
		maxTransactionRows: 100000, maxTransactionBytes: 128 << 20, maxFragments: 128,
		maxRowsPerBatch: 10000, maxBatchBytes: 16 << 20,
	}
	for name, value := range map[string]string{
		"managed_database": cfg.database, "managed_changelog_table": cfg.changelogTable,
		"managed_receipts_table": cfg.receiptsTable, "managed_final_view": cfg.finalView,
	} {
		if err := validateManagedIdentifier(name, value); err != nil {
			return managedConfig{}, err
		}
	}
	if cfg.deployment != managedDeploymentKeeper {
		return managedConfig{}, fmt.Errorf("managed ClickHouse profile requires managed_deployment=%s", managedDeploymentKeeper)
	}
	if cfg.keeperPathPrefix == "" || !strings.HasPrefix(cfg.keeperPathPrefix, "/") || strings.ContainsAny(cfg.keeperPathPrefix, "'\\") {
		return managedConfig{}, errors.New("managed ClickHouse profile requires an absolute managed_keeper_path_prefix")
	}
	if host, port, err := net.SplitHostPort(cfg.keeperAddress); err != nil || strings.TrimSpace(host) == "" || strings.TrimSpace(port) == "" {
		return managedConfig{}, errors.New("managed ClickHouse profile requires managed_keeper_address as host:port")
	}
	if cfg.replicaDSN == "" {
		return managedConfig{}, errors.New("managed ClickHouse profile requires managed_replica_dsn")
	}
	cfg.replicaNames = parseManagedReplicaNames(options["managed_replica_names"])
	if len(cfg.replicaNames) != 2 {
		return managedConfig{}, errors.New("managed ClickHouse profile requires exactly two unique managed_replica_names")
	}
	var err error
	if cfg.insertQuorum, err = parseManagedUintOption(options, "insert_quorum", 2, 2, 2); err != nil {
		return managedConfig{}, err
	}
	if cfg.maxActiveParts, err = parseManagedUintOption(options, "managed_max_active_parts", 180, 1, 100000); err != nil {
		return managedConfig{}, err
	}
	transactionRows, err := parseManagedUintOption(options, "managed_max_transaction_rows", 100000, 1, 100000)
	if err != nil {
		return managedConfig{}, err
	}
	// #nosec G115 -- parseManagedUintOption bounds rows at 100000.
	cfg.maxTransactionRows = int(transactionRows)
	transactionBytes, err := parseManagedUintOption(options, "managed_max_transaction_bytes", 128<<20, 1<<20, 128<<20)
	if err != nil {
		return managedConfig{}, err
	}
	// #nosec G115 -- parseManagedUintOption bounds bytes at 128 MiB.
	cfg.maxTransactionBytes = int64(transactionBytes)
	fragments, err := parseManagedUintOption(options, "managed_max_transaction_fragments", 128, 1, 1024)
	if err != nil {
		return managedConfig{}, err
	}
	// #nosec G115 -- parseManagedUintOption bounds fragments at 1024.
	cfg.maxFragments = int(fragments)
	// #nosec G115 -- maxTransactionRows is positive and bounded at 100000.
	rows, err := parseManagedUintOption(options, "managed_max_rows_per_batch", 10000, 1, uint64(cfg.maxTransactionRows))
	if err != nil {
		return managedConfig{}, err
	}
	// #nosec G115 -- parseManagedUintOption bounds rows at maxTransactionRows.
	cfg.maxRowsPerBatch = int(rows)
	// #nosec G115 -- maxTransactionBytes is positive and bounded at 128 MiB.
	bytes, err := parseManagedUintOption(options, "managed_max_batch_bytes", 16<<20, 1<<20, uint64(cfg.maxTransactionBytes))
	if err != nil {
		return managedConfig{}, err
	}
	// #nosec G115 -- parseManagedUintOption bounds bytes at maxTransactionBytes.
	cfg.maxBatchBytes = int64(bytes)
	if mode := strings.ToLower(strings.TrimSpace(options["batch_mode"])); mode != "target" {
		return managedConfig{}, fmt.Errorf("managed ClickHouse profile requires batch_mode=target; got %q", mode)
	}
	if resolution := strings.ToLower(strings.TrimSpace(options["batch_resolution"])); resolution != "" && resolution != "none" {
		return managedConfig{}, fmt.Errorf("managed ClickHouse profile requires batch_resolution=none; got %q", resolution)
	}
	metaEnabled, err := parseManagedBoolOption(options, "meta_table_enabled", false)
	if err != nil {
		return managedConfig{}, err
	}
	if metaEnabled {
		return managedConfig{}, errors.New("managed ClickHouse profile requires meta_table_enabled=false")
	}
	asyncInsert, err := parseManagedBoolOption(options, "async_insert", false)
	if err != nil {
		return managedConfig{}, err
	}
	if asyncInsert {
		return managedConfig{}, errors.New("managed ClickHouse profile requires async_insert=false")
	}
	waitForAsync, err := parseManagedBoolOption(options, "wait_for_async_insert", false)
	if err != nil {
		return managedConfig{}, err
	}
	if !waitForAsync {
		return managedConfig{}, errors.New("managed ClickHouse profile requires wait_for_async_insert=true")
	}
	return cfg, nil
}

func parseManagedReplicaNames(raw string) []string {
	seen := make(map[string]struct{})
	var replicas []string
	for _, value := range strings.Split(raw, ",") {
		value = strings.TrimSpace(value)
		if value == "" || len(value) > 128 || strings.ContainsAny(value, ".'\\") {
			continue
		}
		if _, duplicate := seen[value]; duplicate {
			continue
		}
		seen[value] = struct{}{}
		replicas = append(replicas, value)
	}
	return replicas
}

func managedReplicaSet(names []string) map[string]struct{} {
	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		set[strings.ToLower(name)] = struct{}{}
	}
	return set
}

func parseManagedBoolOption(options map[string]string, name string, fallback bool) (bool, error) {
	raw := strings.TrimSpace(options[name])
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("managed ClickHouse %s must be true or false", name)
	}
	return value, nil
}

func validateManagedIdentifier(name, value string) error {
	if value == "" {
		return fmt.Errorf("managed ClickHouse %s is required", name)
	}
	if len(value) > 128 || strings.ContainsRune(value, 0) || strings.Contains(value, ".") {
		return fmt.Errorf("managed ClickHouse %s must be one unqualified identifier", name)
	}
	return nil
}

func parseManagedUintOption(options map[string]string, name string, fallback, minimum, maximum uint64) (uint64, error) {
	raw := strings.TrimSpace(options[name])
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("managed ClickHouse %s must be between %d and %d", name, minimum, maximum)
	}
	return value, nil
}

// ManagedClickHouseVersion reports the live server version admitted during Open.
func (d *Destination) ManagedClickHouseVersion() string {
	return d.managedVersion
}

// Apply is intentionally unavailable for the full-transaction append profile.
func (d *Destination) Apply(_ context.Context, intent connector.DeliveryIntent, _ connector.Batch) (connector.DeliveryEvidence, error) {
	if err := intent.Validate(); err != nil {
		return connector.DeliveryEvidence{}, err
	}
	return connector.DeliveryEvidence{}, errors.New("managed ClickHouse append profile requires ApplyTransaction")
}

// ValidateTransaction proves the immutable envelope and dynamic target capacity
// before the PostgreSQL coordinator persists a new external attempt.
func (d *Destination) ValidateTransaction(ctx context.Context, transaction connector.SourceTransaction) error {
	contentHash, logicalBatchID, err := connector.SourceTransactionIdentity(transaction)
	if err != nil {
		return err
	}
	intent := connector.DeliveryIntent{FlowID: "validation", FlowIncarnationID: "validation", SourceLineageID: transaction.SourceLineageID, Generation: 1, AcquisitionID: "validation", LeaseEpoch: 1, DestinationRevisionID: "validation", LogicalBatchID: logicalBatchID, PositionID: transaction.Checkpoint.LSN, ContentHash: contentHash}
	_, err = d.PrepareTransaction(ctx, intent, transaction)
	return err
}

type preparedManagedTransaction struct {
	destination *Destination
	intent      connector.DeliveryIntent
	plan        managedTransactionPlan
}

// PrepareTransaction materializes and validates one bounded plan exactly once.
// The coordinator retains it across PostgreSQL's durable attempt boundary.
func (d *Destination) PrepareTransaction(ctx context.Context, intent connector.DeliveryIntent, transaction connector.SourceTransaction) (connector.PreparedManagedTransaction, error) {
	if d.managedRecoveryOnly {
		return nil, fmt.Errorf("%w: managed ClickHouse opened with one recovery-only replica; quorum writes remain fenced", connector.ErrDeliveryIndeterminate)
	}
	if d.managedConn == nil || d.managedReplicaConn == nil {
		return nil, errors.New("managed ClickHouse destination endpoints not initialized")
	}
	plan, err := planManagedTransactionWithLimits(intent, transaction, d.managedConfig.planLimits())
	if err != nil {
		return nil, err
	}
	if err := d.validateManagedTarget(ctx, false, uint64(len(plan.Fragments)), 1); err != nil {
		return nil, err
	}
	return &preparedManagedTransaction{destination: d, intent: intent, plan: plan}, nil
}

func (p *preparedManagedTransaction) Apply(ctx context.Context) (connector.DeliveryEvidence, error) {
	for _, fragment := range p.plan.Fragments {
		if err := p.destination.insertManagedFragment(ctx, fragment); err != nil {
			return connector.DeliveryEvidence{}, err
		}
		if p.destination.managedHooks.AfterFragment != nil {
			if err := p.destination.managedHooks.AfterFragment(fragment.Ordinal); err != nil {
				return connector.DeliveryEvidence{}, fmt.Errorf("%w: injected after ClickHouse fragment %d commit: %w", connector.ErrDeliveryIndeterminate, fragment.Ordinal, err)
			}
		}
	}
	if err := p.destination.insertManagedReceipt(ctx, p.plan.Receipt); err != nil {
		return connector.DeliveryEvidence{}, err
	}
	if p.destination.managedHooks.AfterReceipt != nil {
		if err := p.destination.managedHooks.AfterReceipt(); err != nil {
			return connector.DeliveryEvidence{}, fmt.Errorf("%w: injected after ClickHouse receipt commit: %w", connector.ErrDeliveryIndeterminate, err)
		}
	}
	return connector.DeliveryEvidence{ExternalID: p.plan.Receipt.ExternalID, ContentHash: p.intent.ContentHash}, nil
}

// ApplyTransaction appends every bounded insert in source order and writes the
// completion marker last. Each native insert has a stable query ID and
// deduplication token; replay convergence does not depend on the finite token
// retention window because the event identity is the ReplacingMergeTree key.
func (d *Destination) ApplyTransaction(ctx context.Context, intent connector.DeliveryIntent, transaction connector.SourceTransaction) (connector.DeliveryEvidence, error) {
	if d.managedConn == nil {
		return connector.DeliveryEvidence{}, errors.New("managed ClickHouse destination not initialized")
	}
	disposition, evidence, err := d.Reconcile(ctx, intent)
	if err != nil {
		return connector.DeliveryEvidence{}, err
	}
	if disposition == connector.DeliveryApplied {
		return evidence, nil
	}
	if disposition == connector.DeliveryIndeterminate {
		return connector.DeliveryEvidence{}, fmt.Errorf("%w: managed ClickHouse receipt reconciliation is indeterminate", connector.ErrDeliveryIndeterminate)
	}
	prepared, err := d.PrepareTransaction(ctx, intent, transaction)
	if err != nil {
		return connector.DeliveryEvidence{}, err
	}
	return prepared.Apply(ctx)
}

func (d *Destination) insertManagedFragment(ctx context.Context, fragment managedFragmentPlan) (resultErr error) {
	if len(fragment.Rows) == 0 {
		return nil
	}
	ctx, endSpan := telemetry.StartClickHouseManagedSpan(ctx, "fragment", fragment.QueryID, fragment.Rows[0].LogicalBatchID, int64(len(fragment.Rows)), fragment.EncodedBytes)
	defer func() { endSpan(resultErr) }()
	return executeManagedWriteWithFailover(ctx, d.managedReplicaConn != nil,
		func() error { return d.insertManagedFragmentOnConn(ctx, d.managedConn, fragment) },
		func() error { return d.insertManagedFragmentOnConn(ctx, d.managedReplicaConn, fragment) },
	)
}

func (d *Destination) insertManagedFragmentOnConn(ctx context.Context, conn chdriver.Conn, fragment managedFragmentPlan) error {
	query := "INSERT INTO " + quoteQualified(d.managedConfig.database+"."+d.managedConfig.changelogTable) + " (" + quoteColumns(managedChangelogColumns()) + ")"
	queryCtx := d.managedInsertContext(ctx, fragment.QueryID, fragment.DeduplicationToken)
	batch, err := conn.PrepareBatch(queryCtx, query)
	if err != nil {
		return fmt.Errorf("prepare managed ClickHouse fragment %d: %w", fragment.Ordinal, err)
	}
	defer func() { _ = batch.Abort() }()
	for _, row := range fragment.Rows {
		if err := batch.Append(managedChangelogValues(row)...); err != nil {
			return fmt.Errorf("append managed ClickHouse fragment %d: %w", fragment.Ordinal, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("%w: send managed ClickHouse fragment %d query_id=%s: %w", connector.ErrDeliveryIndeterminate, fragment.Ordinal, fragment.QueryID, err)
	}
	return nil
}

func (d *Destination) insertManagedReceipt(ctx context.Context, receipt managedReceiptRow) (resultErr error) {
	encodedBytes := int64(len(receipt.FlowID) + len(receipt.FlowIncarnationID) + len(receipt.SourceLineageID) + len(receipt.DestinationRevisionID) + len(receipt.LogicalBatchID) + len(receipt.ContentHash) + len(receipt.SourcePosition) + len(receipt.ExternalID))
	for _, queryID := range receipt.QueryIDs {
		encodedBytes += int64(len(queryID))
	}
	ctx, endSpan := telemetry.StartClickHouseManagedSpan(ctx, "receipt", receipt.QueryID, receipt.LogicalBatchID, 1, encodedBytes)
	defer func() { endSpan(resultErr) }()
	return executeManagedWriteWithFailover(ctx, d.managedReplicaConn != nil,
		func() error { return d.insertManagedReceiptOnConn(ctx, d.managedConn, receipt) },
		func() error { return d.insertManagedReceiptOnConn(ctx, d.managedReplicaConn, receipt) },
	)
}

func (d *Destination) insertManagedReceiptOnConn(ctx context.Context, conn chdriver.Conn, receipt managedReceiptRow) error {
	query := "INSERT INTO " + quoteQualified(d.managedConfig.database+"."+d.managedConfig.receiptsTable) + " (" + quoteColumns(managedReceiptColumns()) + ")"
	queryCtx := d.managedInsertContext(ctx, receipt.QueryID, receipt.DeduplicationToken)
	batch, err := conn.PrepareBatch(queryCtx, query)
	if err != nil {
		return fmt.Errorf("prepare managed ClickHouse receipt: %w", err)
	}
	defer func() { _ = batch.Abort() }()
	if err := batch.Append(managedReceiptValues(receipt)...); err != nil {
		return fmt.Errorf("append managed ClickHouse receipt: %w", err)
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("%w: send managed ClickHouse receipt query_id=%s: %w", connector.ErrDeliveryIndeterminate, receipt.QueryID, err)
	}
	return nil
}

func executeManagedWriteWithFailover(ctx context.Context, replicaAvailable bool, primaryWrite, replicaWrite func() error) error {
	primaryErr := primaryWrite()
	if primaryErr == nil {
		return nil
	}
	if !replicaAvailable || ctx.Err() != nil || !isManagedTransportError(primaryErr) {
		return primaryErr
	}
	if replicaErr := replicaWrite(); replicaErr != nil {
		return errors.Join(
			fmt.Errorf("%w: both managed ClickHouse write endpoints failed", connector.ErrDeliveryIndeterminate),
			fmt.Errorf("primary endpoint: %w", primaryErr),
			fmt.Errorf("replica endpoint: %w", replicaErr),
		)
	}
	return nil
}

func isManagedTransportError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, driver.ErrBadConn) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ETIMEDOUT)
}

func (d *Destination) managedInsertContext(ctx context.Context, queryID, token string) context.Context {
	return chclient.Context(ctx,
		chclient.WithQueryID(queryID),
		chclient.WithSettings(managedWriteSettings(d.managedConfig.insertQuorum, token)),
	)
}

type managedReceiptQueryer interface {
	QueryRow(context.Context, string, ...any) chdriver.Row
}

// Reconcile treats only a matching completion marker as applied. Missing
// markers cause a replay of the entire ordered transaction; partial rows remain
// harmless physical duplicates under the FINAL view. A failed primary read is
// retried on the admitted second replica, whose quorum copy remains sufficient
// to prove a previously acknowledged receipt after primary storage loss.
func (d *Destination) Reconcile(ctx context.Context, intent connector.DeliveryIntent) (disposition connector.DeliveryDisposition, evidence connector.DeliveryEvidence, resultErr error) {
	if err := intent.Validate(); err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	if d.managedConn == nil && d.managedReplicaConn == nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, errors.New("managed ClickHouse destination endpoints not initialized")
	}
	queryID := managedQueryIdentity(intent, "reconcile", 0)
	ctx, endSpan := telemetry.StartClickHouseManagedSpan(ctx, "reconcile", queryID, intent.LogicalBatchID, 0, 0)
	defer func() { endSpan(resultErr) }()
	query := "SELECT content_hash, external_id FROM " + quoteQualified(d.managedConfig.database+"."+d.managedConfig.receiptsTable) + " FINAL WHERE destination_revision_id=? AND logical_batch_id=? LIMIT 1"
	return reconcileManagedReceiptEndpoints(ctx, d.managedConn, d.managedReplicaConn, query, queryID, intent)
}

func reconcileManagedReceiptEndpoints(ctx context.Context, primary, replica managedReceiptQueryer, query, queryID string, intent connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	primaryDisposition, primaryEvidence, primaryErr := reconcileManagedReceiptEndpoint(ctx, primary, query, queryID, intent)
	if errors.Is(primaryErr, connector.ErrDeliveryConflict) || ctx.Err() != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, primaryErr
	}
	replicaQueryID := managedQueryIdentity(intent, "reconcile-replica", 0)
	replicaDisposition, replicaEvidence, replicaErr := reconcileManagedReceiptEndpoint(ctx, replica, query, replicaQueryID, intent)
	if errors.Is(replicaErr, connector.ErrDeliveryConflict) || ctx.Err() != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, replicaErr
	}
	if primaryDisposition == connector.DeliveryApplied {
		return primaryDisposition, primaryEvidence, nil
	}
	if replicaDisposition == connector.DeliveryApplied {
		return replicaDisposition, replicaEvidence, nil
	}
	// insert_quorum=2 means a completed receipt must exist on both admitted
	// replicas. Absence on either readable survivor therefore proves that no
	// quorum receipt completed, even when its peer is unavailable.
	if primaryDisposition == connector.DeliveryNotApplied || replicaDisposition == connector.DeliveryNotApplied {
		return connector.DeliveryNotApplied, connector.DeliveryEvidence{}, nil
	}
	return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, errors.Join(
		fmt.Errorf("%w: no managed ClickHouse receipt endpoint was readable", connector.ErrDeliveryIndeterminate),
		fmt.Errorf("primary managed ClickHouse receipt reconciliation: %w", primaryErr),
		fmt.Errorf("replica managed ClickHouse receipt reconciliation: %w", replicaErr),
	)
}

func reconcileManagedReceiptEndpoint(ctx context.Context, conn managedReceiptQueryer, query, queryID string, intent connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	if conn == nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, errors.New("managed ClickHouse receipt endpoint is unavailable")
	}
	var contentHash, externalID string
	if err := conn.QueryRow(chclient.Context(ctx, chclient.WithQueryID(queryID)), query, intent.DestinationRevisionID, intent.LogicalBatchID).Scan(&contentHash, &externalID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return connector.DeliveryNotApplied, connector.DeliveryEvidence{}, nil
		}
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	if contentHash != intent.ContentHash || externalID != managedDeliveryExternalID(intent) {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, fmt.Errorf("%w: managed ClickHouse receipt differs from delivery intent", connector.ErrDeliveryConflict)
	}
	return connector.DeliveryApplied, connector.DeliveryEvidence{ExternalID: externalID, ContentHash: contentHash}, nil
}

func managedRowBytes(row managedChangelogRow) int64 {
	return int64(len(row.FlowID) + len(row.FlowIncarnationID) + len(row.SourceLineageID) + len(row.DestinationRevisionID) +
		len(row.LogicalBatchID) + len(row.ContentHash) + len(row.SourcePosition) + len(row.BeginLSN) + len(row.CommitLSN) + len(row.EndLSN) +
		len(row.SourceNamespace) + len(row.SourceTable) + len(row.SchemaFingerprint) + len(row.SchemaJSON) + len(row.Operation) +
		len(row.KeyJSON) + len(row.BeforeJSON) + len(row.AfterJSON) + len(row.Payload) + len(row.DDLPlan) + len(row.RecordHash) + 128)
}

func managedChangelogColumns() []string {
	return []string{"flow_id", "flow_incarnation_id", "source_lineage_id", "destination_revision_id", "logical_batch_id", "content_hash", "source_position", "transaction_id", "begin_lsn", "commit_lsn", "end_lsn", "fragment_ordinal", "record_ordinal", "source_namespace", "source_table", "schema_version", "schema_fingerprint", "schema_json", "operation", "tombstone", "key_json", "before_json", "after_json", "payload", "ddl_plan", "event_time", "record_hash", "wallaby_version"}
}

func managedChangelogValues(row managedChangelogRow) []any {
	return []any{row.FlowID, row.FlowIncarnationID, row.SourceLineageID, row.DestinationRevisionID, row.LogicalBatchID, row.ContentHash, row.SourcePosition, row.TransactionID, row.BeginLSN, row.CommitLSN, row.EndLSN, row.FragmentOrdinal, row.RecordOrdinal, row.SourceNamespace, row.SourceTable, row.SchemaVersion, row.SchemaFingerprint, row.SchemaJSON, row.Operation, row.Tombstone, row.KeyJSON, row.BeforeJSON, row.AfterJSON, row.Payload, row.DDLPlan, row.EventTime, row.RecordHash, row.WallabyVersion}
}

func managedReceiptColumns() []string {
	return []string{"flow_id", "flow_incarnation_id", "source_lineage_id", "destination_revision_id", "logical_batch_id", "content_hash", "source_position", "transaction_id", "fragment_count", "record_count", "query_ids", "committed_at", "wallaby_version", "external_id"}
}

func managedReceiptValues(row managedReceiptRow) []any {
	return []any{row.FlowID, row.FlowIncarnationID, row.SourceLineageID, row.DestinationRevisionID, row.LogicalBatchID, row.ContentHash, row.SourcePosition, row.TransactionID, row.FragmentCount, row.RecordCount, row.QueryIDs, row.CommittedAt, row.WallabyVersion, row.ExternalID}
}

func (d *Destination) validateManagedTarget(ctx context.Context, includeStatic bool, plannedChangelogParts, plannedReceiptParts uint64) error {
	connections := []struct {
		conn                 chdriver.Conn
		expectedReplica      string
		verifyImplementation bool
	}{
		{conn: d.managedConn, expectedReplica: d.managedConfig.replicaNames[0], verifyImplementation: includeStatic},
		{conn: d.managedReplicaConn, expectedReplica: d.managedConfig.replicaNames[1]},
	}
	var validationErrs []error
	validated := 0
	for _, connection := range connections {
		if connection.conn == nil {
			validationErrs = append(validationErrs, fmt.Errorf("managed ClickHouse replica %s admission connection is unavailable: %w", connection.expectedReplica, driver.ErrBadConn))
			continue
		}
		if err := d.validateManagedConnectionTarget(ctx, connection.conn, connection.expectedReplica, includeStatic, connection.verifyImplementation, false, plannedChangelogParts, plannedReceiptParts); err != nil {
			validationErrs = append(validationErrs, fmt.Errorf("managed ClickHouse replica %s admission: %w", connection.expectedReplica, err))
			continue
		}
		validated++
	}
	if validated == len(connections) {
		return nil
	}
	if !includeStatic && validated > 0 {
		transportOnly := true
		for _, err := range validationErrs {
			if !isManagedTransportError(err) {
				transportOnly = false
				break
			}
		}
		if transportOnly {
			return nil
		}
	}
	if !includeStatic {
		validationErrs = append([]error{fmt.Errorf("%w: no managed ClickHouse endpoint admitted the quorum write", connector.ErrDeliveryIndeterminate)}, validationErrs...)
	}
	return errors.Join(validationErrs...)
}

func (d *Destination) validateManagedConnectionTarget(ctx context.Context, conn chdriver.Conn, expectedReplica string, includeStatic, verifyKeeperImplementation, allowDegraded bool, plannedChangelogParts, plannedReceiptParts uint64) error {
	if err := d.validateManagedKeeper(ctx, conn, verifyKeeperImplementation, expectedReplica, allowDegraded); err != nil {
		return err
	}
	if includeStatic {
		if err := d.validateManagedTable(ctx, conn, d.managedConfig.changelogTable, managedTableContract{
			columns:        managedExpectedChangelogColumns(),
			sortingKey:     "destination_revision_id, logical_batch_id, fragment_ordinal, record_ordinal",
			keeperPath:     d.managedConfig.keeperPathPrefix + "/" + d.managedConfig.database + "/" + d.managedConfig.changelogTable,
			replicaNames:   managedReplicaSet(d.managedConfig.replicaNames),
			maxActiveParts: d.managedConfig.maxActiveParts,
		}); err != nil {
			return err
		}
		if err := d.validateManagedTable(ctx, conn, d.managedConfig.receiptsTable, managedTableContract{
			columns:        managedExpectedReceiptColumns(),
			sortingKey:     "destination_revision_id, logical_batch_id",
			keeperPath:     d.managedConfig.keeperPathPrefix + "/" + d.managedConfig.database + "/" + d.managedConfig.receiptsTable,
			replicaNames:   managedReplicaSet(d.managedConfig.replicaNames),
			maxActiveParts: d.managedConfig.maxActiveParts,
		}); err != nil {
			return err
		}
		if err := d.validateManagedFinalView(ctx, conn); err != nil {
			return err
		}
	}
	for _, target := range []struct {
		table   string
		planned uint64
	}{
		{table: d.managedConfig.changelogTable, planned: plannedChangelogParts},
		{table: d.managedConfig.receiptsTable, planned: plannedReceiptParts},
	} {
		var activeParts uint64
		query := "SELECT count() FROM system.parts WHERE active AND database=? AND table=?"
		if err := conn.QueryRow(ctx, query, d.managedConfig.database, target.table).Scan(&activeParts); err != nil {
			return fmt.Errorf("read managed ClickHouse active parts for %s: %w", target.table, err)
		}
		if activeParts+target.planned > d.managedConfig.maxActiveParts {
			return fmt.Errorf("managed ClickHouse backpressure: table %s has %d active parts and delivery plans %d more, limit is %d", target.table, activeParts, target.planned, d.managedConfig.maxActiveParts)
		}
	}
	return nil
}

type managedReplicaContract struct {
	keeperPath    string
	replicaNames  map[string]struct{}
	allowDegraded bool
}

type managedReplicaStatus struct {
	keeperPath     string
	replicaName    string
	totalReplicas  uint32
	activeReplicas uint32
	readonly       uint8
	expired        uint8
	queueSize      uint32
	absoluteDelay  uint64
	lostPartCount  uint64
}

func validateManagedReplicaStatus(status managedReplicaStatus, contract managedReplicaContract) error {
	if status.keeperPath != contract.keeperPath {
		return fmt.Errorf("keeper path=%q, want %q", status.keeperPath, contract.keeperPath)
	}
	if _, admitted := contract.replicaNames[strings.ToLower(status.replicaName)]; !admitted {
		return fmt.Errorf("replica name=%q is outside managed_replica_names", status.replicaName)
	}
	if uint64(status.totalReplicas) != uint64(len(contract.replicaNames)) {
		return fmt.Errorf("total replicas=%d, want %d", status.totalReplicas, len(contract.replicaNames))
	}
	if contract.allowDegraded {
		if status.activeReplicas < 1 || uint64(status.activeReplicas) > uint64(len(contract.replicaNames)) {
			return fmt.Errorf("active replicas=%d, want between 1 and %d for recovery", status.activeReplicas, len(contract.replicaNames))
		}
	} else if uint64(status.activeReplicas) != uint64(len(contract.replicaNames)) {
		return fmt.Errorf("active replicas=%d, want %d", status.activeReplicas, len(contract.replicaNames))
	}
	if status.readonly != 0 || status.expired != 0 {
		return fmt.Errorf("replica is not writable (readonly=%d expired=%d)", status.readonly, status.expired)
	}
	if status.queueSize > 100 {
		return fmt.Errorf("replication queue %d exceeds admission limit 100", status.queueSize)
	}
	if status.absoluteDelay != 0 {
		return fmt.Errorf("replica absolute delay=%d, want 0", status.absoluteDelay)
	}
	if status.lostPartCount != 0 {
		return fmt.Errorf("replica lost parts=%d, want 0", status.lostPartCount)
	}
	return nil
}

func validateManagedKeeperVersion(response, clickHouseVersion string) error {
	response = strings.TrimSpace(response)
	if !strings.HasPrefix(response, "ClickHouse Keeper version:") {
		return fmt.Errorf("keeper srvr response does not identify ClickHouse Keeper: %q", response)
	}
	versionPattern := `^ClickHouse Keeper version:\s+v` + regexp.QuoteMeta(clickHouseVersion) + `(?:[-\s]|$)`
	if !regexp.MustCompile(versionPattern).MatchString(response) {
		return fmt.Errorf("keeper version response %q does not match admitted ClickHouse %s", response, clickHouseVersion)
	}
	return nil
}

func (d *Destination) validateManagedKeeperImplementation(ctx context.Context) error {
	dialer := net.Dialer{Timeout: 3 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", d.managedConfig.keeperAddress)
	if err != nil {
		return fmt.Errorf("connect to managed ClickHouse Keeper at %s: %w", d.managedConfig.keeperAddress, err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Write([]byte("srvr")); err != nil {
		return fmt.Errorf("request managed ClickHouse Keeper version: %w", err)
	}
	response, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil && strings.TrimSpace(response) == "" {
		return fmt.Errorf("read managed ClickHouse Keeper version: %w", err)
	}
	return validateManagedKeeperVersion(response, d.managedVersion)
}

func (d *Destination) validateManagedKeeper(ctx context.Context, conn chdriver.Conn, verifyImplementation bool, expectedReplica string, allowDegraded bool) error {
	if verifyImplementation {
		if err := d.validateManagedKeeperImplementation(ctx); err != nil {
			return err
		}
	}
	var keeperRoot uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM system.zookeeper WHERE path='/'").Scan(&keeperRoot); err != nil {
		return fmt.Errorf("managed ClickHouse profile requires reachable ClickHouse Keeper: %w", err)
	}
	replicaNames := managedReplicaSet(d.managedConfig.replicaNames)
	for _, table := range []string{d.managedConfig.changelogTable, d.managedConfig.receiptsTable} {
		expectedPath := d.managedConfig.keeperPathPrefix + "/" + d.managedConfig.database + "/" + table
		var status managedReplicaStatus
		if err := conn.QueryRow(ctx, `SELECT zookeeper_path,replica_name,total_replicas,active_replicas,is_readonly,is_session_expired,queue_size,absolute_delay,lost_part_count FROM system.replicas WHERE database=? AND table=?`, d.managedConfig.database, table).Scan(
			&status.keeperPath, &status.replicaName, &status.totalReplicas, &status.activeReplicas,
			&status.readonly, &status.expired, &status.queueSize, &status.absoluteDelay, &status.lostPartCount,
		); err != nil {
			return fmt.Errorf("%w: managed ClickHouse table %s lacks a Keeper-backed replica: %w", errManagedReplicaLost, table, err)
		}
		if err := validateManagedReplicaStatus(status, managedReplicaContract{keeperPath: expectedPath, replicaNames: replicaNames, allowDegraded: allowDegraded}); err != nil {
			return fmt.Errorf("managed ClickHouse table %s: %w", table, err)
		}
		if !strings.EqualFold(status.replicaName, expectedReplica) {
			return fmt.Errorf("managed ClickHouse table %s endpoint reports replica %q, want %q", table, status.replicaName, expectedReplica)
		}
		rows, err := conn.Query(ctx, "SELECT name FROM system.zookeeper WHERE path=?", expectedPath+"/replicas")
		if err != nil {
			return fmt.Errorf("read managed ClickHouse replica set for %s: %w", table, err)
		}
		actual := make(map[string]struct{}, len(replicaNames))
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				_ = rows.Close()
				return fmt.Errorf("scan managed ClickHouse replica set for %s: %w", table, err)
			}
			actual[strings.ToLower(name)] = struct{}{}
		}
		rowErr := rows.Err()
		_ = rows.Close()
		if rowErr != nil {
			return fmt.Errorf("iterate managed ClickHouse replica set for %s: %w", table, rowErr)
		}
		if len(actual) != len(replicaNames) {
			return fmt.Errorf("managed ClickHouse table %s Keeper replica set has %d members, want %d", table, len(actual), len(replicaNames))
		}
		for name := range replicaNames {
			if _, ok := actual[name]; !ok {
				return fmt.Errorf("managed ClickHouse table %s Keeper replica set lacks %s", table, name)
			}
		}
	}
	return nil
}

func (d *Destination) validateManagedTable(ctx context.Context, conn chdriver.Conn, table string, contract managedTableContract) error {
	var definition managedTableDefinition
	if err := conn.QueryRow(ctx, "SELECT engine,engine_full,create_table_query,sorting_key,primary_key,partition_key FROM system.tables WHERE database=? AND name=?", d.managedConfig.database, table).Scan(
		&definition.engine, &definition.engineFull, &definition.createSQL, &definition.sortingKey, &definition.primaryKey, &definition.partitionKey,
	); err != nil {
		return fmt.Errorf("read managed ClickHouse table %s: %w", table, err)
	}
	definition.columns = make(map[string]string)
	definition.columnKinds = make(map[string]string)
	rows, err := conn.Query(ctx, "SELECT name,type,default_kind FROM system.columns WHERE database=? AND table=? ORDER BY position", d.managedConfig.database, table)
	if err != nil {
		return fmt.Errorf("read managed ClickHouse columns for %s: %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name, typ, kind string
		if err := rows.Scan(&name, &typ, &kind); err != nil {
			return fmt.Errorf("scan managed ClickHouse column for %s: %w", table, err)
		}
		definition.columns[name] = typ
		definition.columnKinds[name] = kind
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate managed ClickHouse columns for %s: %w", table, err)
	}
	if err := validateManagedTableDefinition(definition, contract); err != nil {
		return fmt.Errorf("managed ClickHouse table %s: %w", table, err)
	}
	return nil
}

func validateManagedTableDefinition(definition managedTableDefinition, contract managedTableContract) error {
	if definition.engine != "ReplicatedReplacingMergeTree" {
		return fmt.Errorf("engine must be ReplicatedReplacingMergeTree(..., wallaby_version), got %q", definition.engineFull)
	}
	engineMatch := regexp.MustCompile(`^replicatedreplacingmergetree\('([^']+)','([^']+)',wallaby_version\)`).FindStringSubmatch(normalizeManagedSQL(definition.engineFull))
	if len(engineMatch) != 3 {
		return fmt.Errorf("engine must be ReplicatedReplacingMergeTree(..., wallaby_version), got %q", definition.engineFull)
	}
	if engineMatch[1] != strings.ToLower(contract.keeperPath) {
		return fmt.Errorf("engine Keeper path=%q, want %q", engineMatch[1], contract.keeperPath)
	}
	if _, admitted := contract.replicaNames[engineMatch[2]]; !admitted {
		return fmt.Errorf("engine replica name=%q is outside managed_replica_names", engineMatch[2])
	}
	if normalizeManagedExpression(definition.sortingKey) != normalizeManagedExpression(contract.sortingKey) {
		return fmt.Errorf("sorting key=%q, want %q", definition.sortingKey, contract.sortingKey)
	}
	if normalizeManagedExpression(definition.primaryKey) != normalizeManagedExpression(contract.sortingKey) {
		return fmt.Errorf("primary key=%q, want %q", definition.primaryKey, contract.sortingKey)
	}
	if strings.TrimSpace(definition.partitionKey) != "" {
		return errors.New("partitioned managed tables are not admitted because deduplication and part limits must cover the complete receipt keyspace")
	}
	normalizedCreate := normalizeManagedSQL(definition.createSQL)
	if strings.Contains(normalizedCreate, "ttl") {
		return errors.New("managed tables must not expire changelog or receipt evidence with TTL")
	}
	settings := []struct {
		name string
		min  uint64
	}{
		{name: "replicated_deduplication_window", min: managedMinDedupWindow},
		{name: "replicated_deduplication_window_seconds", min: managedMinDedupWindowSeconds},
		{name: "parts_to_delay_insert", min: 100},
		{name: "parts_to_throw_insert", min: 200},
		{name: "max_parts_in_total", min: 1000},
	}
	for _, setting := range settings {
		value, ok := managedTableSetting(definition.createSQL, setting.name)
		if setting.name == "parts_to_throw_insert" && ok && value <= contract.maxActiveParts {
			return fmt.Errorf("setting parts_to_throw_insert=%d must exceed managed_max_active_parts=%d", value, contract.maxActiveParts)
		}
		if !ok || value < setting.min {
			return fmt.Errorf("setting %s must be explicit and >= %d", setting.name, setting.min)
		}
	}
	if len(definition.columns) != len(contract.columns) {
		return fmt.Errorf("column count=%d, want %d", len(definition.columns), len(contract.columns))
	}
	for name, wantType := range contract.columns {
		gotType, ok := definition.columns[name]
		if !ok || normalizeManagedType(gotType) != normalizeManagedType(wantType) {
			return fmt.Errorf("column %s type=%q, want %q", name, gotType, wantType)
		}
		if kind := strings.TrimSpace(definition.columnKinds[name]); kind != "" {
			return fmt.Errorf("column %s default kind=%q; managed columns must be ordinary stored columns", name, kind)
		}
	}
	return nil
}

func (d *Destination) validateManagedFinalView(ctx context.Context, conn chdriver.Conn) error {
	var engine, createSQL string
	if err := conn.QueryRow(ctx, "SELECT engine,create_table_query FROM system.tables WHERE database=? AND name=?", d.managedConfig.database, d.managedConfig.finalView).Scan(&engine, &createSQL); err != nil {
		return fmt.Errorf("read managed ClickHouse FINAL view: %w", err)
	}
	if err := validateManagedFinalViewDefinition(engine, createSQL, d.managedConfig.database, d.managedConfig.finalView, d.managedConfig.changelogTable); err != nil {
		return fmt.Errorf("managed ClickHouse view %s: %w", d.managedConfig.finalView, err)
	}
	columns := make(map[string]string)
	rows, err := conn.Query(ctx, "SELECT name,type FROM system.columns WHERE database=? AND table=?", d.managedConfig.database, d.managedConfig.finalView)
	if err != nil {
		return fmt.Errorf("read managed ClickHouse FINAL view columns: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return fmt.Errorf("scan managed ClickHouse FINAL view column: %w", err)
		}
		columns[name] = typ
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate managed ClickHouse FINAL view columns: %w", err)
	}
	expected := managedExpectedChangelogColumns()
	if len(columns) != len(expected) {
		return fmt.Errorf("managed ClickHouse FINAL view column count=%d, want %d", len(columns), len(expected))
	}
	for name, wantType := range expected {
		if normalizeManagedType(columns[name]) != normalizeManagedType(wantType) {
			return fmt.Errorf("managed ClickHouse FINAL view column %s type=%q, want %q", name, columns[name], wantType)
		}
	}
	return nil
}

func validateManagedFinalViewDefinition(engine, createSQL, database, view, source string) error {
	expected := normalizeManagedSQL("CREATE VIEW " + database + "." + view + " AS SELECT * FROM " + database + "." + source + " FINAL")
	expectedInvoker := normalizeManagedSQL("CREATE VIEW " + database + "." + view + " SQL SECURITY INVOKER AS SELECT * FROM " + database + "." + source + " FINAL")
	viewPrefix := normalizeManagedSQL("CREATE VIEW " + database + "." + view + "(")
	selectSuffix := normalizeManagedSQL("AS SELECT * FROM " + database + "." + source + " FINAL")
	actual := normalizeManagedSQL(createSQL)
	generatedColumns := strings.HasPrefix(actual, viewPrefix) && strings.HasSuffix(actual, selectSuffix)
	if engine != "View" || (actual != expected && actual != expectedInvoker && !generatedColumns) {
		return fmt.Errorf("must be exactly SELECT * FROM %s.%s FINAL without filters or transforms; got %q", database, source, createSQL)
	}
	return nil
}

func managedTableSetting(createSQL, name string) (uint64, bool) {
	expression := regexp.MustCompile(`(?i)(?:^|[,\s])` + regexp.QuoteMeta(name) + `\s*=\s*([0-9]+)`)
	match := expression.FindStringSubmatch(createSQL)
	if len(match) != 2 {
		return 0, false
	}
	value, err := strconv.ParseUint(match[1], 10, 64)
	return value, err == nil
}

func normalizeManagedSQL(value string) string {
	return strings.ToLower(strings.NewReplacer(" ", "", "\n", "", "\t", "", "`", "").Replace(value))
}

func normalizeManagedExpression(value string) string {
	normalized := normalizeManagedSQL(value)
	if strings.HasPrefix(normalized, "(") && strings.HasSuffix(normalized, ")") {
		normalized = strings.TrimSuffix(strings.TrimPrefix(normalized, "("), ")")
	}
	return normalized
}

func normalizeManagedType(value string) string {
	return strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(value, " ", ""), `"`, `'`))
}

func managedExpectedChangelogColumns() map[string]string {
	return map[string]string{
		"flow_id": "String", "flow_incarnation_id": "String", "source_lineage_id": "String", "destination_revision_id": "String",
		"logical_batch_id": "String", "content_hash": "FixedString(64)", "source_position": "String", "transaction_id": "UInt64",
		"begin_lsn": "String", "commit_lsn": "String", "end_lsn": "String", "fragment_ordinal": "UInt64", "record_ordinal": "UInt64",
		"source_namespace": "String", "source_table": "String", "schema_version": "Int64", "schema_fingerprint": "FixedString(64)",
		"schema_json": "String", "operation": "LowCardinality(String)", "tombstone": "UInt8", "key_json": "String", "before_json": "String",
		"after_json": "String", "payload": "String", "ddl_plan": "String", "event_time": "DateTime64(9, 'UTC')", "record_hash": "FixedString(64)", "wallaby_version": "UInt64",
	}
}

func managedExpectedReceiptColumns() map[string]string {
	return map[string]string{
		"flow_id": "String", "flow_incarnation_id": "String", "source_lineage_id": "String", "destination_revision_id": "String",
		"logical_batch_id": "String", "content_hash": "FixedString(64)", "source_position": "String", "transaction_id": "UInt64",
		"fragment_count": "UInt64", "record_count": "UInt64", "query_ids": "Array(String)", "committed_at": "DateTime64(9, 'UTC')",
		"wallaby_version": "UInt64", "external_id": "String",
	}
}
