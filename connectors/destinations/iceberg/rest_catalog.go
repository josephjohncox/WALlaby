package iceberg

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	iceberggo "github.com/apache/iceberg-go"
	icecatalog "github.com/apache/iceberg-go/catalog"
	icerest "github.com/apache/iceberg-go/catalog/rest"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/josephjohncox/wallaby/internal/artifactlog"

	// The blank import registers the S3-compatible FileIO schemes (s3, s3a,
	// s3n) with iceberg-go's io registry. The base io package only registers
	// file:// and the empty scheme, so without this the REST committer cannot
	// read or write table data and metadata objects on any s3:// warehouse
	// (MinIO here, AWS S3 / S3 Tables in production) and Append fails with
	// "io scheme not registered for path". This matches iceberg-go's own
	// convention of blank-importing the gocloud driver at the program edge.
	_ "github.com/apache/iceberg-go/io/gocloud"
)

type restBackend struct {
	catalog icecatalog.Catalog
}

// NewRESTCommitter constructs the production REST catalog and append-only
// committer. HTTP is rejected unless explicitly enabled for local emulation.
func NewRESTCommitter(ctx context.Context, objects CanonicalObjectReader, config Config, options ...CommitterOption) (*Committer, error) {
	backend, err := newRESTBackend(ctx, config)
	if err != nil {
		return nil, err
	}
	return NewCommitter(objects, backend, config, options...)
}

func newRESTBackend(ctx context.Context, cfg Config) (*restBackend, error) {
	catalog, err := newRESTCatalog(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &restBackend{catalog: catalog}, nil
}

func newRESTCatalog(ctx context.Context, cfg Config) (*icerest.Catalog, error) {
	endpoint, err := url.Parse(cfg.URI)
	if err != nil || endpoint.Host == "" {
		return nil, fmt.Errorf("parse Iceberg REST URI %q: %w", cfg.URI, err)
	}
	if endpoint.Scheme != "https" && (!cfg.AllowHTTP || endpoint.Scheme != "http") {
		return nil, errors.New("iceberg REST URI must use HTTPS; allow_http is only for local emulation")
	}
	if cfg.SigV4 && (cfg.OAuthToken != "" || cfg.OAuthCredential != "") {
		return nil, errors.New("iceberg REST SigV4 and OAuth authentication are mutually exclusive")
	}
	tlsConfig, err := catalogTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	dialer := &net.Dialer{Timeout: minDuration(cfg.RequestTimeout, 10*time.Second), KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment, DialContext: dialer.DialContext,
		TLSClientConfig: tlsConfig, TLSHandshakeTimeout: minDuration(cfg.RequestTimeout, 10*time.Second),
		ResponseHeaderTimeout: cfg.RequestTimeout, ExpectContinueTimeout: time.Second,
		IdleConnTimeout: 90 * time.Second, MaxIdleConns: 32, MaxIdleConnsPerHost: 8,
	}
	wrapped := &requestTimeoutTransport{base: transport, timeout: cfg.RequestTimeout}
	restOptions := []icerest.Option{
		icerest.WithWarehouseLocation(cfg.Warehouse),
		icerest.WithCustomTransport(wrapped),
	}
	if cfg.Prefix != "" {
		restOptions = append(restOptions, icerest.WithPrefix(cfg.Prefix))
	}
	// Client-side FileIO settings let the committer read and write table objects
	// through an S3-compatible endpoint (for example a port-forwarded MinIO)
	// even when the catalog server reports its own in-cluster endpoint. These
	// are additional props, so a catalog override still wins when present.
	if fileIOProps := s3FileIOProps(cfg); len(fileIOProps) > 0 {
		restOptions = append(restOptions, icerest.WithAdditionalProps(fileIOProps))
	}
	if cfg.OAuthToken != "" {
		restOptions = append(restOptions, icerest.WithOAuthToken(cfg.OAuthToken))
	}
	if cfg.OAuthCredential != "" {
		restOptions = append(restOptions, icerest.WithCredential(cfg.OAuthCredential))
	}
	if cfg.OAuthScope != "" {
		restOptions = append(restOptions, icerest.WithScope(cfg.OAuthScope))
	}
	if cfg.OAuthURI != "" {
		authURI, parseErr := url.Parse(cfg.OAuthURI)
		if parseErr != nil {
			return nil, fmt.Errorf("parse Iceberg OAuth URI: %w", parseErr)
		}
		restOptions = append(restOptions, icerest.WithAuthURI(authURI))
	}
	if cfg.SigV4 {
		region := strings.TrimSpace(cfg.Region)
		if region == "" {
			return nil, errors.New("iceberg REST SigV4 requires region")
		}
		service := strings.TrimSpace(cfg.SigningName)
		if service == "" {
			service = "execute-api"
		}
		awsCfg, loadErr := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
		if loadErr != nil {
			return nil, fmt.Errorf("load Iceberg REST AWS credentials: %w", loadErr)
		}
		restOptions = append(restOptions, icerest.WithAwsConfig(awsCfg), icerest.WithSigV4RegionSvc(region, service))
	}
	return icerest.NewCatalog(ctx, "wallaby", cfg.URI, restOptions...)
}

func s3FileIOProps(cfg Config) iceberggo.Properties {
	props := iceberggo.Properties{}
	if value := strings.TrimSpace(cfg.S3Endpoint); value != "" {
		props[icebergio.S3EndpointURL] = value
	}
	if value := strings.TrimSpace(cfg.S3AccessKeyID); value != "" {
		props[icebergio.S3AccessKeyID] = value
	}
	if cfg.S3SecretAccessKey != "" {
		props[icebergio.S3SecretAccessKey] = cfg.S3SecretAccessKey
	}
	region := strings.TrimSpace(cfg.S3Region)
	if region == "" {
		region = strings.TrimSpace(cfg.Region)
	}
	if region != "" {
		props[icebergio.S3Region] = region
	}
	return props
}

func catalogTLSConfig(cfg Config) (*tls.Config, error) {
	// #nosec G402 -- TLS 1.2 remains the minimum required by supported catalogs.
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: cfg.ServerName}
	if cfg.CAFile != "" || cfg.CAData != "" {
		pool, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("load system certificate pool: %w", err)
		}
		if pool == nil {
			pool = x509.NewCertPool()
		}
		pemData := []byte(cfg.CAData)
		if cfg.CAFile != "" {
			// #nosec G304 -- the CA path is explicit deployment configuration.
			fileData, readErr := os.ReadFile(cfg.CAFile)
			if readErr != nil {
				return nil, fmt.Errorf("read Iceberg REST CA file: %w", readErr)
			}
			pemData = append(pemData, fileData...)
		}
		if !pool.AppendCertsFromPEM(pemData) {
			return nil, errors.New("iceberg REST CA data contains no certificates")
		}
		tlsConfig.RootCAs = pool
	}
	if (cfg.ClientCertFile == "") != (cfg.ClientKeyFile == "") {
		return nil, errors.New("iceberg REST client certificate and key must be configured together")
	}
	if cfg.ClientCertFile != "" {
		certificate, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load Iceberg REST client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	return tlsConfig, nil
}

type cancelReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (body *cancelReadCloser) Close() error {
	err := body.ReadCloser.Close()
	body.cancel()
	return err
}

type requestTimeoutTransport struct {
	base    http.RoundTripper
	timeout time.Duration
}

func (transport *requestTimeoutTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(request.Context(), transport.timeout)
	clone := request.Clone(ctx)
	response, err := transport.base.RoundTrip(clone)
	if err != nil {
		cancel()
		return nil, err
	}
	response.Body = &cancelReadCloser{ReadCloser: response.Body, cancel: cancel}
	return response, nil
}

func minDuration(left, right time.Duration) time.Duration {
	if left < right {
		return left
	}
	return right
}

func (backend *restBackend) Load(ctx context.Context, identifier table.Identifier) (catalogTable, error) {
	loaded, err := backend.catalog.LoadTable(ctx, identifier)
	if err != nil {
		if errors.Is(err, icecatalog.ErrNoSuchTable) {
			return catalogTable{}, ErrTableNotFound
		}
		return catalogTable{}, err
	}
	return catalogTableFromIceberg(loaded), nil
}

func (backend *restBackend) Create(ctx context.Context, identifier table.Identifier, schema *iceberggo.Schema) (catalogTable, error) {
	namespace := icecatalog.NamespaceFromIdent(identifier)
	exists, err := backend.catalog.CheckNamespaceExists(ctx, namespace)
	if err != nil {
		return catalogTable{}, err
	}
	if !exists {
		if err := backend.catalog.CreateNamespace(ctx, namespace, iceberggo.Properties{}); err != nil && !errors.Is(err, icecatalog.ErrNamespaceAlreadyExists) {
			return catalogTable{}, err
		}
	}
	created, err := backend.catalog.CreateTable(ctx, identifier, schema, icecatalog.WithProperties(iceberggo.Properties{
		"format-version":                  "2",
		"write.target-file-size-bytes":    strconv.Itoa(artifactlog.TargetEncodedObject),
		"write.parquet.compression-codec": "zstd",
		"write.delete.mode":               "copy-on-write",
	}))
	if err != nil {
		if errors.Is(err, icecatalog.ErrTableAlreadyExists) {
			return catalogTable{}, ErrCatalogConflict
		}
		return catalogTable{}, err
	}
	return catalogTableFromIceberg(created), nil
}

func (backend *restBackend) Evolve(ctx context.Context, state catalogTable, adds []iceberggo.NestedField, renames []renameOp) (catalogTable, error) {
	loaded, ok := state.opaque.(*table.Table)
	if !ok || loaded == nil {
		return catalogTable{}, errors.New("iceberg REST table handle is missing")
	}
	if len(adds) == 0 && len(renames) == 0 {
		return state, nil
	}
	transaction := loaded.NewTransaction()
	update := transaction.UpdateSchema(false, false)
	for _, rename := range renames {
		update.RenameColumn([]string{rename.from}, rename.to)
	}
	for _, add := range adds {
		// Additive columns are nullable so existing rows remain valid; the
		// catalog assigns the fresh field ID.
		update.AddColumn([]string{add.Name}, add.Type, add.Doc, false, nil)
	}
	if err := update.Commit(); err != nil {
		return catalogTable{}, fmt.Errorf("plan Iceberg schema evolution: %w", err)
	}
	committed, err := transaction.Commit(ctx)
	if err != nil {
		switch {
		case errors.Is(err, icerest.ErrCommitFailed):
			return catalogTable{}, ErrCatalogConflict
		case errors.Is(err, icerest.ErrCommitStateUnknown), errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
			return catalogTable{}, fmt.Errorf("%w: %w", ErrCatalogIndeterminate, err)
		default:
			return catalogTable{}, fmt.Errorf("%w: %w", ErrCatalogIndeterminate, err)
		}
	}
	return catalogTableFromIceberg(committed), nil
}

func (backend *restBackend) Append(ctx context.Context, state catalogTable, schema *iceberggo.Schema, records []arrow.RecordBatch, summary map[string]string) (catalogSnapshot, error) {
	loaded, ok := state.opaque.(*table.Table)
	if !ok || loaded == nil {
		return catalogSnapshot{}, errors.New("iceberg REST table handle is missing")
	}
	if len(records) == 0 {
		return catalogSnapshot{}, errors.New("iceberg append has no record batches")
	}
	recordReader, err := array.NewRecordReader(records[0].Schema(), records)
	if err != nil {
		return catalogSnapshot{}, fmt.Errorf("create Iceberg record reader: %w", err)
	}
	defer recordReader.Release()
	transaction := loaded.NewTransaction()
	if err := transaction.Append(ctx, recordReader, iceberggo.Properties(summary)); err != nil {
		return catalogSnapshot{}, fmt.Errorf("plan Iceberg data files: %w", err)
	}
	committed, err := transaction.Commit(ctx)
	if err != nil {
		switch {
		case errors.Is(err, icerest.ErrCommitFailed):
			return catalogSnapshot{}, ErrCatalogConflict
		case errors.Is(err, icerest.ErrCommitStateUnknown), errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
			return catalogSnapshot{}, fmt.Errorf("%w: %w", ErrCatalogIndeterminate, err)
		default:
			// A transport failure after the request left the process cannot prove
			// absence; reconciliation must inspect snapshot summaries.
			return catalogSnapshot{}, fmt.Errorf("%w: %w", ErrCatalogIndeterminate, err)
		}
	}
	current := committed.CurrentSnapshot()
	if current == nil {
		return catalogSnapshot{}, errors.New("iceberg commit returned no current snapshot")
	}
	return catalogSnapshotFromIceberg(*current), nil
}

func catalogTableFromIceberg(loaded *table.Table) catalogTable {
	state := catalogTable{
		Identifier: loaded.Identifier(), Schema: loaded.Schema(), PartitionSpec: loaded.Spec(), opaque: loaded,
	}
	metadata := loaded.Metadata()
	for _, snapshot := range metadata.Snapshots() {
		state.Snapshots = append(state.Snapshots, catalogSnapshotFromIceberg(snapshot))
	}
	if current := loaded.CurrentSnapshot(); current != nil {
		id := current.SnapshotID
		state.CurrentSnapshotID = &id
	}
	return state
}

func catalogSnapshotFromIceberg(snapshot table.Snapshot) catalogSnapshot {
	result := catalogSnapshot{
		ID: snapshot.SnapshotID, ParentID: snapshot.ParentSnapshotID,
		Timestamp: time.UnixMilli(snapshot.TimestampMs).UTC(), Summary: map[string]string{},
	}
	if snapshot.Summary != nil {
		for key, value := range snapshot.Summary.Properties {
			result.Summary[key] = value
		}
	}
	return result
}
