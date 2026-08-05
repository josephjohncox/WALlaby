package iceberg

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	iceberggo "github.com/apache/iceberg-go"
	icecatalog "github.com/apache/iceberg-go/catalog"
	icerest "github.com/apache/iceberg-go/catalog/rest"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
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
func NewRESTCommitter(ctx context.Context, objects CanonicalObjectReader, config Config) (*Committer, error) {
	backend, err := newRESTBackend(ctx, config)
	if err != nil {
		return nil, err
	}
	return NewCommitter(objects, backend, config)
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
	if err != nil {
		return nil, fmt.Errorf("parse Iceberg REST URI %q: %w", cfg.URI, err)
	}
	if endpoint.Host == "" || endpoint.User != nil {
		return nil, fmt.Errorf("iceberg REST URI %q must include a host without user info", cfg.URI)
	}
	authenticated := catalogAuthenticationConfigured(cfg) || cfg.Profile == CatalogProfileS3Tables
	if endpoint.Scheme != "https" {
		if endpoint.Scheme != "http" || !cfg.AllowHTTP || !isLoopbackCatalogHost(endpoint.Hostname()) {
			return nil, errors.New("iceberg REST URI must use HTTPS; allow_http is only for loopback emulation")
		}
		if authenticated {
			return nil, errors.New("authenticated Iceberg REST requires HTTPS")
		}
	}
	var authURI *url.URL
	if strings.TrimSpace(cfg.OAuthURI) != "" {
		authURI, err = url.Parse(cfg.OAuthURI)
		if err != nil {
			return nil, fmt.Errorf("parse Iceberg OAuth URI: %w", err)
		}
		if authURI.Scheme != "https" || authURI.Host == "" || authURI.User != nil {
			return nil, errors.New("iceberg OAuth URI must use HTTPS and include a host without user info")
		}
		if endpointOrigin(authURI) != endpointOrigin(endpoint) {
			return nil, errors.New("iceberg OAuth URI must use the same origin as the deployment-bound catalog")
		}
	}
	if cfg.SigV4 && (cfg.OAuthToken != "" || cfg.OAuthCredential != "") {
		return nil, errors.New("iceberg REST SigV4 and OAuth authentication are mutually exclusive")
	}
	if rawS3Endpoint := strings.TrimSpace(cfg.S3Endpoint); rawS3Endpoint != "" {
		s3Endpoint, parseErr := url.Parse(rawS3Endpoint)
		if parseErr != nil {
			return nil, fmt.Errorf("parse deployment-bound Iceberg S3 endpoint %q: %w", rawS3Endpoint, parseErr)
		}
		if s3Endpoint.Host == "" || s3Endpoint.User != nil {
			return nil, fmt.Errorf("deployment-bound Iceberg S3 endpoint %q must include a host without user info", rawS3Endpoint)
		}
		if s3Endpoint.Scheme != "https" && (s3Endpoint.Scheme != "http" || !cfg.AllowHTTP || !isLoopbackCatalogHost(s3Endpoint.Hostname())) {
			return nil, errors.New("iceberg S3 endpoint must use HTTPS; allow_http is only for loopback emulation")
		}
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
	policyTransport := &catalogPropertyPolicyTransport{base: transport, cfg: cfg}
	boundTransport, err := newEndpointBoundTransport(policyTransport, cfg.URI)
	if err != nil {
		return nil, err
	}
	wrapped := &requestTimeoutTransport{base: boundTransport, timeout: cfg.RequestTimeout}
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
	if authURI != nil {
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
		if expectedRole := strings.TrimSpace(cfg.ExpectedAWSRoleARN); expectedRole != "" {
			identity, identityErr := sts.NewFromConfig(awsCfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
			if identityErr != nil {
				return nil, fmt.Errorf("resolve Iceberg REST AWS caller identity: %w", identityErr)
			}
			actualARN := aws.ToString(identity.Arn)
			if !awsRoleIdentityMatches(expectedRole, actualARN) {
				return nil, fmt.Errorf("iceberg REST AWS caller identity %q does not match deployment-bound role %q", actualARN, expectedRole)
			}
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

const maxCatalogPolicyResponseBytes = 32 << 20

func awsRoleIdentityMatches(expected, actual string) bool {
	expected = strings.TrimSpace(expected)
	actual = strings.TrimSpace(actual)
	if expected == actual {
		return expected != ""
	}
	expectedParts := strings.SplitN(expected, ":", 6)
	actualParts := strings.SplitN(actual, ":", 6)
	if len(expectedParts) != 6 || len(actualParts) != 6 || expectedParts[2] != "iam" || actualParts[2] != "sts" || expectedParts[4] != actualParts[4] {
		return false
	}
	if !strings.HasPrefix(expectedParts[5], "role/") || !strings.HasPrefix(actualParts[5], "assumed-role/") {
		return false
	}
	expectedRole := strings.TrimPrefix(expectedParts[5], "role/")
	if slash := strings.LastIndex(expectedRole, "/"); slash >= 0 {
		expectedRole = expectedRole[slash+1:]
	}
	assumed := strings.Split(strings.TrimPrefix(actualParts[5], "assumed-role/"), "/")
	return len(assumed) >= 2 && expectedRole != "" && assumed[0] == expectedRole
}

func endpointOrigin(endpoint *url.URL) string {
	return strings.ToLower(endpoint.Scheme) + "://" + strings.ToLower(endpoint.Host)
}

func isLoopbackCatalogHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	address := net.ParseIP(host)
	return address != nil && address.IsLoopback()
}

func isAWSObjectEndpoint(raw string) bool {
	endpoint, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil {
		return false
	}
	host := strings.ToLower(endpoint.Hostname())
	if host == "s3.amazonaws.com" {
		return true
	}
	const suffix = ".amazonaws.com"
	if !strings.HasSuffix(host, suffix) {
		return false
	}
	service := strings.TrimSuffix(host, suffix)
	return strings.HasPrefix(service, "s3.") || strings.HasPrefix(service, "s3-") || strings.HasPrefix(service, "s3tables.")
}

func validateCatalogResponseProperties(cfg Config, properties map[string]string) error {
	for key, expected := range map[string]string{
		"uri": cfg.URI, "warehouse": cfg.Warehouse, "prefix": cfg.Prefix,
	} {
		if value := strings.TrimSpace(properties[key]); value != "" && strings.TrimSuffix(value, "/") != strings.TrimSuffix(strings.TrimSpace(expected), "/") {
			return fmt.Errorf("iceberg catalog %s %q differs from deployment-bound value", key, value)
		}
	}
	if value := strings.TrimSpace(properties["s3.endpoint"]); value != "" {
		expected := strings.TrimSpace(cfg.S3Endpoint)
		if expected != "" {
			if strings.TrimSuffix(value, "/") != strings.TrimSuffix(expected, "/") {
				return fmt.Errorf("iceberg catalog s3.endpoint %q differs from deployment-bound value", value)
			}
		} else if !isAWSObjectEndpoint(value) {
			return fmt.Errorf("iceberg catalog s3.endpoint %q is not a deployment-bound or AWS object endpoint", value)
		}
	}
	if value := strings.TrimSpace(properties["s3.region"]); value != "" {
		expected := strings.TrimSpace(cfg.S3Region)
		if expected == "" {
			expected = strings.TrimSpace(cfg.Region)
		}
		if expected == "" || value != expected {
			return fmt.Errorf("iceberg catalog s3.region %q differs from deployment-bound value", value)
		}
	}
	return nil
}

func validateCatalogResponseBody(cfg Config, body []byte) error {
	if !json.Valid(body) {
		return nil // The Iceberg REST decoder reports malformed JSON.
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("decode Iceberg REST endpoint-policy envelope: %w", err)
	}
	for _, name := range []string{"defaults", "overrides", "config"} {
		raw, ok := envelope[name]
		if !ok || string(raw) == "null" {
			continue
		}
		var properties map[string]string
		if err := json.Unmarshal(raw, &properties); err != nil {
			return fmt.Errorf("decode Iceberg REST %s endpoint properties: %w", name, err)
		}
		if err := validateCatalogResponseProperties(cfg, properties); err != nil {
			return err
		}
	}
	if raw, ok := envelope["metadata"]; ok && string(raw) != "null" {
		var metadata struct {
			Properties map[string]string `json:"properties"`
		}
		if err := json.Unmarshal(raw, &metadata); err != nil {
			return fmt.Errorf("decode Iceberg table metadata endpoint properties: %w", err)
		}
		if err := validateCatalogResponseProperties(cfg, metadata.Properties); err != nil {
			return err
		}
	}
	return nil
}

type catalogPropertyPolicyTransport struct {
	base http.RoundTripper
	cfg  Config
}

func (t *catalogPropertyPolicyTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.base.RoundTrip(request)
	if err != nil || response == nil || response.Body == nil || response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return response, err
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, maxCatalogPolicyResponseBytes+1))
	_ = response.Body.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read Iceberg REST response for endpoint policy: %w", readErr)
	}
	if len(body) > maxCatalogPolicyResponseBytes {
		return nil, fmt.Errorf("iceberg REST response exceeds endpoint-policy limit of %d bytes", maxCatalogPolicyResponseBytes)
	}
	if err := validateCatalogResponseBody(t.cfg, body); err != nil {
		return nil, err
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	response.ContentLength = int64(len(body))
	return response, nil
}

type endpointBoundTransport struct {
	base    http.RoundTripper
	allowed map[string]struct{}
}

func newEndpointBoundTransport(base http.RoundTripper, rawEndpoints ...string) (*endpointBoundTransport, error) {
	if base == nil {
		return nil, errors.New("iceberg endpoint-bound transport requires a base transport")
	}
	allowed := make(map[string]struct{}, len(rawEndpoints))
	for _, raw := range rawEndpoints {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		endpoint, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("parse deployment-bound Iceberg endpoint %q: %w", raw, err)
		}
		if endpoint.Scheme == "" || endpoint.Host == "" || endpoint.User != nil {
			return nil, fmt.Errorf("deployment-bound Iceberg endpoint %q must include a scheme and host without user info", raw)
		}
		allowed[strings.ToLower(endpoint.Scheme)+"://"+strings.ToLower(endpoint.Host)] = struct{}{}
	}
	if len(allowed) == 0 {
		return nil, errors.New("iceberg endpoint-bound transport requires at least one endpoint")
	}
	return &endpointBoundTransport{base: base, allowed: allowed}, nil
}

func (t *endpointBoundTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request == nil || request.URL == nil || request.URL.User != nil {
		return nil, errors.New("iceberg request has an invalid deployment-bound endpoint")
	}
	identity := strings.ToLower(request.URL.Scheme) + "://" + strings.ToLower(request.URL.Host)
	if _, ok := t.allowed[identity]; !ok {
		return nil, fmt.Errorf("iceberg request endpoint %q is outside deployment-bound Iceberg endpoints", identity)
	}
	return t.base.RoundTrip(request)
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
		return catalogTable{}, classifyRESTCatalogCommitError(err)
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
		return catalogSnapshot{}, classifyRESTCatalogCommitError(err)
	}
	current := committed.CurrentSnapshot()
	if current == nil {
		return catalogSnapshot{}, errors.New("iceberg commit returned no current snapshot")
	}
	return catalogSnapshotFromIceberg(*current), nil
}

func classifyRESTCatalogCommitError(err error) error {
	if errors.Is(err, icecatalog.ErrNoSuchTable) ||
		errors.Is(err, icecatalog.ErrNoSuchNamespace) ||
		errors.Is(err, icerest.ErrBadRequest) ||
		errors.Is(err, icerest.ErrUnauthorized) ||
		errors.Is(err, icerest.ErrForbidden) ||
		isPermanentRESTTransportError(err) {
		return err
	}
	if errors.Is(err, icerest.ErrCommitFailed) {
		return ErrCatalogConflict
	}
	if errors.Is(err, icerest.ErrCommitStateUnknown) ||
		errors.Is(err, icerest.ErrAuthorizationExpired) ||
		errors.Is(err, icerest.ErrServiceUnavailable) ||
		errors.Is(err, icerest.ErrServerError) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		isTransientRESTTransportError(err) {
		return fmt.Errorf("%w: %w", ErrCatalogIndeterminate, err)
	}
	if errors.Is(err, icerest.ErrRESTError) {
		return err
	}
	// An unclassified transport failure after the request left the process cannot
	// prove absence; reconciliation must inspect snapshot identity.
	return fmt.Errorf("%w: %w", ErrCatalogIndeterminate, err)
}

func isPermanentRESTTransportError(err error) bool {
	var unknownAuthority x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalidCertificate x509.CertificateInvalidError
	var certificateVerification *tls.CertificateVerificationError
	var recordHeader tls.RecordHeaderError
	var alert tls.AlertError
	var echRejection *tls.ECHRejectionError
	if errors.As(err, &unknownAuthority) ||
		errors.As(err, &hostname) ||
		errors.As(err, &invalidCertificate) ||
		errors.As(err, &certificateVerification) ||
		errors.As(err, &recordHeader) ||
		errors.As(err, &alert) ||
		errors.As(err, &echRejection) {
		return true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && !dnsErr.IsTimeout && !dnsErr.IsTemporary {
		return true
	}
	var urlErr *url.Error
	return errors.As(err, &urlErr) && !isTransientRESTTransportError(err)
}

func isTransientRESTTransportError(err error) bool {
	var dnsErr *net.DNSError
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ETIMEDOUT) ||
		(errors.As(err, &dnsErr) && (dnsErr.IsTimeout || dnsErr.IsTemporary))
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
