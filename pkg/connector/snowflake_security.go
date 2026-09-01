package connector

import (
	"bytes"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/snowflakedb/gosnowflake"
)

const maxSnowflakePrivateKeyBytes = 1 << 20

var (
	ErrSnowflakeExecutionDisabled     = errors.New("snowflake execution is disabled by deployment policy")
	ErrSnowflakePolicyInvalid         = errors.New("snowflake deployment policy is invalid")
	ErrSnowflakeStreamingRESTDisabled = errors.New("snowpipe Streaming REST execution is disabled by deployment policy")
	ErrSnowflakeIdentityNotAllowed    = errors.New("snowflake DSN identity is not allowed by deployment policy")
	ErrUnsafeSnowflakeDSN             = errors.New("snowflake DSN contains prohibited credential or connection control material")
	ErrMalformedSnowflakeDSN          = errors.New("snowflake DSN is malformed")
)

var persistedSnowflakeDSNKeys = map[string]struct{}{
	"account": {}, "authenticator": {}, "database": {}, "schema": {}, "warehouse": {}, "role": {}, "region": {},
	"ocspfailopen": {}, "validatedefaultparameters": {},
	"readlatestwrites": {}, "timezone": {}, "clientsessionkeepalive": {},
}

// SnowflakeDeploymentConfig is runtime-only trust configuration. The account,
// user, host, and key never come from a flow definition.
type SnowflakeDeploymentConfig struct {
	Enabled              bool
	StreamingRESTEnabled bool
	Account              string
	User                 string
	Host                 string
	PrivateKeyFile       string
}

// SnowflakeDeploymentPolicy is an immutable, prevalidated deployment trust
// boundary. Its zero value is disabled and no flow option can widen it.
type SnowflakeDeploymentPolicy struct {
	enabled              bool
	account              string
	user                 string
	host                 string
	privateKey           *rsa.PrivateKey
	clientConfigPath     string
	clientConfigDir      string
	state                *snowflakeDeploymentPolicyState
	streamingRESTEnabled bool
}

// SnowflakeStreamingRESTPolicy is an opaque capability derived from an active
// deployment policy whose Streaming REST gate is enabled. It shares revocation
// and key ownership with the base policy.
type SnowflakeStreamingRESTPolicy struct {
	policy SnowflakeDeploymentPolicy
}

type snowflakeDeploymentPolicyState struct {
	mu       sync.RWMutex
	closed   bool
	close    sync.Once
	closeErr error
}

// NewSnowflakeDeploymentPolicy validates and loads deployment identity before
// the server or worker can persist, dispatch, or execute a Snowflake-backed flow.
func NewSnowflakeDeploymentPolicy(cfg SnowflakeDeploymentConfig) (SnowflakeDeploymentPolicy, error) {
	if cfg.StreamingRESTEnabled && !cfg.Enabled {
		return SnowflakeDeploymentPolicy{}, ErrSnowflakePolicyInvalid
	}
	if !cfg.Enabled {
		return SnowflakeDeploymentPolicy{}, nil
	}
	account := cfg.Account
	user := cfg.User
	host := strings.ToLower(cfg.Host)
	if !validSnowflakeAccount(account) || !validSnowflakeUser(user) || !validSnowflakeHost(host) {
		return SnowflakeDeploymentPolicy{}, ErrSnowflakePolicyInvalid
	}
	key, err := LoadSnowflakePrivateKey(cfg.PrivateKeyFile)
	if err != nil {
		return SnowflakeDeploymentPolicy{}, err
	}
	return NewSnowflakeDeploymentPolicyWithPrivateKey(account, user, host, key, cfg.StreamingRESTEnabled)
}

// NewSnowflakeDeploymentPolicyWithPrivateKey supports deployment secret
// providers that resolve key bytes before constructing runtime dependencies.
func NewSnowflakeDeploymentPolicyWithPrivateKey(account, user, host string, key *rsa.PrivateKey, streamingRESTEnabled bool) (SnowflakeDeploymentPolicy, error) {
	host = strings.ToLower(host)
	if !validSnowflakeAccount(account) || !validSnowflakeUser(user) || !validSnowflakeHost(host) || !validSnowflakePrivateKey(key) {
		return SnowflakeDeploymentPolicy{}, ErrSnowflakePolicyInvalid
	}
	key, err := cloneSnowflakePrivateKey(key)
	if err != nil {
		return SnowflakeDeploymentPolicy{}, ErrSnowflakePolicyInvalid
	}
	clientConfigPath, clientConfigDir, err := createSnowflakeClientConfig()
	if err != nil {
		return SnowflakeDeploymentPolicy{}, err
	}
	return SnowflakeDeploymentPolicy{
		enabled: true, account: account, user: user, host: host, privateKey: key,
		clientConfigPath: clientConfigPath, clientConfigDir: clientConfigDir,
		state: &snowflakeDeploymentPolicyState{}, streamingRESTEnabled: streamingRESTEnabled,
	}, nil
}

func validSnowflakePrivateKey(key *rsa.PrivateKey) bool {
	return key != nil && key.N != nil && key.N.BitLen() >= 2048 && key.Validate() == nil
}

func cloneSnowflakePrivateKey(key *rsa.PrivateKey) (*rsa.PrivateKey, error) {
	if !validSnowflakePrivateKey(key) {
		return nil, ErrSnowflakePolicyInvalid
	}
	clone, err := x509.ParsePKCS1PrivateKey(x509.MarshalPKCS1PrivateKey(key))
	if err != nil || !validSnowflakePrivateKey(clone) {
		return nil, ErrSnowflakePolicyInvalid
	}
	return clone, nil
}

func validSnowflakeAccount(value string) bool {
	if value == "" || value != strings.TrimSpace(value) || len(value) > 255 {
		return false
	}
	separator := true
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			separator = false
			continue
		}
		if r != '.' && r != '-' && r != '_' || separator {
			return false
		}
		separator = true
	}
	return !separator
}

func validSnowflakeUser(value string) bool {
	if value == "" || value != strings.TrimSpace(value) || len(value) > 255 {
		return false
	}
	for index, r := range value {
		letter := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z'
		digit := r >= '0' && r <= '9'
		if index == 0 {
			if !letter && r != '_' {
				return false
			}
			continue
		}
		if !letter && !digit && r != '_' && r != '$' {
			return false
		}
	}
	return true
}

func validSnowflakeHost(value string) bool {
	if value == "" || value != strings.TrimSpace(value) || len(value) > 253 || value != strings.ToLower(value) || !strings.HasSuffix(value, ".snowflakecomputing.com") {
		return false
	}
	for _, label := range strings.Split(value, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, r := range label {
			letter := r >= 'a' && r <= 'z'
			digit := r >= '0' && r <= '9'
			if !letter && !digit && r != '-' {
				return false
			}
		}
	}
	return true
}

// CanonicalSnowflakeAccountIdentifier returns Snowflake's key-pair JWT account
// spelling: uppercase with organization separators normalized to hyphens.
func CanonicalSnowflakeAccountIdentifier(value string) (string, error) {
	if !validSnowflakeAccount(value) {
		return "", ErrSnowflakePolicyInvalid
	}
	return strings.ToUpper(strings.ReplaceAll(value, ".", "-")), nil
}

// SnowflakeRESTAccountLabel returns the account label used by Snowflake REST
// hostnames. Snowflake documents underscore-to-hyphen hostname normalization.
func SnowflakeRESTAccountLabel(value string) (string, error) {
	canonical, err := CanonicalSnowflakeAccountIdentifier(value)
	if err != nil {
		return "", err
	}
	return strings.ToLower(strings.ReplaceAll(canonical, "_", "-")), nil
}

func createSnowflakeClientConfig() (string, string, error) {
	dir, err := os.MkdirTemp("", "wallaby-snowflake-client-")
	if err != nil {
		return "", "", errors.New("create Snowflake client logging policy")
	}
	path := filepath.Join(dir, "sf_client_config.json")
	if err := os.WriteFile(path, []byte(`{"common":{"log_level":"OFF"}}`), 0o600); err != nil {
		_ = os.RemoveAll(dir)
		return "", "", errors.New("write Snowflake client logging policy")
	}
	return path, dir, nil
}

func (p SnowflakeDeploymentPolicy) lockActive() (func(), bool) {
	if !p.enabled || p.state == nil {
		return nil, false
	}
	p.state.mu.RLock()
	if p.state.closed || !validSnowflakeAccount(p.account) || !validSnowflakeUser(p.user) || !validSnowflakeHost(p.host) || !validSnowflakePrivateKey(p.privateKey) || p.clientConfigPath == "" || p.clientConfigDir == "" {
		p.state.mu.RUnlock()
		return nil, false
	}
	return p.state.mu.RUnlock, true
}

// Close revokes all value copies and removes the process-local client logging
// policy. Concurrent calls are idempotent.
func (p SnowflakeDeploymentPolicy) Close() error {
	if p.state == nil {
		return nil
	}
	p.state.mu.Lock()
	p.state.closed = true
	p.state.mu.Unlock()
	p.state.close.Do(func() { p.state.closeErr = os.RemoveAll(p.clientConfigDir) })
	return p.state.closeErr
}

// Enabled reports whether this prevalidated policy admits Snowflake execution.
func (p SnowflakeDeploymentPolicy) Enabled() bool {
	unlock, ok := p.lockActive()
	if ok {
		unlock()
	}
	return ok
}

// StreamingRESTPolicy returns the opaque Streaming REST capability only when
// both the base Snowflake policy and the deployment-only Streaming gate are
// active. Every copy shares base-policy revocation.
func (p SnowflakeDeploymentPolicy) StreamingRESTPolicy() (SnowflakeStreamingRESTPolicy, error) {
	unlock, ok := p.lockActive()
	if !ok {
		return SnowflakeStreamingRESTPolicy{}, ErrSnowflakePolicyInvalid
	}
	defer unlock()
	if !p.streamingRESTEnabled {
		return SnowflakeStreamingRESTPolicy{}, ErrSnowflakeStreamingRESTDisabled
	}
	return SnowflakeStreamingRESTPolicy{policy: p}, nil
}

// Enabled reports whether this Streaming capability and its base policy remain active.
func (p SnowflakeStreamingRESTPolicy) Enabled() bool {
	_, err := p.policy.StreamingRESTPolicy()
	return err == nil
}

// Admit validates one Streaming spec against the shared deployment policy.
func (p SnowflakeStreamingRESTPolicy) Admit(spec RuntimeSpec) error {
	if strings.TrimSpace(spec.Options["managed_profile"]) != ManagedProfilePostgresToSnowflakeStreamingRestAppendV1 {
		return ErrSnowflakePolicyInvalid
	}
	return p.policy.Admit([]RuntimeSpec{spec})
}

// BasePolicy returns a value copy that shares revocation and key ownership. It
// is used only for the credential-free Snowflake SQL connection.
func (p SnowflakeStreamingRESTPolicy) BasePolicy() SnowflakeDeploymentPolicy {
	return p.policy
}

// Admit validates every Snowflake-backed spec before allowing execution.
func (p SnowflakeDeploymentPolicy) Admit(specs []RuntimeSpec) error {
	for _, spec := range specs {
		if !IsSnowflakeEndpoint(spec.Type) {
			continue
		}
		if err := ValidateSnowflakeDSN(spec.Options["dsn"]); err != nil {
			return err
		}
		if !p.enabled {
			return ErrSnowflakeExecutionDisabled
		}
		if spec.Type == EndpointSnowflake && strings.TrimSpace(spec.Options["managed_profile"]) == ManagedProfilePostgresToSnowflakeStreamingRestAppendV1 && !p.streamingRESTEnabled {
			return ErrSnowflakeStreamingRESTDisabled
		}
		parsed, err := parsePersistableSnowflakeDSN(spec.Options["dsn"])
		if err != nil {
			return err
		}
		if err := p.validateIdentity(parsed); err != nil {
			return err
		}
	}
	return nil
}

func (p SnowflakeDeploymentPolicy) validateIdentity(cfg *gosnowflake.Config) error {
	unlock, ok := p.lockActive()
	if !ok {
		return ErrSnowflakePolicyInvalid
	}
	defer unlock()
	return p.validateIdentityUnlocked(cfg)
}

func (p SnowflakeDeploymentPolicy) validateIdentityUnlocked(cfg *gosnowflake.Config) error {
	if cfg == nil || !strings.EqualFold(cfg.Account, p.account) || !strings.EqualFold(cfg.User, p.user) || !strings.EqualFold(cfg.Host, p.host) || cfg.Port != 443 {
		return ErrSnowflakeIdentityNotAllowed
	}
	if !strings.EqualFold(cfg.Protocol, "https") || cfg.Authenticator != gosnowflake.AuthTypeJwt || cfg.DisableOCSPChecks || cfg.OCSPFailOpen != gosnowflake.OCSPFailOpenFalse {
		return ErrUnsafeSnowflakeDSN
	}
	if cfg.ProxyHost != "" || cfg.ProxyPort != 0 || cfg.ProxyUser != "" || cfg.ProxyPassword != "" || cfg.ProxyProtocol != "" || cfg.NoProxy != "" || cfg.TLSConfigName != "" {
		return ErrUnsafeSnowflakeDSN
	}
	return nil
}

// IsSnowflakeEndpoint identifies the five admitted execution cells: generic
// Snowflake, generic Snowpipe, and the three Snowflake managed profiles.
func IsSnowflakeEndpoint(endpointType EndpointType) bool {
	return endpointType == EndpointSnowflake || endpointType == EndpointSnowpipe
}

// ValidatePersistedSnowflakeSpec rejects credential-bearing Snowflake DSNs
// independently of deployment admission. This is safe for offline planning.
func ValidatePersistedSnowflakeSpec(spec RuntimeSpec) error {
	if !IsSnowflakeEndpoint(spec.Type) {
		return nil
	}
	return ValidateSnowflakeDSN(spec.Options["dsn"])
}

// ValidateSnowflakeDSN is the single persistence-safe Snowflake DSN validator.
// Every error is static and therefore cannot disclose the DSN, key, or value.
func ValidateSnowflakeDSN(dsn string) error {
	if len(dsn) == 0 || len(dsn) > 16*1024 || strings.ContainsAny(dsn, "\r\n\x00") {
		return ErrMalformedSnowflakeDSN
	}
	queryOffset := strings.IndexByte(dsn, '?')
	authority := dsn
	query := ""
	if queryOffset >= 0 {
		authority = dsn[:queryOffset]
		query = dsn[queryOffset+1:]
		if strings.ContainsRune(query, '?') {
			return ErrMalformedSnowflakeDSN
		}
	}
	if strings.ContainsRune(authority, '#') || strings.ContainsRune(query, '#') {
		return ErrMalformedSnowflakeDSN
	}
	at := strings.LastIndexByte(authority, '@')
	if at >= 0 {
		identity := authority[:at]
		decoded, err := decodeSnowflakeDSNComponent(identity)
		if err != nil {
			return ErrMalformedSnowflakeDSN
		}
		if colon := strings.IndexByte(decoded, ':'); colon >= 0 && strings.TrimSpace(decoded[colon+1:]) != "" {
			return ErrUnsafeSnowflakeDSN
		}
	}
	seen := make(map[string]struct{})
	for _, field := range splitSnowflakeDSNQuery(query) {
		if field == "" {
			return ErrMalformedSnowflakeDSN
		}
		parts := strings.SplitN(field, "=", 2)
		if len(parts) != 2 || parts[0] == "" {
			return ErrMalformedSnowflakeDSN
		}
		key, err := decodeSnowflakeDSNComponent(parts[0])
		if err != nil {
			return ErrMalformedSnowflakeDSN
		}
		value, err := decodeSnowflakeDSNComponent(parts[1])
		if err != nil || len(key) > 128 || len(value) > 2048 {
			return ErrMalformedSnowflakeDSN
		}
		normalized := normalizeSnowflakeDSNKey(key)
		if normalized == "" {
			return ErrMalformedSnowflakeDSN
		}
		if _, duplicate := seen[normalized]; duplicate {
			return ErrUnsafeSnowflakeDSN
		}
		seen[normalized] = struct{}{}
		if sensitiveSnowflakeDSNKey(normalized) || containsPrivateKeyMaterial(value) {
			return ErrUnsafeSnowflakeDSN
		}
		if _, admitted := persistedSnowflakeDSNKeys[normalized]; !admitted {
			return ErrUnsafeSnowflakeDSN
		}
	}
	_, err := parsePersistableSnowflakeDSN(dsn)
	return err
}

func splitSnowflakeDSNQuery(query string) []string {
	if query == "" {
		return nil
	}
	return strings.Split(query, "&")
}

func parsePersistableSnowflakeDSN(dsn string) (*gosnowflake.Config, error) {
	cfg, err := gosnowflake.ParseDSN(dsn)
	if err != nil {
		return nil, ErrMalformedSnowflakeDSN
	}
	if cfg.Password != "" || cfg.Passcode != "" || cfg.PasscodeInPassword || cfg.Token != "" || cfg.PrivateKey != nil || cfg.OauthClientSecret != "" || cfg.ProxyPassword != "" {
		return nil, ErrUnsafeSnowflakeDSN
	}
	if !strings.EqualFold(cfg.Protocol, "https") || cfg.Port != 443 || cfg.Authenticator != gosnowflake.AuthTypeJwt || cfg.DisableOCSPChecks || cfg.OCSPFailOpen != gosnowflake.OCSPFailOpenFalse || cfg.ProxyHost != "" || cfg.ProxyPort != 0 || cfg.ProxyUser != "" || cfg.ProxyProtocol != "" || cfg.NoProxy != "" || cfg.TLSConfigName != "" {
		return nil, ErrUnsafeSnowflakeDSN
	}
	return cfg, nil
}

func decodeSnowflakeDSNComponent(value string) (string, error) {
	const maxDecodePasses = 8
	decoded := value
	for range maxDecodePasses {
		next, err := url.QueryUnescape(decoded)
		if err != nil {
			return "", err
		}
		if next == decoded {
			return decoded, nil
		}
		decoded = next
	}
	if strings.Contains(decoded, "%") {
		return "", ErrMalformedSnowflakeDSN
	}
	return decoded, nil
}

func normalizeSnowflakeDSNKey(value string) string {
	var builder strings.Builder
	for _, character := range strings.ToLower(strings.TrimSpace(value)) {
		switch {
		case character >= 'a' && character <= 'z', character >= '0' && character <= '9':
			builder.WriteRune(character)
		case character == '_', character == '-', character == '.', character == ' ':
		default:
			return ""
		}
	}
	return builder.String()
}

func sensitiveSnowflakeDSNKey(key string) bool {
	if key == "password" || key == "passwd" || key == "passcode" || key == "passphrase" || key == "pwd" || key == "credential" || key == "credentials" || key == "secret" || key == "key" {
		return true
	}
	for _, fragment := range []string{
		"privatekey", "pem", "base64key", "proxypassword", "proxypasswd", "proxypasscode",
		"accesstoken", "refreshtoken", "idtoken", "mfatoken", "authtoken", "bearertoken",
		"oauthsecret", "oauthcredential", "clientsecret", "clientcredential", "credentialsecret",
	} {
		if strings.Contains(key, fragment) {
			return true
		}
	}
	return strings.HasSuffix(key, "token") || strings.HasSuffix(key, "secret") || strings.HasSuffix(key, "credential") || strings.HasSuffix(key, "password") || strings.HasSuffix(key, "passwd") || strings.HasSuffix(key, "passcode")
}

func containsPrivateKeyMaterial(value string) bool {
	trimmed := strings.TrimSpace(value)
	upper := strings.ToUpper(trimmed)
	if strings.Contains(upper, "-----BEGIN") || strings.Contains(normalizeSnowflakeDSNKey(value), "privatekey") {
		return true
	}
	if len(trimmed) < 64 {
		return false
	}
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		decoded, err := encoding.DecodeString(trimmed)
		if err != nil || len(decoded) < 64 {
			continue
		}
		if decoded[0] == 0x30 || bytes.Contains(bytes.ToUpper(decoded), []byte("-----BEGIN")) {
			return true
		}
	}
	return false
}

// OpenSnowflakeDB opens gosnowflake from an in-memory Config populated with a
// deployment-owned key. It never reconstructs a credential-bearing DSN.
func OpenSnowflakeDB(dsn string, policy SnowflakeDeploymentPolicy) (*sql.DB, error) {
	spec := RuntimeSpec{Type: EndpointSnowflake, Options: map[string]string{"dsn": dsn}}
	if err := policy.Admit([]RuntimeSpec{spec}); err != nil {
		return nil, err
	}
	cfg, err := parsePersistableSnowflakeDSN(dsn)
	if err != nil {
		return nil, err
	}
	unlock, ok := policy.lockActive()
	if !ok {
		return nil, ErrSnowflakePolicyInvalid
	}
	defer unlock()
	cfg.PrivateKey = policy.privateKey
	cfg.Authenticator = gosnowflake.AuthTypeJwt
	cfg.LogQueryText = false
	cfg.LogQueryParameters = false
	cfg.ClientConfigFile = policy.clientConfigPath
	return sql.OpenDB(gosnowflake.NewConnector(&gosnowflake.SnowflakeDriver{}, *cfg)), nil
}

// LoadSnowflakePrivateKey reads a bounded, owner-only regular file and accepts
// only unencrypted PKCS#8 or PKCS#1 RSA PEM.
func LoadSnowflakePrivateKey(path string) (*rsa.PrivateKey, error) {
	path = strings.TrimSpace(path)
	if path == "" || strings.IndexByte(path, 0) >= 0 {
		return nil, errors.New("snowflake private key file is required")
	}
	if !filepath.IsAbs(path) {
		return nil, errors.New("snowflake private key file must use an absolute deployment-owned path")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, errors.New("snowflake private key file cannot be opened")
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("snowflake private key file must be a regular file")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("snowflake private key file permissions must deny group and other access")
	}
	if info.Size() <= 0 || info.Size() > maxSnowflakePrivateKeyBytes {
		return nil, errors.New("snowflake private key file size is invalid")
	}
	file, err := os.Open(path) // #nosec G304 -- deployment-owned path, bounded and checked above.
	if err != nil {
		return nil, errors.New("snowflake private key file cannot be opened")
	}
	defer func() { _ = file.Close() }()
	openedInfo, err := file.Stat()
	if err != nil || !sameSecureSnowflakeKeyFile(info, openedInfo) {
		return nil, errors.New("snowflake private key file changed during validation")
	}
	data, err := io.ReadAll(io.LimitReader(file, maxSnowflakePrivateKeyBytes+1))
	if err != nil || len(data) == 0 || len(data) > maxSnowflakePrivateKeyBytes {
		return nil, errors.New("snowflake private key file cannot be read")
	}
	readInfo, err := file.Stat()
	if err != nil || !sameSecureSnowflakeKeyFile(openedInfo, readInfo) || int64(len(data)) != readInfo.Size() {
		return nil, errors.New("snowflake private key file changed while being read")
	}
	block, rest := pem.Decode(data)
	if block == nil || len(bytes.TrimSpace(rest)) != 0 || strings.Contains(strings.ToUpper(block.Type), "ENCRYPTED") {
		return nil, errors.New("snowflake private key file must contain one unencrypted RSA private key PEM block")
	}
	switch block.Type {
	case "PRIVATE KEY":
		parsed, parseErr := x509.ParsePKCS8PrivateKey(block.Bytes)
		if parseErr != nil {
			return nil, errors.New("snowflake private key file contains an invalid PKCS#8 RSA key")
		}
		key, ok := parsed.(*rsa.PrivateKey)
		if !ok || !validSnowflakePrivateKey(key) {
			return nil, errors.New("snowflake private key file does not contain a valid RSA key of at least 2048 bits")
		}
		return key, nil
	case "RSA PRIVATE KEY":
		key, parseErr := x509.ParsePKCS1PrivateKey(block.Bytes)
		if parseErr != nil || !validSnowflakePrivateKey(key) {
			return nil, errors.New("snowflake private key file contains an invalid or weak PKCS#1 RSA key")
		}
		return key, nil
	default:
		return nil, errors.New("snowflake private key file must contain a PKCS#8 or PKCS#1 RSA key")
	}
}

func sameSecureSnowflakeKeyFile(before, after os.FileInfo) bool {
	return before != nil && after != nil && after.Mode().IsRegular() && after.Mode().Perm()&0o077 == 0 &&
		after.Size() > 0 && after.Size() <= maxSnowflakePrivateKeyBytes && os.SameFile(before, after) &&
		before.Mode().Perm() == after.Mode().Perm() && before.Size() == after.Size() && before.ModTime().Equal(after.ModTime())
}
