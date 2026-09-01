package connector

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func testSnowflakeDeploymentPolicy(t *testing.T) SnowflakeDeploymentPolicy {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := NewSnowflakeDeploymentPolicyWithPrivateKey("account", "user", "account.snowflakecomputing.com", key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = policy.Close() })
	return policy
}

func TestValidateSnowflakeDSNRejectsCredentialAliasesEncodingAndMalformedInputWithoutDisclosure(t *testing.T) {
	secret := "never-print-this"
	encodedDER := base64.StdEncoding.EncodeToString(append([]byte{0x30}, make([]byte, 80)...))
	tests := map[string]string{
		"authority password":      "user:" + secret + "@account/db/schema",
		"password":                "user@account/db/schema?PaSs-WoRd=" + secret,
		"passwd":                  "user@account/db/schema?pass_wd=" + secret,
		"passcode":                "user@account/db/schema?PASS.CODE=" + secret,
		"private key":             "user@account/db/schema?private-Key=" + secret,
		"pem material":            "user@account/db/schema?note=-----BEGIN%20PRIVATE%20KEY-----" + secret,
		"base64 key material":     "user@account/db/schema?note=" + encodedDER,
		"access token":            "user@account/db/schema?ACCESS_token=" + secret,
		"refresh token":           "user@account/db/schema?refresh-token=" + secret,
		"id token":                "user@account/db/schema?id.token=" + secret,
		"mfa token":               "user@account/db/schema?mfa_token=" + secret,
		"client secret":           "user@account/db/schema?Client-Secret=" + secret,
		"secret key":              "user@account/db/schema?secretKey=" + secret,
		"api key":                 "user@account/db/schema?apiKey=" + secret,
		"query logging":           "user@account/db/schema?logQueryParameters=true",
		"connection diagnostics":  "user@account/db/schema?connectionDiagnosticsEnabled=true",
		"client config file":      "user@account/db/schema?clientConfigFile=/tmp/attacker.json",
		"insecure transport":      "user@account/db/schema?protocol=http",
		"proxy password":          "user@account/db/schema?proxy_password=" + secret,
		"credential alias":        "user@account/db/schema?api-credential=" + secret,
		"secret alias":            "user@account/db/schema?api_secret=" + secret,
		"single encoded key":      "user@account/db/schema?pass%77ord=" + secret,
		"double encoded key":      "user@account/db/schema?pass%2577ord=" + secret,
		"triple encoded key":      "user@account/db/schema?pass%252577ord=" + secret,
		"encoded authority colon": "user%253A" + secret + "@account/db/schema",
		"repeated normalized key": "user@account/db/schema?role=a&R-O_L.E=b",
		"bad escape":              "user@account/db/schema?role=%zz",
		"missing equals":          "user@account/db/schema?role",
		"empty field":             "user@account/db/schema?role=a&&warehouse=b",
		"fragment":                "user@account/db/schema?role=a#" + secret,
	}
	for name, dsn := range tests {
		t.Run(name, func(t *testing.T) {
			err := ValidateSnowflakeDSN(dsn)
			if err == nil {
				t.Fatal("unsafe DSN accepted")
			}
			if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), dsn) {
				t.Fatalf("error disclosed DSN material: %v", err)
			}
			if !errors.Is(err, ErrUnsafeSnowflakeDSN) && !errors.Is(err, ErrMalformedSnowflakeDSN) {
				t.Fatalf("unexpected static error: %v", err)
			}
		})
	}
	for _, dsn := range []string{
		"user:@account/db/schema?authenticator=snowflake_jwt&ocspFailOpen=false&role=ROLE&warehouse=WH",
		"user:@account/db/schema?authenticator=snowflake_jwt&ocspFailOpen=false&READ_LATEST_WRITES=true&TIMEZONE=UTC",
	} {
		if err := ValidateSnowflakeDSN(dsn); err != nil {
			t.Fatalf("safe nonsecret DSN rejected: %v", err)
		}
	}
}

func TestSnowflakeDeploymentPolicyCoversAllFiveCells(t *testing.T) {
	dsn := "user:@account/db/schema?authenticator=snowflake_jwt&ocspFailOpen=false"
	cells := []RuntimeSpec{
		{Type: EndpointSnowflake, Options: map[string]string{"dsn": dsn}},
		{Type: EndpointSnowpipe, Options: map[string]string{"dsn": dsn}},
		{Type: EndpointSnowflake, Options: map[string]string{"dsn": dsn, "managed_profile": ManagedProfilePostgresToSnowflakeSQLV1}},
		{Type: EndpointSnowflake, Options: map[string]string{"dsn": dsn, "managed_profile": ManagedProfilePostgresToSnowflakeStagedAppendV1}},
		{Type: EndpointSnowflake, Options: map[string]string{"dsn": dsn, "managed_profile": ManagedProfilePostgresToSnowflakeStreamingRestAppendV1}},
	}
	for _, cell := range cells {
		if err := (SnowflakeDeploymentPolicy{}).Admit([]RuntimeSpec{cell}); !errors.Is(err, ErrSnowflakeExecutionDisabled) {
			t.Fatalf("disabled cell %s/%s error=%v", cell.Type, cell.Options["managed_profile"], err)
		}
		if err := testSnowflakeDeploymentPolicy(t).Admit([]RuntimeSpec{cell}); err != nil {
			t.Fatalf("enabled cell %s/%s rejected: %v", cell.Type, cell.Options["managed_profile"], err)
		}
	}
}

func TestSnowflakeDeploymentPolicyBindsAccountUserHostAndSecureTransport(t *testing.T) {
	policy := testSnowflakeDeploymentPolicy(t)
	for name, dsn := range map[string]string{
		"attacker host": "user:@attacker.example:443/db/schema?account=account&authenticator=snowflake_jwt&ocspFailOpen=false",
		"wrong user":    "other:@account/db/schema?authenticator=snowflake_jwt&ocspFailOpen=false",
		"wrong account": "user:@other/db/schema?authenticator=snowflake_jwt&ocspFailOpen=false",
		"fail open":     "user:@account/db/schema?authenticator=snowflake_jwt&ocspFailOpen=true",
	} {
		t.Run(name, func(t *testing.T) {
			if err := policy.Admit([]RuntimeSpec{{Type: EndpointSnowflake, Options: map[string]string{"dsn": dsn}}}); err == nil {
				t.Fatal("untrusted identity or transport admitted")
			}
		})
	}
}

func TestOpenSnowflakeDBUsesPrevalidatedDeploymentKeyWithoutNetworkIO(t *testing.T) {
	attackerConfig := filepath.Join(t.TempDir(), "sf_client_config.json")
	if err := os.WriteFile(attackerConfig, []byte(`{"common":{"log_level":"TRACE","log_path":"STDOUT"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SF_CLIENT_CONFIG_FILE", attackerConfig)
	policy := testSnowflakeDeploymentPolicy(t)
	if policy.clientConfigPath == attackerConfig {
		t.Fatal("external Snowflake easy-logging configuration became authoritative")
	}
	configData, err := os.ReadFile(policy.clientConfigPath)
	if err != nil || !strings.Contains(string(configData), `"log_level":"OFF"`) {
		t.Fatalf("deployment logging policy=%q error=%v", configData, err)
	}
	db, err := OpenSnowflakeDB("user:@account/db/schema?authenticator=snowflake_jwt&ocspFailOpen=false", policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSnowflakeDeploymentPolicyCopiesRevokeAndOwnPrivateKey(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	originalPublic := rsa.PublicKey{N: new(big.Int).Set(key.N), E: key.E}
	policy, err := NewSnowflakeDeploymentPolicyWithPrivateKey("org.account", "runtime_user", "org-account.snowflakecomputing.com", key)
	if err != nil {
		t.Fatal(err)
	}
	copyPolicy := policy
	configPath := policy.clientConfigPath

	// Mutation of the caller-owned key after construction must not affect the
	// policy's fingerprint or signatures.
	key.N.SetInt64(3)
	key.D.SetInt64(1)
	token, err := policy.SnowflakeKeyPairJWT(time.Unix(100, 500_000_000), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("JWT parts=%d, want 3", len(parts))
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(&originalPublic, crypto.SHA256, digest[:], signature); err != nil {
		t.Fatalf("policy retained caller-owned key state: %v", err)
	}

	var wait sync.WaitGroup
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for range 10 {
				_, _ = copyPolicy.SnowflakeKeyPairJWT(time.Now(), time.Minute)
			}
		}()
	}
	wait.Add(1)
	go func() {
		defer wait.Done()
		if err := policy.Close(); err != nil {
			t.Errorf("close policy: %v", err)
		}
	}()
	wait.Wait()

	if policy.Enabled() || copyPolicy.Enabled() {
		t.Fatal("closed policy copy remained enabled")
	}
	if _, _, _, err := copyPolicy.SnowflakeRESTIdentity(); !errors.Is(err, ErrSnowflakePolicyInvalid) {
		t.Fatalf("closed REST identity error=%v", err)
	}
	if _, err := copyPolicy.SnowflakeKeyPairJWT(time.Now(), time.Minute); err == nil {
		t.Fatal("closed policy signed a JWT")
	}
	dsn := "runtime_user:@org.account/db/schema?authenticator=snowflake_jwt&ocspFailOpen=false"
	if err := copyPolicy.Admit([]RuntimeSpec{{Type: EndpointSnowflake, Options: map[string]string{"dsn": dsn}}}); !errors.Is(err, ErrSnowflakePolicyInvalid) {
		t.Fatalf("closed policy admission error=%v", err)
	}
	parsed, err := parsePersistableSnowflakeDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := copyPolicy.validateIdentity(parsed); !errors.Is(err, ErrSnowflakePolicyInvalid) {
		t.Fatalf("closed identity validation error=%v", err)
	}
	if _, err := OpenSnowflakeDB(dsn, copyPolicy); !errors.Is(err, ErrSnowflakePolicyInvalid) {
		t.Fatalf("closed policy database error=%v", err)
	}
	if _, err := os.Stat(configPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("closed client policy path error=%v", err)
	}
	if err := copyPolicy.Close(); err != nil {
		t.Fatalf("idempotent copied close: %v", err)
	}
}

func TestSnowflakeDeploymentPolicyRejectsMalformedPrincipalsAndHosts(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	for _, account := range []string{"", " account", "account ", ".account", "account.", "-account", "account-", "org..account", "org.-account", "org account", "org/account"} {
		if policy, err := NewSnowflakeDeploymentPolicyWithPrivateKey(account, "user", "account.snowflakecomputing.com", key); err == nil {
			_ = policy.Close()
			t.Fatalf("malformed account accepted")
		}
	}
	for _, user := range []string{"", " user", "user ", ".", "user.name", "user-name", "9user", "user name"} {
		if policy, err := NewSnowflakeDeploymentPolicyWithPrivateKey("account", user, "account.snowflakecomputing.com", key); err == nil {
			_ = policy.Close()
			t.Fatalf("malformed user accepted")
		}
	}
	for _, host := range []string{"", "account", "account.example.com", "account..snowflakecomputing.com", "-account.snowflakecomputing.com", "account_.snowflakecomputing.com", "https://account.snowflakecomputing.com", "account.snowflakecomputing.com:443"} {
		if policy, err := NewSnowflakeDeploymentPolicyWithPrivateKey("account", "user", host, key); err == nil {
			_ = policy.Close()
			t.Fatalf("malformed host accepted")
		}
	}
	for _, account := range []string{"account", "org.account", "org-account", "org_account"} {
		hostLabel, err := SnowflakeRESTAccountLabel(account)
		if err != nil {
			t.Fatal(err)
		}
		policy, err := NewSnowflakeDeploymentPolicyWithPrivateKey(account, "user_1$", hostLabel+".snowflakecomputing.com", key)
		if err != nil {
			t.Fatalf("valid account rejected")
		}
		_ = policy.Close()
	}
}

func TestLoadSnowflakePrivateKeyPKCS8PKCS1AndFileSafety(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	write := func(name, blockType string, der []byte, mode os.FileMode) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), name)
		if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), mode); err != nil {
			t.Fatal(err)
		}
		return path
	}
	pkcs8DER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	for name, path := range map[string]string{
		"pkcs8": write("pkcs8.pem", "PRIVATE KEY", pkcs8DER, 0o600),
		"pkcs1": write("pkcs1.pem", "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key), 0o400),
	} {
		t.Run(name, func(t *testing.T) {
			loaded, err := LoadSnowflakePrivateKey(path)
			if err != nil || loaded.N.Cmp(key.N) != 0 {
				t.Fatalf("loaded=%v err=%v", loaded != nil, err)
			}
		})
	}
	preloadPath := write("preload.pem", "PRIVATE KEY", pkcs8DER, 0o600)
	policy, err := NewSnowflakeDeploymentPolicy(SnowflakeDeploymentConfig{
		Enabled: true, Account: "account", User: "user", Host: "account.snowflakecomputing.com", PrivateKeyFile: preloadPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = policy.Close() })
	if err := os.Remove(preloadPath); err != nil {
		t.Fatal(err)
	}
	db, err := OpenSnowflakeDB("user:@account/db/schema?authenticator=snowflake_jwt&ocspFailOpen=false", policy)
	if err != nil {
		t.Fatalf("preloaded deployment key was not retained: %v", err)
	}
	_ = db.Close()

	weakKey, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	weakDER, err := x509.MarshalPKCS8PrivateKey(weakKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadSnowflakePrivateKey(write("weak.pem", "PRIVATE KEY", weakDER, 0o600)); err == nil {
		t.Fatal("RSA key below 2048 bits accepted")
	}

	worldReadable := write("open.pem", "PRIVATE KEY", pkcs8DER, 0o644)
	if _, err := LoadSnowflakePrivateKey(worldReadable); err == nil {
		t.Fatal("world-readable key accepted")
	}
	if _, err := LoadSnowflakePrivateKey("relative-key.pem"); err == nil {
		t.Fatal("relative deployment key path accepted")
	}
	bad := write("bad.pem", "EC PRIVATE KEY", []byte("bad"), 0o600)
	if _, err := LoadSnowflakePrivateKey(bad); err == nil {
		t.Fatal("non-RSA key accepted")
	}
	target := write("target.pem", "PRIVATE KEY", pkcs8DER, 0o600)
	link := filepath.Join(t.TempDir(), "link.pem")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadSnowflakePrivateKey(link); err == nil {
		t.Fatal("symlink key accepted")
	}
}
