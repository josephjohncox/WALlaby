package snowflake

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestDeploymentStreamRESTTokenProviderRefreshAndBoundaries(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := connector.NewSnowflakeDeploymentPolicyWithPrivateKey("account", "user", "account.snowflakecomputing.com", key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = policy.Close() })

	current := time.Unix(1_800_000_000, 0).UTC()
	provider, err := newDeploymentStreamRESTTokenProvider(policy, func() time.Time { return current }, 55*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	first, err := provider.KeypairJWT(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	current = current.Add(time.Minute)
	second, err := provider.KeypairJWT(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("JWT provider did not refresh after the injected clock advanced")
	}
	claims := func(token string) snowflakeJWTTestClaims {
		t.Helper()
		parts := strings.Split(token, ".")
		if len(parts) != 3 {
			t.Fatalf("JWT parts=%d", len(parts))
		}
		payload, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil {
			t.Fatal(err)
		}
		var value snowflakeJWTTestClaims
		if err := json.Unmarshal(payload, &value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	if firstClaims, secondClaims := claims(first), claims(second); secondClaims.IssuedAt-firstClaims.IssuedAt != 60 || secondClaims.ExpiresAt-firstClaims.ExpiresAt != 60 {
		t.Fatalf("refreshed JWT times first=%+v second=%+v", firstClaims, secondClaims)
	}
	if _, err := newDeploymentStreamRESTTokenProvider(connector.SnowflakeDeploymentPolicy{}, func() time.Time { return current }, time.Minute); err == nil {
		t.Fatal("disabled policy created a token provider")
	}
	if _, err := newDeploymentStreamRESTTokenProvider(policy, nil, time.Minute); err == nil {
		t.Fatal("nil clock created a token provider")
	}
	if _, err := newDeploymentStreamRESTTokenProvider(policy, func() time.Time { return current }, time.Second-time.Nanosecond); err == nil {
		t.Fatal("sub-second token TTL created a token provider")
	}
	if _, err := newDeploymentStreamRESTTokenProvider(policy, func() time.Time { return current }, connector.MaxSnowflakeKeyPairJWTTTL+time.Second); err == nil {
		t.Fatal("oversized token TTL created a token provider")
	}
	transport, err := newDeploymentStreamRESTTransport(policy, http.DefaultClient, func() time.Time { return current }, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if transport.controlBase.String() != "https://account.snowflakecomputing.com" {
		t.Fatalf("deployment control base=%q", transport.controlBase)
	}
	if err := transport.validateConfigAccount(streamConfig{account: "account"}); err != nil {
		t.Fatalf("matching stream account rejected: %v", err)
	}
	if err := transport.validateConfigAccount(streamConfig{account: "other"}); err == nil {
		t.Fatal("mismatched stream account accepted before token use")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := provider.KeypairJWT(canceled); err == nil {
		t.Fatal("canceled token request succeeded")
	}
}

func TestDeploymentStreamRESTTransportBindsCanonicalPolicyAccountAndClose(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := connector.NewSnowflakeDeploymentPolicyWithPrivateKey("org.account", "runtime_user", "org-account.snowflakecomputing.com", key)
	if err != nil {
		t.Fatal(err)
	}
	provider, err := newDeploymentStreamRESTTokenProvider(policy, time.Now, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	transport, err := newDeploymentStreamRESTTransport(policy, http.DefaultClient, time.Now, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if transport.controlBase.Hostname() != "org-account.snowflakecomputing.com" {
		t.Fatalf("control host=%q", transport.controlBase.Hostname())
	}
	if err := transport.validateConfigAccount(streamConfig{account: "org.account"}); err != nil {
		t.Fatalf("dotted stream account rejected: %v", err)
	}
	copyPolicy := policy
	if err := copyPolicy.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.KeypairJWT(context.Background()); err == nil {
		t.Fatal("provider signed after copied policy close")
	}
	if _, err := newDeploymentStreamRESTTransport(policy, http.DefaultClient, time.Now, time.Second); err == nil {
		t.Fatal("closed policy constructed a REST transport")
	}

	mismatch, err := connector.NewSnowflakeDeploymentPolicyWithPrivateKey("account", "runtime_user", "evil-account.snowflakecomputing.com", key)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mismatch.Close() }()
	if _, err := newDeploymentStreamRESTTransport(mismatch, http.DefaultClient, time.Now, time.Minute); err == nil {
		t.Fatal("cross-account policy host constructed a REST transport")
	}
}

type snowflakeJWTTestClaims struct {
	IssuedAt  int64 `json:"iat"`
	ExpiresAt int64 `json:"exp"`
}

func TestDeploymentStreamRESTTokenProviderAuthenticatesScopedTokenExchangeWithoutDisclosure(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := connector.NewSnowflakeDeploymentPolicyWithPrivateKey("account", "user", "account.snowflakecomputing.com", key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = policy.Close() })
	fixed := time.Unix(1_800_000_000, 0).UTC()
	provider, err := newDeploymentStreamRESTTokenProvider(policy, func() time.Time { return fixed }, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := provider.KeypairJWT(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var observedJWT string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/streaming/hostname":
			mu.Lock()
			observedJWT = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			mu.Unlock()
			if r.Header.Get("X-Snowflake-Authorization-Token-Type") != "KEYPAIR_JWT" {
				t.Errorf("unexpected token type header")
			}
			_ = json.NewEncoder(w).Encode(streamRESTHostnameResponse{Hostname: r.Host})
		case "/oauth/token":
			body, _ := io.ReadAll(r.Body)
			values, _ := url.ParseQuery(string(body))
			if values.Get("scope") != r.Host {
				t.Errorf("unexpected scoped-token audience")
			}
			_ = json.NewEncoder(w).Encode(streamRESTTokenResponse{Token: "scoped"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	transport, err := newStreamRESTTransport(server.URL, server.Client(), provider)
	if err != nil {
		t.Fatal(err)
	}
	if _, token, err := transport.session(context.Background(), true); err != nil || token != "scoped" {
		t.Fatalf("session token=%q error=%v", token, err)
	}
	mu.Lock()
	actual := observedJWT
	mu.Unlock()
	if actual == "" || actual != expected {
		t.Fatal("scoped-token exchange did not receive the policy-generated JWT")
	}

	secretMarker := actual[len(actual)-16:]
	server.Close()
	transport.invalidateSession()
	if _, _, err := transport.session(context.Background(), true); err == nil {
		t.Fatal("closed token endpoint unexpectedly succeeded")
	} else if strings.Contains(err.Error(), actual) || strings.Contains(err.Error(), secretMarker) {
		t.Fatal("transport error disclosed key-pair JWT material")
	}
}
