package connector

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestSnowflakeDeploymentPolicyKeyPairJWTClaimsSignatureAndDeterminism(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := NewSnowflakeDeploymentPolicyWithPrivateKey("org.account", "mixed_user", "org-account.snowflakecomputing.com", key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = policy.Close() })

	now := time.Date(2026, time.September, 1, 12, 34, 56, 987654321, time.FixedZone("offset", -7*60*60))
	first, err := policy.SnowflakeKeyPairJWT(now, 55*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	second, err := policy.SnowflakeKeyPairJWT(now, 55*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("fixed policy, clock, and TTL produced different JWTs")
	}
	parts := strings.Split(first, ".")
	if len(parts) != 3 {
		t.Fatalf("JWT parts=%d, want 3", len(parts))
	}
	decode := func(value string) []byte {
		t.Helper()
		decoded, err := base64.RawURLEncoding.DecodeString(value)
		if err != nil {
			t.Fatal(err)
		}
		return decoded
	}
	var header snowflakeJWTHeader
	if err := json.Unmarshal(decode(parts[0]), &header); err != nil {
		t.Fatal(err)
	}
	if header != (snowflakeJWTHeader{Algorithm: "RS256", Type: "JWT"}) {
		t.Fatalf("JWT header=%+v", header)
	}
	var claims snowflakeJWTClaims
	if err := json.Unmarshal(decode(parts[1]), &claims); err != nil {
		t.Fatal(err)
	}
	publicDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint := sha256.Sum256(publicDER)
	principal := "ORG-ACCOUNT.MIXED_USER"
	if claims.Issuer != principal+".SHA256:"+base64.StdEncoding.EncodeToString(fingerprint[:]) || claims.Subject != principal {
		t.Fatalf("JWT identity claims differ from normalized deployment identity")
	}
	if claims.IssuedAt != now.UTC().Unix() || claims.ExpiresAt != now.UTC().Add(55*time.Minute).Unix() {
		t.Fatalf("JWT time claims=%d/%d", claims.IssuedAt, claims.ExpiresAt)
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(&key.PublicKey, crypto.SHA256, digest[:], decode(parts[2])); err != nil {
		t.Fatalf("verify JWT signature: %v", err)
	}
	account, user, host, err := policy.SnowflakeRESTIdentity()
	if err != nil || account != "org.account" || user != "mixed_user" || host != "org-account.snowflakecomputing.com" {
		t.Fatalf("REST identity=%q/%q/%q error=%v", account, user, host, err)
	}
}

func TestSnowflakeDeploymentPolicyKeyPairJWTBoundariesFailClosedWithoutIdentityDisclosure(t *testing.T) {
	if _, err := (SnowflakeDeploymentPolicy{}).SnowflakeKeyPairJWT(time.Now(), time.Minute); err == nil {
		t.Fatal("disabled policy signed a JWT")
	}
	if _, _, _, err := (SnowflakeDeploymentPolicy{}).SnowflakeRESTIdentity(); err == nil {
		t.Fatal("disabled policy exposed an identity")
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	const secretAccount = "do-not-disclose-account"
	policy, err := NewSnowflakeDeploymentPolicyWithPrivateKey(secretAccount, "do_not_disclose_user", secretAccount+".snowflakecomputing.com", key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = policy.Close() })
	for _, ttl := range []time.Duration{0, -time.Second, time.Nanosecond, time.Second - time.Nanosecond, MaxSnowflakeKeyPairJWTTTL + time.Nanosecond} {
		if _, err := policy.SnowflakeKeyPairJWT(time.Unix(100, 0), ttl); err == nil {
			t.Fatalf("invalid TTL %s signed a JWT", ttl)
		} else if strings.Contains(err.Error(), secretAccount) {
			t.Fatal("JWT error disclosed deployment identity")
		}
	}
	if _, err := policy.SnowflakeKeyPairJWT(time.Unix(100, 0), time.Second); err != nil {
		t.Fatalf("minimum whole-second TTL rejected: %v", err)
	}
	if _, err := policy.SnowflakeKeyPairJWT(time.Unix(100, 0), MaxSnowflakeKeyPairJWTTTL); err != nil {
		t.Fatalf("maximum JWT TTL rejected: %v", err)
	}
}
