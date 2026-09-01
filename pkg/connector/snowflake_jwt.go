package connector

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// MaxSnowflakeKeyPairJWTTTL is Snowflake's maximum key-pair JWT lifetime.
const MaxSnowflakeKeyPairJWTTTL = time.Hour

var errSnowflakeJWTInvalid = errors.New("snowflake key-pair JWT configuration is invalid")

type snowflakeJWTHeader struct {
	Algorithm string `json:"alg"`
	Type      string `json:"typ"`
}

type snowflakeJWTClaims struct {
	Issuer    string `json:"iss"`
	Subject   string `json:"sub"`
	IssuedAt  int64  `json:"iat"`
	ExpiresAt int64  `json:"exp"`
}

// SnowflakeRESTIdentity returns the nonsecret deployment identity used by the
// Snowpipe Streaming control endpoint. It never exposes the deployment key.
func (p SnowflakeDeploymentPolicy) SnowflakeRESTIdentity() (account, user, host string, err error) {
	unlock, ok := p.lockActive()
	if !ok {
		return "", "", "", ErrSnowflakePolicyInvalid
	}
	defer unlock()
	return p.account, p.user, p.host, nil
}

// SnowflakeKeyPairJWT signs a short-lived Snowflake KEYPAIR_JWT with the
// deployment-owned RSA key. Callers receive only the serialized token.
func (p SnowflakeDeploymentPolicy) SnowflakeKeyPairJWT(now time.Time, ttl time.Duration) (string, error) {
	if ttl < time.Second || ttl > MaxSnowflakeKeyPairJWTTTL {
		return "", errSnowflakeJWTInvalid
	}
	unlock, ok := p.lockActive()
	if !ok {
		return "", errSnowflakeJWTInvalid
	}
	defer unlock()
	now = now.UTC()
	issuedAt := now.Unix()
	expiresAt := now.Add(ttl).Unix()
	if issuedAt < 0 || expiresAt <= issuedAt {
		return "", errSnowflakeJWTInvalid
	}
	account, err := CanonicalSnowflakeAccountIdentifier(p.account)
	user := strings.ToUpper(p.user)
	if err != nil || account == "" || user == "" {
		return "", errSnowflakeJWTInvalid
	}
	publicDER, err := x509.MarshalPKIXPublicKey(&p.privateKey.PublicKey)
	if err != nil {
		return "", errSnowflakeJWTInvalid
	}
	fingerprint := sha256.Sum256(publicDER)
	principal := account + "." + user
	headerJSON, err := json.Marshal(snowflakeJWTHeader{Algorithm: "RS256", Type: "JWT"})
	if err != nil {
		return "", errSnowflakeJWTInvalid
	}
	claimsJSON, err := json.Marshal(snowflakeJWTClaims{
		Issuer:  principal + ".SHA256:" + base64.StdEncoding.EncodeToString(fingerprint[:]),
		Subject: principal, IssuedAt: issuedAt, ExpiresAt: expiresAt,
	})
	if err != nil {
		return "", errSnowflakeJWTInvalid
	}
	encodedHeader := base64.RawURLEncoding.EncodeToString(headerJSON)
	encodedClaims := base64.RawURLEncoding.EncodeToString(claimsJSON)
	unsigned := encodedHeader + "." + encodedClaims
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, p.privateKey, crypto.SHA256, digest[:])
	if err != nil {
		return "", errSnowflakeJWTInvalid
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}
