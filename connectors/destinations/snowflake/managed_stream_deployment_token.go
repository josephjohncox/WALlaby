package snowflake

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

type deploymentStreamRESTTokenProvider struct {
	policy connector.SnowflakeDeploymentPolicy
	now    func() time.Time
	ttl    time.Duration
}

func newDeploymentStreamRESTTokenProvider(policy connector.SnowflakeDeploymentPolicy, now func() time.Time, ttl time.Duration) (*deploymentStreamRESTTokenProvider, error) {
	if now == nil || ttl < time.Second || ttl > connector.MaxSnowflakeKeyPairJWTTTL {
		return nil, errors.New("snowpipe Streaming deployment token provider configuration is invalid")
	}
	if _, _, _, err := policy.SnowflakeRESTIdentity(); err != nil {
		return nil, errors.New("snowpipe Streaming deployment token provider policy is invalid")
	}
	return &deploymentStreamRESTTokenProvider{policy: policy, now: now, ttl: ttl}, nil
}

func (p *deploymentStreamRESTTokenProvider) KeypairJWT(ctx context.Context) (string, error) {
	if p == nil || p.now == nil {
		return "", errors.New("snowpipe Streaming deployment token provider is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	token, err := p.policy.SnowflakeKeyPairJWT(p.now(), p.ttl)
	if err != nil {
		return "", errors.New("snowpipe Streaming deployment token generation failed")
	}
	return token, nil
}

// newDeploymentStreamRESTTransport binds the unlinked REST adapter to the
// deployment policy host and signing key. Runtime construction must not call
// this function until streamingTransportLinked is promoted after commercial
// evidence.
func newDeploymentStreamRESTTransport(policy connector.SnowflakeDeploymentPolicy, client *http.Client, now func() time.Time, ttl time.Duration) (*streamRESTTransport, error) {
	account, _, host, err := policy.SnowflakeRESTIdentity()
	if err != nil {
		return nil, errors.New("snowpipe Streaming deployment policy is invalid")
	}
	expectedAccountLabel, err := connector.SnowflakeRESTAccountLabel(account)
	if err != nil || streamRESTAccountLabel(host) != expectedAccountLabel {
		return nil, errors.New("snowpipe Streaming deployment policy account and host differ")
	}
	provider, err := newDeploymentStreamRESTTokenProvider(policy, now, ttl)
	if err != nil {
		return nil, err
	}
	return newStreamRESTTransport("https://"+host, client, provider)
}

var _ streamRESTTokenProvider = (*deploymentStreamRESTTokenProvider)(nil)
var _ = newDeploymentStreamRESTTransport
