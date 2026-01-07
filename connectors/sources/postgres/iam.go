package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type rdsIAMConfig struct {
	Enabled          bool
	Region           string
	Profile          string
	RoleARN          string
	RoleSessionName  string
	RoleExternalID   string
	EndpointOverride string
}

type rdsIAMTokenProvider struct {
	cfg    aws.Config
	region string
}

func newRDSIAMTokenProvider(ctx context.Context, dsn string, options map[string]string) (*rdsIAMTokenProvider, error) {
	if options == nil || !parseBool(options[optAWSRDSIAM], false) {
		return nil, nil
	}
	connCfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}
	iam, err := rdsIAMConfigFromOptions(options, connCfg.Host)
	if err != nil {
		return nil, err
	}
	if !iam.Enabled {
		return nil, nil
	}

	loader := []func(*config.LoadOptions) error{
		config.WithRegion(iam.Region),
	}
	if iam.Profile != "" {
		loader = append(loader, config.WithSharedConfigProfile(iam.Profile))
	}
	cfg, err := config.LoadDefaultConfig(ctx, loader...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	if iam.RoleARN != "" {
		stsClient := sts.NewFromConfig(cfg)
		roleProvider := stscreds.NewAssumeRoleProvider(stsClient, iam.RoleARN, func(o *stscreds.AssumeRoleOptions) {
			if iam.RoleSessionName != "" {
				o.RoleSessionName = iam.RoleSessionName
			}
			if iam.RoleExternalID != "" {
				o.ExternalID = aws.String(iam.RoleExternalID)
			}
		})
		cfg.Credentials = aws.NewCredentialsCache(roleProvider)
	}
	if iam.EndpointOverride != "" {
		cfg.BaseEndpoint = aws.String(iam.EndpointOverride)
	}

	return &rdsIAMTokenProvider{cfg: cfg, region: iam.Region}, nil
}

func (p *rdsIAMTokenProvider) ApplyToPoolConfig(ctx context.Context, cfg *pgxpool.Config) error {
	if p == nil {
		return nil
	}
	before := cfg.BeforeConnect
	cfg.BeforeConnect = func(ctx context.Context, connCfg *pgx.ConnConfig) error {
		if before != nil {
			if err := before(ctx, connCfg); err != nil {
				return err
			}
		}
		token, err := p.Token(ctx, connCfg.Host, connCfg.Port, connCfg.User)
		if err != nil {
			return err
		}
		connCfg.Password = token
		return nil
	}
	return nil
}

func (p *rdsIAMTokenProvider) ApplyToConnConfig(ctx context.Context, connCfg *pgconn.Config) error {
	if p == nil {
		return nil
	}
	token, err := p.Token(ctx, connCfg.Host, connCfg.Port, connCfg.User)
	if err != nil {
		return err
	}
	connCfg.Password = token
	return nil
}

func (p *rdsIAMTokenProvider) Token(ctx context.Context, host string, port uint16, user string) (string, error) {
	if p == nil {
		return "", errors.New("rds iam provider not configured")
	}
	if host == "" || strings.HasPrefix(host, "/") {
		return "", fmt.Errorf("rds iam requires a TCP hostname (got %q)", host)
	}
	if port == 0 {
		return "", errors.New("rds iam requires a port")
	}
	if user == "" {
		return "", errors.New("rds iam requires a user")
	}

	endpoint := fmt.Sprintf("%s:%d", host, port)
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		endpoint = "https://" + endpoint
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", fmt.Errorf("build rds request: %w", err)
	}
	query := req.URL.Query()
	query.Set("Action", "connect")
	query.Set("DBUser", user)
	query.Set("X-Amz-Expires", "900")
	req.URL.RawQuery = query.Encode()

	creds, err := p.cfg.Credentials.Retrieve(ctx)
	if err != nil {
		return "", fmt.Errorf("retrieve aws credentials: %w", err)
	}

	payloadHash := sha256.Sum256(nil)
	signer := v4.NewSigner()
	signedURL, _, err := signer.PresignHTTP(ctx, creds, req, hex.EncodeToString(payloadHash[:]), "rds-db", p.region, time.Now())
	if err != nil {
		return "", fmt.Errorf("sign rds auth token: %w", err)
	}

	signedURL = strings.TrimPrefix(signedURL, "https://")
	signedURL = strings.TrimPrefix(signedURL, "http://")
	return signedURL, nil
}

func rdsIAMConfigFromOptions(options map[string]string, host string) (rdsIAMConfig, error) {
	cfg := rdsIAMConfig{}
	if options == nil || !parseBool(options[optAWSRDSIAM], false) {
		return cfg, nil
	}
	cfg.Enabled = true
	cfg.Region = strings.TrimSpace(options[optAWSRegion])
	cfg.Profile = strings.TrimSpace(options[optAWSProfile])
	cfg.RoleARN = strings.TrimSpace(options[optAWSRoleARN])
	cfg.RoleSessionName = strings.TrimSpace(options[optAWSRoleSessionName])
	cfg.RoleExternalID = strings.TrimSpace(options[optAWSRoleExternalID])
	cfg.EndpointOverride = strings.TrimSpace(options[optAWSEndpoint])

	if cfg.Region == "" {
		cfg.Region = inferAWSRegionFromHost(host)
	}
	if cfg.Region == "" {
		return cfg, errors.New("aws_region is required when aws_rds_iam is enabled")
	}
	if cfg.RoleARN != "" && cfg.RoleSessionName == "" {
		cfg.RoleSessionName = "wallaby-rds-iam"
	}
	return cfg, nil
}

func inferAWSRegionFromHost(host string) string {
	if host == "" {
		return ""
	}
	host = strings.TrimSpace(host)
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimPrefix(host, "http://")
	host = strings.Split(host, ":")[0]
	parts := strings.Split(host, ".")
	for i := 1; i < len(parts); i++ {
		if parts[i] == "rds" && i > 0 {
			return parts[i-1]
		}
	}
	return ""
}
