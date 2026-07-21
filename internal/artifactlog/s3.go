package artifactlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var (
	ErrObjectIndeterminate = errors.New("artifact object outcome indeterminate")
	ErrObjectConflict      = errors.New("artifact object conflict")
)

// ObjectEvidence identifies one immutable object version exactly.
type ObjectEvidence struct {
	Bucket         string
	Key            string
	VersionID      string
	ChecksumSHA256 string
	Length         int64
	EncryptionMode string
	ObjectLock     string
}

// ObjectStore supplies immutable upload and exact-version reconciliation. List
// operations may discover evidence but never become publication authority.
type ObjectStore interface {
	Bucket() string
	PutImmutable(context.Context, string, []byte, string) (ObjectEvidence, error)
	ReconcileVersion(context.Context, string, string, int64) (ObjectEvidence, error)
	HeadVersion(context.Context, ObjectEvidence) error
	DeleteVersion(context.Context, ObjectEvidence) error
}

// S3Config configures ordinary versioned S3 for canonical recovery objects.
type S3Config struct {
	Bucket         string
	Region         string
	Endpoint       string
	AccessKey      string
	SecretKey      string
	SessionToken   string
	ForcePathStyle bool
}

type S3Store struct {
	bucket string
	client *s3.Client
}

func NewS3Store(ctx context.Context, cfg S3Config) (*S3Store, error) {
	if strings.TrimSpace(cfg.Bucket) == "" {
		return nil, errors.New("artifact S3 bucket is required")
	}
	loadOptions := []func(*config.LoadOptions) error{}
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	loadOptions = append(loadOptions, config.WithRegion(region))
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		loadOptions = append(loadOptions, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, cfg.SessionToken)))
	}
	awsConfig, err := config.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("load artifact S3 config: %w", err)
	}
	client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		if cfg.Endpoint != "" {
			options.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		options.UsePathStyle = cfg.ForcePathStyle
	})
	return &S3Store{bucket: cfg.Bucket, client: client}, nil
}

func (s *S3Store) Bucket() string { return s.bucket }

func (s *S3Store) PutImmutable(ctx context.Context, key string, body []byte, expectedDigest string) (ObjectEvidence, error) {
	digest := sha256.Sum256(body)
	actualDigest := hex.EncodeToString(digest[:])
	if actualDigest != expectedDigest {
		return ObjectEvidence{}, fmt.Errorf("%w: encoded body hash %s, expected %s", ErrObjectConflict, actualDigest, expectedDigest)
	}
	output, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:         aws.String(s.bucket),
		Key:            aws.String(key),
		Body:           bytes.NewReader(body),
		ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(digest[:])),
		ContentLength:  aws.Int64(int64(len(body))),
		ContentType:    aws.String("application/vnd.apache.parquet"),
		IfNoneMatch:    aws.String("*"),
		Metadata: map[string]string{
			"wallaby-encoded-sha256": expectedDigest,
			"wallaby-projection":     ProjectionID,
		},
	})
	if err != nil {
		return ObjectEvidence{}, fmt.Errorf("put immutable artifact: %w", err)
	}
	versionID := aws.ToString(output.VersionId)
	if versionID == "" || versionID == "null" {
		return ObjectEvidence{}, fmt.Errorf("%w: bucket %s did not return an immutable VersionId", ErrObjectIndeterminate, s.bucket)
	}
	evidence := ObjectEvidence{
		Bucket:         s.bucket,
		Key:            key,
		VersionID:      versionID,
		ChecksumSHA256: expectedDigest,
		Length:         int64(len(body)),
	}
	if output.ServerSideEncryption != "" {
		evidence.EncryptionMode = string(output.ServerSideEncryption)
	}
	return evidence, nil
}

func (s *S3Store) ReconcileVersion(ctx context.Context, key, expectedDigest string, expectedLength int64) (ObjectEvidence, error) {
	const maxVersions = 1024
	var matches []ObjectEvidence
	var keyMarker, versionMarker *string
	inspected := 0
	for {
		output, err := s.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
			Bucket:          aws.String(s.bucket),
			Prefix:          aws.String(key),
			KeyMarker:       keyMarker,
			VersionIdMarker: versionMarker,
		})
		if err != nil {
			return ObjectEvidence{}, fmt.Errorf("list artifact versions for reconciliation: %w", err)
		}
		for _, version := range output.Versions {
			if aws.ToString(version.Key) != key || aws.ToString(version.VersionId) == "" {
				continue
			}
			inspected++
			if inspected > maxVersions {
				return ObjectEvidence{}, fmt.Errorf("%w: object key has more than %d versions", ErrObjectIndeterminate, maxVersions)
			}
			evidence := ObjectEvidence{
				Bucket:         s.bucket,
				Key:            key,
				VersionID:      aws.ToString(version.VersionId),
				ChecksumSHA256: expectedDigest,
				Length:         expectedLength,
			}
			if err := s.HeadVersion(ctx, evidence); err == nil {
				matches = append(matches, evidence)
			}
		}
		if !aws.ToBool(output.IsTruncated) || output.NextKeyMarker == nil {
			break
		}
		keyMarker = output.NextKeyMarker
		versionMarker = output.NextVersionIdMarker
	}
	if len(matches) == 0 {
		return ObjectEvidence{}, fmt.Errorf("%w: no exact object version matches %s", ErrObjectIndeterminate, expectedDigest)
	}
	if len(matches) != 1 {
		return ObjectEvidence{}, fmt.Errorf("%w: %d exact object versions match one artifact identity", ErrObjectConflict, len(matches))
	}
	return matches[0], nil
}

func (s *S3Store) HeadVersion(ctx context.Context, evidence ObjectEvidence) error {
	if evidence.Bucket != s.bucket || evidence.VersionID == "" {
		return fmt.Errorf("%w: incomplete or wrong-bucket object evidence", ErrObjectConflict)
	}
	output, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:       aws.String(evidence.Bucket),
		Key:          aws.String(evidence.Key),
		VersionId:    aws.String(evidence.VersionID),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		return fmt.Errorf("head exact artifact version: %w", err)
	}
	actualDigest := output.Metadata["wallaby-encoded-sha256"]
	digestBytes, decodeErr := hex.DecodeString(evidence.ChecksumSHA256)
	if decodeErr != nil {
		return fmt.Errorf("%w: invalid expected SHA-256: %w", ErrObjectConflict, decodeErr)
	}
	expectedChecksum := base64.StdEncoding.EncodeToString(digestBytes)
	if actualDigest != evidence.ChecksumSHA256 || aws.ToString(output.ChecksumSHA256) != expectedChecksum || output.Metadata["wallaby-projection"] != ProjectionID || aws.ToInt64(output.ContentLength) != evidence.Length {
		return fmt.Errorf("%w: exact version checksum, projection, or length differs", ErrObjectConflict)
	}
	if evidence.EncryptionMode != "" && string(output.ServerSideEncryption) != evidence.EncryptionMode {
		return fmt.Errorf("%w: exact version encryption mode differs", ErrObjectConflict)
	}
	return nil
}

func (s *S3Store) DeleteVersion(ctx context.Context, evidence ObjectEvidence) error {
	if evidence.Bucket != s.bucket || evidence.VersionID == "" {
		return fmt.Errorf("%w: exact VersionId is required for deletion", ErrObjectConflict)
	}
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket:    aws.String(evidence.Bucket),
		Key:       aws.String(evidence.Key),
		VersionId: aws.String(evidence.VersionID),
	})
	if err != nil {
		return fmt.Errorf("delete exact artifact version: %w", err)
	}
	return nil
}

var _ ObjectStore = (*S3Store)(nil)
