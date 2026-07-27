package artifactlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var (
	ErrObjectIndeterminate = errors.New("artifact object outcome indeterminate")
	ErrObjectConflict      = errors.New("artifact object conflict")
	ErrObjectNotFound      = errors.New("artifact object version not found")
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
	HeadVersion(context.Context, ObjectEvidence) (ObjectEvidence, error)
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
	versioning, err := client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(cfg.Bucket)})
	if err != nil {
		return nil, fmt.Errorf("inspect artifact S3 bucket versioning: %w", err)
	}
	if versioning.Status != types.BucketVersioningStatusEnabled {
		return nil, fmt.Errorf("artifact S3 bucket %s requires versioning status Enabled; got %q", cfg.Bucket, versioning.Status)
	}
	return &S3Store{bucket: cfg.Bucket, client: client}, nil
}

func (s *S3Store) Bucket() string { return s.bucket }

func immutableVersionID(versionID string) bool {
	return versionID != "" && versionID != "null"
}

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
	if !immutableVersionID(versionID) {
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
			if aws.ToString(version.Key) != key || !immutableVersionID(aws.ToString(version.VersionId)) {
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
			if observed, err := s.HeadVersion(ctx, evidence); err == nil {
				matches = append(matches, observed)
			}
		}
		if !aws.ToBool(output.IsTruncated) || output.NextKeyMarker == nil {
			break
		}
		keyMarker = output.NextKeyMarker
		versionMarker = output.NextVersionIdMarker
	}
	if len(matches) == 0 {
		if inspected > 0 {
			return ObjectEvidence{}, fmt.Errorf("%w: object key has versions but none match %s", ErrObjectConflict, expectedDigest)
		}
		return ObjectEvidence{}, fmt.Errorf("%w: no object version exists for %s", ErrObjectNotFound, expectedDigest)
	}
	if len(matches) != 1 {
		return ObjectEvidence{}, fmt.Errorf("%w: %d exact object versions match one artifact identity", ErrObjectConflict, len(matches))
	}
	return matches[0], nil
}

func (s *S3Store) HeadVersion(ctx context.Context, evidence ObjectEvidence) (ObjectEvidence, error) {
	if evidence.Bucket != s.bucket || !immutableVersionID(evidence.VersionID) {
		return ObjectEvidence{}, fmt.Errorf("%w: incomplete or wrong-bucket object evidence", ErrObjectConflict)
	}
	output, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:       aws.String(evidence.Bucket),
		Key:          aws.String(evidence.Key),
		VersionId:    aws.String(evidence.VersionID),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		return ObjectEvidence{}, fmt.Errorf("head exact artifact version: %w", err)
	}
	actualDigest := output.Metadata["wallaby-encoded-sha256"]
	digestBytes, decodeErr := hex.DecodeString(evidence.ChecksumSHA256)
	if decodeErr != nil {
		return ObjectEvidence{}, fmt.Errorf("%w: invalid expected SHA-256: %w", ErrObjectConflict, decodeErr)
	}
	expectedChecksum := base64.StdEncoding.EncodeToString(digestBytes)
	if actualDigest != evidence.ChecksumSHA256 || aws.ToString(output.ChecksumSHA256) != expectedChecksum || output.Metadata["wallaby-projection"] != ProjectionID || aws.ToInt64(output.ContentLength) != evidence.Length {
		return ObjectEvidence{}, fmt.Errorf("%w: exact version checksum, projection, or length differs", ErrObjectConflict)
	}
	observed := evidence
	observed.EncryptionMode = string(output.ServerSideEncryption)
	observed.ObjectLock = formatObjectLockEvidence(output.ObjectLockMode, output.ObjectLockRetainUntilDate, output.ObjectLockLegalHoldStatus)
	if evidence.EncryptionMode != "" && observed.EncryptionMode != evidence.EncryptionMode {
		return ObjectEvidence{}, fmt.Errorf("%w: exact version encryption mode differs", ErrObjectConflict)
	}
	if evidence.ObjectLock != "" && observed.ObjectLock != evidence.ObjectLock {
		return ObjectEvidence{}, fmt.Errorf("%w: exact version Object Lock evidence differs", ErrObjectConflict)
	}
	return observed, nil
}

// ReadVersion returns one exact canonical object version after revalidating its
// immutable checksum, projection metadata, and length. Consumers never read by
// key without the PostgreSQL-rooted VersionId.
func (s *S3Store) ReadVersion(ctx context.Context, evidence ObjectEvidence) ([]byte, error) {
	if _, err := s.HeadVersion(ctx, evidence); err != nil {
		return nil, err
	}
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(evidence.Bucket), Key: aws.String(evidence.Key),
		VersionId: aws.String(evidence.VersionID), ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		return nil, fmt.Errorf("get exact artifact version: %w", err)
	}
	defer func() { _ = output.Body.Close() }()
	if evidence.Length <= 0 {
		return nil, fmt.Errorf("%w: invalid rooted artifact length %d", ErrObjectConflict, evidence.Length)
	}
	body, err := io.ReadAll(io.LimitReader(output.Body, evidence.Length+1))
	if err != nil {
		return nil, fmt.Errorf("read exact artifact version: %w", err)
	}
	if int64(len(body)) != evidence.Length {
		return nil, fmt.Errorf("%w: exact artifact body length %d, expected %d", ErrObjectConflict, len(body), evidence.Length)
	}
	digest := sha256.Sum256(body)
	actual := hex.EncodeToString(digest[:])
	if actual != evidence.ChecksumSHA256 {
		return nil, fmt.Errorf("%w: exact artifact body checksum %s, expected %s", ErrObjectConflict, actual, evidence.ChecksumSHA256)
	}
	return body, nil
}

func formatObjectLockEvidence(mode types.ObjectLockMode, retainUntil *time.Time, legalHold types.ObjectLockLegalHoldStatus) string {
	parts := make([]string, 0, 3)
	if mode != "" {
		parts = append(parts, "mode="+string(mode))
	}
	if retainUntil != nil {
		parts = append(parts, "retain_until="+retainUntil.UTC().Format(time.RFC3339Nano))
	}
	if legalHold != "" {
		parts = append(parts, "legal_hold="+string(legalHold))
	}
	return strings.Join(parts, ";")
}

func (s *S3Store) DeleteVersion(ctx context.Context, evidence ObjectEvidence) error {
	if evidence.Bucket != s.bucket || !immutableVersionID(evidence.VersionID) {
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
