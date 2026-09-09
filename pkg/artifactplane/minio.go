package artifactplane

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	ErrInvalidObjectConfig = errors.New("invalid S3-compatible object store configuration")
	ErrInsecureObjectStore = errors.New("plaintext object storage is allowed only for explicit loopback development")
	bucketPattern          = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
)

type MinIOConfig struct {
	Endpoint      string
	AccessKey     string
	SecretKey     string
	SessionToken  string
	Bucket        string
	Region        string
	Secure        bool
	AllowInsecure bool
	CreateBucket  bool
	MaxBytes      int64
}

func (c MinIOConfig) Validate() error {
	endpoint := strings.TrimSpace(c.Endpoint)
	if endpoint == "" || len(endpoint) > 512 || strings.Contains(endpoint, "://") || strings.ContainsAny(endpoint, "/\\\x00\r\n") ||
		c.AccessKey == "" || c.SecretKey == "" || !bucketPattern.MatchString(c.Bucket) || c.MaxBytes <= 0 || c.MaxBytes > 16<<20 {
		return ErrInvalidObjectConfig
	}
	if !c.Secure && (!c.AllowInsecure || !loopbackEndpoint(endpoint)) {
		return ErrInsecureObjectStore
	}
	if c.CreateBucket && (!c.AllowInsecure || !loopbackEndpoint(endpoint)) {
		return ErrInvalidObjectConfig
	}
	return nil
}

type MinIOStore struct {
	client   *minio.Client
	bucket   string
	maxBytes int64
}

func NewMinIOStore(ctx context.Context, config MinIOConfig) (*MinIOStore, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	client, err := minio.New(strings.TrimSpace(config.Endpoint), &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKey, config.SecretKey, config.SessionToken),
		Secure: config.Secure, Region: config.Region,
	})
	if err != nil {
		return nil, ErrInvalidObjectConfig
	}
	ctx = nonNilContext(ctx)
	exists, err := client.BucketExists(ctx, config.Bucket)
	if err != nil {
		return nil, fmt.Errorf("probe artifact bucket: %w", err)
	}
	if !exists {
		if !config.CreateBucket {
			return nil, fmt.Errorf("artifact bucket is absent: %w", ErrArtifactStoreUnavailable)
		}
		if err := client.MakeBucket(ctx, config.Bucket, minio.MakeBucketOptions{Region: config.Region}); err != nil {
			return nil, fmt.Errorf("create local artifact bucket: %w", err)
		}
	}
	return &MinIOStore{client: client, bucket: config.Bucket, maxBytes: config.MaxBytes}, nil
}

func (s *MinIOStore) Put(ctx context.Context, key, mimeType string, data []byte) error {
	if s == nil || s.client == nil || key == "" || int64(len(data)) > s.maxBytes {
		return ErrArtifactStoreUnavailable
	}
	_, err := s.client.PutObject(nonNilContext(ctx), s.bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{ContentType: mimeType})
	return err
}
func (s *MinIOStore) Get(ctx context.Context, key string) ([]byte, error) {
	if s == nil || s.client == nil || key == "" {
		return nil, ErrArtifactStoreUnavailable
	}
	object, err := s.client.GetObject(nonNilContext(ctx), s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer object.Close()
	info, err := object.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size < 0 || info.Size > s.maxBytes {
		return nil, ErrArtifactCorrupt
	}
	body, err := io.ReadAll(io.LimitReader(object, s.maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) != info.Size {
		return nil, ErrArtifactCorrupt
	}
	return body, nil
}
func (s *MinIOStore) Delete(ctx context.Context, key string) error {
	if s == nil || s.client == nil || key == "" {
		return ErrArtifactStoreUnavailable
	}
	return s.client.RemoveObject(nonNilContext(ctx), s.bucket, key, minio.RemoveObjectOptions{})
}

func loopbackEndpoint(endpoint string) bool {
	host := endpoint
	if parsedHost, _, err := net.SplitHostPort(endpoint); err == nil {
		host = parsedHost
	}
	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

var _ ObjectStore = (*MinIOStore)(nil)
