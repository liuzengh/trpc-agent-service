// Package s3 implements tenant-scoped runtime object and artifact storage on
// AWS S3-compatible services, including MinIO and OSS S3 endpoints.
package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

const (
	defaultMaxBytes  = 32 << 20
	defaultTimeout   = 15 * time.Second
	maxMetadataBytes = 1800
	metadataPrefix   = "trpc-artifact-"
	metadataVersion  = metadataPrefix + "version"
	metadataSession  = metadataPrefix + "session"
	metadataName     = metadataPrefix + "name"
	metadataMime     = metadataPrefix + "mime"
	metadataCreated  = metadataPrefix + "created"
	metadataUpdated  = metadataPrefix + "updated"
	metadataDigest   = metadataPrefix + "sha256"
)

// API is the subset of the AWS S3 client used by Store. It is consumer-owned
// so contract tests can use a deterministic fake without a network.
type API interface {
	PutObject(context.Context, *awss3.PutObjectInput, ...func(*awss3.Options)) (*awss3.PutObjectOutput, error)
	GetObject(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
	HeadObject(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error)
	DeleteObject(context.Context, *awss3.DeleteObjectInput, ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error)
	ListObjectsV2(context.Context, *awss3.ListObjectsV2Input, ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error)
	HeadBucket(context.Context, *awss3.HeadBucketInput, ...func(*awss3.Options)) (*awss3.HeadBucketOutput, error)
}

// Options controls transfer bounds and per-operation deadlines.
type Options struct {
	MaxBytes       int64
	ConnectTimeout time.Duration
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
}

// Store owns one S3 client and one bucket. The caller owns Store.Close.
type Store struct {
	client         API
	bucket         string
	tenant         string
	maxBytes       int64
	connectTimeout time.Duration
	readTimeout    time.Duration
	writeTimeout   time.Duration
	mu             sync.RWMutex
	closed         bool
	closeIdle      func()
}

// New creates a tenant-bound S3 store around an already configured client.
func New(client API, bucket, tenantID string, options Options) (*Store, error) {
	bucket = strings.TrimSpace(bucket)
	if client == nil || !validateBucket(bucket) || runtimestorage.ValidateTenant(tenantID) != nil {
		return nil, runtimestorage.ErrInvalid
	}
	if options.MaxBytes <= 0 {
		options.MaxBytes = defaultMaxBytes
	}
	if options.MaxBytes == math.MaxInt64 {
		return nil, runtimestorage.ErrInvalid
	}
	if options.ConnectTimeout <= 0 {
		options.ConnectTimeout = defaultTimeout
	}
	if options.ReadTimeout <= 0 {
		options.ReadTimeout = defaultTimeout
	}
	if options.WriteTimeout <= 0 {
		options.WriteTimeout = defaultTimeout
	}
	return &Store{client: client, bucket: bucket, tenant: tenantID, maxBytes: options.MaxBytes,
		connectTimeout: options.ConnectTimeout, readTimeout: options.ReadTimeout, writeTimeout: options.WriteTimeout}, nil
}

// NewFromConfig constructs an AWS SDK client. HTTP endpoints are permitted
// only when allowInsecure is explicitly true, which is intended for local
// MinIO development.
func NewFromConfig(cfg awssdk.Config, bucket, tenantID, endpoint string, usePathStyle, allowInsecure bool, options Options) (*Store, error) {
	if endpoint != "" {
		u, err := url.Parse(endpoint)
		if err != nil || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawPath != "" || u.RawQuery != "" || u.Fragment != "" ||
			(u.Scheme != "https" && !(allowInsecure && u.Scheme == "http")) {
			return nil, runtimestorage.ErrInvalid
		}
	}
	if options.ConnectTimeout <= 0 {
		options.ConnectTimeout = defaultTimeout
	}
	if options.ReadTimeout <= 0 {
		options.ReadTimeout = defaultTimeout
	}
	if options.WriteTimeout <= 0 {
		options.WriteTimeout = defaultTimeout
	}
	baseTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, runtimestorage.ErrInvalid
	}
	transport := baseTransport.Clone()
	transport.DialContext = (&net.Dialer{Timeout: options.ConnectTimeout}).DialContext
	transport.ResponseHeaderTimeout = options.ReadTimeout
	clientHTTP := &http.Client{Transport: transport, Timeout: maxDuration(options.ReadTimeout, options.WriteTimeout)}
	cfg.HTTPClient = clientHTTP
	client := awss3.NewFromConfig(cfg, func(value *awss3.Options) {
		value.UsePathStyle = usePathStyle
		if endpoint != "" {
			value.BaseEndpoint = awssdk.String(endpoint)
		}
	})
	store, err := New(client, bucket, tenantID, options)
	if err != nil {
		transport.CloseIdleConnections()
		return nil, err
	}
	store.closeIdle = transport.CloseIdleConnections
	return store, nil
}

func maxDuration(left, right time.Duration) time.Duration {
	if left > right {
		return left
	}
	return right
}

func (s *Store) operationContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc, error) {
	if ctx == nil {
		return nil, nil, runtimestorage.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return nil, nil, runtimestorage.ErrStorage
	}
	operation, cancel := context.WithTimeout(ctx, timeout)
	return operation, cancel, nil
}

func (s *Store) remoteKey(kind, key string) string {
	// Tenant is part of the key even though the Store is already tenant-bound;
	// this protects against accidental bucket sharing by callers and makes
	// cross-tenant IDs non-colliding at the storage boundary.
	return path.Join("tenants", base64.RawURLEncoding.EncodeToString([]byte(s.tenant)), kind, base64.RawURLEncoding.EncodeToString([]byte(key)))
}

func (s *Store) validRemoteKey(kind, key string) bool {
	return len([]byte(s.remoteKey(kind, key))) <= 1024
}

func validateKey(key string) bool {
	if !runtimestorage.ValidateText(key, 1024, true) || strings.ContainsAny(key, "\\\r\n") || path.Clean(key) != key {
		return false
	}
	for _, segment := range strings.Split(key, "/") {
		if segment == "." || segment == ".." {
			return false
		}
	}
	return true
}

// validateArtifactID preserves the identifier limit shared by the existing
// runtime artifact providers while retaining S3 key-safety checks.
func validateArtifactID(artifactID string) bool {
	return runtimestorage.ValidateText(artifactID, 256, true) && validateKey(artifactID)
}

func validateBucket(bucket string) bool {
	if len(bucket) < 3 || len(bucket) > 63 || strings.ToLower(bucket) != bucket || net.ParseIP(bucket) != nil {
		return false
	}
	for _, label := range strings.Split(bucket, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, value := range label {
			if (value >= 'a' && value <= 'z') || (value >= '0' && value <= '9') || value == '-' {
				continue
			}
			return false
		}
	}
	return true
}

// PutObject stores or replaces one object. Repeating the same content is
// idempotent and returns the existing metadata.
func (s *Store) PutObject(ctx context.Context, tenantID, objectKey string, content io.Reader, contentType string) (runtimestorage.ObjectInfo, error) {
	if runtimestorage.ValidateTenant(tenantID) != nil || tenantID != s.tenant || !validateKey(objectKey) || !s.validRemoteKey("objects", objectKey) || !runtimestorage.ValidateText(contentType, 256, false) || content == nil {
		return runtimestorage.ObjectInfo{}, runtimestorage.ErrInvalid
	}
	operation, cancel, err := s.operationContext(ctx, s.writeTimeout)
	if err != nil {
		return runtimestorage.ObjectInfo{}, err
	}
	defer cancel()
	data, err := readBounded(operation, content, s.maxBytes)
	if err != nil {
		return runtimestorage.ObjectInfo{}, transferError(operation, err)
	}
	if int64(len(data)) > s.maxBytes {
		return runtimestorage.ObjectInfo{}, runtimestorage.ErrInvalid
	}
	digest := sha256.Sum256(data)
	etag := hex.EncodeToString(digest[:])
	remote := s.remoteKey("objects", objectKey)
	created := time.Now().UTC()
	existing, headErr := s.head(operation, remote)
	if headErr == nil {
		persistedCreated, valid := parseArtifactTime(existing.Metadata[metadataCreated])
		if !valid {
			return runtimestorage.ObjectInfo{}, runtimestorage.ErrStorage
		}
		created = persistedCreated
		if existing.Metadata[metadataDigest] == etag && awssdk.ToInt64(existing.ContentLength) == int64(len(data)) && awssdk.ToString(existing.ContentType) == contentType {
			return runtimestorage.ObjectInfo{TenantID: tenantID, ObjectKey: objectKey, ContentType: contentType, Size: int64(len(data)), ETag: etag, CreatedAt: created}, nil
		}
	} else if !errors.Is(headErr, runtimestorage.ErrNotFound) {
		return runtimestorage.ObjectInfo{}, headErr
	}
	out, err := s.client.PutObject(operation, &awss3.PutObjectInput{Bucket: awssdk.String(s.bucket), Key: awssdk.String(remote), Body: bytes.NewReader(data), ContentLength: awssdk.Int64(int64(len(data))), ContentType: awssdk.String(contentType), Metadata: map[string]string{metadataDigest: etag, metadataCreated: created.Format(time.RFC3339Nano)}})
	if err != nil {
		return runtimestorage.ObjectInfo{}, translate(err)
	}
	_ = out
	return runtimestorage.ObjectInfo{TenantID: tenantID, ObjectKey: objectKey, ContentType: contentType, Size: int64(len(data)), ETag: etag, CreatedAt: created}, nil
}

// GetObject returns a caller-owned reader; closing it does not close Store.
func (s *Store) GetObject(ctx context.Context, tenantID, objectKey string) (io.ReadCloser, runtimestorage.ObjectInfo, error) {
	if runtimestorage.ValidateTenant(tenantID) != nil || tenantID != s.tenant || !validateKey(objectKey) || !s.validRemoteKey("objects", objectKey) {
		return nil, runtimestorage.ObjectInfo{}, runtimestorage.ErrInvalid
	}
	operation, cancel, err := s.operationContext(ctx, s.readTimeout)
	if err != nil {
		return nil, runtimestorage.ObjectInfo{}, err
	}
	defer cancel()
	out, err := s.client.GetObject(operation, &awss3.GetObjectInput{Bucket: awssdk.String(s.bucket), Key: awssdk.String(s.remoteKey("objects", objectKey))})
	if err != nil {
		return nil, runtimestorage.ObjectInfo{}, translate(err)
	}
	data, bodyErr := readS3Body(operation, out, s.maxBytes)
	if bodyErr != nil {
		return nil, runtimestorage.ObjectInfo{}, bodyErr
	}
	digest := sha256.Sum256(data)
	etag := hex.EncodeToString(digest[:])
	if out.Metadata == nil || out.Metadata[metadataDigest] != etag {
		return nil, runtimestorage.ObjectInfo{}, runtimestorage.ErrStorage
	}
	contentType := ""
	if out.ContentType != nil {
		contentType = *out.ContentType
	}
	if !runtimestorage.ValidateText(contentType, 256, false) {
		return nil, runtimestorage.ObjectInfo{}, runtimestorage.ErrStorage
	}
	created, valid := parseArtifactTime(out.Metadata[metadataCreated])
	if !valid {
		return nil, runtimestorage.ObjectInfo{}, runtimestorage.ErrStorage
	}
	return io.NopCloser(bytes.NewReader(data)), runtimestorage.ObjectInfo{TenantID: tenantID, ObjectKey: objectKey, ContentType: contentType, Size: int64(len(data)), ETag: etag, CreatedAt: created}, nil
}

func readS3Body(ctx context.Context, out *awss3.GetObjectOutput, maxBytes int64) ([]byte, error) {
	if out == nil || out.Body == nil || out.ContentLength == nil || *out.ContentLength < 0 || *out.ContentLength > maxBytes {
		if out != nil && out.Body != nil {
			_ = out.Body.Close()
		}
		return nil, runtimestorage.ErrStorage
	}
	data, readErr := readBounded(ctx, out.Body, maxBytes)
	closeErr := out.Body.Close()
	if readErr != nil {
		return nil, transferError(ctx, readErr)
	}
	if closeErr != nil || int64(len(data)) != *out.ContentLength || int64(len(data)) > maxBytes {
		return nil, runtimestorage.ErrStorage
	}
	return data, nil
}

// DeleteObject removes one object. Missing objects are reported consistently
// with the other runtime storage implementations.
func (s *Store) DeleteObject(ctx context.Context, tenantID, objectKey string) error {
	if runtimestorage.ValidateTenant(tenantID) != nil || tenantID != s.tenant || !validateKey(objectKey) || !s.validRemoteKey("objects", objectKey) {
		return runtimestorage.ErrInvalid
	}
	operation, cancel, err := s.operationContext(ctx, s.writeTimeout)
	if err != nil {
		return err
	}
	defer cancel()
	if _, err := s.head(operation, s.remoteKey("objects", objectKey)); err != nil {
		return err
	}
	_, err = s.client.DeleteObject(operation, &awss3.DeleteObjectInput{Bucket: awssdk.String(s.bucket), Key: awssdk.String(s.remoteKey("objects", objectKey))})
	if err != nil {
		return translate(err)
	}
	return nil
}

// PutArtifact stores or replaces one artifact and returns its committed metadata.
func (s *Store) PutArtifact(ctx context.Context, value runtimestorage.ArtifactRecord) (runtimestorage.ArtifactRecord, error) {
	if !s.validArtifactInput(value) {
		return runtimestorage.ArtifactRecord{}, runtimestorage.ErrInvalid
	}
	metadata, valid := artifactMetadataForWrite(value)
	if !valid {
		return runtimestorage.ArtifactRecord{}, runtimestorage.ErrInvalid
	}
	operation, cancel, err := s.operationContext(ctx, s.writeTimeout)
	if err != nil {
		return runtimestorage.ArtifactRecord{}, err
	}
	defer cancel()
	now := time.Now().UTC()
	digestHex := metadata[metadataDigest]
	remote := s.remoteKey("artifacts", value.ArtifactID)
	version, created, existing, err := s.artifactWriteState(operation, remote, value, digestHex, now)
	if err != nil {
		return runtimestorage.ArtifactRecord{}, err
	}
	if existing != nil {
		return *existing, nil
	}
	if version > 1 && !now.After(created) {
		now = created.Add(time.Nanosecond)
	}
	metadata[metadataVersion] = fmt.Sprint(version)
	metadata[metadataCreated] = created.Format(time.RFC3339Nano)
	metadata[metadataUpdated] = now.Format(time.RFC3339Nano)
	if !validS3MetadataSize(metadata) {
		return runtimestorage.ArtifactRecord{}, runtimestorage.ErrInvalid
	}
	out, err := s.client.PutObject(operation, &awss3.PutObjectInput{Bucket: awssdk.String(s.bucket), Key: awssdk.String(remote), Body: bytes.NewReader(value.Content), ContentLength: awssdk.Int64(int64(len(value.Content))), ContentType: awssdk.String(value.MimeType), Metadata: metadata})
	if err != nil {
		return runtimestorage.ArtifactRecord{}, translate(err)
	}
	_ = out
	value.Content = append([]byte(nil), value.Content...)
	value.Version = version
	value.CreatedAt = created
	value.UpdatedAt = now
	return value, nil
}

func (s *Store) validArtifactInput(value runtimestorage.ArtifactRecord) bool {
	return runtimestorage.ValidateTenant(value.TenantID) == nil && value.TenantID == s.tenant && validateArtifactID(value.ArtifactID) && s.validRemoteKey("artifacts", value.ArtifactID) &&
		runtimestorage.ValidateText(value.SessionID, 256, false) && runtimestorage.ValidateText(value.Name, 512, false) && runtimestorage.ValidateText(value.MimeType, 256, false) && len(value.Content) > 0 && int64(len(value.Content)) <= s.maxBytes
}

func artifactMetadataForWrite(value runtimestorage.ArtifactRecord) (map[string]string, bool) {
	metadata := map[string]string{metadataDigest: hexDigest(value.Content), metadataSession: encodeMetadata(value.SessionID), metadataName: encodeMetadata(value.Name), metadataMime: encodeMetadata(value.MimeType)}
	return metadata, validS3MetadataSize(metadata)
}

func (s *Store) artifactWriteState(ctx context.Context, remote string, value runtimestorage.ArtifactRecord, digest string, now time.Time) (int64, time.Time, *runtimestorage.ArtifactRecord, error) {
	existing, headErr := s.head(ctx, remote)
	if errors.Is(headErr, runtimestorage.ErrNotFound) {
		return 1, now, nil, nil
	}
	if headErr != nil {
		return 0, time.Time{}, nil, headErr
	}
	if !validArtifactHead(existing.Metadata, existing.ContentType) {
		return 0, time.Time{}, nil, runtimestorage.ErrStorage
	}
	created := artifactTime(existing.Metadata[metadataCreated], existing.LastModified)
	if existing.Metadata[metadataDigest] == digest && decodeMetadata(existing.Metadata[metadataSession]) == value.SessionID && decodeMetadata(existing.Metadata[metadataName]) == value.Name && decodeMetadata(existing.Metadata[metadataMime]) == value.MimeType {
		record := runtimestorage.ArtifactRecord{TenantID: value.TenantID, ArtifactID: value.ArtifactID, SessionID: value.SessionID, Name: value.Name, MimeType: value.MimeType, Content: append([]byte(nil), value.Content...), Version: artifactVersion(existing.Metadata), CreatedAt: created, UpdatedAt: artifactTime(existing.Metadata[metadataUpdated], existing.LastModified)}
		return 0, time.Time{}, &record, nil
	}
	return artifactVersion(existing.Metadata) + 1, created, nil, nil
}

// GetArtifact loads one validated artifact and returns a defensive content copy.
func (s *Store) GetArtifact(ctx context.Context, tenantID, artifactID string) (runtimestorage.ArtifactRecord, error) {
	if runtimestorage.ValidateTenant(tenantID) != nil || tenantID != s.tenant || !validateArtifactID(artifactID) || !s.validRemoteKey("artifacts", artifactID) {
		return runtimestorage.ArtifactRecord{}, runtimestorage.ErrInvalid
	}
	operation, cancel, err := s.operationContext(ctx, s.readTimeout)
	if err != nil {
		return runtimestorage.ArtifactRecord{}, err
	}
	defer cancel()
	return s.getArtifact(operation, tenantID, artifactID)
}

func (s *Store) getArtifact(ctx context.Context, tenantID, artifactID string) (runtimestorage.ArtifactRecord, error) {
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{Bucket: awssdk.String(s.bucket), Key: awssdk.String(s.remoteKey("artifacts", artifactID))})
	if err != nil {
		return runtimestorage.ArtifactRecord{}, translate(err)
	}
	content, bodyErr := readS3Body(ctx, out, s.maxBytes)
	if bodyErr != nil {
		return runtimestorage.ArtifactRecord{}, bodyErr
	}
	if len(content) == 0 {
		return runtimestorage.ArtifactRecord{}, runtimestorage.ErrStorage
	}
	metadata := out.Metadata
	if !validArtifactMetadata(metadata, content, out.ContentType) {
		return runtimestorage.ArtifactRecord{}, runtimestorage.ErrStorage
	}
	created, updated, version, sessionID, name, mimeType := artifactMetadata(metadata, out.LastModified)
	return runtimestorage.ArtifactRecord{TenantID: tenantID, ArtifactID: artifactID, SessionID: sessionID, Name: name, MimeType: mimeType, Content: append([]byte(nil), content...), Version: version, CreatedAt: created, UpdatedAt: updated}, nil
}

// ListArtifacts lists validated artifacts for a tenant, optionally filtered by session.
func (s *Store) ListArtifacts(ctx context.Context, tenantID, sessionID string) ([]runtimestorage.ArtifactRecord, error) {
	if runtimestorage.ValidateTenant(tenantID) != nil || tenantID != s.tenant || !runtimestorage.ValidateText(sessionID, 256, false) {
		return nil, runtimestorage.ErrInvalid
	}
	operation, cancel, err := s.operationContext(ctx, s.readTimeout)
	if err != nil {
		return nil, err
	}
	defer cancel()
	prefix := s.remoteKey("artifacts", "")
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	keys, err := s.listArtifactKeys(operation, prefix)
	if err != nil {
		return nil, err
	}
	values := make([]runtimestorage.ArtifactRecord, 0, len(keys))
	for _, artifactID := range keys {
		artifact, getErr := s.getArtifact(operation, tenantID, artifactID)
		if getErr != nil {
			return nil, getErr
		}
		if sessionID == "" || artifact.SessionID == sessionID {
			values = append(values, artifact)
		}
	}
	sort.Slice(values, func(i, j int) bool { return values[i].ArtifactID < values[j].ArtifactID })
	return values, nil
}

func (s *Store) listArtifactKeys(ctx context.Context, prefix string) ([]string, error) {
	seenTokens := make(map[string]struct{})
	var token *string
	var keys []string
	for {
		out, listErr := s.client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{Bucket: awssdk.String(s.bucket), Prefix: awssdk.String(prefix), ContinuationToken: token})
		if listErr != nil {
			return nil, translate(listErr)
		}
		if out == nil {
			return nil, runtimestorage.ErrStorage
		}
		pageKeys, pageErr := s.parseArtifactKeys(out.Contents, prefix)
		if pageErr != nil {
			return nil, pageErr
		}
		keys = append(keys, pageKeys...)
		if !awssdk.ToBool(out.IsTruncated) {
			break
		}
		if out.NextContinuationToken == nil || *out.NextContinuationToken == "" {
			return nil, runtimestorage.ErrStorage
		}
		if _, exists := seenTokens[*out.NextContinuationToken]; exists {
			return nil, runtimestorage.ErrStorage
		}
		seenTokens[*out.NextContinuationToken] = struct{}{}
		token = out.NextContinuationToken
	}
	return keys, nil
}

func (s *Store) parseArtifactKeys(items []awss3types.Object, prefix string) ([]string, error) {
	keys := make([]string, 0, len(items))
	for _, item := range items {
		if item.Key == nil || !strings.HasPrefix(*item.Key, prefix) {
			return nil, runtimestorage.ErrStorage
		}
		encoded := path.Base(*item.Key)
		artifactID, err := base64.RawURLEncoding.DecodeString(encoded)
		if err != nil || !validateArtifactID(string(artifactID)) || *item.Key != s.remoteKey("artifacts", string(artifactID)) {
			return nil, runtimestorage.ErrStorage
		}
		keys = append(keys, string(artifactID))
	}
	return keys, nil
}

// DeleteArtifact removes one artifact.
func (s *Store) DeleteArtifact(ctx context.Context, tenantID, artifactID string) error {
	if runtimestorage.ValidateTenant(tenantID) != nil || tenantID != s.tenant || !validateArtifactID(artifactID) || !s.validRemoteKey("artifacts", artifactID) {
		return runtimestorage.ErrInvalid
	}
	operation, cancel, err := s.operationContext(ctx, s.writeTimeout)
	if err != nil {
		return err
	}
	defer cancel()
	if _, err := s.head(operation, s.remoteKey("artifacts", artifactID)); err != nil {
		return err
	}
	_, err = s.client.DeleteObject(operation, &awss3.DeleteObjectInput{Bucket: awssdk.String(s.bucket), Key: awssdk.String(s.remoteKey("artifacts", artifactID))})
	return translate(err)
}

// Probe performs a bounded metadata request for the configured bucket.
func (s *Store) Probe(ctx context.Context) error {
	operation, cancel, err := s.operationContext(ctx, s.connectTimeout)
	if err != nil {
		return err
	}
	defer cancel()
	_, err = s.client.HeadBucket(operation, &awss3.HeadBucketInput{Bucket: awssdk.String(s.bucket)})
	return translate(err)
}

// Close prevents new operations and releases idle SDK HTTP connections.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	closeIdle := s.closeIdle
	s.mu.Unlock()
	if closeIdle != nil {
		closeIdle()
	}
	return nil
}

func (s *Store) head(ctx context.Context, remote string) (*awss3.HeadObjectOutput, error) {
	out, err := s.client.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: awssdk.String(s.bucket), Key: awssdk.String(remote)})
	if err != nil {
		return nil, translate(err)
	}
	if out == nil {
		return nil, runtimestorage.ErrStorage
	}
	return out, nil
}

func transferError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return runtimestorage.ErrStorage
}

func readBounded(ctx context.Context, reader io.Reader, maxBytes int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	limit := maxBytes
	if limit < math.MaxInt64 {
		limit++
	}
	data, err := io.ReadAll(io.LimitReader(reader, limit))
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return data, nil
}
func encodeMetadata(value string) string { return base64.RawURLEncoding.EncodeToString([]byte(value)) }
func decodeMetadata(value string) string {
	decoded, ok := decodeMetadataValue(value)
	if !ok {
		return ""
	}
	return string(decoded)
}

func decodeMetadataValue(value string) ([]byte, bool) {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	return decoded, err == nil
}
func hexDigest(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}
func artifactVersion(metadata map[string]string) int64 {
	raw := metadata[metadataVersion]
	version, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || version < 1 || strconv.FormatInt(version, 10) != raw {
		return 0
	}
	return version
}
func artifactTime(value string, fallback *time.Time) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err == nil && !parsed.IsZero() {
		return parsed.UTC()
	}
	if fallback != nil {
		return fallback.UTC()
	}
	return time.Now().UTC()
}

func parseArtifactTime(value string) (time.Time, bool) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || parsed.IsZero() {
		return time.Time{}, false
	}
	return parsed.UTC(), true
}

func validArtifactMetadata(metadata map[string]string, content []byte, contentType *string) bool {
	if metadata == nil || metadata[metadataDigest] != hexDigest(content) || !validArtifactHead(metadata, contentType) {
		return false
	}
	return true
}

func validArtifactHead(metadata map[string]string, contentType *string) bool {
	if metadata == nil || artifactVersion(metadata) == 0 || !validDigest(metadata[metadataDigest]) {
		return false
	}
	sessionEncoded, sessionOK := metadata[metadataSession]
	nameEncoded, nameOK := metadata[metadataName]
	mimeEncoded, mimeOK := metadata[metadataMime]
	sessionBytes, sessionDecoded := decodeMetadataValue(sessionEncoded)
	nameBytes, nameDecoded := decodeMetadataValue(nameEncoded)
	mimeBytes, mimeDecoded := decodeMetadataValue(mimeEncoded)
	sessionID, name, mimeType := string(sessionBytes), string(nameBytes), string(mimeBytes)
	if !sessionOK || !nameOK || !mimeOK || !sessionDecoded || !nameDecoded || !mimeDecoded || !runtimestorage.ValidateText(sessionID, 256, false) ||
		!runtimestorage.ValidateText(name, 512, false) || !runtimestorage.ValidateText(mimeType, 256, false) || contentType == nil || *contentType != mimeType {
		return false
	}
	created, createdOK := parseArtifactTime(metadata[metadataCreated])
	updated, updatedOK := parseArtifactTime(metadata[metadataUpdated])
	if !createdOK || !updatedOK || updated.Before(created) {
		return false
	}
	return true
}

func validDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validS3MetadataSize(metadata map[string]string) bool {
	total := 0
	for key, value := range metadata {
		total += len("x-amz-meta-") + len(key) + len(value)
		if total > maxMetadataBytes {
			return false
		}
	}
	return true
}

func artifactMetadata(metadata map[string]string, fallback *time.Time) (time.Time, time.Time, int64, string, string, string) {
	created := artifactTime(metadata[metadataCreated], fallback)
	updated := artifactTime(metadata[metadataUpdated], fallback)
	return created, updated, artifactVersion(metadata), decodeMetadata(metadata[metadataSession]), decodeMetadata(metadata[metadataName]), decodeMetadata(metadata[metadataMime])
}

func translate(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var api smithy.APIError
	if errors.As(err, &api) {
		switch api.ErrorCode() {
		case "NoSuchKey", "NoSuchObject", "NotFound", "NoSuchVersion":
			return runtimestorage.ErrNotFound
		case "PreconditionFailed", "ConditionalRequestConflict":
			return runtimestorage.ErrConflict
		}
	}
	return runtimestorage.ErrStorage
}

var _ runtimestorage.ArtifactStore = (*Store)(nil)
var _ runtimestorage.ObjectStore = (*Store)(nil)
