package s3object

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const DefaultPresignTTL = 10 * time.Minute

type Config struct {
	Endpoint       string
	Region         string
	Bucket         string
	AccessKey      string
	SecretKey      string
	UsePathStyle   bool
	MaxObjectBytes int64
	PresignMaxTTL  time.Duration
}

type Store struct {
	client         *minio.Client
	bucket         string
	maxObjectBytes int64
	presignMaxTTL  time.Duration
}

var _ storage.ObjectStore = (*Store)(nil)
var _ storage.ObjectStoreReadiness = (*Store)(nil)

func New(cfg Config) (*Store, error) {
	cfg.Endpoint = strings.TrimSpace(cfg.Endpoint)
	cfg.Region = strings.TrimSpace(cfg.Region)
	cfg.Bucket = strings.TrimSpace(cfg.Bucket)
	if cfg.Endpoint == "" || cfg.Region == "" || cfg.Bucket == "" || cfg.AccessKey == "" || cfg.SecretKey == "" {
		return nil, errors.New("object storage configuration is incomplete")
	}
	parsed, err := url.Parse(cfg.Endpoint)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Path != "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("object storage endpoint is invalid")
	}
	if !validBucket(cfg.Bucket) || !validRegion(cfg.Region) {
		return nil, errors.New("object storage bucket or region is invalid")
	}
	if cfg.MaxObjectBytes == 0 {
		cfg.MaxObjectBytes = storage.DefaultMaxObjectBytes
	}
	if cfg.MaxObjectBytes < 1 {
		return nil, errors.New("object storage max object size is invalid")
	}
	if cfg.PresignMaxTTL == 0 {
		cfg.PresignMaxTTL = DefaultPresignTTL
	}
	if cfg.PresignMaxTTL <= 0 || cfg.PresignMaxTTL > 7*24*time.Hour {
		return nil, errors.New("object storage presign ttl is invalid")
	}
	client, err := minio.New(parsed.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: parsed.Scheme == "https",
		Region: cfg.Region,
		BucketLookup: func() minio.BucketLookupType {
			if cfg.UsePathStyle {
				return minio.BucketLookupPath
			}
			return minio.BucketLookupAuto
		}(),
	})
	if err != nil {
		return nil, errors.New("object storage client initialization failed")
	}
	return &Store{client: client, bucket: cfg.Bucket, maxObjectBytes: cfg.MaxObjectBytes, presignMaxTTL: cfg.PresignMaxTTL}, nil
}

func (s *Store) Put(ctx context.Context, tc tenant.TenantContext, upload storage.ObjectUpload) (storage.ObjectInfo, error) {
	if err := contextAndTenant(ctx, tc); err != nil {
		return storage.ObjectInfo{}, err
	}
	if err := storage.ValidateObjectUpload(upload, s.maxObjectBytes); err != nil {
		return storage.ObjectInfo{}, err
	}
	data, err := io.ReadAll(io.LimitReader(upload.Body, s.maxObjectBytes+1))
	if err != nil {
		return storage.ObjectInfo{}, errors.Join(storage.ErrObjectUnavailable, errors.New("object upload could not be read"))
	}
	if int64(len(data)) != upload.ExpectedSize {
		if int64(len(data)) > s.maxObjectBytes {
			return storage.ObjectInfo{}, storage.ErrObjectTooLarge
		}
		return storage.ObjectInfo{}, fmt.Errorf("%w: object size differs from declared size", storage.ErrObjectInvalid)
	}
	digest := sha256.Sum256(data)
	actualSHA := hex.EncodeToString(digest[:])
	if !strings.EqualFold(actualSHA, upload.ExpectedSHA256) {
		return storage.ObjectInfo{}, storage.ErrObjectChecksum
	}
	key, _ := storage.CanonicalObjectKey(tc.TenantID, upload.ArtifactID)
	_, err = s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType:      upload.MIMEType,
		UserMetadata:     map[string]string{"sha256": actualSHA},
		DisableMultipart: true,
	})
	if err != nil {
		return storage.ObjectInfo{}, mapError(err)
	}
	return storage.ObjectInfo{TenantID: tc.TenantID, ArtifactID: upload.ArtifactID, ObjectKey: key, MIMEType: upload.MIMEType, SizeBytes: int64(len(data)), SHA256: actualSHA}, nil
}

func (s *Store) Get(ctx context.Context, tc tenant.TenantContext, artifactID string) (io.ReadCloser, storage.ObjectInfo, error) {
	key, err := s.key(ctx, tc, artifactID)
	if err != nil {
		return nil, storage.ObjectInfo{}, err
	}
	object, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, storage.ObjectInfo{}, mapError(err)
	}
	info, err := object.Stat()
	if err != nil {
		_ = object.Close()
		return nil, storage.ObjectInfo{}, mapError(err)
	}
	objectInfoValue := objectInfo(tc.TenantID, artifactID, key, info)
	return newVerifiedObjectReader(object, objectInfoValue), objectInfoValue, nil
}

type verifiedObjectReader struct {
	body         io.ReadCloser
	expectedSize int64
	expectedSHA  string
	digest       hash.Hash
	readSize     int64
	verified     bool
	verification error
}

func newVerifiedObjectReader(body io.ReadCloser, info storage.ObjectInfo) io.ReadCloser {
	return &verifiedObjectReader{body: body, expectedSize: info.SizeBytes, expectedSHA: info.SHA256, digest: sha256.New()}
}

func (r *verifiedObjectReader) Read(p []byte) (int, error) {
	if r == nil || r.body == nil {
		return 0, storage.ErrObjectUnavailable
	}
	n, err := r.body.Read(p)
	if n > 0 {
		_, _ = r.digest.Write(p[:n])
		r.readSize += int64(n)
	}
	if err == io.EOF {
		if verifyErr := r.verify(); verifyErr != nil {
			return n, verifyErr
		}
		return n, io.EOF
	}
	if err != nil {
		return n, mapError(err)
	}
	return n, nil
}

func (r *verifiedObjectReader) verify() error {
	if r.verified {
		return r.verification
	}
	r.verified = true
	if r.expectedSize >= 0 && r.readSize != r.expectedSize {
		r.verification = fmt.Errorf("%w: object size changed while reading", storage.ErrObjectInvalid)
		return r.verification
	}
	actualSHA := hex.EncodeToString(r.digest.Sum(nil))
	if r.expectedSHA != "" && !strings.EqualFold(actualSHA, r.expectedSHA) {
		r.verification = storage.ErrObjectChecksum
	}
	return r.verification
}

func (r *verifiedObjectReader) Close() error {
	if r == nil || r.body == nil {
		return nil
	}
	return mapError(r.body.Close())
}

func (s *Store) Head(ctx context.Context, tc tenant.TenantContext, artifactID string) (storage.ObjectInfo, error) {
	key, err := s.key(ctx, tc, artifactID)
	if err != nil {
		return storage.ObjectInfo{}, err
	}
	info, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return storage.ObjectInfo{}, mapError(err)
	}
	return objectInfo(tc.TenantID, artifactID, key, info), nil
}

func (s *Store) Delete(ctx context.Context, tc tenant.TenantContext, artifactID string) error {
	key, err := s.key(ctx, tc, artifactID)
	if err != nil {
		return err
	}
	if err := s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{}); err != nil {
		return mapError(err)
	}
	return nil
}

func (s *Store) PresignedURL(ctx context.Context, tc tenant.TenantContext, artifactID string, ttl time.Duration) (string, error) {
	key, err := s.key(ctx, tc, artifactID)
	if err != nil {
		return "", err
	}
	if ttl <= 0 || ttl > s.presignMaxTTL {
		return "", fmt.Errorf("%w: presign ttl is outside the allowed bound", storage.ErrObjectInvalid)
	}
	u, err := s.client.PresignedGetObject(ctx, s.bucket, key, ttl, nil)
	if err != nil {
		return "", mapError(err)
	}
	return u.String(), nil
}

func (s *Store) key(ctx context.Context, tc tenant.TenantContext, artifactID string) (string, error) {
	if err := contextAndTenant(ctx, tc); err != nil {
		return "", err
	}
	key, err := storage.CanonicalObjectKey(tc.TenantID, artifactID)
	if err != nil {
		return "", err
	}
	return key, nil
}

func contextAndTenant(ctx context.Context, tc tenant.TenantContext) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", storage.ErrObjectInvalid)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tc.Validate(); err != nil {
		return fmt.Errorf("%w: tenant context is invalid", storage.ErrObjectUnauthorized)
	}
	return nil
}

func objectInfo(tenantID, artifactID, key string, info minio.ObjectInfo) storage.ObjectInfo {
	return storage.ObjectInfo{TenantID: tenantID, ArtifactID: artifactID, ObjectKey: key, MIMEType: info.ContentType, SizeBytes: info.Size, SHA256: strings.TrimSpace(info.Metadata.Get("X-Amz-Meta-Sha256")), ETag: info.ETag}
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var response minio.ErrorResponse
	response = minio.ToErrorResponse(err)
	switch response.Code {
	case "NoSuchKey", "NoSuchObject", "NoSuchBucket", "NotFound":
		return storage.ErrObjectNotFound
	case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch":
		return storage.ErrObjectUnauthorized
	}
	return errors.Join(storage.ErrObjectUnavailable, errors.New("object provider request failed"))
}

func validBucket(value string) bool {
	if len(value) < 3 || len(value) > 63 || value[0] == '-' || value[len(value)-1] == '-' {
		return false
	}
	for _, r := range value {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-') {
			return false
		}
	}
	return true
}

func validRegion(value string) bool {
	return len(value) >= 1 && len(value) <= 64 && !strings.ContainsAny(value, "\r\n\x00 /")
}

// Ready performs a bounded authenticated provider probe. It does not expose
// endpoint, bucket, credentials, or the provider response in its error.
func (s *Store) Ready(ctx context.Context) error {
	if s == nil || s.client == nil || strings.TrimSpace(s.bucket) == "" {
		return storage.ErrObjectUnavailable
	}
	if ctx == nil {
		return fmt.Errorf("%w: readiness context is required", storage.ErrObjectInvalid)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := s.client.ListBuckets(ctx); err != nil {
		return mapError(err)
	}
	return nil
}
