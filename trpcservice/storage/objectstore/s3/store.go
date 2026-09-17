// Package s3 implements the immutable ObjectStore contract on an S3-compatible
// versioned bucket. Bucket versioning is mandatory so guarded cleanup can
// delete the exact version that was inspected instead of racing a replacement.
package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"net/url"
	"strings"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore"
)

const digestMetadataKey = "trpc-content-sha256"

type API interface {
	PutObject(context.Context, *awss3.PutObjectInput, ...func(*awss3.Options)) (*awss3.PutObjectOutput, error)
	GetObject(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
	HeadObject(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error)
	DeleteObject(context.Context, *awss3.DeleteObjectInput, ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error)
	GetBucketVersioning(context.Context, *awss3.GetBucketVersioningInput, ...func(*awss3.Options)) (*awss3.GetBucketVersioningOutput, error)
}

type Store struct {
	client   API
	bucket   string
	maxBytes int64
}

type Options struct {
	MaxBytes      int64
	AllowInsecure bool
}

func New(client API, bucket string, options Options) (*Store, error) {
	if client == nil || strings.TrimSpace(bucket) == "" {
		return nil, runtime.ErrInvalidEnvelope
	}
	maximum := options.MaxBytes
	if maximum <= 0 {
		maximum = 16 << 20
	}
	return &Store{client: client, bucket: bucket, maxBytes: maximum}, nil
}

// NewFromConfig constructs a real SDK-backed provider. Endpoint and path-style
// selection remain explicit so the same adapter works with AWS S3 and MinIO;
// credentials must already be scoped into cfg by the composition root.
func NewFromConfig(cfg awssdk.Config, bucket, endpoint string, usePathStyle bool, options Options) (*Store, error) {
	if endpoint != "" {
		value, err := url.Parse(endpoint)
		if err != nil || value.Host == "" || value.User != nil || value.Fragment != "" ||
			(value.Scheme != "https" && !(options.AllowInsecure && value.Scheme == "http")) {
			return nil, runtime.ErrInvalidEnvelope
		}
	}
	client := awss3.NewFromConfig(cfg, func(value *awss3.Options) {
		value.UsePathStyle = usePathStyle
		if endpoint != "" {
			value.BaseEndpoint = awssdk.String(endpoint)
		}
	})
	return New(client, bucket, options)
}

// Probe verifies the capability required for safe conditional cleanup. A
// reachable but unversioned bucket is intentionally not production-ready.
func (s *Store) Probe(ctx context.Context) error {
	if s == nil || s.client == nil {
		return runtime.ErrCapabilityUnsupported
	}
	value, err := s.client.GetBucketVersioning(ctx, &awss3.GetBucketVersioningInput{Bucket: awssdk.String(s.bucket)})
	if err != nil {
		return translate(err)
	}
	if value == nil || value.Status != types.BucketVersioningStatusEnabled {
		return runtime.ErrCapabilityUnsupported
	}
	return nil
}

func (s *Store) PutObject(ctx context.Context, in objectstore.Object) (objectstore.Object, error) {
	if s == nil || s.client == nil {
		return objectstore.Object{}, runtime.ErrCapabilityUnsupported
	}
	if err := objectstore.Validate(in); err != nil {
		return objectstore.Object{}, err
	}
	if int64(len(in.Content)) > s.maxBytes {
		return objectstore.Object{}, runtime.ErrInvalidEnvelope
	}
	checksum := sha256.Sum256(in.Content)
	output, err := s.client.PutObject(ctx, &awss3.PutObjectInput{Bucket: awssdk.String(s.bucket), Key: awssdk.String(in.ObjectKey),
		Body: bytes.NewReader(in.Content), ContentLength: awssdk.Int64(int64(len(in.Content))), IfNoneMatch: awssdk.String("*"),
		ChecksumSHA256: awssdk.String(base64.StdEncoding.EncodeToString(checksum[:])),
		Metadata:       map[string]string{digestMetadataKey: in.ContentDigest}})
	if err != nil {
		if isPrecondition(err) {
			existing, getErr := s.GetObject(ctx, in.TenantID, in.ObjectKey)
			if getErr != nil {
				return objectstore.Object{}, getErr
			}
			return compare(existing, in)
		}
		return objectstore.Object{}, translate(err)
	}
	if output == nil || awssdk.ToString(output.VersionId) == "" {
		return objectstore.Object{}, runtime.ErrCapabilityUnsupported
	}
	return clone(in), nil
}

func (s *Store) GetObject(ctx context.Context, tenantID, objectKey string) (objectstore.Object, error) {
	if s == nil || s.client == nil {
		return objectstore.Object{}, runtime.ErrCapabilityUnsupported
	}
	if err := objectstore.ValidateKey(tenantID, objectKey); err != nil {
		return objectstore.Object{}, err
	}
	output, err := s.client.GetObject(ctx, &awss3.GetObjectInput{Bucket: awssdk.String(s.bucket), Key: awssdk.String(objectKey)})
	if err != nil {
		return objectstore.Object{}, translate(err)
	}
	if output == nil || output.Body == nil || output.ContentLength == nil || *output.ContentLength <= 0 ||
		*output.ContentLength > s.maxBytes || awssdk.ToString(output.VersionId) == "" {
		closeBody(output)
		return objectstore.Object{}, runtime.ErrVersionMismatch
	}
	defer output.Body.Close()
	content, err := io.ReadAll(io.LimitReader(output.Body, s.maxBytes+1))
	if err != nil {
		return objectstore.Object{}, runtime.ErrBackendUnavailable
	}
	if int64(len(content)) != *output.ContentLength || int64(len(content)) > s.maxBytes {
		return objectstore.Object{}, runtime.ErrVersionMismatch
	}
	digest := sha256.Sum256(content)
	contentDigest := hex.EncodeToString(digest[:])
	if output.Metadata[digestMetadataKey] != contentDigest {
		return objectstore.Object{}, runtime.ErrVersionMismatch
	}
	value := objectstore.Object{TenantID: tenantID, ObjectKey: objectKey, ContentDigest: contentDigest, Content: content}
	if output.LastModified != nil {
		value.CreatedAt = output.LastModified.UTC()
	}
	if err := objectstore.Validate(value); err != nil {
		return objectstore.Object{}, runtime.ErrVersionMismatch
	}
	return value, nil
}

func (s *Store) DeleteObject(ctx context.Context, tenantID, objectKey, contentDigest string) error {
	if s == nil || s.client == nil {
		return runtime.ErrCapabilityUnsupported
	}
	if err := objectstore.ValidateKey(tenantID, objectKey); err != nil {
		return err
	}
	if len(contentDigest) != sha256.Size*2 {
		return runtime.ErrInvalidEnvelope
	}
	head, err := s.client.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: awssdk.String(s.bucket), Key: awssdk.String(objectKey)})
	if err != nil {
		if errors.Is(translate(err), runtime.ErrNotFound) {
			return nil
		}
		return translate(err)
	}
	if head == nil || head.Metadata[digestMetadataKey] != contentDigest {
		return runtime.ErrVersionMismatch
	}
	versionID := awssdk.ToString(head.VersionId)
	if versionID == "" {
		return runtime.ErrCapabilityUnsupported
	}
	// Verify bytes, not only mutable user metadata, before deleting the exact
	// inspected version. Versioning makes a concurrent replacement a new version.
	get, err := s.client.GetObject(ctx, &awss3.GetObjectInput{Bucket: awssdk.String(s.bucket), Key: awssdk.String(objectKey), VersionId: &versionID})
	if err != nil {
		return translate(err)
	}
	if get == nil || get.Body == nil {
		return runtime.ErrVersionMismatch
	}
	content, readErr := io.ReadAll(io.LimitReader(get.Body, s.maxBytes+1))
	closeErr := get.Body.Close()
	if readErr != nil || closeErr != nil || int64(len(content)) > s.maxBytes {
		return runtime.ErrBackendUnavailable
	}
	digest := sha256.Sum256(content)
	if hex.EncodeToString(digest[:]) != contentDigest || awssdk.ToString(get.VersionId) != versionID {
		return runtime.ErrVersionMismatch
	}
	_, err = s.client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: awssdk.String(s.bucket), Key: awssdk.String(objectKey), VersionId: &versionID})
	return translate(err)
}

func compare(existing, requested objectstore.Object) (objectstore.Object, error) {
	if existing.TenantID != requested.TenantID || existing.ObjectKey != requested.ObjectKey ||
		existing.ContentDigest != requested.ContentDigest || !bytes.Equal(existing.Content, requested.Content) {
		return objectstore.Object{}, runtime.ErrIdempotencyCollision
	}
	return existing, nil
}

func clone(value objectstore.Object) objectstore.Object {
	value.Content = append([]byte(nil), value.Content...)
	return value
}

func closeBody(output *awss3.GetObjectOutput) {
	if output != nil && output.Body != nil {
		_ = output.Body.Close()
	}
}

func isPrecondition(err error) bool {
	var api smithy.APIError
	return errors.As(err, &api) && (api.ErrorCode() == "PreconditionFailed" || api.ErrorCode() == "ConditionalRequestConflict")
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
		case "NoSuchKey", "NotFound", "NoSuchVersion":
			return runtime.ErrNotFound
		case "PreconditionFailed", "ConditionalRequestConflict":
			return runtime.ErrVersionConflict
		}
	}
	return runtime.ErrBackendUnavailable
}

var _ objectstore.Store = (*Store)(nil)
