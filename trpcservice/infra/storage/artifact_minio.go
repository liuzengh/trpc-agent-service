// Artifact persistence on an S3-compatible object store (MinIO).
//
// The service implements trpc-agent-go/artifact.Service, which the framework
// runner injects (runner.WithArtifactService) so code-execution tools persist
// named, versioned artifacts automatically. Bytes live in MinIO; version
// bookkeeping is encoded in the object key layout so no extra index is
// needed.
package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"trpc.group/trpc-go/trpc-agent-go/artifact"
)

// Artifact object layout mirrors the framework COS convention so a bucket can
// be shared across storage implementations:
//
//	session-scoped:  {app}/{user}/{session}/{filename}/{revision}
//	user-namespaced: {app}/{user}/user/{filename}/{revision}
//
// app is the tenant id, which isolates tenants at the key-namespace level.
// revision is the trailing numeric leaf: the first save of a filename is
// revision 0 and each subsequent save increments it by one.

const artifactUserNamespaceMarker = "user:"

func artifactHasUserNamespace(filename string) bool {
	return strings.HasPrefix(filename, artifactUserNamespaceMarker)
}

func artifactObjectName(si artifact.SessionInfo, filename string, revision int) string {
	if artifactHasUserNamespace(filename) {
		return fmt.Sprintf("%s/%s/user/%s/%d", si.AppName, si.UserID, filename, revision)
	}
	return fmt.Sprintf("%s/%s/%s/%s/%d", si.AppName, si.UserID, si.SessionID, filename, revision)
}

func artifactObjectPrefix(si artifact.SessionInfo, filename string) string {
	if artifactHasUserNamespace(filename) {
		return fmt.Sprintf("%s/%s/user/%s/", si.AppName, si.UserID, filename)
	}
	return fmt.Sprintf("%s/%s/%s/%s/", si.AppName, si.UserID, si.SessionID, filename)
}

// artifactSessionPrefix lists every session-scoped object of a session.
func artifactSessionPrefix(si artifact.SessionInfo) string {
	return fmt.Sprintf("%s/%s/%s/", si.AppName, si.UserID, si.SessionID)
}

// parseArtifactRevision extracts the trailing revision number of an object
// key. ok is false when the key does not end in a numeric revision.
func parseArtifactRevision(key string) (int, bool) {
	slash := strings.LastIndex(key, "/")
	if slash < 0 || slash == len(key)-1 {
		return 0, false
	}
	rev, err := strconv.Atoi(key[slash+1:])
	if err != nil {
		return 0, false
	}
	return rev, true
}

// MinioArtifactService stores versioned artifacts in one S3 bucket.
type MinioArtifactService struct {
	client *minio.Client
	bucket string
}

// NewMinioArtifactService dials MinIO and ensures the bucket exists
// (idempotent). An empty endpoint returns an error; callers configure a
// nil service when artifact persistence is disabled.
func NewMinioArtifactService(ctx context.Context, endpoint, accessKey, secretKey, bucket string, useSSL bool) (*MinioArtifactService, error) {
	if endpoint == "" || bucket == "" {
		return nil, errors.New("storage: minio endpoint and bucket are required")
	}
	if accessKey == "" || secretKey == "" {
		return nil, errors.New("storage: minio access key and secret key are required")
	}
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("storage: minio client: %w", err)
	}
	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		return nil, fmt.Errorf("storage: minio bucket check: %w", err)
	}
	if !exists {
		if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("storage: minio make bucket %q: %w", bucket, err)
		}
	}
	return &MinioArtifactService{client: client, bucket: bucket}, nil
}

// SaveArtifact writes revision len(versions) of the artifact and returns it,
// following the framework in-memory semantics (first save returns 0).
func (s *MinioArtifactService) SaveArtifact(ctx context.Context, si artifact.SessionInfo, filename string, art *artifact.Artifact) (int, error) {
	if art == nil {
		return 0, errors.New("storage: cannot save a nil artifact")
	}
	revisions, err := s.listRevisions(ctx, si, filename)
	if err != nil {
		return 0, err
	}
	revision := len(revisions) // first save -> 0
	name := artifactObjectName(si, filename, revision)
	_, err = s.client.PutObject(ctx, s.bucket, name,
		bytes.NewReader(art.Data), int64(len(art.Data)),
		minio.PutObjectOptions{ContentType: artifactMime(art.MimeType)})
	if err != nil {
		return 0, fmt.Errorf("storage: put artifact: %w", err)
	}
	return revision, nil
}

// LoadArtifact reads the latest (version nil) or a specific revision.
// Returns (nil, nil) when the artifact does not exist at all.
func (s *MinioArtifactService) LoadArtifact(ctx context.Context, si artifact.SessionInfo, filename string, version *int) (*artifact.Artifact, error) {
	revisions, err := s.listRevisions(ctx, si, filename)
	if err != nil {
		return nil, err
	}
	if len(revisions) == 0 {
		return nil, nil
	}
	rev := revisions[len(revisions)-1]
	if version != nil {
		if *version < 0 || *version >= len(revisions) {
			return nil, fmt.Errorf("storage: artifact version %d does not exist", *version)
		}
		rev = *version
	}
	name := artifactObjectName(si, filename, rev)
	obj, err := s.client.GetObject(ctx, s.bucket, name, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("storage: get artifact object: %w", err)
	}
	defer obj.Close()
	stat, err := obj.Stat()
	if err != nil {
		var resp minio.ErrorResponse
		if errors.As(err, &resp) && resp.StatusCode == 404 {
			return nil, nil // removed between listing and read
		}
		return nil, fmt.Errorf("storage: stat artifact: %w", err)
	}
	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("storage: read artifact: %w", err)
	}
	return &artifact.Artifact{
		Data:     data,
		MimeType: stat.ContentType,
		Name:     filename,
	}, nil
}

// ListArtifactKeys returns the distinct artifact filenames visible to a
// session: its session-scoped files plus any user-namespaced (user:...) files
// of the same user, mirroring the framework in-memory semantics.
func (s *MinioArtifactService) ListArtifactKeys(ctx context.Context, si artifact.SessionInfo) ([]string, error) {
	keys := map[string]bool{}
	collect := func(prefix, base string) error {
		for obj := range s.client.ListObjects(ctx, s.bucket,
			minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
			if obj.Err != nil {
				return fmt.Errorf("storage: list artifacts: %w", obj.Err)
			}
			if _, ok := parseArtifactRevision(obj.Key); !ok {
				continue
			}
			rel := strings.TrimPrefix(obj.Key, base)
			keys[rel[:strings.LastIndex(rel, "/")]] = true // drop /{revision}
		}
		return nil
	}
	if err := collect(artifactSessionPrefix(si), artifactSessionPrefix(si)); err != nil {
		return nil, err
	}
	userBase := fmt.Sprintf("%s/%s/user/", si.AppName, si.UserID)
	if err := collect(userBase, userBase); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(keys))
	for k := range keys {
		out = append(out, k)
	}
	sort.Strings(out)
	return out, nil
}

// DeleteArtifact removes every revision of an artifact. Deleting a missing
// artifact is not an error (matches the framework semantics).
func (s *MinioArtifactService) DeleteArtifact(ctx context.Context, si artifact.SessionInfo, filename string) error {
	prefix := artifactObjectPrefix(si, filename)
	var errs []error
	for obj := range s.client.ListObjects(ctx, s.bucket,
		minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return fmt.Errorf("storage: delete artifacts: %w", obj.Err)
		}
		if err := s.client.RemoveObject(ctx, s.bucket, obj.Key, minio.RemoveObjectOptions{}); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// ListVersions returns the revisions of an artifact in ascending order.
func (s *MinioArtifactService) ListVersions(ctx context.Context, si artifact.SessionInfo, filename string) ([]int, error) {
	return s.listRevisions(ctx, si, filename)
}

func (s *MinioArtifactService) listRevisions(ctx context.Context, si artifact.SessionInfo, filename string) ([]int, error) {
	revisions := []int{}
	prefix := artifactObjectPrefix(si, filename)
	for obj := range s.client.ListObjects(ctx, s.bucket,
		minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return nil, fmt.Errorf("storage: list artifact versions: %w", obj.Err)
		}
		if rev, ok := parseArtifactRevision(obj.Key); ok {
			revisions = append(revisions, rev)
		}
	}
	sort.Ints(revisions)
	return revisions, nil
}

// artifactMime falls back to application/octet-stream.
func artifactMime(mime string) string {
	if mime == "" {
		return "application/octet-stream"
	}
	return mime
}
