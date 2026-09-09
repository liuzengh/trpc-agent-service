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

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

// s3DefaultContentType is stored when the artifact carries no MIME type.
const s3DefaultContentType = "application/octet-stream"

// S3ArtifactConfig holds the S3 artifact backend configuration. Secret
// material is carried as references and resolved through Secrets, never
// logged.
type S3ArtifactConfig struct {
	Endpoint     string // host:port, no scheme (compose MinIO: localhost:9000)
	AccessKeyRef string // secret ref for the access key, resolved via Secrets
	SecretKeyRef string // secret ref for the secret key, resolved via Secrets
	Bucket       string // created at startup when missing
	Secure       bool   // true for TLS endpoints (cloud OSS), false for local MinIO
	Prefix       string // optional key prefix, e.g. "artifact/"; normalized to end with "/"

	// Secrets resolves AccessKeyRef/SecretKeyRef at startup (fail fast on
	// misconfiguration); the plaintext never enters logs or traces.
	Secrets config.SecretResolver
}

// S3ArtifactService implements the framework artifact.Service over any
// S3-compatible object store (MinIO in the compose stack, cloud OSS in
// production).
//
// Key layout: {prefix}{app}/{user}/{session}/{filename}/v{n} — every saved
// revision is its own object, revision IDs start at 0. The app segment is
// the agent_app UUID, so tenant isolation comes for free from the key space.
//
// Versions are derived from ListObjects instead of a "latest" pointer
// object: plain S3 has no multi-object transaction, so a pointer would be a
// second source of truth that a crash between the two PUTs can leave stale,
// while the version objects are the single source of truth and listing them
// is cheap at one object per revision. The cost is that SaveArtifact is
// read-then-write: two concurrent saves of the same filename may pick the
// same next version and overwrite each other's object (last-writer-wins).
// That is accepted — concurrent writes to one artifact are not a platform
// flow.
type S3ArtifactService struct {
	client *minio.Client
	bucket string
	prefix string
}

var _ artifact.Service = (*S3ArtifactService)(nil)

// NewS3ArtifactService resolves the credentials, creates the client and
// ensures the bucket exists; any failure is returned as an error (fail fast
// at startup).
func NewS3ArtifactService(cfg S3ArtifactConfig) (*S3ArtifactService, error) {
	if cfg.Endpoint == "" || cfg.Bucket == "" {
		return nil, errors.New("s3 artifact: Endpoint and Bucket are required")
	}
	if cfg.AccessKeyRef == "" || cfg.SecretKeyRef == "" {
		return nil, errors.New("s3 artifact: AccessKeyRef and SecretKeyRef are required")
	}
	if cfg.Secrets == nil {
		return nil, errors.New("s3 artifact: Secrets resolver is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	accessKey, err := cfg.Secrets.Resolve(ctx, cfg.AccessKeyRef)
	if err != nil {
		return nil, fmt.Errorf("s3 artifact: resolve access key: %w", err)
	}
	secretKey, err := cfg.Secrets.Resolve(ctx, cfg.SecretKeyRef)
	if err != nil {
		return nil, fmt.Errorf("s3 artifact: resolve secret key: %w", err)
	}

	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: cfg.Secure,
	})
	if err != nil {
		return nil, fmt.Errorf("s3 artifact: create client for %s: %w", cfg.Endpoint, err)
	}

	exists, err := client.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("s3 artifact: check bucket %q: %w", cfg.Bucket, err)
	}
	if !exists {
		if err := client.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{}); err != nil {
			// A concurrent instance may have created the bucket in between.
			if retry, rerr := client.BucketExists(ctx, cfg.Bucket); rerr != nil || !retry {
				return nil, fmt.Errorf("s3 artifact: create bucket %q: %w", cfg.Bucket, err)
			}
		} else {
			plog.Infof("s3 artifact: bucket %q created", cfg.Bucket)
		}
	}

	prefix := cfg.Prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return &S3ArtifactService{client: client, bucket: cfg.Bucket, prefix: prefix}, nil
}

// SaveArtifact implements artifact.Service: the next revision (max+1, first
// is 0) is written as its own object and its number returned.
func (s *S3ArtifactService) SaveArtifact(ctx context.Context, sessionInfo artifact.SessionInfo, filename string, art *artifact.Artifact) (int, error) {
	if err := validateArtifactSession(sessionInfo); err != nil {
		return 0, err
	}
	if err := validateArtifactFilename(filename); err != nil {
		return 0, err
	}
	if art == nil {
		return 0, errors.New("s3 artifact: nil artifact")
	}

	versions, err := s.ListVersions(ctx, sessionInfo, filename)
	if err != nil {
		return 0, err
	}
	next := 0
	if len(versions) > 0 {
		next = versions[len(versions)-1] + 1
	}

	mimeType := art.MimeType
	if mimeType == "" {
		mimeType = s3DefaultContentType
	}
	key := s.objectKey(sessionInfo, filename, next)
	_, err = s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(art.Data), int64(len(art.Data)),
		minio.PutObjectOptions{ContentType: mimeType})
	if err != nil {
		return 0, fmt.Errorf("s3 artifact: put %q: %w", key, err)
	}
	plog.Debugf("s3 artifact: saved bucket=%s key=%s bytes=%d", s.bucket, key, len(art.Data))
	return next, nil
}

// LoadArtifact implements artifact.Service: nil version loads the latest
// revision; a missing artifact (or version) returns (nil, nil).
func (s *S3ArtifactService) LoadArtifact(ctx context.Context, sessionInfo artifact.SessionInfo, filename string, version *int) (*artifact.Artifact, error) {
	if err := validateArtifactSession(sessionInfo); err != nil {
		return nil, err
	}
	if err := validateArtifactFilename(filename); err != nil {
		return nil, err
	}

	target := 0
	if version == nil {
		versions, err := s.ListVersions(ctx, sessionInfo, filename)
		if err != nil {
			return nil, err
		}
		if len(versions) == 0 {
			return nil, nil
		}
		target = versions[len(versions)-1]
	} else {
		target = *version
	}

	obj, err := s.client.GetObject(ctx, s.bucket, s.objectKey(sessionInfo, filename, target), minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("s3 artifact: get %s v%d: %w", filename, target, err)
	}
	defer func() { _ = obj.Close() }()
	// GetObject defers most errors to the first read; Stat surfaces a
	// missing key here.
	stat, err := obj.Stat()
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return nil, nil
		}
		return nil, fmt.Errorf("s3 artifact: stat %s v%d: %w", filename, target, err)
	}
	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("s3 artifact: read %s v%d: %w", filename, target, err)
	}
	mimeType := stat.ContentType
	if mimeType == "" {
		mimeType = s3DefaultContentType
	}
	return &artifact.Artifact{Data: data, MimeType: mimeType, Name: filename}, nil
}

// ListArtifactKeys implements artifact.Service: the distinct artifact
// filenames within the session, sorted, without the version suffix.
func (s *S3ArtifactService) ListArtifactKeys(ctx context.Context, sessionInfo artifact.SessionInfo) ([]string, error) {
	if err := validateArtifactSession(sessionInfo); err != nil {
		return nil, err
	}

	prefix := s.sessionPrefix(sessionInfo)
	set := make(map[string]struct{})
	for obj := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return nil, fmt.Errorf("s3 artifact: list under %q: %w", prefix, obj.Err)
		}
		// rel is "{filename}/v{n}"; the filename itself may contain "/".
		rel := strings.TrimPrefix(obj.Key, prefix)
		idx := strings.LastIndex(rel, "/")
		if idx <= 0 {
			continue
		}
		if _, ok := parseArtifactVersion(rel[idx+1:]); !ok {
			continue
		}
		set[rel[:idx]] = struct{}{}
	}

	names := make([]string, 0, len(set))
	for name := range set {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// DeleteArtifact implements artifact.Service: all revisions of the file are
// removed; deleting a missing artifact is a no-op.
func (s *S3ArtifactService) DeleteArtifact(ctx context.Context, sessionInfo artifact.SessionInfo, filename string) error {
	versions, err := s.ListVersions(ctx, sessionInfo, filename)
	if err != nil {
		return err
	}
	for _, v := range versions {
		key := s.objectKey(sessionInfo, filename, v)
		if err := s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{}); err != nil {
			return fmt.Errorf("s3 artifact: delete %q: %w", key, err)
		}
	}
	return nil
}

// ListVersions implements artifact.Service: all existing revisions of the
// file, ascending.
func (s *S3ArtifactService) ListVersions(ctx context.Context, sessionInfo artifact.SessionInfo, filename string) ([]int, error) {
	if err := validateArtifactSession(sessionInfo); err != nil {
		return nil, err
	}
	if err := validateArtifactFilename(filename); err != nil {
		return nil, err
	}

	prefix := s.sessionPrefix(sessionInfo) + filename + "/"
	set := make(map[int]struct{})
	for obj := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return nil, fmt.Errorf("s3 artifact: list under %q: %w", prefix, obj.Err)
		}
		seg := strings.TrimPrefix(obj.Key, prefix)
		if strings.Contains(seg, "/") {
			continue // not a direct version object
		}
		if n, ok := parseArtifactVersion(seg); ok {
			set[n] = struct{}{}
		}
	}

	versions := make([]int, 0, len(set))
	for n := range set {
		versions = append(versions, n)
	}
	sort.Ints(versions)
	return versions, nil
}

// sessionPrefix returns the key prefix all artifacts of the session live under.
func (s *S3ArtifactService) sessionPrefix(info artifact.SessionInfo) string {
	return s.prefix + info.AppName + "/" + info.UserID + "/" + info.SessionID + "/"
}

// objectKey returns the key of one artifact revision.
func (s *S3ArtifactService) objectKey(info artifact.SessionInfo, filename string, version int) string {
	return s.sessionPrefix(info) + filename + "/v" + strconv.Itoa(version)
}

// parseArtifactVersion extracts n from a "v{n}" key segment.
func parseArtifactVersion(seg string) (int, bool) {
	if !strings.HasPrefix(seg, "v") {
		return 0, false
	}
	n, err := strconv.Atoi(seg[1:])
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

func validateArtifactSession(info artifact.SessionInfo) error {
	if info.AppName == "" || info.UserID == "" || info.SessionID == "" {
		return errors.New("s3 artifact: AppName, UserID and SessionID are required")
	}
	return nil
}

func validateArtifactFilename(filename string) error {
	if strings.TrimSpace(filename) == "" || strings.Contains(filename, "\x00") {
		return fmt.Errorf("s3 artifact: invalid filename %q", filename)
	}
	return nil
}

// SaveMedia implements channels.MediaStore: IM media fetched by channel
// adapters lands under a media-scoped session tree and the reference handed
// to the message pipeline is a compact "s3://bucket/key" string (the agent
// only ever sees the reference).
func (s *S3ArtifactService) SaveMedia(ctx context.Context, channel, msgID, filename, mimeType string, data []byte) (string, error) {
	info := artifact.SessionInfo{AppName: "inbound-media", UserID: channel, SessionID: msgID}
	ver, err := s.SaveArtifact(ctx, info, filename, &artifact.Artifact{Data: data, MimeType: mimeType})
	if err != nil {
		return "", err
	}
	return "s3://" + s.bucket + "/" + s.objectKey(info, filename, ver), nil
}
