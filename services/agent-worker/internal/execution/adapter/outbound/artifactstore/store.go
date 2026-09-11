// Package artifactstore implements SDK artifacts with S3 bytes and metadata in
// the existing Worker PostgreSQL database (worker-artifact-metadata-v1).
package artifactstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
)

const MetadataContract = "worker-artifact-metadata-v1"

var (
	ErrIdentity    = errors.New("artifact scope or backend mismatch")
	ErrUnavailable = errors.New("artifact backend unavailable")
	ErrCorrupt     = errors.New("artifact content integrity failure")
	ErrCapacity    = errors.New("artifact exceeds backend capacity")
	ErrClosed      = errors.New("artifact service closed")
)

type Credentials struct{ AccessKeyID, SecretAccessKey string }
type Store struct {
	pool            *pgxpool.Pool
	client          *minio.Client
	transport       *http.Transport
	scope           artifact.SessionInfo
	scopeID, bucket string
	maxBytes        int64
	timeout         time.Duration
	gate            chan struct{}
	closed          atomic.Bool
}

// Open borrows the Worker pool; Close does not close it. Only the exact trusted
// SDK scope supplied here may be used by any service method. No DDL or bucket
// creation occurs. Snapshot validation and credential resolution are distinct.
func Open(ctx context.Context, pool *pgxpool.Pool, backend datav1.Snapshot, secret Credentials, tenantID string, scope artifact.SessionInfo) (*Store, error) {
	if pool == nil || backend.ValidateForRole("artifact") != nil || backend.Kind != datav1.S3 || backend.TenantID != tenantID || !valid(scope.AppName) || !valid(scope.UserID) || !valid(scope.SessionID) || !valid(secret.AccessKeyID) || !valid(secret.SecretAccessKey) {
		return nil, ErrIdentity
	}
	d, err := backend.Digest()
	if err != nil {
		return nil, ErrIdentity
	}
	endpoint, _ := url.Parse(backend.S3.Endpoint)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	transport.MaxConnsPerHost = int(backend.Limits.MaxConcurrency)
	transport.ResponseHeaderTimeout = time.Duration(backend.Limits.TimeoutMS) * time.Millisecond
	lookup := minio.BucketLookupDNS
	if backend.S3.PathStyle {
		lookup = minio.BucketLookupPath
	}
	client, err := minio.New(endpoint.Host, &minio.Options{Creds: credentials.NewStaticV4(secret.AccessKeyID, secret.SecretAccessKey, ""), Secure: endpoint.Scheme == "https", Region: backend.S3.Region, BucketLookup: lookup, Transport: transport, MaxRetries: 1})
	if err != nil {
		transport.CloseIdleConnections()
		return nil, ErrIdentity
	}
	s := &Store{pool: pool, client: client, transport: transport, scope: scope, scopeID: hash(tenantID, d, scope.AppName, scope.UserID, scope.SessionID), bucket: backend.S3.Bucket, maxBytes: backend.Limits.MaxBytes, timeout: time.Duration(backend.Limits.TimeoutMS) * time.Millisecond, gate: make(chan struct{}, int(backend.Limits.MaxConcurrency))}
	op, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	var ready bool
	if err = pool.QueryRow(op, "SELECT to_regclass('worker_artifact_files') IS NOT NULL AND to_regclass('worker_artifact_versions') IS NOT NULL").Scan(&ready); err != nil || !ready {
		s.Close()
		return nil, failure(op, err)
	}
	return s, nil
}
func valid(v string) bool {
	return strings.TrimSpace(v) != "" && utf8.ValidString(v) && !strings.ContainsRune(v, 0)
}
func hash(parts ...string) string {
	body, _ := json.Marshal(parts)
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
func checksum(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }
func failure(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return ErrUnavailable
}
func (s *Store) Close() { s.closed.Store(true); s.transport.CloseIdleConnections() }
func (s *Store) begin(ctx context.Context, scope artifact.SessionInfo) (context.Context, func(), error) {
	if scope != s.scope {
		return nil, nil, ErrIdentity
	}
	if s.closed.Load() {
		return nil, nil, ErrClosed
	}
	op, cancel := context.WithTimeout(ctx, s.timeout)
	select {
	case s.gate <- struct{}{}:
	case <-op.Done():
		cancel()
		return nil, nil, op.Err()
	}
	if s.closed.Load() {
		<-s.gate
		cancel()
		return nil, nil, ErrClosed
	}
	return op, func() { <-s.gate; cancel() }, nil
}
func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
func (s *Store) SaveArtifact(ctx context.Context, scope artifact.SessionInfo, filename string, value *artifact.Artifact) (int, error) {
	op, done, err := s.begin(ctx, scope)
	if err != nil {
		return 0, err
	}
	defer done()
	if !valid(filename) || value == nil || !valid(value.MimeType) || !utf8.ValidString(value.Name) || !utf8.ValidString(value.URL) {
		return 0, ErrIdentity
	}
	if int64(len(value.Data)) > s.maxBytes {
		return 0, ErrCapacity
	}
	data := bytes.Clone(value.Data)
	mime, name, link := value.MimeType, value.Name, value.URL
	id := hash(s.scopeID, filename)
	tx, err := s.pool.Begin(op)
	if err != nil {
		return 0, failure(op, err)
	}
	defer rollback(tx)
	if _, err = tx.Exec(op, "INSERT INTO worker_artifact_files(file_id,scope_id,filename) VALUES($1,$2,$3) ON CONFLICT DO NOTHING", id, s.scopeID, filename); err != nil {
		return 0, failure(op, err)
	}
	var version int64
	if err = tx.QueryRow(op, "SELECT next_version FROM worker_artifact_files WHERE file_id=$1 AND scope_id=$2 AND filename=$3 FOR UPDATE", id, s.scopeID, filename).Scan(&version); err != nil {
		return 0, failure(op, err)
	}
	if version >= math.MaxInt64 || version > int64(math.MaxInt) {
		return 0, ErrCapacity
	}
	random := make([]byte, 16)
	if _, err = rand.Read(random); err != nil {
		return 0, ErrUnavailable
	}
	key := "runtime_artifact/" + s.scopeID + "/" + id + "/" + hex.EncodeToString(random)
	info, err := s.client.PutObject(op, s.bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{ContentType: mime, NumThreads: 1})
	if err != nil {
		return 0, failure(op, err)
	}
	if info.Size != int64(len(data)) {
		return 0, ErrCorrupt
	}
	if _, err = tx.Exec(op, "INSERT INTO worker_artifact_versions(file_id,version,object_key,content_sha256,content_length,mime_type,display_name,artifact_url) VALUES($1,$2,$3,$4,$5,$6,$7,$8)", id, version, key, checksum(data), len(data), mime, name, link); err != nil {
		return 0, failure(op, err)
	}
	if _, err = tx.Exec(op, "UPDATE worker_artifact_files SET next_version=next_version+1 WHERE file_id=$1", id); err != nil {
		return 0, failure(op, err)
	}
	if err = tx.Commit(op); err != nil {
		return 0, failure(op, err)
	}
	return int(version), nil
}
func (s *Store) LoadArtifact(ctx context.Context, scope artifact.SessionInfo, filename string, version *int) (*artifact.Artifact, error) {
	op, done, err := s.begin(ctx, scope)
	if err != nil {
		return nil, err
	}
	defer done()
	if !valid(filename) || (version != nil && *version < 0) {
		return nil, ErrIdentity
	}
	var selected any
	if version != nil {
		selected = int64(*version)
	}
	var key, digest string
	var size int64
	out := &artifact.Artifact{}
	err = s.pool.QueryRow(op, "SELECT object_key,content_sha256,content_length,mime_type,display_name,artifact_url FROM worker_artifact_versions WHERE file_id=$1 AND ($2::bigint IS NULL OR version=$2) ORDER BY version DESC LIMIT 1", hash(s.scopeID, filename), selected).Scan(&key, &digest, &size, &out.MimeType, &out.Name, &out.URL)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, failure(op, err)
	}
	if size < 0 || size > s.maxBytes {
		return nil, ErrCapacity
	}
	if !strings.HasPrefix(key, "runtime_artifact/"+s.scopeID+"/"+hash(s.scopeID, filename)+"/") {
		return nil, ErrCorrupt
	}
	object, err := s.client.GetObject(op, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, failure(op, err)
	}
	defer object.Close()
	data, err := io.ReadAll(io.LimitReader(object, size+1))
	if err != nil {
		return nil, failure(op, err)
	}
	if int64(len(data)) != size || checksum(data) != digest {
		return nil, ErrCorrupt
	}
	out.Data = data
	return out, nil
}

// DeleteArtifact atomically withdraws all visible versions. S3 bytes are retained;
// this is logical deletion, not physical erasure or a cross-store transaction.
// The version sequence is retained, so later saves never revive deleted versions.
func (s *Store) DeleteArtifact(ctx context.Context, scope artifact.SessionInfo, filename string) error {
	op, done, err := s.begin(ctx, scope)
	if err != nil {
		return err
	}
	defer done()
	if !valid(filename) {
		return ErrIdentity
	}
	tx, err := s.pool.Begin(op)
	if err != nil {
		return failure(op, err)
	}
	defer rollback(tx)
	id := hash(s.scopeID, filename)
	var found string
	err = tx.QueryRow(op, "SELECT file_id FROM worker_artifact_files WHERE file_id=$1 FOR UPDATE", id).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return failure(op, err)
	}
	if _, err = tx.Exec(op, "DELETE FROM worker_artifact_versions WHERE file_id=$1", id); err != nil {
		return failure(op, err)
	}
	if err = tx.Commit(op); err != nil {
		return failure(op, err)
	}
	return nil
}
func (s *Store) ListArtifactKeys(ctx context.Context, scope artifact.SessionInfo) ([]string, error) {
	op, done, err := s.begin(ctx, scope)
	if err != nil {
		return nil, err
	}
	defer done()
	rows, err := s.pool.Query(op, "SELECT f.filename FROM worker_artifact_files f WHERE f.scope_id=$1 AND EXISTS(SELECT 1 FROM worker_artifact_versions v WHERE v.file_id=f.file_id) ORDER BY f.filename", s.scopeID)
	if err != nil {
		return nil, failure(op, err)
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return nil, failure(op, err)
		}
		out = append(out, name)
	}
	if err = rows.Err(); err != nil {
		return nil, failure(op, err)
	}
	return out, nil
}
func (s *Store) ListVersions(ctx context.Context, scope artifact.SessionInfo, filename string) ([]int, error) {
	op, done, err := s.begin(ctx, scope)
	if err != nil {
		return nil, err
	}
	defer done()
	if !valid(filename) {
		return nil, ErrIdentity
	}
	rows, err := s.pool.Query(op, "SELECT version FROM worker_artifact_versions WHERE file_id=$1 ORDER BY version", hash(s.scopeID, filename))
	if err != nil {
		return nil, failure(op, err)
	}
	defer rows.Close()
	out := []int{}
	for rows.Next() {
		var version int64
		if err = rows.Scan(&version); err != nil {
			return nil, failure(op, err)
		}
		if version < 0 || version > int64(math.MaxInt) {
			return nil, ErrCorrupt
		}
		out = append(out, int(version))
	}
	if err = rows.Err(); err != nil {
		return nil, failure(op, err)
	}
	return out, nil
}

var _ artifact.Service = (*Store)(nil)
