// Package artifact is the file store for things a tool produced during an
// execution.
//
// The lifecycle the approved plan requires — "工具产生的文件在最终 execution
// 提交前不可对用户下载" — is enforced by where the state flip happens: a
// tool writes bytes and a 'staged' row, and only the execution's own commit
// transaction promotes staged → ready. A run that fails, blocks or is
// fenced away leaves its artifacts staged, which is exactly "not visible",
// with no second garbage collector needed to say so.
package artifact

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/minio"
)

// ErrNotFound covers "no such artifact", "not this tenant", and "not ready
// yet" with one answer on purpose: a caller who cannot download must not be
// able to tell a staged artifact from a cross-tenant probe.
var ErrNotFound = errors.New("artifact: not found or not available")

// MaxArtifactBytes bounds one artifact; the object store adapter enforces
// the same ceiling.
const MaxArtifactBytes = 8 << 20

// Service stores artifacts in the object store with their metadata in SQL.
type Service struct {
	db      *controlplane.DB
	objects *minio.Client
}

// New wires the service.
func New(db *controlplane.DB, objects *minio.Client) (*Service, error) {
	if db == nil || objects == nil {
		return nil, errors.New("artifact: db and object store are required")
	}
	return &Service{db: db, objects: objects}, nil
}

// Artifact is the SQL row as callers see it.
type Artifact struct {
	PublicID  string
	Name      string
	MIME      string
	SizeBytes int64
	Status    string
}

// SaveStaged writes the bytes and the staged row. It deliberately does not
// need a transaction of the caller's: a staged row is worthless until the
// commit promotes it, so "the row without the commit" is a safe state by
// construction.
func (s *Service) SaveStaged(ctx context.Context, tenantID, executionID string, sessionPK int64, publicID, name, mime string, data []byte) (Artifact, error) {
	if publicID == "" {
		return Artifact{}, fmt.Errorf("artifact: a public id is required")
	}
	if len(data) == 0 {
		return Artifact{}, fmt.Errorf("artifact: refusing an empty artifact")
	}
	if len(data) > MaxArtifactBytes {
		return Artifact{}, fmt.Errorf("artifact: %d bytes exceeds the %d limit", len(data), MaxArtifactBytes)
	}
	if mime == "" {
		mime = "application/octet-stream"
	}
	key := objectKeyFor(tenantID, publicID)
	if err := s.objects.Put(ctx, key, data, mime); err != nil {
		return Artifact{}, err
	}
	sum := sha256.Sum256(data)
	scope, err := s.db.Scope(tenantID)
	if err != nil {
		return Artifact{}, err
	}
	if _, err := scope.Exec(ctx, `
		INSERT INTO artifacts
			(tenant_id, execution_id, session_pk, public_id, name, mime, size_bytes, content_sha256, object_key, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'staged')`,
		tenantID, executionID, sessionPK, publicID, name, mime, len(data), hex.EncodeToString(sum[:]), key); err != nil {
		return Artifact{}, fmt.Errorf("artifact: insert: %w", err)
	}
	return Artifact{PublicID: publicID, Name: name, MIME: mime, SizeBytes: int64(len(data)), Status: "staged"}, nil
}

// PromoteExecution flips every staged artifact of one execution to ready. It
// is called inside the execution's commit transaction, so "the reply went
// out" and "the file is downloadable" are the same fact.
func PromoteExecution(ctx context.Context, tx *controlplane.TxScope, tenantID, executionID string) error {
	if _, err := tx.Exec(ctx, `
		UPDATE artifacts SET status = 'ready'
		WHERE tenant_id = ? AND execution_id = ? AND status = 'staged'`,
		tenantID, executionID); err != nil {
		return fmt.Errorf("artifact: promote execution artifacts: %w", err)
	}
	return nil
}

// AbandonExecution marks staged artifacts of an execution failed — the path
// a blocked or failed run leaves behind. The bytes stay in the object store
// until an operator cleanup; the SQL row is what a download checks.
func AbandonExecution(ctx context.Context, tx *controlplane.TxScope, tenantID, executionID string) error {
	if _, err := tx.Exec(ctx, `
		UPDATE artifacts SET status = 'failed'
		WHERE tenant_id = ? AND execution_id = ? AND status = 'staged'`,
		tenantID, executionID); err != nil {
		return fmt.Errorf("artifact: abandon staged artifacts: %w", err)
	}
	return nil
}

// Open reads one artifact through the per-request authorization the plan
// requires: tenant scope, status ready, then bytes. There is no presigned
// URL anywhere in this path, because a URL is an authorization that outlives
// the check that made it.
func (s *Service) Open(ctx context.Context, tenantID, publicID string) (Artifact, []byte, error) {
	scope, err := s.db.Scope(tenantID)
	if err != nil {
		return Artifact{}, nil, err
	}
	var (
		a   Artifact
		key string
	)
	row, err := scope.QueryRow(ctx, `
		SELECT public_id, name, mime, size_bytes, status, object_key, execution_id
		FROM artifacts WHERE tenant_id = ? AND public_id = ?`, tenantID, publicID)
	if err != nil {
		return Artifact{}, nil, err
	}
	var execID string
	switch err := row.Scan(&a.PublicID, &a.Name, &a.MIME, &a.SizeBytes, &a.Status, &key, &execID); {
	case errors.Is(err, sql.ErrNoRows):
		return Artifact{}, nil, ErrNotFound
	case err != nil:
		return Artifact{}, nil, fmt.Errorf("artifact: load: %w", err)
	}
	if a.Status != "ready" {
		// A staged artifact belongs to an execution that has not committed;
		// saying so would leak the existence of in-flight work.
		return Artifact{}, nil, ErrNotFound
	}
	data, err := s.objects.Get(ctx, key)
	if err != nil {
		if errors.Is(err, minio.ErrNotFound) {
			return Artifact{}, nil, ErrNotFound
		}
		return Artifact{}, nil, err
	}
	return a, data, nil
}

// objectKeyFor is deterministic per artifact; the public id is a UUID drawn
// by the caller, so two artifacts cannot collide on a key.
func objectKeyFor(tenantID, publicID string) string {
	return fmt.Sprintf("%s/artifacts/%s", sanitizeSegment(tenantID), publicID)
}

func sanitizeSegment(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			out = append(out, r)
		default:
			out = append(out, '-')
		}
	}
	return string(out)
}
