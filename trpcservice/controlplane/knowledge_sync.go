package controlplane

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"
)

type KnowledgeIntent struct {
	RevisionID string          `json:"revision_id"`
	Document   json.RawMessage `json:"document"`
	Deleted    bool            `json:"deleted"`
}
type KnowledgeProgress struct {
	Epoch            int64  `json:"epoch"`
	Cursor           string `json:"cursor"`
	Backfilled       bool   `json:"backfilled"`
	Passed           bool   `json:"passed"`
	Chunks           int    `json:"chunks"`
	VerifyCursor     string `json:"verify_cursor"`
	VerifiedChunks   int    `json:"verified_chunks"`
	RevisionChecksum string `json:"revision_checksum"`
}
type KnowledgeSync struct {
	Epoch      int64                        `json:"epoch"`
	Intents    map[string]KnowledgeIntent   `json:"intents"`
	Migrations map[string]KnowledgeProgress `json:"migrations"`
}

func emptyKnowledgeSync() KnowledgeSync {
	return KnowledgeSync{Intents: map[string]KnowledgeIntent{}, Migrations: map[string]KnowledgeProgress{}}
}

type KnowledgeSyncRepository interface {
	WithKnowledgeSync(context.Context, string, string, func(context.Context, *KnowledgeSync, func() error) error) error
}

type KnowledgeMigrationStatus struct {
	Epoch         int64             `json:"epoch"`
	PendingWrites int               `json:"pending_writes"`
	Progress      KnowledgeProgress `json:"progress"`
}

func knowledgeStatus(raw []byte, id string) (KnowledgeMigrationStatus, error) {
	s := emptyKnowledgeSync()
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &s); err != nil {
			return KnowledgeMigrationStatus{}, err
		}
	}
	p := s.Migrations[id]
	if p.Epoch != s.Epoch || len(s.Intents) > 0 {
		p.Passed = false
	}
	return KnowledgeMigrationStatus{Epoch: s.Epoch, PendingWrites: len(s.Intents), Progress: p}, nil
}
func (r *MemoryRepository) GetKnowledgeMigrationStatus(ctx context.Context, t, a, id string) (KnowledgeMigrationStatus, error) {
	if err := r.check(ctx); err != nil {
		return KnowledgeMigrationStatus{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return knowledgeStatus(r.knowledgeStates[scopedKey(t, a)], id)
}
func (r *PostgresRepository) GetKnowledgeMigrationStatus(ctx context.Context, t, a, id string) (KnowledgeMigrationStatus, error) {
	var raw []byte
	err := r.dbFor(ctx).QueryRowContext(ctx, "SELECT state FROM knowledge_sync WHERE tenant_id=$1 AND app_id=$2", t, a).Scan(&raw)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return KnowledgeMigrationStatus{}, err
	}
	return knowledgeStatus(raw, id)
}

type scopedKnowledgeConnection struct {
	database   *sql.DB
	connection *sql.Conn
}
type knowledgeConnectionKey struct{}
type sqlExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func (r *PostgresRepository) dbFor(ctx context.Context) sqlExecutor {
	if scoped, ok := ctx.Value(knowledgeConnectionKey{}).(scopedKnowledgeConnection); ok && scoped.database == r.db {
		return scoped.connection
	}
	return r.db
}

func KnowledgeLockName(tenantID, appID string) string { return "knowledge:" + tenantID + "/" + appID }

// The callback saves before contacting the vector backend and again after it
// succeeds. A crash between those saves leaves a durable repair intent.
func (r *PostgresRepository) WithKnowledgeSync(ctx context.Context, t, a string, fn func(context.Context, *KnowledgeSync, func() error) error) error {
	conn, err := r.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(conn)
	if _, err = conn.ExecContext(ctx, "SELECT pg_advisory_lock(hashtextextended($1,0))", KnowledgeLockName(t, a)); err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		var released bool
		if err := conn.QueryRowContext(cleanup, "SELECT pg_advisory_unlock(hashtextextended($1,0))", KnowledgeLockName(t, a)).Scan(&released); err != nil || !released {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
	}()
	state := emptyKnowledgeSync()
	var raw []byte
	err = conn.QueryRowContext(ctx, "SELECT state FROM knowledge_sync WHERE tenant_id=$1 AND app_id=$2", t, a).Scan(&raw)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil {
		if err = json.Unmarshal(raw, &state); err != nil {
			return err
		}
	}
	save := func() error {
		raw, err := json.Marshal(state)
		if err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `INSERT INTO knowledge_sync(tenant_id,app_id,state) VALUES($1,$2,$3::jsonb) ON CONFLICT(tenant_id,app_id) DO UPDATE SET state=EXCLUDED.state,updated_at=now()`, t, a, string(raw))
		return err
	}
	return fn(context.WithValue(ctx, knowledgeConnectionKey{}, scopedKnowledgeConnection{r.db, conn}), &state, save)
}

func (r *MemoryRepository) WithKnowledgeSync(ctx context.Context, t, a string, fn func(context.Context, *KnowledgeSync, func() error) error) error {
	// InMemory is intentionally single-process; all routers sharing this repo
	// still use the same cancelable lock.
	select {
	case r.knowledgeLock <- struct{}{}:
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	defer func() { <-r.knowledgeLock }()
	r.mu.RLock()
	raw := append([]byte(nil), r.knowledgeStates[scopedKey(t, a)]...)
	r.mu.RUnlock()
	state := emptyKnowledgeSync()
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &state); err != nil {
			return err
		}
	}
	save := func() error {
		raw, err := json.Marshal(state)
		if err != nil {
			return err
		}
		r.mu.Lock()
		r.knowledgeStates[scopedKey(t, a)] = raw
		r.mu.Unlock()
		return nil
	}
	return fn(ctx, &state, save)
}

func validKnowledgeProof(raw []byte, m BackendMigration) bool {
	var s KnowledgeSync
	if json.Unmarshal(raw, &s) != nil || len(s.Intents) != 0 {
		return false
	}
	p, ok := s.Migrations[m.ID]
	return ok && p.Passed && p.Backfilled && p.Epoch == s.Epoch && p.RevisionChecksum != ""
}
