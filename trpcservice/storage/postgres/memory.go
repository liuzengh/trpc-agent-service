package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/memory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
)

// projectionTaskEnqueuer is the narrow transaction-aware enqueue capability
// the Memory write path needs. It is satisfied by the durable vector task
// repository through a composition-local adapter; this package deliberately
// does not import the vector task package so its internal integration tests
// keep a clean import graph.
type projectionTaskEnqueuer interface {
	EnqueueTx(ctx context.Context, tx pgx.Tx, tc tenant.TenantContext, ref vector.VectorDocumentRef, now time.Time) (bool, error)
}

const defaultMemoryQueryTimeout = 5 * time.Second

// MemoryProjectionConfig is the server-owned projection configuration applied
// to every Memory mutation. Callers can never supply projection identity.
type MemoryProjectionConfig struct {
	Model         string
	ModelVersion  string
	SchemaVersion string
	Dimension     int
	QueryTimeout  time.Duration
}

// MemoryRepository is the production PostgreSQL Memory write path. Every
// mutation commits the authoritative memory fact and its vector projection
// task in the SAME PostgreSQL transaction; embedding, Redis and VectorStore
// calls never happen inside that transaction. The repository implements the
// existing storage.MemoryRepository contract and keeps memory.vector_ref
// untouched (opaque compatibility field).
type MemoryRepository struct {
	pool       *pgxpool.Pool
	tasks      projectionTaskEnqueuer
	projection MemoryProjectionConfig
}

// NewMemoryRepository fails closed without a pool, a task enqueuer or a
// complete projection configuration.
func NewMemoryRepository(pool *pgxpool.Pool, tasks projectionTaskEnqueuer, config MemoryProjectionConfig) (*MemoryRepository, error) {
	if pool == nil || tasks == nil {
		return nil, storage.ErrInvalidArgument
	}
	if !validMemoryComponent(config.Model, 256) || !validMemoryComponent(config.ModelVersion, 128) || !validMemoryComponent(config.SchemaVersion, 128) {
		return nil, storage.ErrInvalidArgument
	}
	if config.Dimension < 1 || config.Dimension > vector.MaxVectorDimension {
		return nil, storage.ErrInvalidArgument
	}
	if config.QueryTimeout == 0 {
		config.QueryTimeout = defaultMemoryQueryTimeout
	}
	if config.QueryTimeout < time.Second || config.QueryTimeout > time.Minute {
		return nil, storage.ErrInvalidArgument
	}
	return &MemoryRepository{pool: pool, tasks: tasks, projection: config}, nil
}

func validMemoryComponent(value string, max int) bool {
	if value == "" || len(value) > max {
		return false
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func memoryError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return storage.ErrBackendUnavailable
}

func memoryQueryContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc, error) {
	if ctx == nil {
		return nil, nil, storage.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	return queryCtx, cancel, nil
}

// Put commits the authoritative Memory fact and its upsert projection task in
// one transaction. Version and source sequence are server-owned monotonic
// values derived from the current row under a row lock; caller supplied
// version fields are ignored.
func (r *MemoryRepository) Put(ctx context.Context, tc tenant.TenantContext, value memory.Memory) error {
	if err := tc.Validate(); err != nil {
		return storage.ErrTenantMismatch
	}
	if value.TenantID != tc.TenantID {
		return storage.ErrTenantMismatch
	}
	normalized := value
	normalized.TenantID = tc.TenantID
	normalized.Version = 1
	normalized.SourceSeq = 0
	normalized.Deleted = false
	if err := normalized.Validate(); err != nil {
		return storage.ErrInvalidArgument
	}
	if len(normalized.Content) > vector.MaxContentBytes {
		return storage.ErrInvalidArgument
	}
	queryCtx, cancel, err := memoryQueryContext(ctx, r.projection.QueryTimeout)
	if err != nil {
		return err
	}
	defer cancel()
	tx, err := r.pool.Begin(queryCtx)
	if err != nil {
		return memoryError(err)
	}
	defer func() { _ = tx.Rollback(queryCtx) }()
	if _, err = tx.Exec(queryCtx, `SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='15s'`); err != nil {
		return memoryError(err)
	}
	if err = SetTenantContext(queryCtx, tx, tc.TenantID); err != nil {
		return memoryError(err)
	}
	var currentVersion, currentSequence int64
	var currentDeleted bool
	err = tx.QueryRow(queryCtx, `SELECT version, source_seq, deleted FROM memory WHERE tenant_id=$1 AND memory_id=$2 FOR UPDATE`,
		tc.TenantID, normalized.ID).Scan(&currentVersion, &currentSequence, &currentDeleted)
	nextVersion := int64(1)
	nextSequence := int64(0)
	switch {
	case err == pgx.ErrNoRows:
	case err != nil:
		return memoryError(err)
	default:
		nextVersion, nextSequence = currentVersion+1, currentSequence+1
	}
	var sessionArg interface{}
	if normalized.Scope == memory.ScopeSession {
		sessionArg = normalized.SessionID
	}
	if _, err = tx.Exec(queryCtx, `INSERT INTO memory
        (tenant_id, memory_id, scope, scope_id, session_id, kind, content, version, source_seq, deleted, updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,false,now())
        ON CONFLICT (tenant_id, memory_id) DO UPDATE
        SET content=EXCLUDED.content, kind=EXCLUDED.kind, scope=EXCLUDED.scope, scope_id=EXCLUDED.scope_id,
            session_id=EXCLUDED.session_id, version=EXCLUDED.version, source_seq=EXCLUDED.source_seq,
            deleted=false, updated_at=now()`,
		tc.TenantID, normalized.ID, string(normalized.Scope), normalized.ScopeID, sessionArg,
		normalized.Kind, normalized.Content, nextVersion, nextSequence); err != nil {
		return memoryError(err)
	}
	source := vector.SourceDocument{
		SourceType:      vector.SourceTypeMemory,
		SourceID:        normalized.ID,
		ProjectionScope: "memory:" + string(normalized.Scope),
		SourceVersion:   nextVersion,
		SourceSequence:  nextSequence,
		Content:         normalized.Content,
		Deleted:         false,
		Model:           r.projection.Model,
		ModelVersion:    r.projection.ModelVersion,
		Dimension:       r.projection.Dimension,
		SchemaVersion:   r.projection.SchemaVersion,
	}
	ref, err := vector.BuildDocumentRef(tenant.WithContext(queryCtx, tc), source)
	if err != nil {
		return memoryError(err)
	}
	if _, err = r.tasks.EnqueueTx(queryCtx, tx, tc, ref, time.Now().UTC()); err != nil {
		return memoryError(err)
	}
	if err = tx.Commit(queryCtx); err != nil {
		return memoryError(err)
	}
	return nil
}

// Delete tombstones the authoritative Memory fact and enqueues the delete
// projection task in the same transaction, keeping the stable document
// identity. Deleting an already tombstoned row is an idempotent no-op and
// never creates a new task.
func (r *MemoryRepository) Delete(ctx context.Context, tc tenant.TenantContext, memoryID string) error {
	if err := tc.Validate(); err != nil {
		return storage.ErrTenantMismatch
	}
	if memoryID == "" {
		return storage.ErrInvalidArgument
	}
	queryCtx, cancel, err := memoryQueryContext(ctx, r.projection.QueryTimeout)
	if err != nil {
		return err
	}
	defer cancel()
	tx, err := r.pool.Begin(queryCtx)
	if err != nil {
		return memoryError(err)
	}
	defer func() { _ = tx.Rollback(queryCtx) }()
	if _, err = tx.Exec(queryCtx, `SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='15s'`); err != nil {
		return memoryError(err)
	}
	if err = SetTenantContext(queryCtx, tx, tc.TenantID); err != nil {
		return memoryError(err)
	}
	var currentVersion, currentSequence int64
	var scope string
	var currentDeleted bool
	err = tx.QueryRow(queryCtx, `SELECT version, source_seq, scope, deleted FROM memory WHERE tenant_id=$1 AND memory_id=$2 FOR UPDATE`,
		tc.TenantID, memoryID).Scan(&currentVersion, &currentSequence, &scope, &currentDeleted)
	if err == pgx.ErrNoRows {
		return storage.ErrNotFound
	}
	if err != nil {
		return memoryError(err)
	}
	if currentDeleted {
		return nil
	}
	nextVersion, nextSequence := currentVersion+1, currentSequence+1
	if _, err = tx.Exec(queryCtx, `UPDATE memory SET deleted=true, content='', version=$3, source_seq=$4, updated_at=now()
        WHERE tenant_id=$1 AND memory_id=$2`, tc.TenantID, memoryID, nextVersion, nextSequence); err != nil {
		return memoryError(err)
	}
	source := vector.SourceDocument{
		SourceType:      vector.SourceTypeMemory,
		SourceID:        memoryID,
		ProjectionScope: "memory:" + scope,
		SourceVersion:   nextVersion,
		SourceSequence:  nextSequence,
		Content:         "",
		Deleted:         true,
		Model:           r.projection.Model,
		ModelVersion:    r.projection.ModelVersion,
		Dimension:       r.projection.Dimension,
		SchemaVersion:   r.projection.SchemaVersion,
	}
	ref, err := vector.BuildDocumentRef(tenant.WithContext(queryCtx, tc), source)
	if err != nil {
		return memoryError(err)
	}
	if _, err = r.tasks.EnqueueTx(queryCtx, tx, tc, ref, time.Now().UTC()); err != nil {
		return memoryError(err)
	}
	if err = tx.Commit(queryCtx); err != nil {
		return memoryError(err)
	}
	return nil
}

// Get returns the tenant-scoped authoritative Memory fact.
func (r *MemoryRepository) Get(ctx context.Context, tc tenant.TenantContext, memoryID string) (memory.Memory, error) {
	if err := tc.Validate(); err != nil {
		return memory.Memory{}, storage.ErrTenantMismatch
	}
	if memoryID == "" {
		return memory.Memory{}, storage.ErrInvalidArgument
	}
	queryCtx, cancel, err := memoryQueryContext(ctx, r.projection.QueryTimeout)
	if err != nil {
		return memory.Memory{}, err
	}
	defer cancel()
	var value memory.Memory
	err = WithTenantContext(queryCtx, r.pool, tc.TenantID, "memory get", func(ctx context.Context, tx pgx.Tx) error {
		var scanErr error
		value, scanErr = scanMemory(tx.QueryRow(ctx, `SELECT tenant_id, memory_id, scope, scope_id, session_id, kind, content, version, source_seq, deleted, created_at, updated_at
        FROM memory WHERE tenant_id=$1 AND memory_id=$2`, tc.TenantID, memoryID))
		return scanErr
	})
	if err != nil {
		return memory.Memory{}, err
	}
	return value, nil
}

// Search is the bounded tenant-scoped read of active memories. An empty query
// returns the most recently updated active rows; a non-empty query filters by
// exact content, mirroring the existing storage.MemoryRepository semantics.
func (r *MemoryRepository) Search(ctx context.Context, tc tenant.TenantContext, query string, limit int) ([]memory.Memory, error) {
	if err := tc.Validate(); err != nil {
		return nil, storage.ErrTenantMismatch
	}
	if limit < 1 || limit > 100 {
		return nil, storage.ErrInvalidArgument
	}
	queryCtx, cancel, err := memoryQueryContext(ctx, r.projection.QueryTimeout)
	if err != nil {
		return nil, err
	}
	defer cancel()
	result := make([]memory.Memory, 0, limit)
	err = WithTenantContext(queryCtx, r.pool, tc.TenantID, "memory search", func(ctx context.Context, tx pgx.Tx) error {
		var rows pgx.Rows
		var queryErr error
		if query == "" {
			rows, queryErr = tx.Query(ctx, `SELECT tenant_id, memory_id, scope, scope_id, session_id, kind, content, version, source_seq, deleted, created_at, updated_at
        FROM memory WHERE tenant_id=$1 AND deleted=false ORDER BY updated_at DESC LIMIT $2`, tc.TenantID, limit)
		} else {
			rows, queryErr = tx.Query(ctx, `SELECT tenant_id, memory_id, scope, scope_id, session_id, kind, content, version, source_seq, deleted, created_at, updated_at
        FROM memory WHERE tenant_id=$1 AND deleted=false AND content=$2 ORDER BY updated_at DESC LIMIT $3`, tc.TenantID, query, limit)
		}
		if queryErr != nil {
			return queryErr
		}
		defer rows.Close()
		for rows.Next() {
			value, scanErr := scanMemory(rows)
			if scanErr != nil {
				return scanErr
			}
			result = append(result, value)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

type rowScanner interface{ Scan(dest ...any) error }

func scanMemory(row rowScanner) (memory.Memory, error) {
	var value memory.Memory
	var scope, kind string
	var sessionID *string
	err := row.Scan(&value.TenantID, &value.ID, &scope, &value.ScopeID, &sessionID, &kind,
		&value.Content, &value.Version, &value.SourceSeq, &value.Deleted, &value.CreatedAt, &value.UpdatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return memory.Memory{}, storage.ErrNotFound
		}
		return memory.Memory{}, memoryError(err)
	}
	value.Scope = memory.Scope(scope)
	value.Kind = kind
	if sessionID != nil {
		value.SessionID = *sessionID
	}
	return value, nil
}

var _ storage.MemoryRepository = (*MemoryRepository)(nil)
