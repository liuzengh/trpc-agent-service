package task

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	tenantctx "github.com/liuzengh/trpc-agent-service/trpcservice/storage/tenantctx"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
)

const defaultSourceProjectorTimeout = 5 * time.Second

// SourceProjectorConfig is the server-owned projection configuration of the
// production source projector. Zero or out-of-bounds values fail closed.
type SourceProjectorConfig struct {
	Model         string
	ModelVersion  string
	Dimension     int
	SchemaVersion string
	QueryTimeout  time.Duration
}

// PostgresSourceProjector loads the authoritative Memory fact for a durable
// vector task inside the worker process. It is tenant-scoped by the task row,
// only serves allowlisted source types, and never reads memory.vector_ref.
// The returned SourceDocument is the P1-06A projection input; it is verified
// against the task by verifyLoadedSource before any backend call.
type PostgresSourceProjector struct {
	pool   *pgxpool.Pool
	config SourceProjectorConfig
}

// NewPostgresSourceProjector fails closed without a pool or with an
// incomplete configuration.
func NewPostgresSourceProjector(pool *pgxpool.Pool, config SourceProjectorConfig) (*PostgresSourceProjector, error) {
	if pool == nil {
		return nil, ErrInvalidTask
	}
	if !validSourceComponent(config.Model, 256) || !validSourceComponent(config.ModelVersion, 128) || !validSourceComponent(config.SchemaVersion, 128) {
		return nil, ErrInvalidTask
	}
	if config.Dimension < 1 || config.Dimension > vector.MaxVectorDimension {
		return nil, ErrInvalidTask
	}
	if config.QueryTimeout == 0 {
		config.QueryTimeout = defaultSourceProjectorTimeout
	}
	if config.QueryTimeout < time.Second || config.QueryTimeout > time.Minute {
		return nil, ErrInvalidTask
	}
	return &PostgresSourceProjector{pool: pool, config: config}, nil
}

func validSourceComponent(value string, max int) bool {
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

// Load reads the authoritative Memory row for the task. Stale versions,
// hash drift, deleted/active mismatches and unknown source types surface as
// category-only vector errors so the worker state machine classifies them
// without raw SQL or DSN leakage.
func (p *PostgresSourceProjector) Load(ctx context.Context, tc tenant.TenantContext, task Task) (vector.SourceDocument, error) {
	if p == nil || p.pool == nil {
		return vector.SourceDocument{}, ErrUnavailable
	}
	if ctx == nil {
		return vector.SourceDocument{}, ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return vector.SourceDocument{}, err
	}
	if err := tc.Validate(); err != nil {
		return vector.SourceDocument{}, vector.ErrInvalidTenant
	}
	if tc.TenantID != task.TenantID {
		return vector.SourceDocument{}, vector.ErrInvalidTenant
	}
	if task.SourceType != vector.SourceTypeMemory {
		return vector.SourceDocument{}, vector.ErrInvalidDocument
	}
	queryCtx, cancel := context.WithTimeout(ctx, p.config.QueryTimeout)
	defer cancel()
	var content string
	var deleted bool
	var version, sequence int64
	var scope string
	var err error
	err = tenantctx.WithTenantContext(queryCtx, p.pool, tc.TenantID, "vector source load", func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT content, deleted, version, source_seq, scope
        FROM memory WHERE tenant_id = $1 AND memory_id = $2`, task.TenantID, task.SourceID).Scan(&content, &deleted, &version, &sequence, &scope)
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, storage.ErrNotFound) {
			return vector.SourceDocument{}, vector.ErrNotFound
		}
		return vector.SourceDocument{}, ErrUnavailable
	}
	if "memory:"+scope != task.ProjectionScope {
		return vector.SourceDocument{}, vector.ErrInvalidDocument
	}
	if version != task.SourceVersion || sequence != task.SourceSequence {
		return vector.SourceDocument{}, vector.ErrStale
	}
	if task.Operation == vector.OperationUpsert {
		if deleted {
			return vector.SourceDocument{}, vector.ErrStale
		}
	} else {
		if !deleted {
			return vector.SourceDocument{}, vector.ErrStale
		}
		content = ""
	}
	if vector.ContentHash(content) != task.ContentHash {
		return vector.SourceDocument{}, vector.ErrStale
	}
	return vector.SourceDocument{
		SourceType:      vector.SourceTypeMemory,
		SourceID:        task.SourceID,
		ProjectionScope: task.ProjectionScope,
		SourceVersion:   version,
		SourceSequence:  sequence,
		Content:         content,
		Deleted:         deleted,
		Model:           p.config.Model,
		ModelVersion:    p.config.ModelVersion,
		Dimension:       p.config.Dimension,
		SchemaVersion:   p.config.SchemaVersion,
	}, nil
}

var _ SourceProjector = (*PostgresSourceProjector)(nil)
