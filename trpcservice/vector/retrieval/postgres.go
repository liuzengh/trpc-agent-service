package retrieval

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
)

const defaultHydratorTimeout = 5 * time.Second

// HydratorConfig is the server-owned hydration reader configuration. Zero
// values fail closed.
type HydratorConfig struct {
	QueryTimeout time.Duration
	BatchLimit   int
}

// PostgresHydrator reads authoritative memory facts for retrieval hydration.
// Queries are tenant-scoped by the trusted context, batched by server-owned
// bounds, and only read the columns this phase needs: no vector_ref, no
// session identifiers, no unrelated business fields.
type PostgresHydrator struct {
	pool   *pgxpool.Pool
	config HydratorConfig
}

// NewPostgresHydrator fails closed without a pool or with out-of-bounds
// configuration.
func NewPostgresHydrator(pool *pgxpool.Pool, config HydratorConfig) (*PostgresHydrator, error) {
	if pool == nil {
		return nil, ErrInvalidConfig
	}
	if config.QueryTimeout == 0 {
		config.QueryTimeout = defaultHydratorTimeout
	}
	if config.QueryTimeout < time.Second || config.QueryTimeout > time.Minute {
		return nil, ErrInvalidConfig
	}
	if config.BatchLimit == 0 {
		config.BatchLimit = 64
	}
	if config.BatchLimit < 1 || config.BatchLimit > 128 {
		return nil, ErrInvalidConfig
	}
	return &PostgresHydrator{pool: pool, config: config}, nil
}

// Hydrate loads memory facts for the requested source identifiers inside the
// trusted tenant. Unknown identifiers are simply absent from the result; the
// caller filters such candidates. PostgreSQL failures surface as
// ErrUnavailable without raw database error text.
func (h *PostgresHydrator) Hydrate(ctx context.Context, tc tenant.TenantContext, sourceIDs []string) (map[string]Fact, error) {
	if h == nil || h.pool == nil {
		return nil, ErrUnavailable
	}
	if ctx == nil {
		return nil, ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := tc.Validate(); err != nil {
		return nil, vector.ErrInvalidTenant
	}
	if len(sourceIDs) == 0 {
		return map[string]Fact{}, nil
	}
	if len(sourceIDs) > h.config.BatchLimit {
		return nil, ErrUnavailable
	}
	queryCtx, cancel := context.WithTimeout(ctx, h.config.QueryTimeout)
	defer cancel()
	rows, err := h.pool.Query(queryCtx, `SELECT memory_id, scope, content, version, source_seq, deleted
        FROM memory WHERE tenant_id = $1 AND memory_id = ANY($2)`, tc.TenantID, sourceIDs)
	if err != nil {
		return nil, mapHydrationError(err)
	}
	defer rows.Close()
	facts := make(map[string]Fact, len(sourceIDs))
	for rows.Next() {
		var fact Fact
		if err := rows.Scan(&fact.MemoryID, &fact.Scope, &fact.Content, &fact.SourceVersion, &fact.SourceSequence, &fact.Deleted); err != nil {
			return nil, ErrUnavailable
		}
		facts[fact.MemoryID] = fact
	}
	if err := rows.Err(); err != nil {
		return nil, mapHydrationError(err)
	}
	return facts, nil
}

func mapHydrationError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return ErrUnavailable
}
