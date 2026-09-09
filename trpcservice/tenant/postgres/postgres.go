// Package postgres stores the multi-tenant control plane — tenants, agent apps
// and agent revisions — in PostgreSQL.
//
// It is a second implementation of tenant.Repository, held to the same
// contract as the in-memory reference by the shared suite in
// tenant/tenanttest. A behaviour difference between the two is a bug in one of
// them, not a property of the backend.
//
// # Ownership
//
// A Repository borrows the *pgxpool.Pool it is handed. It never closes the
// pool, holds nothing else that needs closing, and is safe for concurrent use,
// so one pool can back several repositories and the caller closes it once,
// after the last user has stopped. Migrate borrows a pool the same way.
//
// # Migrations are explicit
//
// New performs no DDL and does not reach the database at all. A process
// starting against an unmigrated database has to call Migrate itself. Keeping
// the two apart is what lets a deployment migrate from one place — a job, a
// leader, an operator — while every other process only reads and writes.
//
// # Schema placement
//
// Every statement names its tables unqualified, so Migrate and Repository both
// act on the first schema of the connection's search_path. Point a pool at a
// schema once on the pool config rather than per query:
//
//	config, err := pgxpool.ParseConfig(dsn)
//	config.ConnConfig.RuntimeParams["search_path"] = "control_plane"
//
// # Errors
//
// There are three kinds, and which one a failure belongs to decides the status
// the admin API answers with.
//
// A caller's mistake comes back as one of the tenant package sentinels
// (tenant.ErrNotFound, tenant.ErrAlreadyExists, ...) carrying a message built
// only from values that caller supplied. The admin API echoes that message and
// answers 4xx, which is correct precisely because the caller can act on it.
//
// A fault in the stored data comes back as tenant.ErrConfigIntegrity. It is a
// tenant sentinel because both implementations have to report it the same way,
// but it is not a caller mistake: the request was well formed and no edit to it
// would help. The admin API has no case for it, so it falls through to a 500.
//
// Everything else — a dropped connection, a missing table, a constraint this
// package does not recognise — comes back under ErrStorage, wrapping the driver
// error, and is likewise answered with a generic 500.
//
// The last split is load bearing for more than the status code: the admin API
// echoes the text of a matched domain error, so a raw driver error spliced into
// one would put SQL text, column names and conflicting key values into an HTTP
// response body.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// ErrInvalidConfig reports a Repository or Migrate that was wired wrong. It is
// separate from ErrStorage because it can never be caused by the database.
var ErrInvalidConfig = errors.New("tenant/postgres: invalid configuration")

// ErrStorage is the sentinel behind every failure that is the database's or
// this package's fault rather than the caller's. It deliberately matches none
// of the tenant sentinels, so a caller mapping errors to HTTP statuses cannot
// mistake an unreachable database for a 404 or a 409.
var ErrStorage = errors.New("tenant/postgres: control plane storage failure")

// SQLSTATE classes this package maps to domain errors. Everything else stays
// an ErrStorage: guessing at an unrecognised code is how an infrastructure
// fault turns into a misleading 409.
const (
	uniqueViolation     = "23505"
	foreignKeyViolation = "23503"
)

// Constraint names. Each one is declared explicitly in the migration rather
// than left to PostgreSQL's default naming, because these strings are the only
// thing that tells a unique violation on the id apart from one on the slug.
const (
	constraintTenantsPrimaryKey   = "tenants_pkey"
	constraintTenantsSlug         = "tenants_slug_key"
	constraintAgentAppsPrimaryKey = "agent_apps_pkey"
	constraintAgentAppsTenant     = "agent_apps_tenant_fkey"
	constraintRevisionsPrimaryKey = "agent_revisions_pkey"
	constraintRevisionsRevisionNo = "agent_revisions_revision_no_key"
	constraintRevisionsAgentApp   = "agent_revisions_agent_app_fkey"
	constraintProfilesPrimaryKey  = "backend_profiles_pkey"
	constraintProfilesTenant      = "backend_profiles_tenant_fkey"
)

// defaultRouteWeight is the whole-traffic weight a publish writes into the
// routing policy. It mirrors MemoryRepository; the shared conformance suite
// asserts the value against both implementations, so the two cannot drift.
const defaultRouteWeight uint32 = 10000

// Repository is the PostgreSQL implementation of tenant.Repository.
type Repository struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

var _ tenant.Repository = (*Repository)(nil)

// New wraps an existing pool. It does not connect, migrate, or take ownership
// of the pool; see the package documentation.
func New(pool *pgxpool.Pool) (*Repository, error) {
	if pool == nil {
		return nil, errInvalidPool()
	}
	return &Repository{pool: pool, now: time.Now}, nil
}

func errInvalidPool() error {
	return fmt.Errorf("%w: a pgxpool.Pool is required", ErrInvalidConfig)
}

// storedNow returns the timestamp a write should record.
//
// It is UTC and truncated to microseconds because that is exactly what
// timestamptz keeps. Truncating here, rather than letting the column do it,
// keeps a value the caller was just handed equal to the value the next read
// returns — which is what makes an idempotent publish observably a no-op
// instead of a write that only looks unchanged to the nearest microsecond.
func (r *Repository) storedNow() time.Time {
	return r.now().UTC().Truncate(time.Microsecond)
}

// contextError mirrors the check every MemoryRepository entry point makes, so
// both implementations refuse a spent context before doing anything else.
func contextError(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", tenant.ErrInvalidArgument)
	}
	return ctx.Err()
}

// storageError is the tail of every database call.
//
// A cancelled context takes priority over whatever the driver reported: once
// the caller has given up, pgx reports the cancellation as a connection
// failure, and reporting that as an infrastructure fault would blame the
// database for the caller's own deadline.
//
// byConstraint maps the constraint names this call site understands to the
// domain error each one means. A constraint that is not in the map is not a
// domain condition here, so it stays an ErrStorage.
//
// ctx is never nil: every entry point runs contextError first.
func storageError(
	ctx context.Context,
	operation string,
	err error,
	byConstraint map[string]error,
) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if name, ok := violatedConstraint(err); ok {
		if mapped, found := byConstraint[name]; found {
			return mapped
		}
	}
	return storageFailure(operation, err)
}

// storageFailure wraps a non-domain failure. The driver error is kept in the
// chain on purpose: this error never reaches a client body, and without it an
// operator has nothing to debug from.
func storageFailure(operation string, err error) error {
	return fmt.Errorf("%w: %s: %w", ErrStorage, operation, err)
}

// violatedConstraint returns the name of the constraint err violated, for the
// SQLSTATE classes where a constraint name is meaningful.
func violatedConstraint(err error) (string, bool) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return "", false
	}
	switch pgErr.Code {
	case uniqueViolation, foreignKeyViolation:
		return pgErr.ConstraintName, true
	default:
		return "", false
	}
}

// notFound builds the ErrNotFound a read returns. The message is assembled
// from the caller's own identifiers only.
func notFound(kind string, id string) error {
	return fmt.Errorf("%w: %s %q", tenant.ErrNotFound, kind, id)
}

func encodeJSON(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		// Unreachable in practice: every value encoded here is a struct of
		// scalars and slices. It is still checked rather than ignored, because
		// silently writing "null" would corrupt a revision's config.
		return nil, fmt.Errorf("encode %T: %w", value, err)
	}
	return encoded, nil
}

func decodeJSON(raw []byte, target any) error {
	// A NULL jsonb scans as no bytes. No column here is nullable, so this can
	// only mean an out-of-band write; leaving the zero value in place is the
	// safe reading.
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("decode %T: %w", target, err)
	}
	return nil
}

// unsignedFromBigint converts a bigint that models an unsigned counter. A
// negative value cannot be produced by this package, so it means the row was
// written by something else and must not be reinterpreted as a huge uint64.
func unsignedFromBigint(column string, value int64) (uint64, error) {
	if value < 0 {
		return 0, fmt.Errorf("%s is negative (%d)", column, value)
	}
	return uint64(value), nil
}

const tenantColumns = `id, slug, name, status, quota, audit_policy, created_at, updated_at`

func scanTenant(row pgx.Row) (tenant.Tenant, error) {
	var (
		item        tenant.Tenant
		status      string
		quota       []byte
		auditPolicy []byte
	)
	if err := row.Scan(
		&item.ID, &item.Slug, &item.Name, &status,
		&quota, &auditPolicy, &item.CreatedAt, &item.UpdatedAt,
	); err != nil {
		return tenant.Tenant{}, err
	}
	item.Status = tenant.Status(status)
	if err := decodeJSON(quota, &item.Quota); err != nil {
		return tenant.Tenant{}, err
	}
	if err := decodeJSON(auditPolicy, &item.AuditPolicy); err != nil {
		return tenant.Tenant{}, err
	}
	// pgx decodes timestamptz into time.Local. Normalising to UTC here keeps a
	// read equal to the write, which was UTC.
	item.CreatedAt = item.CreatedAt.UTC()
	item.UpdatedAt = item.UpdatedAt.UTC()
	return item, nil
}

const agentAppColumns = `tenant_id, id, name, status, routing_version, routing_policy, created_at, updated_at`

func scanAgentApp(row pgx.Row) (tenant.AgentApp, error) {
	var (
		item           tenant.AgentApp
		status         string
		routingVersion int64
		routingPolicy  []byte
	)
	if err := row.Scan(
		&item.TenantID, &item.ID, &item.Name, &status,
		&routingVersion, &routingPolicy, &item.CreatedAt, &item.UpdatedAt,
	); err != nil {
		return tenant.AgentApp{}, err
	}
	item.Status = tenant.AppStatus(status)
	version, err := unsignedFromBigint("routing_version", routingVersion)
	if err != nil {
		return tenant.AgentApp{}, err
	}
	item.RoutingVersion = version
	if err := decodeJSON(routingPolicy, &item.RoutingPolicy); err != nil {
		return tenant.AgentApp{}, err
	}
	item.CreatedAt = item.CreatedAt.UTC()
	item.UpdatedAt = item.UpdatedAt.UTC()
	return item, nil
}

const agentRevisionColumns = `tenant_id, agent_app_id, id, revision_no, status, ` +
	`created_by, config, config_digest, created_at, published_at`

func scanAgentRevision(row pgx.Row) (tenant.AgentRevision, error) {
	var (
		item        tenant.AgentRevision
		revisionNo  int64
		status      string
		config      []byte
		publishedAt *time.Time
	)
	if err := row.Scan(
		&item.TenantID, &item.AgentAppID, &item.ID, &revisionNo, &status,
		&item.CreatedBy, &config, &item.ConfigDigest, &item.CreatedAt, &publishedAt,
	); err != nil {
		return tenant.AgentRevision{}, err
	}
	number, err := unsignedFromBigint("revision_no", revisionNo)
	if err != nil {
		return tenant.AgentRevision{}, err
	}
	item.RevisionNo = number
	item.Status = tenant.RevisionStatus(status)
	if err := decodeJSON(config, &item.Config); err != nil {
		return tenant.AgentRevision{}, err
	}
	item.CreatedAt = item.CreatedAt.UTC()
	if publishedAt != nil {
		normalized := publishedAt.UTC()
		item.PublishedAt = &normalized
	}
	return item, nil
}
