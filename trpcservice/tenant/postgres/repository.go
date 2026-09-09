package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	insertTenantSQL = `INSERT INTO tenants (` + tenantColumns + `)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

	selectTenantSQL = `SELECT ` + tenantColumns + ` FROM tenants WHERE id = $1`

	// selectTenantStatusSQL takes no row lock on purpose. Inserting an app or a
	// revision already takes a FOR KEY SHARE lock on the tenant row to enforce
	// the foreign key, so a publish that took FOR UPDATE here would block every
	// concurrent create in the same tenant for no gain: nothing in this package
	// ever updates a tenant row.
	selectTenantStatusSQL = `SELECT status FROM tenants WHERE id = $1`

	insertAgentAppSQL = `INSERT INTO agent_apps (` + agentAppColumns + `)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

	selectAgentAppSQL = `SELECT ` + agentAppColumns + `
		FROM agent_apps WHERE tenant_id = $1 AND id = $2`

	selectAgentAppForUpdateSQL = selectAgentAppSQL + ` FOR UPDATE`

	// updateAgentAppRoutingSQL increments in SQL rather than writing back a
	// number this process read. The row is already locked by the publish
	// transaction, but an increment that cannot lose an update regardless of
	// how it was reached is one less invariant resting on the caller.
	updateAgentAppRoutingSQL = `UPDATE agent_apps
		SET routing_version = routing_version + 1, routing_policy = $3, updated_at = $4
		WHERE tenant_id = $1 AND id = $2
		RETURNING routing_version`

	insertRevisionSQL = `INSERT INTO agent_revisions (` + agentRevisionColumns + `)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`

	selectRevisionSQL = `SELECT ` + agentRevisionColumns + `
		FROM agent_revisions WHERE tenant_id = $1 AND agent_app_id = $2 AND id = $3`

	selectRevisionForUpdateSQL = selectRevisionSQL + ` FOR UPDATE`

	publishRevisionSQL = `UPDATE agent_revisions
		SET status = $4, published_at = $5
		WHERE tenant_id = $1 AND agent_app_id = $2 AND id = $3`
)

func (r *Repository) CreateTenant(ctx context.Context, item tenant.Tenant) (tenant.Tenant, error) {
	if err := contextError(ctx); err != nil {
		return tenant.Tenant{}, err
	}
	now := r.storedNow()
	if item.Status == "" {
		item.Status = tenant.StatusActive
	}
	item.CreatedAt = now
	item.UpdatedAt = now
	// Validation runs before the first statement, so a malformed tenant never
	// costs a round trip and never depends on a column type to reject it.
	if err := item.Validate(); err != nil {
		return tenant.Tenant{}, err
	}
	quota, err := encodeJSON(item.Quota)
	if err != nil {
		return tenant.Tenant{}, storageFailure("create tenant", err)
	}
	auditPolicy, err := encodeJSON(item.AuditPolicy)
	if err != nil {
		return tenant.Tenant{}, storageFailure("create tenant", err)
	}

	if _, err := r.pool.Exec(ctx, insertTenantSQL,
		item.ID, item.Slug, item.Name, string(item.Status),
		quota, auditPolicy, item.CreatedAt, item.UpdatedAt,
	); err != nil {
		return tenant.Tenant{}, storageError(ctx, "create tenant", err, map[string]error{
			constraintTenantsPrimaryKey: fmt.Errorf(
				"%w: tenant %q", tenant.ErrAlreadyExists, item.ID),
			constraintTenantsSlug: fmt.Errorf(
				"%w: tenant slug %q", tenant.ErrAlreadyExists, item.Slug),
		})
	}
	return item, nil
}

func (r *Repository) GetTenant(ctx context.Context, tenantID string) (tenant.Tenant, error) {
	if err := contextError(ctx); err != nil {
		return tenant.Tenant{}, err
	}
	if err := tenant.ValidateResourceID("tenant id", tenantID); err != nil {
		return tenant.Tenant{}, err
	}
	item, err := scanTenant(r.pool.QueryRow(ctx, selectTenantSQL, tenantID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return tenant.Tenant{}, notFound("tenant", tenantID)
		}
		return tenant.Tenant{}, storageError(ctx, "get tenant", err, nil)
	}
	return item, nil
}

func (r *Repository) CreateAgentApp(
	ctx context.Context,
	scope tenant.TenantContext,
	item tenant.AgentApp,
) (tenant.AgentApp, error) {
	if err := contextError(ctx); err != nil {
		return tenant.AgentApp{}, err
	}
	if item.Status == "" {
		item.Status = tenant.AppStatusActive
	}
	now := r.storedNow()
	item.CreatedAt = now
	item.UpdatedAt = now
	// Routing is repository-owned. Whatever the caller sent is overwritten
	// rather than rejected, so a client can round-trip a whole app object.
	item.RoutingVersion = 0
	item.RoutingPolicy = tenant.RoutingPolicy{}
	if err := item.Validate(scope); err != nil {
		return tenant.AgentApp{}, err
	}
	routingPolicy, err := encodeJSON(item.RoutingPolicy)
	if err != nil {
		return tenant.AgentApp{}, storageFailure("create agent app", err)
	}

	// The tenant is checked before the insert so a suspended tenant reports
	// ErrTenantInactive. The foreign key would only ever say "missing".
	if err := r.requireActiveTenant(ctx, r.pool, scope.TenantID); err != nil {
		return tenant.AgentApp{}, err
	}
	if _, err := r.pool.Exec(ctx, insertAgentAppSQL,
		item.TenantID, item.ID, item.Name, string(item.Status),
		int64(item.RoutingVersion), routingPolicy, item.CreatedAt, item.UpdatedAt,
	); err != nil {
		return tenant.AgentApp{}, storageError(ctx, "create agent app", err, map[string]error{
			constraintAgentAppsPrimaryKey: fmt.Errorf(
				"%w: app %q", tenant.ErrAlreadyExists, item.ID),
			// Only reachable if the tenant was deleted between the check above
			// and this insert.
			constraintAgentAppsTenant: notFound("tenant", scope.TenantID),
		})
	}
	return item, nil
}

func (r *Repository) GetAgentApp(
	ctx context.Context,
	scope tenant.TenantContext,
	appID string,
) (tenant.AgentApp, error) {
	if err := contextError(ctx); err != nil {
		return tenant.AgentApp{}, err
	}
	if err := scope.Validate(); err != nil {
		return tenant.AgentApp{}, err
	}
	if err := tenant.ValidateResourceID("app id", appID); err != nil {
		return tenant.AgentApp{}, err
	}
	item, err := scanAgentApp(r.pool.QueryRow(ctx, selectAgentAppSQL, scope.TenantID, appID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return tenant.AgentApp{}, notFound("app", appID)
		}
		return tenant.AgentApp{}, storageError(ctx, "get agent app", err, nil)
	}
	return item, nil
}

func (r *Repository) CreateRevision(
	ctx context.Context,
	scope tenant.TenantContext,
	item tenant.AgentRevision,
) (tenant.AgentRevision, error) {
	if err := contextError(ctx); err != nil {
		return tenant.AgentRevision{}, err
	}
	if item.Status == "" {
		item.Status = tenant.RevisionStatusDraft
	}
	item.CreatedAt = r.storedNow()
	item.PublishedAt = nil
	// ValidateForCreate bounds RevisionNo at MaxInt64, which is what makes the
	// int64 conversion in the insert below lossless.
	if err := item.ValidateForCreate(scope); err != nil {
		return tenant.AgentRevision{}, err
	}
	// The digest is computed once, here, and never recomputed on a write. It
	// is what later proves the stored config is the config that was approved.
	digest, err := item.Config.Digest()
	if err != nil {
		return tenant.AgentRevision{}, err
	}
	item.ConfigDigest = digest
	config, err := encodeJSON(item.Config)
	if err != nil {
		return tenant.AgentRevision{}, storageFailure("create revision", err)
	}

	if err := r.requireActiveTenant(ctx, r.pool, scope.TenantID); err != nil {
		return tenant.AgentRevision{}, err
	}
	if _, err := r.pool.Exec(ctx, insertRevisionSQL,
		item.TenantID, item.AgentAppID, item.ID, int64(item.RevisionNo), string(item.Status),
		item.CreatedBy, config, item.ConfigDigest, item.CreatedAt, item.PublishedAt,
	); err != nil {
		return tenant.AgentRevision{}, storageError(ctx, "create revision", err, map[string]error{
			constraintRevisionsPrimaryKey: fmt.Errorf(
				"%w: revision %q", tenant.ErrAlreadyExists, item.ID),
			constraintRevisionsRevisionNo: fmt.Errorf(
				"%w: revision number %d", tenant.ErrAlreadyExists, item.RevisionNo),
			// The composite key means this fires for an app that does not
			// exist and for one that belongs to another tenant alike; both are
			// "no such app in this tenant".
			constraintRevisionsAgentApp: notFound("app", item.AgentAppID),
		})
	}
	return item, nil
}

func (r *Repository) GetRevision(
	ctx context.Context,
	scope tenant.TenantContext,
	appID string,
	revisionID string,
) (tenant.AgentRevision, error) {
	if err := contextError(ctx); err != nil {
		return tenant.AgentRevision{}, err
	}
	if err := scope.Validate(); err != nil {
		return tenant.AgentRevision{}, err
	}
	if err := tenant.ValidateResourceID("app id", appID); err != nil {
		return tenant.AgentRevision{}, err
	}
	if err := tenant.ValidateResourceID("revision id", revisionID); err != nil {
		return tenant.AgentRevision{}, err
	}
	item, err := scanAgentRevision(
		r.pool.QueryRow(ctx, selectRevisionSQL, scope.TenantID, appID, revisionID),
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return tenant.AgentRevision{}, notFound("revision", revisionID)
		}
		return tenant.AgentRevision{}, storageError(ctx, "get revision", err, nil)
	}
	return item, nil
}

// PublishRevision makes revisionID the app's default and, if it is still a
// draft, marks it published.
//
// It runs in a READ COMMITTED transaction that locks rows in a fixed order:
// the tenant is read without a lock, then the app FOR UPDATE, then the
// revision FOR UPDATE. Taking the app lock first is what serialises two
// concurrent publishes on the same app; taking it always in this order is what
// keeps two publishes of different revisions of the same app from deadlocking.
//
// Under READ COMMITTED a statement that waits on a lock re-reads the row once
// it is granted, so the second publish of the same draft sees the first one's
// result and takes the idempotent path rather than incrementing again.
//
// The app's status is deliberately not checked: a disabled app still has to be
// able to roll back, and ResolveRevision is where a disabled app stops serving.
func (r *Repository) PublishRevision(
	ctx context.Context,
	scope tenant.TenantContext,
	appID string,
	revisionID string,
) (tenant.AgentApp, tenant.AgentRevision, error) {
	if err := contextError(ctx); err != nil {
		return tenant.AgentApp{}, tenant.AgentRevision{}, err
	}
	if err := scope.Validate(); err != nil {
		return tenant.AgentApp{}, tenant.AgentRevision{}, err
	}
	if err := tenant.ValidateResourceID("app id", appID); err != nil {
		return tenant.AgentApp{}, tenant.AgentRevision{}, err
	}
	if err := tenant.ValidateResourceID("revision id", revisionID); err != nil {
		return tenant.AgentApp{}, tenant.AgentRevision{}, err
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return tenant.AgentApp{}, tenant.AgentRevision{},
			storageError(ctx, "begin publish revision", err, nil)
	}
	// A no-op once the transaction has committed.
	defer func() { _ = tx.Rollback(ctx) }()

	app, revision, err := r.publishLocked(ctx, tx, scope, appID, revisionID)
	if err != nil {
		return tenant.AgentApp{}, tenant.AgentRevision{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return tenant.AgentApp{}, tenant.AgentRevision{},
			storageError(ctx, "commit publish revision", err, nil)
	}
	return app, revision, nil
}

func (r *Repository) publishLocked(
	ctx context.Context,
	tx pgx.Tx,
	scope tenant.TenantContext,
	appID string,
	revisionID string,
) (tenant.AgentApp, tenant.AgentRevision, error) {
	if err := r.requireActiveTenant(ctx, tx, scope.TenantID); err != nil {
		return tenant.AgentApp{}, tenant.AgentRevision{}, err
	}

	app, err := scanAgentApp(
		tx.QueryRow(ctx, selectAgentAppForUpdateSQL, scope.TenantID, appID),
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return tenant.AgentApp{}, tenant.AgentRevision{}, notFound("app", appID)
		}
		return tenant.AgentApp{}, tenant.AgentRevision{},
			storageError(ctx, "lock agent app", err, nil)
	}

	revision, err := scanAgentRevision(
		tx.QueryRow(ctx, selectRevisionForUpdateSQL, scope.TenantID, appID, revisionID),
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return tenant.AgentApp{}, tenant.AgentRevision{}, notFound("revision", revisionID)
		}
		return tenant.AgentApp{}, tenant.AgentRevision{},
			storageError(ctx, "lock revision", err, nil)
	}

	switch revision.Status {
	case tenant.RevisionStatusPublished:
		if app.RoutingPolicy.DefaultRevisionID == revisionID {
			// Already the default. Writing the same values back would still
			// move updated_at and routing_version, which every cache keyed on
			// routing_version would then treat as a real change.
			return app, revision, nil
		}
		// Rollback to an earlier published revision. published_at keeps its
		// original value: it records when this config first went live, not
		// when it was last routed to.
		app, err = r.applyRouting(ctx, tx, app, revision)
		if err != nil {
			return tenant.AgentApp{}, tenant.AgentRevision{}, err
		}
		return app, revision, nil

	case tenant.RevisionStatusDraft:
		// Re-derive the digest from the row just read under lock. This is the
		// check that the config being published is the config that was
		// approved at create time; comparing against anything held in this
		// process would only prove this process is self-consistent.
		//
		// A mismatch is a fault in the stored row, not in the request, so it is
		// not a domain rejection of the caller's input. See ErrConfigIntegrity.
		digest, err := revision.Config.Digest()
		if err != nil || digest != revision.ConfigDigest {
			return tenant.AgentApp{}, tenant.AgentRevision{}, fmt.Errorf(
				"%w: revision %q", tenant.ErrConfigIntegrity, revisionID)
		}

		publishedAt := r.storedNow()
		if _, err := tx.Exec(ctx, publishRevisionSQL,
			scope.TenantID, appID, revisionID,
			string(tenant.RevisionStatusPublished), publishedAt,
		); err != nil {
			return tenant.AgentApp{}, tenant.AgentRevision{},
				storageError(ctx, "publish revision", err, nil)
		}
		revision.Status = tenant.RevisionStatusPublished
		revision.PublishedAt = &publishedAt

		app, err = r.applyRouting(ctx, tx, app, revision)
		if err != nil {
			return tenant.AgentApp{}, tenant.AgentRevision{}, err
		}
		return app, revision, nil

	default:
		return tenant.AgentApp{}, tenant.AgentRevision{}, fmt.Errorf(
			"%w: cannot publish status %q", tenant.ErrInvalidArgument, revision.Status)
	}
}

// applyRouting points the app at revision and advances routing_version. The
// returned app carries the version the database assigned, not one this process
// computed.
func (r *Repository) applyRouting(
	ctx context.Context,
	tx pgx.Tx,
	app tenant.AgentApp,
	revision tenant.AgentRevision,
) (tenant.AgentApp, error) {
	policy := tenant.RoutingPolicy{
		DefaultRevisionID: revision.ID,
		Routes: []tenant.RevisionRoute{
			{RevisionID: revision.ID, Weight: defaultRouteWeight},
		},
	}
	encoded, err := encodeJSON(policy)
	if err != nil {
		return tenant.AgentApp{}, storageFailure("publish revision", err)
	}
	updatedAt := r.storedNow()

	var routingVersion int64
	if err := tx.QueryRow(ctx, updateAgentAppRoutingSQL,
		app.TenantID, app.ID, encoded, updatedAt,
	).Scan(&routingVersion); err != nil {
		return tenant.AgentApp{}, storageError(ctx, "update agent app routing", err, nil)
	}
	version, err := unsignedFromBigint("routing_version", routingVersion)
	if err != nil {
		return tenant.AgentApp{}, storageFailure("update agent app routing", err)
	}

	app.RoutingVersion = version
	app.RoutingPolicy = policy
	app.UpdatedAt = updatedAt
	return app, nil
}

// ResolveRevision returns a published pinned revision, or the app default.
//
// The three reads are not wrapped in a transaction. A publish commits the
// revision row and the app's routing policy together, so a default this sees
// is always backed by a revision row that is already published; there is no
// interleaving that produces a default pointing at a draft.
func (r *Repository) ResolveRevision(
	ctx context.Context,
	scope tenant.TenantContext,
	appID string,
	pinnedRevisionID string,
) (tenant.AgentRevision, error) {
	if err := contextError(ctx); err != nil {
		return tenant.AgentRevision{}, err
	}
	if err := scope.Validate(); err != nil {
		return tenant.AgentRevision{}, err
	}
	if err := tenant.ValidateResourceID("app id", appID); err != nil {
		return tenant.AgentRevision{}, err
	}
	if pinnedRevisionID != "" {
		if err := tenant.ValidateResourceID("revision id", pinnedRevisionID); err != nil {
			return tenant.AgentRevision{}, err
		}
	}
	if err := r.requireActiveTenant(ctx, r.pool, scope.TenantID); err != nil {
		return tenant.AgentRevision{}, err
	}

	app, err := scanAgentApp(r.pool.QueryRow(ctx, selectAgentAppSQL, scope.TenantID, appID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return tenant.AgentRevision{}, notFound("active app", appID)
		}
		return tenant.AgentRevision{}, storageError(ctx, "resolve revision", err, nil)
	}
	// A disabled app is reported as missing rather than as a distinct state:
	// the conversation path has no use for the difference, and saying "exists
	// but disabled" tells an unauthenticated caller an app id is real.
	if app.Status != tenant.AppStatusActive {
		return tenant.AgentRevision{}, notFound("active app", appID)
	}

	revisionID := pinnedRevisionID
	if revisionID == "" {
		revisionID = app.RoutingPolicy.DefaultRevisionID
	}
	if revisionID == "" {
		return tenant.AgentRevision{}, fmt.Errorf(
			"%w: app %q", tenant.ErrNoPublishedRevision, appID)
	}

	revision, err := scanAgentRevision(
		r.pool.QueryRow(ctx, selectRevisionSQL, scope.TenantID, appID, revisionID),
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return tenant.AgentRevision{}, notFound("revision", revisionID)
		}
		return tenant.AgentRevision{}, storageError(ctx, "resolve revision", err, nil)
	}
	// A pinned draft is refused here rather than at pin time: a revision can be
	// pinned before it is published, and only the read has to be strict.
	if revision.Status != tenant.RevisionStatusPublished {
		return tenant.AgentRevision{}, fmt.Errorf(
			"%w: revision %q", tenant.ErrRevisionNotPublished, revisionID)
	}
	return revision, nil
}

// querier is the part of pgxpool.Pool and pgx.Tx this package shares, so the
// tenant check is written once and runs both inside and outside a transaction.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// requireActiveTenant is the first gate of every tenant-scoped operation.
func (r *Repository) requireActiveTenant(
	ctx context.Context,
	q querier,
	tenantID string,
) error {
	var status string
	if err := q.QueryRow(ctx, selectTenantStatusSQL, tenantID).Scan(&status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return notFound("tenant", tenantID)
		}
		return storageError(ctx, "check tenant status", err, nil)
	}
	if tenant.Status(status) != tenant.StatusActive {
		return fmt.Errorf(
			"%w: tenant %q has status %q", tenant.ErrTenantInactive, tenantID, status)
	}
	return nil
}
