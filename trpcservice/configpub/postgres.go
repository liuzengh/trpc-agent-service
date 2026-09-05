package configpub

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	tenantctx "github.com/liuzengh/trpc-agent-service/trpcservice/storage/tenantctx"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const queryTimeout = 5 * time.Second

// PostgresRepository is the authoritative durable fact boundary. PostgreSQL
// owns every configuration fact; nothing here touches Redis or in-process
// caches, and revision history is never deleted.
type PostgresRepository struct {
	pool    *pgxpool.Pool
	timeout time.Duration
}

// NewPostgresRepository fails closed without a pool.
func NewPostgresRepository(pool *pgxpool.Pool) (*PostgresRepository, error) {
	if pool == nil {
		return nil, ErrInvalidArgument
	}
	return &PostgresRepository{pool: pool, timeout: queryTimeout}, nil
}

func (r *PostgresRepository) queryCtx(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if r == nil || r.pool == nil || ctx == nil {
		return nil, nil, ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, classify("postgres", err)
	}
	bounded, cancel := context.WithTimeout(ctx, r.timeout)
	return bounded, cancel, nil
}

// classifyPostgres maps a raw pgx failure to a safe boundary error. Raw
// messages, constraints, DSNs and SQL never leave this function.
func classifyPostgres(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return classify("postgres", err)
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23503": // foreign_key_violation: unprovisioned tenant/revision
			return ErrTenantMismatch
		case "23514", "23508": // check violations
			return ErrRevisionState
		case "P0001": // revision guard triggers: immutable content / illegal transition
			return ErrRevisionState
		}
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrRevisionNotFound
	}
	return classify("postgres", err)
}

const revisionColumns = `tenant_id, config_version, status, config, checksum, created_by, reason_category, created_at, validated_at, published_at, superseded_at, recalled_at, rejected_at`

func scanRevision(row pgx.Row) (Revision, error) {
	var rev Revision
	var status string
	var rawConfig []byte
	var createdBy, reason *string
	var validatedAt, publishedAt, supersededAt, recalledAt, rejectedAt *time.Time
	if err := row.Scan(&rev.TenantID, &rev.Version, &status, &rawConfig, &rev.Fingerprint, &createdBy, &reason, &rev.CreatedAt, &validatedAt, &publishedAt, &supersededAt, &recalledAt, &rejectedAt); err != nil {
		return Revision{}, err
	}
	rev.Status = Status(status)
	if createdBy != nil {
		rev.ActorCategory = *createdBy
	}
	if reason != nil {
		rev.ReasonCategory = *reason
	}
	for target, value := range map[*time.Time]*time.Time{
		&rev.ValidatedAt: validatedAt, &rev.PublishedAt: publishedAt, &rev.SupersededAt: supersededAt,
		&rev.RecalledAt: recalledAt, &rev.RejectedAt: rejectedAt,
	} {
		if value != nil {
			*target = *value
		}
	}
	doc, err := DecodeDocument(rawConfig)
	if err != nil {
		return Revision{}, err
	}
	rev.Document = doc
	return rev, nil
}

// CreateRevision inserts a draft revision. Re-submitting the exact same
// (tenant, version, content) converges to the stored revision; version reuse
// with different content or a duplicate fingerprint under a new version fails.
func (r *PostgresRepository) CreateRevision(ctx context.Context, tc tenant.TenantContext, version int64, doc Document, actorCategory string, now time.Time) (Revision, error) {
	bounded, cancel, err := r.queryCtx(ctx)
	if err != nil {
		return Revision{}, err
	}
	defer cancel()
	if version < 1 {
		return Revision{}, ErrInvalidArgument
	}
	raw, err := EncodeDocument(doc)
	if err != nil {
		return Revision{}, err
	}
	fingerprint, err := Fingerprint(doc)
	if err != nil {
		return Revision{}, err
	}
	var inserted Revision
	insertErr := tenantctx.WithTenantContext(bounded, r.pool, tc.TenantID, "config revision create", func(ctx context.Context, tx pgx.Tx) error {
		rev, scanErr := scanRevision(tx.QueryRow(ctx, `INSERT INTO tenant_config_version
		(tenant_id, config_version, status, config, checksum, created_by, created_at)
		VALUES ($1, $2, 'draft', $3, $4, $5, $6)
		RETURNING `+revisionColumns, tc.TenantID, version, raw, fingerprint, actorCategory, now))
		if scanErr == nil {
			inserted = rev
			return nil
		}
		var conflict *pgconn.PgError
		if errors.As(scanErr, &conflict) && conflict.Code == "23505" {
			switch conflict.ConstraintName {
			case "tenant_config_version_checksum_key", "tenant_config_version_tenant_id_checksum_key":
				return ErrDuplicateContent
			case "tenant_config_version_pkey":
				// The failed insert aborted this transaction; the idempotent
				// convergence read must run in a fresh transaction.
				return idempotentRetrySignal{}
			}
		}
		return scanErr
	})
	if insertErr == nil {
		return inserted, nil
	}
	var idem idempotentRetrySignal
	if !errors.As(insertErr, &idem) {
		return Revision{}, classifyPostgres(insertErr)
	}
	err = tenantctx.WithTenantContext(bounded, r.pool, tc.TenantID, "config revision idempotent read", func(ctx context.Context, tx pgx.Tx) error {
		existing, readErr := scanRevision(tx.QueryRow(ctx, `SELECT `+revisionColumns+` FROM tenant_config_version WHERE tenant_id=$1 AND config_version=$2`, tc.TenantID, version))
		if readErr != nil {
			return ErrRevisionState
		}
		if existing.Fingerprint != fingerprint {
			return ErrRevisionState
		}
		inserted = existing // idempotent create convergence
		return nil
	})
	if err != nil {
		return Revision{}, err
	}
	return inserted, nil
}

// idempotentRetrySignal marks a unique-key conflict whose idempotent
// convergence read must run in a fresh transaction after the abort.
type idempotentRetrySignal struct{}

func (idempotentRetrySignal) Error() string { return "configpub: idempotent retry" }
func (r *PostgresRepository) Revision(ctx context.Context, tenantID string, version int64) (Revision, error) {
	bounded, cancel, err := r.queryCtx(ctx)
	if err != nil {
		return Revision{}, err
	}
	defer cancel()
	var rev Revision
	err = tenantctx.WithTenantContext(bounded, r.pool, tenantID, "config revision read", func(ctx context.Context, tx pgx.Tx) error {
		var scanErr error
		rev, scanErr = scanRevision(tx.QueryRow(ctx, `SELECT `+revisionColumns+` FROM tenant_config_version WHERE tenant_id=$1 AND config_version=$2`, tenantID, version))
		return scanErr
	})
	if err != nil {
		return Revision{}, classifyPostgres(err)
	}
	return rev, nil
}

// ValidateRevision transitions draft -> validated or draft -> rejected inside
// one transaction. Semantic validation runs while holding the row lock.
func (r *PostgresRepository) ValidateRevision(ctx context.Context, tc tenant.TenantContext, version int64, validator Validator, actorCategory string, now time.Time) (Revision, error) {
	bounded, cancel, err := r.queryCtx(ctx)
	if err != nil {
		return Revision{}, err
	}
	defer cancel()
	tx, err := r.pool.Begin(bounded)
	if err != nil {
		return Revision{}, classify("postgres", err)
	}
	defer func() { _ = tx.Rollback(bounded) }()
	if err = tenantctx.SetTenantContext(bounded, tx, tc.TenantID); err != nil {
		return Revision{}, err
	}
	var rawConfig []byte
	var status string
	err = tx.QueryRow(bounded, `SELECT status, config FROM tenant_config_version WHERE tenant_id=$1 AND config_version=$2 FOR UPDATE`, tc.TenantID, version).Scan(&status, &rawConfig)
	if err != nil {
		return Revision{}, classifyPostgres(err)
	}
	if status != string(StatusDraft) {
		return Revision{}, ErrRevisionState
	}
	doc, err := DecodeDocument(rawConfig)
	if err != nil {
		// A draft whose stored content no longer decodes must not pass.
		return Revision{}, err
	}
	if err := validator.Validate(bounded, tc.TenantID, doc); err != nil {
		if isUnavailable(err) || errors.Is(err, ErrInvalidArgument) {
			return Revision{}, err
		}
		category := CategoryOf(err)
		if category == "" {
			category = ReasonValidationFailed
		}
		var rejected Revision
		if rejected, err = scanRevision(tx.QueryRow(bounded, `UPDATE tenant_config_version
			SET status='rejected', rejected_at=$3, reason_category=$4
			WHERE tenant_id=$1 AND config_version=$2 AND status='draft'
			RETURNING `+revisionColumns, tc.TenantID, version, now, category)); err != nil {
			return Revision{}, classifyPostgres(err)
		}
		if err := tx.Commit(bounded); err != nil {
			return Revision{}, classify("postgres", err)
		}
		return rejected, nil
	}
	validated, err := scanRevision(tx.QueryRow(bounded, `UPDATE tenant_config_version
		SET status='validated', validated_at=$3, reason_category=''
		WHERE tenant_id=$1 AND config_version=$2 AND status='draft'
		RETURNING `+revisionColumns, tc.TenantID, version, now))
	if err != nil {
		return Revision{}, classifyPostgres(err)
	}
	if err := tx.Commit(bounded); err != nil {
		return Revision{}, classify("postgres", err)
	}
	return validated, nil
}

// Operation returns the recorded committed operation for an idempotency key.
func (r *PostgresRepository) Operation(ctx context.Context, tenantID, operationID string) (OperationRecord, bool, error) {
	bounded, cancel, err := r.queryCtx(ctx)
	if err != nil {
		return OperationRecord{}, false, err
	}
	defer cancel()
	var record OperationRecord
	var resultVersion *int64
	var reason *string
	err = tenantctx.WithTenantContext(bounded, r.pool, tenantID, "config operation read", func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT tenant_id, operation_id, kind, target_version, expected_active_version, requested_percentage, result_active_version, outcome, reason_category, actor_category, created_at
		FROM tenant_config_operation WHERE tenant_id=$1 AND operation_id=$2`, tenantID, operationID).
			Scan(&record.TenantID, &record.OperationID, &record.Kind, &record.TargetVersion, &record.ExpectedActiveVersion, &record.RequestedPercentage, &resultVersion, &record.Outcome, &reason, &record.ActorCategory, &record.CreatedAt)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return OperationRecord{}, false, nil
	}
	if err != nil {
		return OperationRecord{}, false, classifyPostgres(err)
	}
	if resultVersion != nil {
		record.ResultActiveVersion = *resultVersion
	}
	if reason != nil {
		record.ReasonCategory = *reason
	}
	return record, true, nil
}

func insertOperation(tx pgx.Tx, ctx context.Context, op OperationRecord, now time.Time) error {
	_, err := tx.Exec(ctx, `INSERT INTO tenant_config_operation
		(tenant_id, operation_id, kind, target_version, expected_active_version, requested_percentage, result_active_version, outcome, reason_category, actor_category, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,'committed','',$8,$9)`,
		op.TenantID, op.OperationID, op.Kind, op.TargetVersion, op.ExpectedActiveVersion, op.RequestedPercentage, op.ResultActiveVersion, op.ActorCategory, now)
	return err
}

func lockConfigTenant(ctx context.Context, tx pgx.Tx, tenantID string) error {
	var lockedTenant string
	err := tx.QueryRow(ctx, `SELECT tenant_id FROM tenant WHERE tenant_id=$1 FOR UPDATE`, tenantID).Scan(&lockedTenant)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrTenantMismatch
	}
	if err != nil {
		return classifyPostgres(err)
	}
	return nil
}

// Publish atomically: CAS on the active revision, demote previous, activate
// the validated target, upsert rollout state, and record the operation.
func (r *PostgresRepository) Publish(ctx context.Context, tc tenant.TenantContext, op OperationRecord, percentage int, now time.Time) (OperationRecord, error) {
	bounded, cancel, err := r.queryCtx(ctx)
	if err != nil {
		return OperationRecord{}, err
	}
	defer cancel()
	tx, err := r.pool.Begin(bounded)
	if err != nil {
		return OperationRecord{}, classify("postgres", err)
	}
	defer func() { _ = tx.Rollback(bounded) }()
	if err = tenantctx.SetTenantContext(bounded, tx, tc.TenantID); err != nil {
		return OperationRecord{}, err
	}
	if err := lockConfigTenant(bounded, tx, tc.TenantID); err != nil {
		return OperationRecord{}, err
	}

	var targetStatus string
	err = tx.QueryRow(bounded, `SELECT status FROM tenant_config_version WHERE tenant_id=$1 AND config_version=$2 FOR UPDATE`, tc.TenantID, op.TargetVersion).Scan(&targetStatus)
	if err != nil {
		return OperationRecord{}, classifyPostgres(err)
	}
	if targetStatus != string(StatusValidated) {
		return OperationRecord{}, ErrRevisionState
	}
	var activeVersion int64
	activeFound := true
	err = tx.QueryRow(bounded, `SELECT config_version FROM tenant_config_version WHERE tenant_id=$1 AND status='published' LIMIT 1 FOR UPDATE`, tc.TenantID).Scan(&activeVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		activeFound = false
		activeVersion = 0
	} else if err != nil {
		return OperationRecord{}, classifyPostgres(err)
	}
	if activeVersion != op.ExpectedActiveVersion || (!activeFound && op.ExpectedActiveVersion != 0) {
		return OperationRecord{}, ErrStaleExpectedVersion
	}
	if percentage < 100 && !activeFound {
		// A partial rollout needs a baseline for the out-of-bucket population.
		return OperationRecord{}, ErrInvalidRollout
	}
	if activeFound {
		if _, err = tx.Exec(bounded, `UPDATE tenant_config_version SET status='superseded', superseded_at=$3
			WHERE tenant_id=$1 AND config_version=$2 AND status='published'`, tc.TenantID, activeVersion, now); err != nil {
			return OperationRecord{}, classifyPostgres(err)
		}
	}
	if _, err = tx.Exec(bounded, `UPDATE tenant_config_version SET status='published', published_at=$3
		WHERE tenant_id=$1 AND config_version=$2 AND status='validated'`, tc.TenantID, op.TargetVersion, now); err != nil {
		return OperationRecord{}, classifyPostgres(err)
	}
	baseline := activeVersion
	if baseline == 0 {
		baseline = op.TargetVersion
	}
	if _, err = tx.Exec(bounded, `INSERT INTO tenant_config_rollout (tenant_id, active_version, baseline_version, percentage, updated_at)
		VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (tenant_id) DO UPDATE SET active_version=EXCLUDED.active_version, baseline_version=EXCLUDED.baseline_version, percentage=EXCLUDED.percentage, updated_at=EXCLUDED.updated_at`,
		tc.TenantID, op.TargetVersion, baseline, percentage, now); err != nil {
		return OperationRecord{}, classifyPostgres(err)
	}
	op.ResultActiveVersion = op.TargetVersion
	op.Outcome = OutcomeCommitted
	if err = insertOperation(tx, bounded, op, now); err != nil {
		return OperationRecord{}, classifyPostgres(err)
	}
	if err = tx.Commit(bounded); err != nil {
		return OperationRecord{}, classify("postgres", err)
	}
	return op, nil
}

// SetRollout updates the canary percentage of the currently active revision
// under CAS.
func (r *PostgresRepository) SetRollout(ctx context.Context, tc tenant.TenantContext, op OperationRecord, percentage int, now time.Time) (OperationRecord, error) {
	bounded, cancel, err := r.queryCtx(ctx)
	if err != nil {
		return OperationRecord{}, err
	}
	defer cancel()
	tx, err := r.pool.Begin(bounded)
	if err != nil {
		return OperationRecord{}, classify("postgres", err)
	}
	defer func() { _ = tx.Rollback(bounded) }()
	if err = tenantctx.SetTenantContext(bounded, tx, tc.TenantID); err != nil {
		return OperationRecord{}, err
	}
	if err := lockConfigTenant(bounded, tx, tc.TenantID); err != nil {
		return OperationRecord{}, err
	}

	var activeVersion int64
	activeFound := true
	err = tx.QueryRow(bounded, `SELECT config_version FROM tenant_config_version WHERE tenant_id=$1 AND status='published' LIMIT 1 FOR UPDATE`, tc.TenantID).Scan(&activeVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		activeFound = false
	} else if err != nil {
		return OperationRecord{}, classifyPostgres(err)
	}
	if !activeFound || activeVersion != op.TargetVersion || activeVersion != op.ExpectedActiveVersion {
		if !activeFound && op.ExpectedActiveVersion == 0 && op.TargetVersion == 0 {
			return OperationRecord{}, ErrRevisionState
		}
		return OperationRecord{}, ErrStaleExpectedVersion
	}
	tag, err := tx.Exec(bounded, `UPDATE tenant_config_rollout SET percentage=$2, updated_at=$3 WHERE tenant_id=$1`, tc.TenantID, percentage, now)
	if err != nil {
		return OperationRecord{}, classifyPostgres(err)
	}
	if tag.RowsAffected() != 1 {
		return OperationRecord{}, ErrRevisionState
	}
	op.ResultActiveVersion = activeVersion
	op.Outcome = OutcomeCommitted
	if err = insertOperation(tx, bounded, op, now); err != nil {
		return OperationRecord{}, classifyPostgres(err)
	}
	if err = tx.Commit(bounded); err != nil {
		return OperationRecord{}, classify("postgres", err)
	}
	return op, nil
}

// Rollback recalls the active revision and re-activates a previously
// published target under CAS. History is never deleted.
func (r *PostgresRepository) Rollback(ctx context.Context, tc tenant.TenantContext, op OperationRecord, now time.Time) (OperationRecord, error) {
	bounded, cancel, err := r.queryCtx(ctx)
	if err != nil {
		return OperationRecord{}, err
	}
	defer cancel()
	tx, err := r.pool.Begin(bounded)
	if err != nil {
		return OperationRecord{}, classify("postgres", err)
	}
	defer func() { _ = tx.Rollback(bounded) }()
	if err = tenantctx.SetTenantContext(bounded, tx, tc.TenantID); err != nil {
		return OperationRecord{}, err
	}
	if err := lockConfigTenant(bounded, tx, tc.TenantID); err != nil {
		return OperationRecord{}, err
	}

	var activeVersion int64
	activeFound := true
	err = tx.QueryRow(bounded, `SELECT config_version FROM tenant_config_version WHERE tenant_id=$1 AND status='published' LIMIT 1 FOR UPDATE`, tc.TenantID).Scan(&activeVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		activeFound = false
	} else if err != nil {
		return OperationRecord{}, classifyPostgres(err)
	}
	if !activeFound || activeVersion != op.ExpectedActiveVersion {
		return OperationRecord{}, ErrStaleExpectedVersion
	}
	if op.TargetVersion == activeVersion {
		return OperationRecord{}, ErrRevisionState
	}
	var targetStatus string
	err = tx.QueryRow(bounded, `SELECT status FROM tenant_config_version WHERE tenant_id=$1 AND config_version=$2 FOR UPDATE`, tc.TenantID, op.TargetVersion).Scan(&targetStatus)
	if err != nil {
		return OperationRecord{}, classifyPostgres(err)
	}
	// Rollback targets must have been published before: superseded or recalled.
	if targetStatus != string(StatusSuperseded) && targetStatus != string(StatusRecalled) {
		return OperationRecord{}, ErrRevisionState
	}
	if _, err = tx.Exec(bounded, `UPDATE tenant_config_version SET status='recalled', recalled_at=$3
		WHERE tenant_id=$1 AND config_version=$2 AND status='published'`, tc.TenantID, activeVersion, now); err != nil {
		return OperationRecord{}, classifyPostgres(err)
	}
	if _, err = tx.Exec(bounded, `UPDATE tenant_config_version SET status='published', published_at=$3
		WHERE tenant_id=$1 AND config_version=$2`, tc.TenantID, op.TargetVersion, now); err != nil {
		return OperationRecord{}, classifyPostgres(err)
	}
	if _, err = tx.Exec(bounded, `INSERT INTO tenant_config_rollout (tenant_id, active_version, baseline_version, percentage, updated_at)
		VALUES ($1,$2,$2,100,$3)
		ON CONFLICT (tenant_id) DO UPDATE SET active_version=EXCLUDED.active_version, baseline_version=EXCLUDED.baseline_version, percentage=100, updated_at=EXCLUDED.updated_at`,
		tc.TenantID, op.TargetVersion, now); err != nil {
		return OperationRecord{}, classifyPostgres(err)
	}
	op.ResultActiveVersion = op.TargetVersion
	op.Outcome = OutcomeCommitted
	if err = insertOperation(tx, bounded, op, now); err != nil {
		return OperationRecord{}, classifyPostgres(err)
	}
	if err = tx.Commit(bounded); err != nil {
		return OperationRecord{}, classify("postgres", err)
	}
	return op, nil
}

// Rollout reads the durable assignment state. managed=false means the tenant
// has no publication state and keeps using the registry config version.
func (r *PostgresRepository) Rollout(ctx context.Context, tenantID string) (RolloutState, bool, error) {
	bounded, cancel, err := r.queryCtx(ctx)
	if err != nil {
		return RolloutState{}, false, err
	}
	defer cancel()
	var state RolloutState
	var baseline *int64
	err = tenantctx.WithTenantContext(bounded, r.pool, tenantID, "config rollout read", func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT tenant_id, active_version, baseline_version, percentage, updated_at FROM tenant_config_rollout WHERE tenant_id=$1`, tenantID).
			Scan(&state.TenantID, &state.ActiveVersion, &baseline, &state.Percentage, &state.UpdatedAt)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return RolloutState{}, false, nil
	}
	if err != nil {
		return RolloutState{}, false, classifyPostgres(err)
	}
	if baseline != nil {
		state.BaselineVersion = *baseline
	}
	return state, true, nil
}

// BindingOwnershipFor resolves binding references during validation. It never
// returns secret material.
func (r *PostgresRepository) BindingOwnershipFor(ctx context.Context, tenantID, channel, bindingID string) (BindingOwnership, error) {
	bounded, cancel, err := r.queryCtx(ctx)
	if err != nil {
		return BindingMissing, err
	}
	defer cancel()
	var owned, foreign bool
	err = tenantctx.WithTenantContext(bounded, r.pool, tenantID, "binding ownership", func(ctx context.Context, tx pgx.Tx) error {
		rows, queryErr := tx.Query(ctx, `SELECT tenant_id FROM channel_binding WHERE channel=$1 AND binding_id=$2 AND tenant_id=$3`, channel, bindingID, tenantID)
		if queryErr != nil {
			return queryErr
		}
		defer rows.Close()
		for rows.Next() {
			var owner string
			if scanErr := rows.Scan(&owner); scanErr != nil {
				return scanErr
			}
			switch {
			case owner == tenantID:
				owned = true
			default:
				foreign = true
			}
		}
		return rows.Err()
	})
	if err != nil {
		return BindingMissing, classifyPostgres(err)
	}
	if owned {
		return BindingOwned, nil
	}
	if foreign {
		return BindingForeignTenant, nil
	}
	return BindingMissing, nil
}

var _ Repository = (*PostgresRepository)(nil)
var _ BindingChecker = (*PostgresRepository)(nil)
