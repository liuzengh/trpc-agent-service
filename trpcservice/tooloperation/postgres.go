package tooloperation

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const recordProjection = `
	tenant_id, operation_key, payload_hash, tool_name, operation_class,
	target_hash, state, state_version, attempt_count, current_attempt,
	lease_expires_at, next_attempt_at, outcome_code, result_hash,
	replay_reference, created_at, updated_at`

// Postgres is a multi-process Ledger backed by PostgreSQL. Its queries always
// include tenant_id in keys, locks, leases, attempts, and resolutions. Schema
// installation is intentionally not a runtime method; a privileged migrator
// must apply PostgreSQLSchemaDDL before constructing this data-plane store.
type Postgres struct {
	pool *pgxpool.Pool
}

// durationInterval preserves the full duration without passing Go's duration
// syntax through PostgreSQL's text interval parser. PostgreSQL intervals have
// microsecond precision, so a non-zero remainder is rounded away from zero: a
// positive lease must never become shorter merely because it crossed the
// storage boundary.
func durationInterval(d time.Duration) pgtype.Interval {
	microseconds := d / time.Microsecond
	if remainder := d % time.Microsecond; remainder != 0 {
		if d > 0 {
			microseconds++
		} else {
			microseconds--
		}
	}
	return pgtype.Interval{Microseconds: int64(microseconds), Valid: true}
}

// NewPostgres binds a migrated pool to the tool operation ledger.
func NewPostgres(pool *pgxpool.Pool) (*Postgres, error) {
	if pool == nil {
		return nil, ErrInvalidRequest
	}
	return &Postgres{pool: pool}, nil
}

func (p *Postgres) Reserve(
	ctx context.Context,
	req ReserveRequest,
	now time.Time,
) (*ReserveResult, error) {
	if err := validateReserve(req); err != nil || now.IsZero() {
		return nil, ErrInvalidRequest
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fixedError("begin reserve", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	row := tx.QueryRow(ctx, `
		INSERT INTO tool_operations
			(tenant_id, operation_key, payload_hash, tool_name, operation_class,
			 target_hash, state, next_attempt_at)
		VALUES ($1, $2, $3, $4, $5, $6, 'reserved', clock_timestamp())
		ON CONFLICT (tenant_id, operation_key) DO NOTHING
		RETURNING `+recordProjection,
		req.TenantID, req.OperationKey, req.PayloadHash, req.Metadata.ToolName,
		req.Metadata.OperationClass, req.Metadata.TargetHash)
	record, scanErr := scanOperationRecord(row)
	created := scanErr == nil
	if errors.Is(scanErr, pgx.ErrNoRows) {
		record, scanErr = scanOperationRecord(tx.QueryRow(ctx, `
			SELECT `+recordProjection+`
			FROM tool_operations
			WHERE tenant_id = $1 AND operation_key = $2
			FOR SHARE`, req.TenantID, req.OperationKey))
	}
	if scanErr != nil {
		return nil, fixedError("read reserve", scanErr)
	}
	if !created && (record.PayloadHash != req.PayloadHash || !sameMetadata(record.Metadata, req.Metadata)) {
		return nil, ErrConflict
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fixedError("commit reserve", err)
	}
	result := &ReserveResult{Record: cloneRecord(record), Existing: !created}
	if !created && record.State == StateConfirmed {
		result.Replayed = true
		result.Confirmation = cloneConfirmation(record.Confirmation)
	}
	return result, nil
}

func (p *Postgres) Lease(ctx context.Context, req LeaseRequest) ([]LeasedOperation, error) {
	if err := validateLease(req); err != nil {
		return nil, err
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fixedError("begin lease", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Batch leasing is tenant-scoped. Only a reserved (not yet dispatched)
	// attempt may be safely reclaimed here; executing expiry is handled by the
	// explicit global reconciler and must become unknown there.
	if err := reclaimExpiredReservedForTenantTx(ctx, tx, req.TenantID, maxLeaseBatch); err != nil {
		return nil, err
	}

	ownerDigest := ownerHash(req.Owner)
	rows, err := tx.Query(ctx, `
		WITH candidates AS MATERIALIZED (
			SELECT tenant_id, operation_key
			FROM tool_operations
			WHERE tenant_id = $1
				AND state IN ('reserved', 'retryable_not_applied')
				AND lease_expires_at IS NULL
				AND next_attempt_at <= clock_timestamp()
			ORDER BY next_attempt_at, created_at, operation_key
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		), leased AS (
			UPDATE tool_operations AS target SET
				state = 'reserved',
				state_version = target.state_version + 1,
				attempt_count = target.attempt_count + 1,
				current_attempt = target.attempt_count + 1,
				lease_owner_hash = $3,
				lease_expires_at = clock_timestamp() + $4::interval,
				next_attempt_at = NULL,
				outcome_code = '', result_hash = '', replay_reference = '',
				updated_at = clock_timestamp()
			FROM candidates
			WHERE target.tenant_id = candidates.tenant_id
				AND target.operation_key = candidates.operation_key
			RETURNING target.*
		), attempts AS (
			INSERT INTO tool_operation_attempts
				(tenant_id, operation_key, attempt_no, owner_hash, phase)
			SELECT tenant_id, operation_key, current_attempt, $3, 'reserved'
			FROM leased
			RETURNING tenant_id, operation_key, attempt_no
		)
		SELECT `+qualifiedRecordProjection("leased")+`
		FROM leased
		JOIN attempts USING (tenant_id, operation_key)
		ORDER BY leased.created_at, leased.operation_key`,
		req.TenantID, req.Limit, ownerDigest, durationInterval(req.TTL))
	if err != nil {
		return nil, fixedError("lease operations", err)
	}
	leased := make([]LeasedOperation, 0)
	for rows.Next() {
		record, err := scanOperationRecord(rows)
		if err != nil {
			rows.Close()
			return nil, fixedError("scan lease", err)
		}
		leased = append(leased, LeasedOperation{
			Record: record,
			Fence: Fence{
				TenantID: record.TenantID, OperationKey: record.OperationKey,
				AttemptNo: record.CurrentAttempt, Owner: req.Owner,
			},
		})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fixedError("iterate leases", err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, fixedError("commit leases", err)
	}
	return leased, nil
}

func (p *Postgres) LeaseOperation(
	ctx context.Context,
	tenantID, operationID, owner string,
	now time.Time,
	ttl time.Duration,
) (*LeasedOperation, error) {
	if err := validateLeaseOperation(tenantID, operationID, owner, now, ttl); err != nil {
		return nil, err
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fixedError("begin exact lease", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var state string
	var currentAttempt int
	var storedOwnerHash string
	var due, hasLease, leaseExpired bool
	err = tx.QueryRow(ctx, `
		SELECT state, current_attempt, lease_owner_hash,
			COALESCE(next_attempt_at <= clock_timestamp(), true),
			lease_expires_at IS NOT NULL,
			COALESCE(lease_expires_at <= clock_timestamp(), false)
		FROM tool_operations
		WHERE tenant_id = $1 AND operation_key = $2
		FOR UPDATE`, tenantID, operationID).Scan(
		&state, &currentAttempt, &storedOwnerHash, &due, &hasLease, &leaseExpired)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fixedError("lock exact lease", err)
	}
	if State(state) == StateUnknown {
		return nil, ErrUnknownRequiresResolution
	}
	if State(state) != StateReserved && State(state) != StateRetryableNotApplied {
		return nil, ErrInvalidTransition
	}
	if !due {
		return nil, ErrInvalidTransition
	}
	if hasLease && !leaseExpired {
		return nil, ErrInvalidTransition
	}
	if leaseExpired {
		if err := finalizeExpiredOperationTx(ctx, tx, expiredOperation{
			tenantID: tenantID, operationKey: operationID, state: state,
			ownerHash: storedOwnerHash, attemptNo: currentAttempt,
		}); err != nil {
			return nil, err
		}
	}

	ownerDigest := ownerHash(owner)
	record, err := scanOperationRecord(tx.QueryRow(ctx, `
		UPDATE tool_operations SET
			state = 'reserved', state_version = state_version + 1,
			attempt_count = attempt_count + 1,
			current_attempt = attempt_count + 1,
			lease_owner_hash = $3,
			lease_expires_at = clock_timestamp() + $4::interval,
			next_attempt_at = NULL,
			outcome_code = '', result_hash = '', replay_reference = '',
			updated_at = clock_timestamp()
		WHERE tenant_id = $1 AND operation_key = $2
		RETURNING `+recordProjection,
		tenantID, operationID, ownerDigest, durationInterval(ttl)))
	if err != nil {
		return nil, fixedError("lease exact operation", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO tool_operation_attempts
			(tenant_id, operation_key, attempt_no, owner_hash, phase)
		VALUES ($1, $2, $3, $4, 'reserved')`,
		tenantID, operationID, record.CurrentAttempt, ownerDigest); err != nil {
		return nil, fixedError("record exact lease attempt", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fixedError("commit exact lease", err)
	}
	return &LeasedOperation{
		Record: record,
		Fence: Fence{
			TenantID: tenantID, OperationKey: operationID,
			AttemptNo: record.CurrentAttempt, Owner: owner,
		},
	}, nil
}

func (p *Postgres) Renew(
	ctx context.Context,
	fence Fence,
	now time.Time,
	ttl time.Duration,
) error {
	if err := validateFence(fence); err != nil || now.IsZero() || ttl <= 0 {
		return ErrInvalidRequest
	}
	tag, err := p.pool.Exec(ctx, `
		UPDATE tool_operations SET
			lease_expires_at = clock_timestamp() + $1::interval,
			updated_at = clock_timestamp()
		WHERE tenant_id = $2 AND operation_key = $3
			AND current_attempt = $4 AND lease_owner_hash = $5
			AND state IN ('reserved', 'executing')
			AND lease_expires_at > clock_timestamp()`,
		durationInterval(ttl), fence.TenantID, fence.OperationKey, fence.AttemptNo, ownerHash(fence.Owner))
	if err != nil {
		return fixedError("renew lease", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (p *Postgres) MarkExecuting(
	ctx context.Context,
	fence Fence,
	now time.Time,
) (*Record, error) {
	if err := validateFence(fence); err != nil || now.IsZero() {
		return nil, ErrInvalidRequest
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fixedError("begin executing transition", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var state string
	var currentAttempt int
	var storedOwnerHash string
	var leaseActive bool
	err = tx.QueryRow(ctx, `
		SELECT state, current_attempt, lease_owner_hash,
			COALESCE(lease_expires_at > clock_timestamp(), false)
		FROM tool_operations
		WHERE tenant_id = $1 AND operation_key = $2
		FOR UPDATE`, fence.TenantID, fence.OperationKey).Scan(
		&state, &currentAttempt, &storedOwnerHash, &leaseActive)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fixedError("lock executing transition", err)
	}
	if currentAttempt != fence.AttemptNo || storedOwnerHash != ownerHash(fence.Owner) || !leaseActive {
		return nil, ErrLeaseLost
	}
	var phase string
	err = tx.QueryRow(ctx, `
		SELECT phase FROM tool_operation_attempts
		WHERE tenant_id = $1 AND operation_key = $2 AND attempt_no = $3
		FOR UPDATE`, fence.TenantID, fence.OperationKey, fence.AttemptNo).Scan(&phase)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrInvalidTransition
	}
	if err != nil {
		return nil, fixedError("lock executing attempt", err)
	}
	switch State(state) {
	case StateReserved:
		if AttemptPhase(phase) != AttemptReserved {
			return nil, ErrInvalidTransition
		}
		if _, err := tx.Exec(ctx, `
			UPDATE tool_operation_attempts SET
				phase = 'executing', executing_at = clock_timestamp()
			WHERE tenant_id = $1 AND operation_key = $2 AND attempt_no = $3`,
			fence.TenantID, fence.OperationKey, fence.AttemptNo); err != nil {
			return nil, fixedError("mark attempt executing", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE tool_operations SET
				state = 'executing', state_version = state_version + 1,
				updated_at = clock_timestamp()
			WHERE tenant_id = $1 AND operation_key = $2`, fence.TenantID, fence.OperationKey); err != nil {
			return nil, fixedError("mark operation executing", err)
		}
	case StateExecuting:
		if AttemptPhase(phase) != AttemptExecuting {
			return nil, ErrInvalidTransition
		}
	default:
		return nil, ErrInvalidTransition
	}
	record, err := scanOperationRecord(tx.QueryRow(ctx, `
		SELECT `+recordProjection+` FROM tool_operations
		WHERE tenant_id = $1 AND operation_key = $2`, fence.TenantID, fence.OperationKey))
	if err != nil {
		return nil, fixedError("read executing operation", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fixedError("commit executing transition", err)
	}
	return &record, nil
}

func (p *Postgres) Finish(
	ctx context.Context,
	req FinishRequest,
	now time.Time,
) (*Record, error) {
	if err := validateFinish(req, now); err != nil {
		return nil, err
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fixedError("begin finish", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var state string
	var currentAttempt int
	var storedOwnerHash string
	var leaseActive bool
	err = tx.QueryRow(ctx, `
		SELECT state, current_attempt, lease_owner_hash,
			COALESCE(lease_expires_at > clock_timestamp(), false)
		FROM tool_operations
		WHERE tenant_id = $1 AND operation_key = $2
		FOR UPDATE`, req.Fence.TenantID, req.Fence.OperationKey).Scan(
		&state, &currentAttempt, &storedOwnerHash, &leaseActive)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fixedError("lock finish operation", err)
	}

	var phase, attemptOwnerHash, storedFingerprint string
	err = tx.QueryRow(ctx, `
		SELECT phase, owner_hash, finish_fingerprint
		FROM tool_operation_attempts
		WHERE tenant_id = $1 AND operation_key = $2 AND attempt_no = $3
		FOR UPDATE`, req.Fence.TenantID, req.Fence.OperationKey, req.Fence.AttemptNo).Scan(
		&phase, &attemptOwnerHash, &storedFingerprint)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrInvalidTransition
	}
	if err != nil {
		return nil, fixedError("lock finish attempt", err)
	}
	fingerprint := finishFingerprint(req)
	if AttemptPhase(phase) == AttemptFinished {
		if currentAttempt != req.Fence.AttemptNo || attemptOwnerHash != ownerHash(req.Fence.Owner) ||
			storedFingerprint != fingerprint || State(state) != req.Outcome {
			return nil, ErrInvalidTransition
		}
		record, err := scanOperationRecord(tx.QueryRow(ctx, `
			SELECT `+recordProjection+` FROM tool_operations
			WHERE tenant_id = $1 AND operation_key = $2`, req.Fence.TenantID, req.Fence.OperationKey))
		if err != nil {
			return nil, fixedError("read finished replay", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fixedError("commit finished replay", err)
		}
		return &record, nil
	}
	if currentAttempt != req.Fence.AttemptNo || storedOwnerHash != ownerHash(req.Fence.Owner) ||
		attemptOwnerHash != ownerHash(req.Fence.Owner) || !leaseActive {
		return nil, ErrLeaseLost
	}
	if State(state) == StateReserved {
		if AttemptPhase(phase) != AttemptReserved ||
			(req.Outcome != StateRetryableNotApplied && req.Outcome != StatePermanentRejected) {
			return nil, ErrInvalidTransition
		}
	} else if State(state) == StateExecuting {
		if AttemptPhase(phase) != AttemptExecuting {
			return nil, ErrInvalidTransition
		}
	} else {
		return nil, ErrInvalidTransition
	}

	retryDelay := time.Duration(0)
	if req.Outcome == StateRetryableNotApplied {
		retryDelay = req.RetryAt.Sub(now)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE tool_operation_attempts SET
			phase = 'finished', outcome = $1, outcome_code = $2,
			result_hash = $3, replay_reference = $4, finish_fingerprint = $5,
			finished_at = clock_timestamp(),
			retry_at = CASE WHEN $1 = 'retryable_not_applied'
				THEN clock_timestamp() + $6::interval ELSE NULL END
		WHERE tenant_id = $7 AND operation_key = $8 AND attempt_no = $9`,
		string(req.Outcome), req.OutcomeCode, req.ResultHash, req.ReplayReference,
		fingerprint, durationInterval(retryDelay), req.Fence.TenantID, req.Fence.OperationKey,
		req.Fence.AttemptNo); err != nil {
		return nil, fixedError("finish attempt", err)
	}
	record, err := scanOperationRecord(tx.QueryRow(ctx, `
		UPDATE tool_operations SET
			state = $1, state_version = state_version + 1,
			lease_owner_hash = '', lease_expires_at = NULL,
			next_attempt_at = CASE WHEN $1 = 'retryable_not_applied'
				THEN clock_timestamp() + $2::interval ELSE NULL END,
			outcome_code = $3,
			result_hash = CASE WHEN $1 = 'confirmed' THEN $4 ELSE '' END,
			replay_reference = CASE WHEN $1 = 'confirmed' THEN $5 ELSE '' END,
			updated_at = clock_timestamp()
		WHERE tenant_id = $6 AND operation_key = $7
		RETURNING `+recordProjection,
		string(req.Outcome), durationInterval(retryDelay), req.OutcomeCode, req.ResultHash,
		req.ReplayReference, req.Fence.TenantID, req.Fence.OperationKey))
	if err != nil {
		return nil, fixedError("finish operation", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fixedError("commit finish", err)
	}
	return &record, nil
}

func (p *Postgres) Get(ctx context.Context, tenantID, operationID string) (*Record, error) {
	if !validTenant(tenantID) || !validOperationKey(operationID) {
		return nil, ErrInvalidRequest
	}
	record, err := scanOperationRecord(p.pool.QueryRow(ctx, `
		SELECT `+recordProjection+` FROM tool_operations
		WHERE tenant_id = $1 AND operation_key = $2`, tenantID, operationID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fixedError("get operation", err)
	}
	return &record, nil
}

func (p *Postgres) GetAttempt(
	ctx context.Context,
	tenantID, operationID string,
	attemptNo int,
) (*Attempt, error) {
	if !validTenant(tenantID) || !validOperationKey(operationID) || attemptNo <= 0 {
		return nil, ErrInvalidRequest
	}
	attempt, err := scanAttempt(p.pool.QueryRow(ctx, `
		SELECT tenant_id, operation_key, attempt_no, owner_hash, phase, outcome,
			outcome_code, result_hash, replay_reference, started_at, executing_at,
			finished_at, retry_at
		FROM tool_operation_attempts
		WHERE tenant_id = $1 AND operation_key = $2 AND attempt_no = $3`,
		tenantID, operationID, attemptNo))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fixedError("get attempt", err)
	}
	return &attempt, nil
}

func (p *Postgres) ReclaimExpired(
	ctx context.Context,
	now time.Time,
	limit int,
) (ReclaimResult, error) {
	if now.IsZero() || limit <= 0 || limit > maxLeaseBatch {
		return ReclaimResult{}, ErrInvalidRequest
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return ReclaimResult{}, fixedError("begin reclaim", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	result, err := reclaimExpiredTx(ctx, tx, limit)
	if err != nil {
		return ReclaimResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ReclaimResult{}, fixedError("commit reclaim", err)
	}
	return result, nil
}

func (p *Postgres) ListUnknown(ctx context.Context, tenantID string, limit int) ([]Record, error) {
	if !validTenant(tenantID) || limit <= 0 || limit > maxLeaseBatch {
		return nil, ErrInvalidRequest
	}
	rows, err := p.pool.Query(ctx, `
		SELECT `+recordProjection+` FROM tool_operations
		WHERE tenant_id = $1 AND state = 'unknown'
		ORDER BY updated_at, operation_key
		LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, fixedError("list unknown", err)
	}
	defer rows.Close()
	records := make([]Record, 0)
	for rows.Next() {
		record, err := scanOperationRecord(rows)
		if err != nil {
			return nil, fixedError("scan unknown", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fixedError("iterate unknown", err)
	}
	return records, nil
}

func (p *Postgres) ResolveUnknown(
	ctx context.Context,
	req ResolveRequest,
	now time.Time,
) (*Record, error) {
	if err := validateResolve(req, now); err != nil {
		return nil, err
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fixedError("begin resolution", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	replayed, found, err := readResolutionReplay(ctx, tx, req)
	if err != nil {
		return nil, err
	}
	if found {
		if err := tx.Commit(ctx); err != nil {
			return nil, fixedError("commit resolution replay", err)
		}
		return replayed, nil
	}

	var state string
	var stateVersion int64
	err = tx.QueryRow(ctx, `
		SELECT state, state_version FROM tool_operations
		WHERE tenant_id = $1 AND operation_key = $2
		FOR UPDATE`, req.TenantID, req.OperationKey).Scan(&state, &stateVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fixedError("lock unknown operation", err)
	}
	if State(state) != StateUnknown || stateVersion != req.ExpectedVersion {
		// A concurrent request can observe no resolution before waiting for the
		// operation row, then wake after the winner commits. Re-read under the
		// row lock so the same resolution ID is an idempotent acknowledgement,
		// not a spurious state-transition error.
		replayed, found, err := readResolutionReplay(ctx, tx, req)
		if err != nil {
			return nil, err
		}
		if found {
			if err := tx.Commit(ctx); err != nil {
				return nil, fixedError("commit concurrent resolution replay", err)
			}
			return replayed, nil
		}
		if State(state) != StateUnknown {
			return nil, ErrUnknownRequiresResolution
		}
		return nil, ErrInvalidTransition
	}

	toState := StatePermanentRejected
	switch req.Action {
	case ResolveConfirm:
		toState = StateConfirmed
	case ResolveRetryNotApplied:
		toState = StateRetryableNotApplied
	case ResolveReject:
		toState = StatePermanentRejected
	}
	retryDelay := time.Duration(0)
	if req.Action == ResolveRetryNotApplied {
		retryDelay = req.RetryAt.Sub(now)
	}
	record, err := scanOperationRecord(tx.QueryRow(ctx, `
		UPDATE tool_operations SET
			state = $1, state_version = state_version + 1,
			next_attempt_at = CASE WHEN $1 = 'retryable_not_applied'
				THEN clock_timestamp() + $2::interval ELSE NULL END,
			outcome_code = $3,
			result_hash = CASE WHEN $1 = 'confirmed' THEN $4 ELSE '' END,
			replay_reference = CASE WHEN $1 = 'confirmed' THEN $5 ELSE '' END,
			updated_at = clock_timestamp()
		WHERE tenant_id = $6 AND operation_key = $7
		RETURNING `+recordProjection,
		string(toState), durationInterval(retryDelay), req.ReasonCode, req.ResultHash,
		req.ReplayReference, req.TenantID, req.OperationKey))
	if err != nil {
		return nil, fixedError("apply unknown resolution", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO tool_operation_resolutions
			(tenant_id, resolution_id, operation_key, from_version, to_version,
			 action, actor_hash, reason_code, result_hash, replay_reference,
			 requested_retry_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		req.TenantID, req.ResolutionID, req.OperationKey, req.ExpectedVersion,
		record.StateVersion, string(req.Action), req.ActorHash, req.ReasonCode,
		req.ResultHash, req.ReplayReference, nullableTime(req.RetryAt))
	if isUniqueViolation(err) {
		return nil, ErrConflict
	}
	if err != nil {
		return nil, fixedError("record unknown resolution", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fixedError("commit unknown resolution", err)
	}
	return &record, nil
}

type operationScanner interface {
	Scan(...any) error
}

func scanOperationRecord(scanner operationScanner) (Record, error) {
	var record Record
	var toolName, operationClass, targetHash, state string
	var resultHash, replayReference string
	var leaseExpiresAt, nextAttemptAt pgtype.Timestamptz
	err := scanner.Scan(
		&record.TenantID, &record.OperationKey, &record.PayloadHash,
		&toolName, &operationClass, &targetHash, &state, &record.StateVersion,
		&record.AttemptCount, &record.CurrentAttempt, &leaseExpiresAt,
		&nextAttemptAt, &record.OutcomeCode, &resultHash, &replayReference,
		&record.CreatedAt, &record.UpdatedAt,
	)
	if err != nil {
		return Record{}, err
	}
	record.Metadata = Metadata{ToolName: toolName, OperationClass: operationClass, TargetHash: targetHash}
	record.State = State(state)
	if leaseExpiresAt.Valid {
		record.LeaseExpiresAt = leaseExpiresAt.Time
	}
	if nextAttemptAt.Valid {
		record.NextAttemptAt = nextAttemptAt.Time
	}
	if record.State == StateConfirmed {
		record.Confirmation = &Confirmation{
			ResultHash: resultHash, ReplayReference: replayReference, OutcomeCode: record.OutcomeCode,
		}
	}
	return record, nil
}

func scanAttempt(scanner operationScanner) (Attempt, error) {
	var attempt Attempt
	var phase, outcome string
	var executingAt, finishedAt, retryAt pgtype.Timestamptz
	err := scanner.Scan(
		&attempt.TenantID, &attempt.OperationKey, &attempt.AttemptNo,
		&attempt.OwnerHash, &phase, &outcome, &attempt.OutcomeCode,
		&attempt.ResultHash, &attempt.ReplayReference, &attempt.StartedAt,
		&executingAt, &finishedAt, &retryAt,
	)
	if err != nil {
		return Attempt{}, err
	}
	attempt.Phase = AttemptPhase(phase)
	attempt.Outcome = State(outcome)
	if executingAt.Valid {
		attempt.ExecutingAt = executingAt.Time
	}
	if finishedAt.Valid {
		attempt.FinishedAt = finishedAt.Time
	}
	if retryAt.Valid {
		attempt.RetryAt = retryAt.Time
	}
	return attempt, nil
}

type expiredOperation struct {
	tenantID, operationKey, state, ownerHash string
	attemptNo                                int
}

func reclaimExpiredTx(ctx context.Context, tx pgx.Tx, limit int) (ReclaimResult, error) {
	rows, err := tx.Query(ctx, `
		SELECT tenant_id, operation_key, state, current_attempt, lease_owner_hash
		FROM tool_operations
		WHERE lease_expires_at IS NOT NULL
			AND lease_expires_at <= clock_timestamp()
			AND state IN ('reserved', 'executing')
		ORDER BY lease_expires_at, tenant_id, operation_key
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return ReclaimResult{}, fixedError("select expired operations", err)
	}
	var expired []expiredOperation
	for rows.Next() {
		var item expiredOperation
		if err := rows.Scan(&item.tenantID, &item.operationKey, &item.state, &item.attemptNo, &item.ownerHash); err != nil {
			rows.Close()
			return ReclaimResult{}, fixedError("scan expired operation", err)
		}
		expired = append(expired, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ReclaimResult{}, fixedError("iterate expired operations", err)
	}
	rows.Close()

	result := ReclaimResult{}
	for _, item := range expired {
		if State(item.state) == StateExecuting {
			result.Unknown++
		} else {
			result.Retryable++
		}
		if err := finalizeExpiredOperationTx(ctx, tx, item); err != nil {
			return ReclaimResult{}, err
		}
	}
	return result, nil
}

func reclaimExpiredReservedForTenantTx(
	ctx context.Context,
	tx pgx.Tx,
	tenantID string,
	limit int,
) error {
	rows, err := tx.Query(ctx, `
		SELECT tenant_id, operation_key, state, current_attempt, lease_owner_hash
		FROM tool_operations
		WHERE tenant_id = $1
			AND lease_expires_at IS NOT NULL
			AND lease_expires_at <= clock_timestamp()
			AND state = 'reserved'
		ORDER BY lease_expires_at, operation_key
		LIMIT $2
		FOR UPDATE SKIP LOCKED`, tenantID, limit)
	if err != nil {
		return fixedError("select tenant expired reservations", err)
	}
	var expired []expiredOperation
	for rows.Next() {
		var item expiredOperation
		if err := rows.Scan(
			&item.tenantID, &item.operationKey, &item.state,
			&item.attemptNo, &item.ownerHash,
		); err != nil {
			rows.Close()
			return fixedError("scan tenant expired reservation", err)
		}
		expired = append(expired, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fixedError("iterate tenant expired reservations", err)
	}
	rows.Close()
	for _, item := range expired {
		if err := finalizeExpiredOperationTx(ctx, tx, item); err != nil {
			return err
		}
	}
	return nil
}

func finalizeExpiredOperationTx(ctx context.Context, tx pgx.Tx, item expiredOperation) error {
	outcome := StateRetryableNotApplied
	outcomeCode := leaseExpiredBeforeExec
	nextState := StateReserved
	if State(item.state) == StateExecuting {
		outcome = StateUnknown
		outcomeCode = leaseExpiredAfterExec
		nextState = StateUnknown
	}
	// The insert arm is a fail-closed repair for a corrupt/missing audit row.
	// It still stores only the already-hashed lease capability.
	if _, err := tx.Exec(ctx, `
		INSERT INTO tool_operation_attempts
			(tenant_id, operation_key, attempt_no, owner_hash, phase, outcome,
			 outcome_code, started_at, finished_at, retry_at)
		VALUES ($1, $2, $3, $4, 'finished', $5, $6,
			clock_timestamp(), clock_timestamp(),
			CASE WHEN $5 = 'retryable_not_applied' THEN clock_timestamp() ELSE NULL END)
		ON CONFLICT (tenant_id, operation_key, attempt_no) DO UPDATE SET
			phase = 'finished', outcome = EXCLUDED.outcome,
			outcome_code = EXCLUDED.outcome_code,
			result_hash = '', replay_reference = '', finish_fingerprint = '',
			finished_at = clock_timestamp(),
			retry_at = CASE WHEN EXCLUDED.outcome = 'retryable_not_applied'
				THEN clock_timestamp() ELSE NULL END`,
		item.tenantID, item.operationKey, item.attemptNo, item.ownerHash,
		string(outcome), outcomeCode); err != nil {
		return fixedError("finish expired attempt", err)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE tool_operations SET
			state = $1, state_version = state_version + 1,
			lease_owner_hash = '', lease_expires_at = NULL,
			next_attempt_at = CASE WHEN $1 = 'reserved' THEN clock_timestamp() ELSE NULL END,
			outcome_code = $2, result_hash = '', replay_reference = '',
			updated_at = clock_timestamp()
		WHERE tenant_id = $3 AND operation_key = $4
			AND state = $5 AND current_attempt = $6 AND lease_owner_hash = $7`,
		string(nextState), outcomeCode, item.tenantID, item.operationKey,
		item.state, item.attemptNo, item.ownerHash)
	if err != nil {
		return fixedError("reclaim expired operation", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

func readResolution(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, resolutionID string,
) (Resolution, bool, error) {
	var result Resolution
	var retryAt pgtype.Timestamptz
	var action string
	err := tx.QueryRow(ctx, `
		SELECT resolution_id, tenant_id, operation_key, from_version, to_version,
			action, actor_hash, reason_code, result_hash, replay_reference,
			requested_retry_at, resolved_at
		FROM tool_operation_resolutions
		WHERE tenant_id = $1 AND resolution_id = $2
		FOR SHARE`, tenantID, resolutionID).Scan(
		&result.ResolutionID, &result.TenantID, &result.OperationKey,
		&result.FromVersion, &result.ToVersion, &action, &result.ActorHash,
		&result.ReasonCode, &result.ResultHash, &result.ReplayReference,
		&retryAt, &result.ResolvedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Resolution{}, false, nil
	}
	if err != nil {
		return Resolution{}, false, fixedError("read resolution", err)
	}
	result.Action = ResolutionAction(action)
	if retryAt.Valid {
		result.RetryAt = retryAt.Time
	}
	return result, true, nil
}

func readResolutionReplay(
	ctx context.Context,
	tx pgx.Tx,
	req ResolveRequest,
) (*Record, bool, error) {
	previous, found, err := readResolution(ctx, tx, req.TenantID, req.ResolutionID)
	if err != nil || !found {
		return nil, found, err
	}
	if !samePostgresResolutionRequest(previous, req) {
		return nil, true, ErrConflict
	}
	record, err := scanOperationRecord(tx.QueryRow(ctx, `
		SELECT `+recordProjection+` FROM tool_operations
		WHERE tenant_id = $1 AND operation_key = $2`, req.TenantID, req.OperationKey))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, true, ErrNotFound
	}
	if err != nil {
		return nil, true, fixedError("read resolved operation", err)
	}
	if record.StateVersion != previous.ToVersion {
		return nil, true, ErrInvalidTransition
	}
	return &record, true, nil
}

func samePostgresResolutionRequest(previous Resolution, req ResolveRequest) bool {
	// pgx encodes timestamptz at PostgreSQL's microsecond precision. Compare
	// the persisted representation so a sub-microsecond caller timestamp does
	// not turn an otherwise identical retry into a false conflict.
	return previous.ResolutionID == req.ResolutionID && previous.TenantID == req.TenantID &&
		previous.OperationKey == req.OperationKey && previous.FromVersion == req.ExpectedVersion &&
		previous.Action == req.Action && previous.ActorHash == req.ActorHash &&
		previous.ReasonCode == req.ReasonCode && previous.ResultHash == req.ResultHash &&
		previous.ReplayReference == req.ReplayReference &&
		previous.RetryAt.Equal(req.RetryAt.Truncate(time.Microsecond))
}

func qualifiedRecordProjection(alias string) string {
	return alias + `.tenant_id, ` + alias + `.operation_key, ` + alias + `.payload_hash, ` +
		alias + `.tool_name, ` + alias + `.operation_class, ` + alias + `.target_hash, ` +
		alias + `.state, ` + alias + `.state_version, ` + alias + `.attempt_count, ` +
		alias + `.current_attempt, ` + alias + `.lease_expires_at, ` + alias + `.next_attempt_at, ` +
		alias + `.outcome_code, ` + alias + `.result_hash, ` + alias + `.replay_reference, ` +
		alias + `.created_at, ` + alias + `.updated_at`
}

func finishFingerprint(req FinishRequest) string {
	// This digest is only for idempotent acknowledgement replay; all fields are
	// already hashes or grammar-restricted redacted metadata.
	h := &digestWriter{}
	for _, value := range []string{
		string(req.Outcome), req.OutcomeCode, req.ResultHash,
		req.ReplayReference, req.RetryAt.UTC().Format(time.RFC3339Nano),
	} {
		h.add(value)
	}
	return h.sum()
}

type digestWriter struct {
	parts []byte
}

func (w *digestWriter) add(value string) {
	// Reuse the package's canonical length framing without exposing a hash
	// object through the public API.
	w.parts = append(w.parts, byte(len(value)>>24), byte(len(value)>>16), byte(len(value)>>8), byte(len(value)))
	w.parts = append(w.parts, value...)
}

func (w *digestWriter) sum() string { return HashPayload(w.parts) }

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

var _ Ledger = (*Postgres)(nil)
