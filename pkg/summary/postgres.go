package summary

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// PostgresStore is the durable summary coordination store. The complete
// embedded migration set must be applied before constructing the store.
type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(db *sql.DB) *PostgresStore { return &PostgresStore{db: db} }

func (s *PostgresStore) Enqueue(ctx context.Context, request EnqueueRequest) (EnqueueResult, error) {
	if err := request.Validate(); err != nil {
		return EnqueueResult{}, err
	}
	if s == nil || s.db == nil {
		return EnqueueResult{}, ErrStoreUnavailable
	}
	ctx = nonNilContext(ctx)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return EnqueueResult{}, fmt.Errorf("enqueue summary job begin: %w", err)
	}
	defer tx.Rollback()
	result, err := EnqueueTx(ctx, tx, request)
	if err != nil {
		return EnqueueResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return EnqueueResult{}, fmt.Errorf("enqueue summary job commit: %w", err)
	}
	return result, nil
}

// EnqueueTx creates or coalesces a Summary job inside a caller-owned
// PostgreSQL transaction. It is the atomic bridge used by Inbox completion;
// callers retain responsibility for commit or rollback.
func EnqueueTx(ctx context.Context, tx *sql.Tx, request EnqueueRequest) (EnqueueResult, error) {
	if err := request.Validate(); err != nil {
		return EnqueueResult{}, err
	}
	if tx == nil {
		return EnqueueResult{}, ErrStoreUnavailable
	}
	ctx = nonNilContext(ctx)
	maxAttempts := request.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = DefaultMaxAttempts
	}
	// Insert first with DO NOTHING. A concurrent enqueue either returns the
	// newly-created row or waits for the conflicting unique-key transaction;
	// the follow-up SELECT then locks the canonical row instead of surfacing a
	// spurious unique-violation to the caller.
	row := tx.QueryRowContext(ctx, `
			INSERT INTO summary_jobs (
				tenant_id, agent_app_id, agent_version_id, session_owner_id, session_id, filter_key,
				target_event_sequence, status, max_attempts,
				attempts, completed_event_sequence, last_error, target_resolution_lease_version, session_incarnation_id
			) VALUES ($1,$2,$3,$4,$5,$6,$7,'PENDING',$8,0,0,'',CASE WHEN $7::bigint=0 THEN 1 ELSE 0 END,$9)
			ON CONFLICT (tenant_id, agent_app_id, session_owner_id, session_id, filter_key, session_incarnation_id) DO NOTHING
			RETURNING `+summaryColumns,
		request.TenantID, request.AgentAppID, request.AgentVersionID, request.SessionOwnerID, request.SessionID, request.FilterKey,
		request.TargetEventSequence, maxAttempts, request.SessionIncarnationID)
	job, err := scanJob(row)
	created := true
	if errors.Is(err, sql.ErrNoRows) {
		row = tx.QueryRowContext(ctx, summarySelect+` WHERE tenant_id=$1 AND agent_app_id=$2 AND session_owner_id=$3 AND session_id=$4 AND filter_key=$5 AND session_incarnation_id=$6 FOR UPDATE`,
			request.TenantID, request.AgentAppID, request.SessionOwnerID, request.SessionID, request.FilterKey, request.SessionIncarnationID)
		job, err = scanJob(row)
		created = false
	}
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return EnqueueResult{}, ErrJobNotFound
		}
		return EnqueueResult{}, fmt.Errorf("enqueue summary job load: %w", err)
	}
	if !created {
		updated, err := mergeEnqueue(&job, request, time.Now().UTC())
		if err != nil {
			return EnqueueResult{}, err
		}
		if updated {
			row = tx.QueryRowContext(ctx, `
				UPDATE summary_jobs
				SET agent_version_id=$2, target_event_sequence=$3, status=$4, attempts=$5,
				    max_attempts=$6, next_attempt_at=$7, last_error=$8,
				    target_resolution_lease_version=$9, updated_at=now()
				WHERE id=$1
				RETURNING `+summaryColumns,
				job.ID, job.AgentVersionID, job.TargetEventSequence, job.Status, job.Attempts,
				job.MaxAttempts, nullableTime(job.NextAttemptAt), job.LastError, job.TargetResolutionLeaseVersion)
			job, err = scanJob(row)
			if err != nil {
				return EnqueueResult{}, fmt.Errorf("enqueue summary job update: %w", err)
			}
		}
	}
	return EnqueueResult{Job: job, Created: created, Coalesced: !created}, nil
}

func (s *PostgresStore) Get(ctx context.Context, id int64) (Job, error) {
	if s == nil || s.db == nil {
		return Job{}, ErrStoreUnavailable
	}
	row := s.db.QueryRowContext(nonNilContext(ctx), summarySelect+` WHERE id=$1`, id)
	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrJobNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("get summary job: %w", err)
	}
	return job, nil
}

func (s *PostgresStore) Claim(ctx context.Context, owner string, ttl time.Duration) (Job, error) {
	if s == nil || s.db == nil {
		return Job{}, ErrStoreUnavailable
	}
	if err := validateLeaseRequest(owner, ttl); err != nil {
		return Job{}, err
	}
	ctx = nonNilContext(ctx)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, fmt.Errorf("claim summary job begin: %w", err)
	}
	defer tx.Rollback()
	row := tx.QueryRowContext(ctx, summarySelect+`
		WHERE attempts < max_attempts
		  AND ((status IN ('PENDING','FAILED')
		        AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp()))
		    OR (status='PROCESSING' AND lease_until IS NOT NULL AND lease_until <= clock_timestamp()))
		ORDER BY updated_at, id
		FOR UPDATE SKIP LOCKED
		LIMIT 1`)
	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrNoWork
	}
	if err != nil {
		return Job{}, fmt.Errorf("claim summary job load: %w", err)
	}
	row = tx.QueryRowContext(ctx, `
		UPDATE summary_jobs
		SET status='PROCESSING', attempts=attempts+1,
		    lease_owner=$1, lease_version=lease_version+1,
		    lease_until=clock_timestamp()+($2 * INTERVAL '1 millisecond'),
		    next_attempt_at=NULL, updated_at=now()
		WHERE id=$3
		RETURNING `+summaryColumns, owner, ttl.Milliseconds(), job.ID)
	job, err = scanJob(row)
	if err != nil {
		return Job{}, fmt.Errorf("claim summary job update: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Job{}, fmt.Errorf("claim summary job commit: %w", err)
	}
	return job, nil
}

func (s *PostgresStore) Renew(ctx context.Context, claimed Job, ttl time.Duration) (Job, error) {
	if s == nil || s.db == nil {
		return Job{}, ErrStoreUnavailable
	}
	if err := validateLeaseRequest(claimed.LeaseOwner, ttl); err != nil {
		return Job{}, err
	}
	row := s.db.QueryRowContext(nonNilContext(ctx), `
		UPDATE summary_jobs
		SET lease_until=clock_timestamp()+($1 * INTERVAL '1 millisecond'), updated_at=now()
		WHERE id=$2 AND status='PROCESSING' AND lease_owner=$3
		  AND lease_version=$4 AND lease_until > clock_timestamp()
		RETURNING `+summaryColumns, ttl.Milliseconds(), claimed.ID,
		claimed.LeaseOwner, claimed.LeaseVersion)
	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrStaleLease
	}
	if err != nil {
		return Job{}, fmt.Errorf("renew summary job: %w", err)
	}
	return job, nil
}

func (s *PostgresStore) ResolveTarget(ctx context.Context, claimed Job, sequence int64) (Job, error) {
	if s == nil || s.db == nil {
		return Job{}, ErrStoreUnavailable
	}
	if sequence <= 0 {
		return Job{}, fmt.Errorf("%w: resolved target event sequence", ErrInvalidJob)
	}
	row := s.db.QueryRowContext(nonNilContext(ctx), `
		UPDATE summary_jobs
		SET target_event_sequence=CASE WHEN target_event_sequence=0 OR ($5::bigint>0 AND target_resolution_lease_version>0)
		                              THEN GREATEST(target_event_sequence,$1) ELSE target_event_sequence END,
		    target_resolution_lease_version=CASE WHEN target_resolution_lease_version <= $4
		                                         THEN 0 ELSE target_resolution_lease_version END,
		    updated_at=now()
		WHERE id=$2 AND status='PROCESSING' AND lease_owner=$3
		  AND lease_version=$4 AND lease_until > clock_timestamp()
		RETURNING `+summaryColumns, sequence, claimed.ID, claimed.LeaseOwner, claimed.LeaseVersion, claimed.TargetResolutionLeaseVersion)
	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrStaleLease
	}
	if err != nil {
		return Job{}, fmt.Errorf("resolve summary target: %w", err)
	}
	return job, nil
}

func (s *PostgresStore) Fail(ctx context.Context, claimed Job, cause error, retryAt time.Time) (Job, error) {
	if s == nil || s.db == nil {
		return Job{}, ErrStoreUnavailable
	}
	row := s.db.QueryRowContext(nonNilContext(ctx), `
		UPDATE summary_jobs
		SET status='FAILED', last_error=$1,
		    next_attempt_at=CASE WHEN attempts < max_attempts AND $2::timestamptz > clock_timestamp()
		                         THEN $2 ELSE NULL END,
		    lease_owner=NULL, lease_until=NULL, updated_at=now()
		WHERE id=$3 AND status='PROCESSING' AND lease_owner=$4
		  AND lease_version=$5 AND lease_until > clock_timestamp()
		RETURNING `+summaryColumns, errorText(cause), nullableTime(retryAt), claimed.ID,
		claimed.LeaseOwner, claimed.LeaseVersion)
	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrStaleLease
	}
	if err != nil {
		return Job{}, fmt.Errorf("fail summary job: %w", err)
	}
	return job, nil
}

func (s *PostgresStore) Complete(ctx context.Context, claimed Job, observedSequence int64) (Job, error) {
	if s == nil || s.db == nil {
		return Job{}, ErrStoreUnavailable
	}
	if observedSequence < 0 {
		return Job{}, fmt.Errorf("%w: observed sequence", ErrInvalidCandidate)
	}
	row := s.db.QueryRowContext(nonNilContext(ctx), `
		UPDATE summary_jobs
		SET status=CASE WHEN $1 >= target_event_sequence AND target_resolution_lease_version=0 THEN 'COMPLETED' ELSE 'PENDING' END,
		    attempts=CASE WHEN $1 < target_event_sequence OR target_resolution_lease_version>0 THEN 0 ELSE attempts END,
		    completed_event_sequence=GREATEST(completed_event_sequence,$1),
		    last_error='', lease_owner=NULL, lease_until=NULL,
		    next_attempt_at=NULL, updated_at=now()
		WHERE id=$2 AND status='PROCESSING' AND lease_owner=$3
		  AND lease_version=$4 AND lease_until > clock_timestamp()
		  AND $1 >= completed_event_sequence
		RETURNING `+summaryColumns, observedSequence, claimed.ID,
		claimed.LeaseOwner, claimed.LeaseVersion)
	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrStaleLease
	}
	if err != nil {
		return Job{}, fmt.Errorf("complete summary job: %w", err)
	}
	return job, nil
}

const summaryColumns = `
	id, tenant_id, agent_app_id, agent_version_id, session_owner_id, session_id, filter_key,
	target_event_sequence, status, COALESCE(lease_owner,''), lease_version,
	lease_until, attempts, max_attempts, next_attempt_at, COALESCE(last_error,''),
	completed_event_sequence, created_at, updated_at, target_resolution_lease_version, session_incarnation_id`

const summarySelect = `SELECT ` + summaryColumns + ` FROM summary_jobs`

type rowScanner interface{ Scan(...any) error }

func scanJob(row rowScanner) (Job, error) {
	var (
		job                       Job
		status                    string
		leaseUntil, nextAttemptAt sql.NullTime
	)
	err := row.Scan(
		&job.ID, &job.TenantID, &job.AgentAppID, &job.AgentVersionID, &job.SessionOwnerID, &job.SessionID, &job.FilterKey,
		&job.TargetEventSequence, &status, &job.LeaseOwner, &job.LeaseVersion,
		&leaseUntil, &job.Attempts, &job.MaxAttempts, &nextAttemptAt, &job.LastError,
		&job.CompletedEventSequence, &job.CreatedAt, &job.UpdatedAt, &job.TargetResolutionLeaseVersion, &job.SessionIncarnationID,
	)
	if err != nil {
		return Job{}, err
	}
	job.Status = Status(status)
	if leaseUntil.Valid {
		job.LeaseUntil = leaseUntil.Time
	}
	if nextAttemptAt.Valid {
		job.NextAttemptAt = nextAttemptAt.Time
	}
	if err := job.Validate(); err != nil {
		return Job{}, err
	}
	return job, nil
}

func mergeEnqueue(job *Job, request EnqueueRequest, now time.Time) (bool, error) {
	oldTarget := job.TargetEventSequence
	oldMaxAttempts := job.MaxAttempts
	oldVersionID := job.AgentVersionID
	oldResolutionLease := job.TargetResolutionLeaseVersion
	if request.TargetEventSequence > 0 && request.TargetEventSequence == job.TargetEventSequence && request.AgentVersionID != job.AgentVersionID {
		return false, ErrSummaryVersionConflict
	}
	if request.TargetEventSequence > job.TargetEventSequence {
		job.TargetEventSequence = request.TargetEventSequence
		job.AgentVersionID = request.AgentVersionID
	}
	if request.TargetEventSequence == 0 {
		job.TargetResolutionLeaseVersion = max(job.TargetResolutionLeaseVersion, job.LeaseVersion+1)
		job.AgentVersionID = request.AgentVersionID
	}
	if job.Status == StatusProcessing && (request.TargetEventSequence == 0 || request.TargetEventSequence > oldTarget) {
		// The current attempt belongs to the earlier target. Fresh work must
		// retain its own retry budget if that attempt fails or loses its lease.
		job.Attempts = 0
	}
	reset := request.Force || request.TargetEventSequence == 0
	if job.Status == StatusCompleted && job.TargetEventSequence > job.CompletedEventSequence {
		reset = true
	}
	if job.Status == StatusFailed && (request.Force || request.TargetEventSequence > oldTarget) {
		reset = true
	}
	if reset && job.Status != StatusProcessing {
		job.Status = StatusPending
		job.Attempts = 0
		job.NextAttemptAt = time.Time{}
		job.LastError = ""
	}
	if request.MaxAttempts > 0 && request.MaxAttempts > job.MaxAttempts {
		job.MaxAttempts = request.MaxAttempts
	}
	changed := job.TargetEventSequence != oldTarget || job.AgentVersionID != oldVersionID || reset ||
		job.TargetResolutionLeaseVersion != oldResolutionLease ||
		(request.MaxAttempts > 0 && request.MaxAttempts > oldMaxAttempts)
	job.UpdatedAt = now
	return changed, job.Validate()
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}

func nonNilContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// PostgresSink stores visible summaries and performs the sequence/hash CAS
// under a row lock. It is independent from the job store so a custom Session
// backend can implement the same contract without copying platform tables.
type PostgresSink struct {
	db *sql.DB
}

func NewPostgresSink(db *sql.DB) *PostgresSink { return &PostgresSink{db: db} }

func (s *PostgresSink) Publish(ctx context.Context, candidate Candidate) (PublishResult, error) {
	return s.publish(ctx, candidate, nil)
}

// PublishFenced binds the checkpoint write to the exact summary job lease in
// the same PostgreSQL transaction. This closes the race where an old worker
// finishes generation after its lease has been replaced.
func (s *PostgresSink) PublishFenced(ctx context.Context, candidate Candidate, claimed Job) (PublishResult, error) {
	return s.publish(ctx, candidate, &claimed)
}

func (s *PostgresSink) publish(ctx context.Context, candidate Candidate, claimed *Job) (PublishResult, error) {
	if err := candidate.Validate(); err != nil {
		return PublishResult{}, err
	}
	if s == nil || s.db == nil {
		return PublishResult{}, ErrSinkUnavailable
	}
	candidate.ContentSHA256 = strings.ToLower(candidate.ContentSHA256)
	ctx = nonNilContext(ctx)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PublishResult{}, fmt.Errorf("publish summary begin: %w", err)
	}
	defer tx.Rollback()
	if claimed != nil {
		var tenantID, appID, sessionOwnerID, sessionID, filterKey, incarnationID string
		err := tx.QueryRowContext(ctx, `
			SELECT tenant_id, agent_app_id, session_owner_id, session_id, filter_key, session_incarnation_id
			FROM summary_jobs
			WHERE id=$1 AND status='PROCESSING' AND lease_owner=$2
			  AND lease_version=$3 AND lease_until > clock_timestamp()
			FOR UPDATE`, claimed.ID, claimed.LeaseOwner, claimed.LeaseVersion).
			Scan(&tenantID, &appID, &sessionOwnerID, &sessionID, &filterKey, &incarnationID)
		if errors.Is(err, sql.ErrNoRows) {
			return PublishResult{}, ErrStaleLease
		}
		if err != nil {
			return PublishResult{}, fmt.Errorf("fence summary publication: %w", err)
		}
		if tenantID != candidate.Key.TenantID || appID != candidate.Key.AgentAppID || sessionOwnerID != candidate.Key.SessionOwnerID ||
			sessionID != candidate.Key.SessionID || filterKey != candidate.Key.FilterKey || incarnationID != candidate.Key.SessionIncarnationID {
			return PublishResult{}, ErrSummaryScope
		}
	}
	// Insert first with DO NOTHING. Concurrent first writers then converge on
	// the locked canonical row instead of surfacing a unique-key race.
	row := tx.QueryRowContext(ctx, `
		INSERT INTO summary_checkpoints (
			tenant_id, agent_app_id, session_owner_id, session_id, filter_key,
			max_event_sequence, content, content_sha256, cutoff_at, last_event_id, session_incarnation_id
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (tenant_id, agent_app_id, session_owner_id, session_id, filter_key, session_incarnation_id) DO NOTHING
		RETURNING tenant_id, agent_app_id, session_owner_id, session_id, filter_key,
		          max_event_sequence, content, content_sha256, cutoff_at, last_event_id, updated_at, session_incarnation_id`,
		candidate.Key.TenantID, candidate.Key.AgentAppID, candidate.Key.SessionOwnerID, candidate.Key.SessionID, candidate.Key.FilterKey,
		candidate.EventSequence, candidate.Content, candidate.ContentSHA256, candidate.CutoffAt.UTC(), candidate.LastEventID, candidate.Key.SessionIncarnationID)
	current, err := scanCheckpoint(row)
	inserted := true
	if errors.Is(err, sql.ErrNoRows) {
		inserted = false
		row = tx.QueryRowContext(ctx, `
			SELECT tenant_id, agent_app_id, session_owner_id, session_id, filter_key,
			       max_event_sequence, content, content_sha256, cutoff_at, last_event_id, updated_at, session_incarnation_id
			FROM summary_checkpoints
			WHERE tenant_id=$1 AND agent_app_id=$2 AND session_owner_id=$3 AND session_id=$4 AND filter_key=$5 AND session_incarnation_id=$6
			FOR UPDATE`, candidate.Key.TenantID, candidate.Key.AgentAppID, candidate.Key.SessionOwnerID, candidate.Key.SessionID, candidate.Key.FilterKey, candidate.Key.SessionIncarnationID)
		current, err = scanCheckpoint(row)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return PublishResult{}, fmt.Errorf("publish summary conflict row disappeared")
			}
			return PublishResult{}, fmt.Errorf("publish summary load after conflict: %w", err)
		}
	} else if err != nil {
		return PublishResult{}, fmt.Errorf("publish summary insert: %w", err)
	}
	if inserted {
		if err := tx.Commit(); err != nil {
			return PublishResult{}, fmt.Errorf("publish summary insert commit: %w", err)
		}
		return PublishResult{Checkpoint: current, Applied: true}, nil
	}
	if candidate.EventSequence < current.EventSequence {
		return PublishResult{Checkpoint: current}, ErrSummaryStale
	}
	if candidate.EventSequence == current.EventSequence {
		if !strings.EqualFold(candidate.ContentSHA256, current.ContentSHA256) ||
			!candidate.CutoffAt.Equal(current.CutoffAt) || candidate.LastEventID != current.LastEventID {
			return PublishResult{Checkpoint: current}, ErrSummaryConflict
		}
		if err := tx.Commit(); err != nil {
			return PublishResult{}, fmt.Errorf("publish summary idempotent commit: %w", err)
		}
		return PublishResult{Checkpoint: current}, nil
	}
	row = tx.QueryRowContext(ctx, `
		UPDATE summary_checkpoints
		SET max_event_sequence=$1, content=$2, content_sha256=$3,
		    cutoff_at=$4, last_event_id=$5, updated_at=now()
		WHERE tenant_id=$6 AND agent_app_id=$7 AND session_owner_id=$8 AND session_id=$9 AND filter_key=$10 AND session_incarnation_id=$11
		RETURNING tenant_id, agent_app_id, session_owner_id, session_id, filter_key,
		          max_event_sequence, content, content_sha256, cutoff_at, last_event_id, updated_at, session_incarnation_id`,
		candidate.EventSequence, candidate.Content, candidate.ContentSHA256, candidate.CutoffAt.UTC(), candidate.LastEventID,
		candidate.Key.TenantID, candidate.Key.AgentAppID, candidate.Key.SessionOwnerID, candidate.Key.SessionID, candidate.Key.FilterKey, candidate.Key.SessionIncarnationID)
	current, err = scanCheckpoint(row)
	if err != nil {
		return PublishResult{}, fmt.Errorf("publish summary update: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return PublishResult{}, fmt.Errorf("publish summary update commit: %w", err)
	}
	return PublishResult{Checkpoint: current, Applied: true}, nil
}

func (s *PostgresSink) Get(ctx context.Context, key Key) (Checkpoint, bool, error) {
	if err := key.Validate(); err != nil {
		return Checkpoint{}, false, err
	}
	if s == nil || s.db == nil {
		return Checkpoint{}, false, ErrSinkUnavailable
	}
	row := s.db.QueryRowContext(nonNilContext(ctx), `
		SELECT tenant_id, agent_app_id, session_owner_id, session_id, filter_key,
		       max_event_sequence, content, content_sha256, cutoff_at, last_event_id, updated_at, session_incarnation_id
		FROM summary_checkpoints
		WHERE tenant_id=$1 AND agent_app_id=$2 AND session_owner_id=$3 AND session_id=$4 AND filter_key=$5 AND session_incarnation_id=$6`,
		key.TenantID, key.AgentAppID, key.SessionOwnerID, key.SessionID, key.FilterKey, key.SessionIncarnationID)
	checkpoint, err := scanCheckpoint(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Checkpoint{}, false, nil
	}
	if err != nil {
		return Checkpoint{}, false, fmt.Errorf("get summary checkpoint: %w", err)
	}
	return checkpoint, true, nil
}

func scanCheckpoint(row rowScanner) (Checkpoint, error) {
	var checkpoint Checkpoint
	err := row.Scan(
		&checkpoint.Key.TenantID, &checkpoint.Key.AgentAppID, &checkpoint.Key.SessionOwnerID, &checkpoint.Key.SessionID,
		&checkpoint.Key.FilterKey, &checkpoint.EventSequence, &checkpoint.Content,
		&checkpoint.ContentSHA256, &checkpoint.CutoffAt, &checkpoint.LastEventID, &checkpoint.UpdatedAt, &checkpoint.Key.SessionIncarnationID)
	if err != nil {
		return Checkpoint{}, err
	}
	if err := checkpoint.Validate(); err != nil {
		return Checkpoint{}, err
	}
	return checkpoint, nil
}
