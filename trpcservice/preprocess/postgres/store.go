package postgres

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

type Store struct{ db *sql.DB }

func New(db *sql.DB) *Store { return &Store{db: db} }

func (s *Store) ClaimInboxAndSchedule(ctx context.Context, in preprocess.ClaimRequest) (messaging.InboxRecord, preprocess.Job, error) {
	if s == nil || s.db == nil || in.Inbox.TenantID == "" || in.Inbox.Channel == "" || in.Inbox.ExternalAccountID == "" ||
		in.Inbox.ExternalMessageID == "" || in.Inbox.AgentAppID == "" || in.Inbox.SessionID == "" || in.Inbox.PayloadDigest == "" ||
		in.Inbox.KeyVersion < 1 || in.Inbox.InitialState != messaging.InboxPreprocessPending || in.TenantVersion < 1 ||
		in.ConfigVersion < 1 || in.ChannelBindingID == "" || in.UserID == "" {
		return messaging.InboxRecord{}, preprocess.Job{}, runtime.ErrInvariantViolation
	}
	requestID, payloadRef := messaging.StableInboxIdentity(in.Inbox.InboxKey)
	jobID, _ := preprocess.StableJobID(in.Inbox.TenantID, requestID)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return messaging.InboxRecord{}, preprocess.Job{}, err
	}
	defer func() { _ = tx.Rollback() }()
	inbox, err := scanInbox(tx.QueryRowContext(ctx, `SELECT tenant_id,channel,external_account_id,external_message_id,
request_id,agent_app_id,COALESCE(session_id,''),external_chat_id,external_user_id,COALESCE(input_seq,0),state,payload_ref,payload_digest,
key_version,version,COALESCE(terminal_reason,''),COALESCE(result_ref,''),created_at,updated_at
FROM claim_channel_inbox($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		in.Inbox.TenantID, in.Inbox.Channel, in.Inbox.ExternalAccountID, in.Inbox.ExternalMessageID, requestID,
		in.Inbox.AgentAppID, in.Inbox.SessionID, in.Inbox.ExternalChatID, in.Inbox.ExternalUserID, payloadRef,
		in.Inbox.PayloadDigest, in.Inbox.KeyVersion, string(in.Inbox.InitialState)))
	if err != nil {
		return messaging.InboxRecord{}, preprocess.Job{}, classify(err)
	}
	job, err := scanJob(tx.QueryRowContext(ctx, `INSERT INTO preprocess_job(
tenant_id,request_id,job_id,tenant_version,config_version,channel_binding_id,agent_app_id,session_id,user_id,channel,payload_ref,traceparent)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
ON CONFLICT (tenant_id,request_id) DO UPDATE SET request_id=EXCLUDED.request_id
RETURNING tenant_id,request_id,job_id,payload_ref,COALESCE(prepared_payload_ref,''),agent_app_id,session_id,user_id,channel,channel_binding_id,traceparent,tenant_version,config_version,
state,attempt,version,COALESCE(lease_owner,''),reject_reason,COALESCE(lease_until,'epoch'),not_before,created_at,updated_at,COALESCE(dispatched_at,'epoch')`,
		in.Inbox.TenantID, requestID, jobID, in.TenantVersion, in.ConfigVersion, in.ChannelBindingID,
		in.Inbox.AgentAppID, in.Inbox.SessionID, in.UserID, in.Inbox.Channel, payloadRef, in.TraceParent))
	if err != nil {
		return messaging.InboxRecord{}, preprocess.Job{}, classify(err)
	}
	if job.JobID != jobID || job.TenantVersion != in.TenantVersion || job.AgentAppID != in.Inbox.AgentAppID ||
		job.SessionID != in.Inbox.SessionID || job.UserID != in.UserID || job.Channel != in.Inbox.Channel ||
		job.ChannelBindingID != in.ChannelBindingID || job.ConfigVersion != in.ConfigVersion ||
		job.PayloadRef != payloadRef {
		return messaging.InboxRecord{}, preprocess.Job{}, runtime.ErrIdempotencyCollision
	}
	if err := tx.Commit(); err != nil {
		return messaging.InboxRecord{}, preprocess.Job{}, err
	}
	return inbox, job, nil
}

func (s *Store) ClaimJobs(ctx context.Context, options preprocess.ClaimOptions) ([]preprocess.Job, error) {
	if s == nil || s.db == nil || options.Owner == "" || options.Now.IsZero() || options.TTL <= 0 || options.Limit < 1 {
		return nil, runtime.ErrInvariantViolation
	}
	rows, err := s.db.QueryContext(ctx, `WITH candidates AS (
SELECT tenant_id,job_id FROM preprocess_job
WHERE ((state IN ('pending','retry_wait') AND not_before <= $1) OR (state='running' AND lease_until <= $1))
ORDER BY not_before,created_at FOR UPDATE SKIP LOCKED LIMIT $2
)
UPDATE preprocess_job j SET state='running',attempt=j.attempt+1,lease_owner=$3,
lease_until=$1+($4 * interval '1 microsecond'),version=j.version+1,updated_at=$1
FROM candidates c WHERE j.tenant_id=c.tenant_id AND j.job_id=c.job_id
RETURNING j.tenant_id,j.request_id,j.job_id,j.payload_ref,COALESCE(j.prepared_payload_ref,''),j.agent_app_id,j.session_id,j.user_id,j.channel,j.channel_binding_id,j.traceparent,j.tenant_version,j.config_version,
j.state,j.attempt,j.version,COALESCE(j.lease_owner,''),j.reject_reason,COALESCE(j.lease_until,'epoch'),j.not_before,j.created_at,j.updated_at,COALESCE(j.dispatched_at,'epoch')`,
		options.Now, options.Limit, options.Owner, options.TTL.Microseconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []preprocess.Job
	for rows.Next() {
		job, scanErr := scanJob(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (s *Store) FinishReady(ctx context.Context, job preprocess.Job) (preprocess.Job, error) {
	return s.finish(ctx, job, preprocess.Ready, time.Time{}, "")
}

func (s *Store) FinishRetry(ctx context.Context, job preprocess.Job, notBefore time.Time, reason string) (preprocess.Job, error) {
	if notBefore.IsZero() || reason == "" {
		return preprocess.Job{}, runtime.ErrInvariantViolation
	}
	return s.finish(ctx, job, preprocess.RetryWait, notBefore, reason)
}

func (s *Store) FinishRejected(ctx context.Context, job preprocess.Job, reason string) (preprocess.Job, error) {
	if reason == "" {
		return preprocess.Job{}, runtime.ErrInvariantViolation
	}
	return s.finish(ctx, job, preprocess.Rejected, time.Time{}, reason)
}

func (s *Store) finish(ctx context.Context, job preprocess.Job, state preprocess.State, notBefore time.Time, reason string) (preprocess.Job, error) {
	if state != preprocess.Ready && job.PreparedPayloadRef != "" {
		return preprocess.Job{}, runtime.ErrInvariantViolation
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return preprocess.Job{}, err
	}
	defer func() { _ = tx.Rollback() }()
	updated, err := scanJob(tx.QueryRowContext(ctx, `UPDATE preprocess_job SET state=$4,not_before=CASE WHEN $4='retry_wait' THEN $5 ELSE not_before END,
prepared_payload_ref=CASE WHEN $8 <> '' THEN $8 ELSE prepared_payload_ref END,
reject_reason=$6,lease_owner=NULL,lease_until=NULL,version=version+1,updated_at=now()
WHERE tenant_id=$1 AND job_id=$2 AND version=$3 AND state='running' AND lease_owner=$7
RETURNING tenant_id,request_id,job_id,payload_ref,COALESCE(prepared_payload_ref,''),agent_app_id,session_id,user_id,channel,channel_binding_id,traceparent,tenant_version,config_version,
state,attempt,version,COALESCE(lease_owner,''),reject_reason,COALESCE(lease_until,'epoch'),not_before,created_at,updated_at,COALESCE(dispatched_at,'epoch')`,
		job.TenantID, job.JobID, job.Version, string(state), nullableTime(notBefore), reason, job.LeaseOwner, job.PreparedPayloadRef))
	if errors.Is(err, runtime.ErrNotFound) {
		return preprocess.Job{}, runtime.ErrVersionConflict
	}
	if err != nil {
		return preprocess.Job{}, err
	}
	if state == preprocess.Ready || state == preprocess.Rejected {
		inboxState, terminalReason := string(messaging.InboxDispatchPending), ""
		if state == preprocess.Rejected {
			inboxState, terminalReason = string(messaging.InboxTerminal), reason
		}
		result, execErr := tx.ExecContext(ctx, `UPDATE inbox SET state=$3,terminal_reason=NULLIF($4,''),version=version+1,updated_at=now()
WHERE tenant_id=$1 AND request_id=$2 AND state='preprocess_pending'`, job.TenantID, job.RequestID, inboxState, terminalReason)
		if execErr != nil {
			return preprocess.Job{}, classify(execErr)
		}
		rows, _ := result.RowsAffected()
		if rows != 1 {
			return preprocess.Job{}, runtime.ErrVersionConflict
		}
	}
	if err := tx.Commit(); err != nil {
		return preprocess.Job{}, err
	}
	return updated, nil
}

func (s *Store) ListReadyForDispatch(ctx context.Context, limit int) ([]preprocess.Job, error) {
	if s == nil || s.db == nil || limit < 1 {
		return nil, runtime.ErrInvariantViolation
	}
	rows, err := s.db.QueryContext(ctx, `SELECT tenant_id,request_id,job_id,payload_ref,COALESCE(prepared_payload_ref,''),agent_app_id,session_id,user_id,channel,channel_binding_id,traceparent,tenant_version,config_version,
state,attempt,version,COALESCE(lease_owner,''),reject_reason,COALESCE(lease_until,'epoch'),not_before,created_at,updated_at,COALESCE(dispatched_at,'epoch')
FROM preprocess_job WHERE state='ready' AND dispatched_at IS NULL ORDER BY created_at,job_id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []preprocess.Job
	for rows.Next() {
		job, scanErr := scanJob(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (s *Store) ClaimReadyForDispatch(ctx context.Context, options preprocess.ClaimOptions) ([]preprocess.Job, error) {
	if s == nil || s.db == nil || options.Owner == "" || options.Now.IsZero() || options.TTL <= 0 || options.Limit < 1 {
		return nil, runtime.ErrInvariantViolation
	}
	rows, err := s.db.QueryContext(ctx, `WITH candidates AS (
SELECT tenant_id,job_id FROM preprocess_job
WHERE state='ready' AND dispatched_at IS NULL AND (lease_owner IS NULL OR lease_until <= $1)
ORDER BY created_at,job_id FOR UPDATE SKIP LOCKED LIMIT $2
)
UPDATE preprocess_job j SET lease_owner=$3,lease_until=$1+($4 * interval '1 microsecond'),version=j.version+1,updated_at=$1
FROM candidates c WHERE j.tenant_id=c.tenant_id AND j.job_id=c.job_id
RETURNING j.tenant_id,j.request_id,j.job_id,j.payload_ref,COALESCE(j.prepared_payload_ref,''),j.agent_app_id,j.session_id,j.user_id,j.channel,j.channel_binding_id,j.traceparent,j.tenant_version,j.config_version,
j.state,j.attempt,j.version,COALESCE(j.lease_owner,''),j.reject_reason,COALESCE(j.lease_until,'epoch'),j.not_before,j.created_at,j.updated_at,COALESCE(j.dispatched_at,'epoch')`,
		options.Now, options.Limit, options.Owner, options.TTL.Microseconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []preprocess.Job
	for rows.Next() {
		job, scanErr := scanJob(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (s *Store) MarkDispatched(ctx context.Context, job preprocess.Job, at time.Time) (preprocess.Job, error) {
	if s == nil || s.db == nil || at.IsZero() {
		return preprocess.Job{}, runtime.ErrInvariantViolation
	}
	updated, err := scanJob(s.db.QueryRowContext(ctx, `UPDATE preprocess_job SET dispatched_at=$4,lease_owner=NULL,lease_until=NULL,version=version+1,updated_at=$4
WHERE tenant_id=$1 AND job_id=$2 AND version=$3 AND state='ready' AND dispatched_at IS NULL AND lease_owner=$5
RETURNING tenant_id,request_id,job_id,payload_ref,COALESCE(prepared_payload_ref,''),agent_app_id,session_id,user_id,channel,channel_binding_id,traceparent,tenant_version,config_version,
state,attempt,version,COALESCE(lease_owner,''),reject_reason,COALESCE(lease_until,'epoch'),not_before,created_at,updated_at,COALESCE(dispatched_at,'epoch')`,
		job.TenantID, job.JobID, job.Version, at, job.LeaseOwner))
	if errors.Is(err, runtime.ErrNotFound) {
		return preprocess.Job{}, runtime.ErrVersionConflict
	}
	return updated, err
}

type scanner interface{ Scan(...any) error }

func scanJob(row scanner) (preprocess.Job, error) {
	var job preprocess.Job
	if err := row.Scan(&job.TenantID, &job.RequestID, &job.JobID, &job.PayloadRef, &job.PreparedPayloadRef, &job.AgentAppID, &job.SessionID,
		&job.UserID, &job.Channel, &job.ChannelBindingID, &job.TraceParent, &job.TenantVersion, &job.ConfigVersion, &job.State, &job.Attempt, &job.Version,
		&job.LeaseOwner, &job.RejectReason, &job.LeaseUntil, &job.NotBefore, &job.CreatedAt, &job.UpdatedAt, &job.DispatchedAt); err != nil {
		return preprocess.Job{}, classify(err)
	}
	if job.LeaseUntil.Equal(time.Unix(0, 0).UTC()) {
		job.LeaseUntil = time.Time{}
	}
	if job.DispatchedAt.Equal(time.Unix(0, 0).UTC()) {
		job.DispatchedAt = time.Time{}
	}
	return job, nil
}

func scanInbox(row scanner) (messaging.InboxRecord, error) {
	var record messaging.InboxRecord
	if err := row.Scan(&record.TenantID, &record.Channel, &record.ExternalAccountID, &record.ExternalMessageID,
		&record.RequestID, &record.AgentAppID, &record.SessionID, &record.ExternalChatID, &record.ExternalUserID, &record.InputSeq,
		&record.State, &record.PayloadRef, &record.PayloadDigest, &record.KeyVersion, &record.Version, &record.TerminalReason,
		&record.ResultRef, &record.CreatedAt, &record.UpdatedAt); err != nil {
		return messaging.InboxRecord{}, classify(err)
	}
	return record, nil
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

type sqlStater interface{ SQLState() string }

func classify(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.ErrNotFound
	}
	var state sqlStater
	if errors.As(err, &state) {
		switch state.SQLState() {
		case "23505":
			return runtime.ErrIdempotencyCollision
		case "23503", "23514", "22023", "42501":
			return runtime.ErrInvariantViolation
		case "40001":
			return runtime.ErrVersionConflict
		}
	}
	return err
}

var _ preprocess.Store = (*Store)(nil)
