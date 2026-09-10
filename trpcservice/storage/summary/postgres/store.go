// Package postgres persists immutable summary bodies and compaction claims.
package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/summary"
)

type DB interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

type Store struct{ db DB }

func New(db DB) *Store { return &Store{db: db} }

func (s *Store) Put(ctx context.Context, in summary.Body) (summary.Body, error) {
	if s == nil || s.db == nil {
		return summary.Body{}, runtime.ErrCapabilityUnsupported
	}
	value, err := summary.ValidateBody(in)
	if err != nil {
		return summary.Body{}, err
	}
	// The SELECT makes ContentRef authority come from the already committed
	// session boundary, rather than trusting a summarizer-provided reference.
	_, err = s.db.ExecContext(ctx, `INSERT INTO session_summary_content(
tenant_id,agent_app_id,session_id,summary_id,content_ref,content_digest,content,created_at)
SELECT $1,$2,$3,$4,$5,$6,$7,$8
WHERE EXISTS (SELECT 1 FROM session_summary WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3 AND summary_id=$4 AND content_ref=$5)
ON CONFLICT (tenant_id,agent_app_id,session_id,summary_id) DO NOTHING`,
		value.TenantID, value.AgentAppID, value.SessionID, value.SummaryID, value.ContentRef, value.ContentDigest, value.Content, value.CreatedAt.UTC())
	if err != nil {
		return summary.Body{}, translate(err)
	}
	stored, err := s.Get(ctx, value.Key)
	if err != nil {
		return summary.Body{}, err
	}
	if stored.ContentRef != value.ContentRef || stored.ContentDigest != value.ContentDigest || !bytes.Equal(stored.Content, value.Content) {
		return summary.Body{}, runtime.ErrIdempotencyCollision
	}
	return stored, nil
}

func (s *Store) Get(ctx context.Context, key summary.Key) (summary.Body, error) {
	if s == nil || s.db == nil || !validKey(key) {
		return summary.Body{}, runtime.ErrTenantScope
	}
	var out summary.Body
	out.Key = key
	err := s.db.QueryRowContext(ctx, `SELECT content_ref,content_digest,content,created_at FROM session_summary_content WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3 AND summary_id=$4`, key.TenantID, key.AgentAppID, key.SessionID, key.SummaryID).Scan(&out.ContentRef, &out.ContentDigest, &out.Content, &out.CreatedAt)
	if err != nil {
		return summary.Body{}, translate(err)
	}
	out.Content = append([]byte(nil), out.Content...)
	return out, nil
}

func (s *Store) Supersede(ctx context.Context, source summary.Key, replacement string, at time.Time) error {
	if s == nil || s.db == nil || !validKey(source) || replacement == "" || at.IsZero() {
		return runtime.ErrInvalidEnvelope
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return translate(err)
	}
	defer tx.Rollback()
	// session_head must already point at the higher boundary. This prevents a
	// future summary body from deleting the currently readable one.
	result, err := tx.ExecContext(ctx, `UPDATE session_summary_content c SET state='superseded',superseded_by_summary_id=$5,superseded_at=$6,record_version=record_version+1
WHERE c.tenant_id=$1 AND c.agent_app_id=$2 AND c.session_id=$3 AND c.summary_id=$4 AND c.state='active'
AND EXISTS (SELECT 1 FROM session_head h WHERE h.tenant_id=c.tenant_id AND h.agent_app_id=c.agent_app_id AND h.session_id=c.session_id AND h.summary_id=$5)
AND EXISTS (SELECT 1 FROM session_summary newer JOIN session_summary older ON newer.tenant_id=older.tenant_id AND newer.agent_app_id=older.agent_app_id AND newer.session_id=older.session_id
  WHERE newer.tenant_id=c.tenant_id AND newer.agent_app_id=c.agent_app_id AND newer.session_id=c.session_id AND newer.summary_id=$5 AND older.summary_id=c.summary_id AND newer.base_session_seq>older.base_session_seq)`, source.TenantID, source.AgentAppID, source.SessionID, source.SummaryID, replacement, at.UTC())
	if err != nil {
		return translate(err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		var current string
		err = tx.QueryRowContext(ctx, `SELECT COALESCE(superseded_by_summary_id,'') FROM session_summary_content WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3 AND summary_id=$4`, source.TenantID, source.AgentAppID, source.SessionID, source.SummaryID).Scan(&current)
		if errors.Is(err, sql.ErrNoRows) {
			return runtime.ErrNotFound
		}
		if err != nil {
			return translate(err)
		}
		if current == replacement {
			return tx.Commit()
		}
		return runtime.ErrVersionMismatch
	}
	return tx.Commit()
}

func (s *Store) ClaimSuperseded(ctx context.Context, now time.Time, owner string, ttl time.Duration, limit int) ([]summary.ClaimedBody, error) {
	if s == nil || s.db == nil || now.IsZero() || owner == "" || ttl <= 0 || limit <= 0 {
		return nil, runtime.ErrInvariantViolation
	}
	if limit > 1000 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx, `WITH candidates AS (
 SELECT c.tenant_id,c.agent_app_id,c.session_id,c.summary_id FROM session_summary_content c
 JOIN session_head h ON h.tenant_id=c.tenant_id AND h.agent_app_id=c.agent_app_id AND h.session_id=c.session_id AND h.summary_id=c.superseded_by_summary_id
 WHERE c.state='superseded' AND NOT c.frozen AND c.not_before <= $1 AND (c.claim_until IS NULL OR c.claim_until <= $1)
 ORDER BY c.superseded_at,c.tenant_id,c.agent_app_id,c.session_id,c.summary_id LIMIT $4 FOR UPDATE OF c SKIP LOCKED
)
UPDATE session_summary_content c SET state='delete_claimed',claim_owner=$2,claim_until=$3,record_version=c.record_version+1
FROM candidates x WHERE c.tenant_id=x.tenant_id AND c.agent_app_id=x.agent_app_id AND c.session_id=x.session_id AND c.summary_id=x.summary_id
RETURNING c.tenant_id,c.agent_app_id,c.session_id,c.summary_id,c.content_ref,c.content_digest,c.content,c.created_at,c.superseded_by_summary_id,c.claim_owner,c.claim_until,c.delete_attempt,c.record_version`, now.UTC(), owner, now.Add(ttl).UTC(), limit)
	if err != nil {
		return nil, translate(err)
	}
	defer rows.Close()
	out := make([]summary.ClaimedBody, 0, limit)
	for rows.Next() {
		var value summary.ClaimedBody
		var created time.Time
		if err := rows.Scan(&value.TenantID, &value.AgentAppID, &value.SessionID, &value.SummaryID, &value.ContentRef, &value.ContentDigest, &value.Content, &created, &value.SupersededBy, &value.ClaimOwner, &value.ClaimUntil, &value.DeleteAttempt, &value.Version); err != nil {
			return nil, err
		}
		value.CreatedAt = created.UTC()
		value.Content = append([]byte(nil), value.Content...)
		out = append(out, value)
	}
	return out, rows.Err()
}

func (s *Store) FinishDelete(ctx context.Context, value summary.ClaimedBody) error {
	if s == nil || s.db == nil || !validClaim(value) {
		return runtime.ErrInvariantViolation
	}
	result, err := s.db.ExecContext(ctx, `DELETE FROM session_summary_content WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3 AND summary_id=$4 AND state='delete_claimed' AND claim_owner=$5 AND record_version=$6`, value.TenantID, value.AgentAppID, value.SessionID, value.SummaryID, value.ClaimOwner, value.Version)
	if err != nil {
		return translate(err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return runtime.ErrVersionConflict
	}
	return nil
}
func (s *Store) DeferDelete(ctx context.Context, value summary.ClaimedBody, until time.Time, class string) error {
	if s == nil || s.db == nil || !validClaim(value) || until.IsZero() || class == "" {
		return runtime.ErrInvariantViolation
	}
	result, err := s.db.ExecContext(ctx, `UPDATE session_summary_content SET state='superseded',claim_owner=NULL,claim_until=NULL,not_before=$7,delete_attempt=delete_attempt+1,last_error_class=$8,record_version=record_version+1 WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3 AND summary_id=$4 AND state='delete_claimed' AND claim_owner=$5 AND record_version=$6`, value.TenantID, value.AgentAppID, value.SessionID, value.SummaryID, value.ClaimOwner, value.Version, until.UTC(), class)
	if err != nil {
		return translate(err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return runtime.ErrVersionConflict
	}
	return nil
}
func validKey(value summary.Key) bool {
	return value.TenantID != "" && value.AgentAppID != "" && value.SessionID != "" && value.SummaryID != ""
}
func validClaim(value summary.ClaimedBody) bool {
	return validKey(value.Key) && value.ClaimOwner != "" && value.Version > 0
}
func translate(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.ErrNotFound
	}
	return err
}

var _ summary.Store = (*Store)(nil)
