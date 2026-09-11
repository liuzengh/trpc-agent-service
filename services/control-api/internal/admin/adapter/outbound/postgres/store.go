// Package postgresadapter persists Admin-owned Operator Grants in PostgreSQL.
package postgresadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/admin/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/admin/domain"
)

type DB interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

type Store struct {
	db DB
}

func NewStore(db DB) *Store {
	return &Store{db: db}
}

func (s *Store) IsActiveOperator(ctx context.Context, userID string) (bool, error) {
	const query = `
		SELECT EXISTS (
			SELECT 1
			FROM platform_operator_grants
			WHERE user_id = $1 AND revoked_at IS NULL
		)
	`
	var active bool
	if err := s.db.QueryRow(ctx, query, userID).Scan(&active); err != nil {
		return false, fmt.Errorf("query active operator: %w", err)
	}
	return active, nil
}

type grantRecord struct {
	UserID             string     `json:"user_id"`
	GrantedByActorType string     `json:"granted_by_actor_type"`
	GrantedByUserID    string     `json:"granted_by_user_id"`
	GrantedAt          time.Time  `json:"granted_at"`
	RevokedByUserID    string     `json:"revoked_by_user_id"`
	RevokedAt          *time.Time `json:"revoked_at"`
}

func (s *Store) ListOperatorGrants(ctx context.Context) ([]domain.OperatorGrant, error) {
	const query = `
		SELECT COALESCE(jsonb_agg(
			jsonb_build_object(
				'user_id', user_id,
				'granted_by_actor_type', granted_by_actor_type,
				'granted_by_user_id', COALESCE(granted_by_user_id, ''),
				'granted_at', granted_at,
				'revoked_by_user_id', COALESCE(revoked_by_user_id, ''),
				'revoked_at', revoked_at
			) ORDER BY granted_at, user_id
		), '[]'::jsonb)
		FROM platform_operator_grants
		WHERE revoked_at IS NULL
	`
	var encoded []byte
	if err := s.db.QueryRow(ctx, query).Scan(&encoded); err != nil {
		return nil, fmt.Errorf("query operator grants: %w", err)
	}
	var records []grantRecord
	if err := json.Unmarshal(encoded, &records); err != nil {
		return nil, fmt.Errorf("decode operator grants: %w", err)
	}
	grants := make([]domain.OperatorGrant, 0, len(records))
	for _, record := range records {
		grants = append(grants, domain.OperatorGrant{
			UserID:             record.UserID,
			GrantedByActorType: domain.GrantedByActorType(record.GrantedByActorType),
			GrantedByUserID:    record.GrantedByUserID,
			GrantedAt:          record.GrantedAt,
			RevokedByUserID:    record.RevokedByUserID,
			RevokedAt:          record.RevokedAt,
		})
	}
	return grants, nil
}

func (s *Store) GrantOperator(ctx context.Context, grant domain.OperatorGrant) error {
	const statement = `
		INSERT INTO platform_operator_grants (
			user_id, granted_by_actor_type, granted_by_user_id, granted_at
		) VALUES ($1, $2, NULLIF($3, ''), $4)
		ON CONFLICT (user_id) DO UPDATE
		SET granted_by_actor_type = EXCLUDED.granted_by_actor_type,
			granted_by_user_id = EXCLUDED.granted_by_user_id,
			granted_at = EXCLUDED.granted_at,
			revoked_by_user_id = NULL,
			revoked_at = NULL
	`
	_, err := s.db.Exec(
		ctx,
		statement,
		grant.UserID,
		string(grant.GrantedByActorType),
		grant.GrantedByUserID,
		grant.GrantedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert operator grant: %w", err)
	}
	return nil
}

// RevokeOperator serializes the active-count check and state transition inside
// one statement so concurrent requests cannot both revoke the last operator.
func (s *Store) RevokeOperator(
	ctx context.Context,
	targetUserID, actorUserID string,
	revokedAt time.Time,
) error {
	const query = `
		WITH lock AS (
			SELECT pg_advisory_xact_lock(731947201)
		), active AS (
			SELECT count(*)::int AS count
			FROM platform_operator_grants, lock
			WHERE revoked_at IS NULL
		), target AS (
			SELECT EXISTS (
				SELECT 1
				FROM platform_operator_grants, lock
				WHERE user_id = $1 AND revoked_at IS NULL
			) AS active
		), updated AS (
			UPDATE platform_operator_grants
			SET revoked_by_user_id = $2, revoked_at = $3
			WHERE user_id = $1
				AND revoked_at IS NULL
				AND (SELECT count FROM active) > 1
			RETURNING user_id
		)
		SELECT
			(SELECT count FROM active),
			(SELECT active FROM target),
			(SELECT count(*)::int FROM updated)
	`
	var activeCount, updatedCount int
	var targetActive bool
	if err := s.db.QueryRow(ctx, query, targetUserID, actorUserID, revokedAt).Scan(
		&activeCount, &targetActive, &updatedCount,
	); err != nil {
		return fmt.Errorf("revoke operator grant: %w", err)
	}
	if updatedCount == 1 {
		return nil
	}
	if activeCount == 1 && targetActive {
		return application.ErrLastOperator
	}
	return application.ErrOperatorNotFound
}

var _ application.OperatorStore = (*Store)(nil)
