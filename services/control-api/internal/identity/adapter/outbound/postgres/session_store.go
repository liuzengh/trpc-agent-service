package postgresadapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/domain"
)

// CreateSession persists only the token hash and server-side session state.
func (s *Store) CreateSession(ctx context.Context, session domain.Session) error {
	const statement = `
		INSERT INTO user_sessions (
			id,
			user_id,
			token_hash,
			restricted,
			created_at,
			expires_at
		) VALUES ($1, $2, $3, $4, $5, $6)
	`
	_, err := s.db.Exec(
		ctx,
		statement,
		session.ID,
		session.UserID,
		session.TokenHash,
		session.Restricted,
		session.CreatedAt,
		session.ExpiresAt,
	)
	if err != nil {
		return fmt.Errorf("insert user session: %w", err)
	}
	return nil
}

// FindSessionIdentity resolves an opaque token hash to current session and
// account state in one query.
func (s *Store) FindSessionIdentity(
	ctx context.Context,
	tokenHash []byte,
) (domain.SessionIdentity, error) {
	const query = `
		SELECT
			s.id,
			s.user_id,
			s.restricted,
			s.created_at,
			s.expires_at,
			s.revoked_at,
			a.username,
			a.display_name,
			a.status
		FROM user_sessions AS s
		JOIN user_accounts AS a ON a.id = s.user_id
		WHERE s.token_hash = $1
	`
	var state domain.SessionIdentity
	var status string
	err := s.db.QueryRow(ctx, query, tokenHash).Scan(
		&state.Session.ID,
		&state.Session.UserID,
		&state.Session.Restricted,
		&state.Session.CreatedAt,
		&state.Session.ExpiresAt,
		&state.Session.RevokedAt,
		&state.Account.Username,
		&state.Account.DisplayName,
		&status,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.SessionIdentity{}, application.ErrSessionNotFound
	}
	if err != nil {
		return domain.SessionIdentity{}, fmt.Errorf("query session identity: %w", err)
	}
	state.Account.ID = state.Session.UserID
	state.Account.Status = domain.AccountStatus(status)
	return state, nil
}

// RevokeSession idempotently revokes the caller's current session.
func (s *Store) RevokeSession(
	ctx context.Context,
	userID, sessionID string,
	revokedAt time.Time,
) error {
	const statement = `
		UPDATE user_sessions
		SET revoked_at = COALESCE(revoked_at, $3)
		WHERE user_id = $1 AND id = $2
	`
	if _, err := s.db.Exec(ctx, statement, userID, sessionID, revokedAt); err != nil {
		return fmt.Errorf("revoke user session: %w", err)
	}
	return nil
}

var _ application.SessionWriter = (*Store)(nil)
var _ application.SessionIdentityReader = (*Store)(nil)
var _ application.SessionRevoker = (*Store)(nil)
