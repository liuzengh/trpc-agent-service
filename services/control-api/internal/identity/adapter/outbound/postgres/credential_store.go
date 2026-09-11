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

// FindPasswordCredential returns the current credential for password change.
func (s *Store) FindPasswordCredential(
	ctx context.Context,
	userID string,
) (domain.PasswordCredential, error) {
	const query = `
		SELECT encoded_hash, must_change_at_next_login
		FROM password_credentials
		WHERE user_id = $1
	`
	var credential domain.PasswordCredential
	err := s.db.QueryRow(ctx, query, userID).Scan(
		&credential.EncodedHash,
		&credential.MustChangeAtNextLogin,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.PasswordCredential{}, application.ErrAccountNotFound
	}
	if err != nil {
		return domain.PasswordCredential{}, fmt.Errorf("query password credential: %w", err)
	}
	return credential, nil
}

// ChangePassword atomically replaces the credential, revokes other sessions,
// and unlocks the current restricted session.
func (s *Store) ChangePassword(
	ctx context.Context,
	userID, currentSessionID, encodedHash string,
	changedAt time.Time,
) error {
	const query = `
		WITH updated_credential AS (
			UPDATE password_credentials
			SET encoded_hash = $3,
				must_change_at_next_login = false,
				changed_at = $4
			WHERE user_id = $1
			RETURNING user_id
		), revoked_sessions AS (
			UPDATE user_sessions
			SET revoked_at = $4
			WHERE user_id = $1
				AND id <> $2
				AND revoked_at IS NULL
				AND EXISTS (SELECT 1 FROM updated_credential)
		)
		UPDATE user_sessions
		SET restricted = false
		WHERE id = $2
			AND user_id = $1
			AND revoked_at IS NULL
			AND EXISTS (SELECT 1 FROM updated_credential)
		RETURNING id
	`
	var changedSessionID string
	err := s.db.QueryRow(ctx, query, userID, currentSessionID, encodedHash, changedAt).Scan(&changedSessionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return application.ErrSessionNotFound
	}
	if err != nil {
		return fmt.Errorf("update password and sessions: %w", err)
	}
	return nil
}

var _ application.PasswordChangeStore = (*Store)(nil)
