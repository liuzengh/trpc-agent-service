// Package postgresadapter persists Identity state in PostgreSQL.
package postgresadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/domain"
)

// DB is the narrow pgx surface used by the Identity store. Both pgxpool.Pool
// and transaction-bound adapters satisfy it.
type DB interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

// Store implements Identity persistence behind application-owned ports.
type Store struct {
	db DB
}

// NewStore returns a PostgreSQL Identity store.
func NewStore(db DB) *Store {
	return &Store{db: db}
}

// FindLoginIdentity returns the account and current password credential in one
// query so callers never coordinate repository state themselves.
func (s *Store) FindLoginIdentity(
	ctx context.Context,
	normalizedUsername string,
) (domain.LoginIdentity, error) {
	const query = `
		SELECT
			a.id,
			a.username,
			a.normalized_username,
			a.display_name,
			a.status,
			c.encoded_hash,
			c.must_change_at_next_login
		FROM user_accounts AS a
		JOIN password_credentials AS c ON c.user_id = a.id
		WHERE a.normalized_username = $1
	`

	var identity domain.LoginIdentity
	var status string
	err := s.db.QueryRow(ctx, query, normalizedUsername).Scan(
		&identity.Account.ID,
		&identity.Account.Username,
		&identity.Account.NormalizedUsername,
		&identity.Account.DisplayName,
		&status,
		&identity.Credential.EncodedHash,
		&identity.Credential.MustChangeAtNextLogin,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.LoginIdentity{}, application.ErrLoginIdentityNotFound
	}
	if err != nil {
		return domain.LoginIdentity{}, fmt.Errorf("query login identity: %w", err)
	}
	identity.Account.Status = domain.AccountStatus(status)
	return identity, nil
}

// CreateAccount atomically creates a global account and its temporary
// credential with one PostgreSQL statement.
func (s *Store) CreateAccount(
	ctx context.Context,
	account domain.UserAccount,
	credential domain.PasswordCredential,
) error {
	const statement = `
		WITH inserted_account AS (
			INSERT INTO user_accounts (
				id, username, normalized_username, display_name, status, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7)
			RETURNING id
		)
		INSERT INTO password_credentials (
			user_id, encoded_hash, must_change_at_next_login, changed_at
		)
		SELECT id, $8, $9, $7 FROM inserted_account
	`
	_, err := s.db.Exec(
		ctx,
		statement,
		account.ID,
		account.Username,
		account.NormalizedUsername,
		account.DisplayName,
		string(account.Status),
		account.CreatedAt,
		account.UpdatedAt,
		credential.EncodedHash,
		credential.MustChangeAtNextLogin,
	)
	if err != nil {
		var pgError *pgconn.PgError
		if errors.As(err, &pgError) && pgError.Code == "23505" &&
			pgError.ConstraintName == "user_accounts_normalized_username_unique" {
			return application.ErrUsernameTaken
		}
		return fmt.Errorf("insert account and credential: %w", err)
	}
	return nil
}

// GetAccount returns the current global account state.
func (s *Store) GetAccount(ctx context.Context, userID string) (domain.UserAccount, error) {
	const query = `
		SELECT id, username, normalized_username, display_name, status, created_at, updated_at
		FROM user_accounts
		WHERE id = $1
	`
	var account domain.UserAccount
	var status string
	err := s.db.QueryRow(ctx, query, userID).Scan(
		&account.ID,
		&account.Username,
		&account.NormalizedUsername,
		&account.DisplayName,
		&status,
		&account.CreatedAt,
		&account.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.UserAccount{}, application.ErrAccountNotFound
	}
	if err != nil {
		return domain.UserAccount{}, fmt.Errorf("query account: %w", err)
	}
	account.Status = domain.AccountStatus(status)
	return account, nil
}

type accountRecord struct {
	ID          string    `json:"id"`
	Username    string    `json:"username"`
	DisplayName string    `json:"display_name"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// ListAccounts returns the platform user read model and total count.
func (s *Store) ListAccounts(
	ctx context.Context,
	page application.Page,
) (application.AccountPage, error) {
	const query = `
		SELECT
			COALESCE((
				SELECT jsonb_agg(
					jsonb_build_object(
						'id', p.id,
						'username', p.username,
						'display_name', p.display_name,
						'status', p.status,
						'created_at', p.created_at,
						'updated_at', p.updated_at
					) ORDER BY p.created_at DESC, p.id
				)
				FROM (
					SELECT id, username, display_name, status, created_at, updated_at
					FROM user_accounts
					ORDER BY created_at DESC, id
					LIMIT $1 OFFSET $2
				) AS p
			), '[]'::jsonb),
			(SELECT count(*)::int FROM user_accounts)
	`
	var encoded []byte
	var total int
	if err := s.db.QueryRow(ctx, query, page.Limit, page.Offset).Scan(&encoded, &total); err != nil {
		return application.AccountPage{}, fmt.Errorf("query accounts: %w", err)
	}
	var records []accountRecord
	if err := json.Unmarshal(encoded, &records); err != nil {
		return application.AccountPage{}, fmt.Errorf("decode account page: %w", err)
	}
	accounts := make([]domain.UserAccount, 0, len(records))
	for _, record := range records {
		accounts = append(accounts, domain.UserAccount{
			ID:          record.ID,
			Username:    record.Username,
			DisplayName: record.DisplayName,
			Status:      domain.AccountStatus(record.Status),
			CreatedAt:   record.CreatedAt,
			UpdatedAt:   record.UpdatedAt,
		})
	}
	return application.AccountPage{Accounts: accounts, Total: total}, nil
}

var _ application.AccountReader = (*Store)(nil)
var _ application.AccountStore = (*Store)(nil)
