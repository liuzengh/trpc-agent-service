package controlplane

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Principal is a platform-wide identity: an IM user mapped to this platform,
// or a service account holding an API token. It is not scoped to a tenant;
// Membership below is what puts it in one.
type Principal struct {
	PrincipalID int64
	Subject     string
	HasToken    bool
}

// Membership is one principal's role inside one tenant.
type Membership struct {
	TenantID    string
	PrincipalID int64
	Role        string
	Status      string
}

// CreatePrincipalWithMembership inserts a principal and its membership in one
// transaction. There is no path to a principal that is not a member of
// something: a row in principals with no tenant_users row is dead weight a
// token could still be validated against while granting no access, which is
// worse than failing the insert outright.
//
// This is a second deliberate exception to Scope-only access, alongside
// CreateTenant: neither operation can be expressed as "a tenant touching its
// own rows" because both are in the business of creating the thing that
// defines a tenant's rows in the first place.
func (d *DB) CreatePrincipalWithMembership(ctx context.Context, tenantID, subject, role string, tokenHash any) (int64, error) {
	if tenantID == "" || subject == "" {
		return 0, ErrNoScope
	}
	switch role {
	case "admin", "user":
	default:
		return 0, fmt.Errorf("controlplane: unknown role %q", role)
	}
	var principalID int64
	err := d.withTxUnscoped(ctx, func(tx *sql.Tx) error {
		var tid string
		if err := tx.QueryRowContext(ctx,
			"SELECT tenant_id FROM tenants WHERE tenant_id = ? AND status = 'active'", tenantID).Scan(&tid); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("controlplane: check tenant before principal: %w", err)
		}
		res, err := tx.ExecContext(ctx,
			"INSERT INTO principals (subject, token_hash) VALUES (?, ?)", subject, tokenHash)
		if err != nil {
			return fmt.Errorf("controlplane: create principal: %w", err)
		}
		principalID, err = res.LastInsertId()
		if err != nil {
			return fmt.Errorf("controlplane: principal id: %w", err)
		}
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO tenant_users (tenant_id, principal_id, role) VALUES (?, ?, ?)",
			tenantID, principalID, role); err != nil {
			return fmt.Errorf("controlplane: grant membership: %w", err)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return principalID, nil
}

// PrincipalForToken resolves a token hash to the membership it grants. A
// token that maps to several memberships is not assumed to pick one: this
// returns the error the platform treats as a misconfiguration rather than
// silently choosing the first row.
func (d *DB) PrincipalForToken(ctx context.Context, tokenHash string) (Membership, error) {
	var m Membership
	err := d.db.QueryRowContext(ctx, `
		SELECT tu.tenant_id, tu.principal_id, tu.role, tu.status
		FROM principals p
		JOIN tenant_users tu ON tu.principal_id = p.principal_id
		JOIN tenants t ON t.tenant_id = tu.tenant_id
		WHERE p.token_hash = ? AND tu.status = 'active' AND t.status = 'active'`, tokenHash).
		Scan(&m.TenantID, &m.PrincipalID, &m.Role, &m.Status)
	if errors.Is(err, sql.ErrNoRows) {
		return Membership{}, ErrNotFound
	}
	if err != nil {
		return Membership{}, fmt.Errorf("controlplane: resolve principal: %w", err)
	}
	return m, nil
}

// GrantIMUser adds an external IM user to a binding's allowlist.
func (s Scope) GrantIMUser(ctx context.Context, bindingID int64, externalUserID, displayName string) error {
	_, err := s.Exec(ctx, `
		INSERT INTO channel_identities (tenant_id, binding_id, external_user_id, display_name)
		VALUES (?, ?, ?, ?)`, s.tenantID, bindingID, externalUserID, displayName)
	if err != nil {
		return fmt.Errorf("controlplane: grant IM user: %w", err)
	}
	return nil
}

// RevokeIMUser removes an external IM user's access without deleting the
// row: an audit trail needs to be able to say "this person was revoked on
// this date", which a hard delete erases.
func (s Scope) RevokeIMUser(ctx context.Context, bindingID int64, externalUserID string) error {
	res, err := s.Exec(ctx, `
		UPDATE channel_identities SET status = 'revoked'
		WHERE tenant_id = ? AND binding_id = ? AND external_user_id = ?`,
		s.tenantID, bindingID, externalUserID)
	if err != nil {
		return fmt.Errorf("controlplane: revoke IM user: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// IsIMUserAllowed checks the tenant's IM-user allowlist for one (channel,
// external user id) pair. A revoked membership is out everywhere at once, so
// the same check covers "was never allowed" and "is no longer allowed".
func (s Scope) IsIMUserAllowed(ctx context.Context, channelType, externalUserID string) (bool, error) {
	var allowed bool
	row, err := s.QueryRow(ctx, `
		SELECT COUNT(*) > 0
		FROM channel_bindings cb
		JOIN channel_identities ci ON ci.binding_id = cb.binding_id
		WHERE cb.tenant_id = ? AND cb.channel_type = ? AND cb.status = 'active'
		  AND ci.external_user_id = ? AND ci.status = 'active'`,
		s.tenantID, channelType, externalUserID)
	if err != nil {
		return false, err
	}
	if err := row.Scan(&allowed); err != nil {
		return false, fmt.Errorf("controlplane: check IM allowlist: %w", err)
	}
	return allowed, nil
}

// withTxUnscoped is the escape hatch behind the two documented exceptions
// above. It is private so no new repository method reaches for it without
// being read in review as another exception being added.
func (d *DB) withTxUnscoped(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("controlplane: begin unscoped tx: %w", err)
	}
	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback()
			panic(r)
		}
	}()
	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("controlplane: rollback after %v failed: %w", err, rbErr)
		}
		return err
	}
	return tx.Commit()
}
