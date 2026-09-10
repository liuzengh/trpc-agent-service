package identity

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

var (
	ErrPlatformUserNotFound     = errors.New("platform user not found")
	ErrChannelIdentityConflict  = errors.New("channel identity belongs to another platform user")
	ErrChannelIdentityNotLinked = errors.New("channel identity is not linked to the platform user")
	ErrLastSystemAdmin          = errors.New("cannot remove the last usable system administrator")
	ErrLastTenantAdmin          = errors.New("cannot remove the last active tenant administrator")
	ErrTenantNotFound           = errors.New("tenant not found")
	ErrTenantNeedsAdmin         = errors.New("active tenant requires an active tenant administrator")
	ErrLoginIdentityConflict    = errors.New("login identity belongs to another platform user")
	ErrLastLoginIdentity        = errors.New("cannot remove the last usable login identity")
)

const (
	TenantActive    = "active"
	TenantSuspended = "suspended"
)

// PlatformUser is the canonical identity for one person. Provider identities
// and channel accounts only point at this ID; they never substitute for it.
type PlatformUser struct {
	PlatformUserID string
	DisplayName    string
	Email          string
	Status         string
	FirstSeenAt    time.Time
	LastLoginAt    time.Time
}

type LoginIdentity struct {
	ProviderID     string
	SubjectID      string
	PlatformUserID string
	DisplayName    string
	Email          string
	LastLoginAt    time.Time
}

type LoginMethod struct {
	ProviderID   string       `json:"provider_id"`
	ProviderType ProviderType `json:"type"`
	DisplayName  string       `json:"display_name"`
	SubjectID    string       `json:"subject_id"`
	LinkedAt     time.Time    `json:"linked_at"`
}

// LoginProviderActivityStore exposes provider-level login activity for
// administrative verification surfaces without widening the core IdentityStore
// contract used by runtime identity resolution.
type LoginProviderActivityStore interface {
	LatestLoginAtForProvider(ctx context.Context, providerID string) (time.Time, bool, error)
}

// TenantMembership assigns a role to a Platform User inside one tenant.
type TenantMembership struct {
	TenantID                 string `json:"tenant_id"`
	PlatformUserID           string `json:"platform_user_id"`
	Role                     Role   `json:"role"`
	Status                   string `json:"status"`
	ConversationContentAudit bool   `json:"conversation_content_audit"`
}

// TenantSummary is the explicit tenant-selector contract.
type TenantSummary struct {
	TenantID    string
	DisplayName string
	Role        Role
	Status      string
}

// TenantModelGrant authorizes one platform-managed model for one tenant.
// Provider connection details and credentials remain outside the tenant domain.
type TenantModelGrant struct {
	ProviderID string `json:"provider_id"`
	ModelName  string `json:"name"`
}

type MemberSummary struct {
	PlatformUserID           string    `json:"platform_user_id"`
	DisplayName              string    `json:"display_name"`
	Email                    string    `json:"email"`
	Role                     string    `json:"role"`
	Status                   string    `json:"status"`
	IsSystemAdmin            bool      `json:"is_system_admin"`
	LastLoginAt              time.Time `json:"last_login_at"`
	Providers                []string  `json:"providers"`
	ConversationContentAudit bool      `json:"conversation_content_audit"`
}

type MemberCursor struct {
	DisplayName    string
	PlatformUserID string
}

type MemberPageRequest struct {
	Limit int
	Query string
	After *MemberCursor
}

type MemberPage struct {
	Members []MemberSummary
	Next    *MemberCursor
}

// ChannelIdentity is one concrete external messaging identity observed through
// a binding. TrustedEnterpriseID is only needed by in-memory/test stores; the
// PostgreSQL store derives it from channel_bindings so the durable identity
// row does not duplicate routing metadata.
type ChannelIdentity struct {
	TenantID            string
	Channel             channels.Channel
	BindingID           string
	ExternalUserID      string
	PlatformUserID      string
	TrustedEnterpriseID string
	LinkedAt            time.Time
}

// IdentityStore owns canonical people, login identities, memberships,
// tenant summaries, and channel-side identity projections used by ingress.
type IdentityStore interface {
	UpsertLoginProvider(ctx context.Context, descriptor ProviderDescriptor, enterpriseID string) error
	ResolveLoginIdentity(ctx context.Context, identity Identity) (PlatformUser, error)
	LookupLoginIdentity(ctx context.Context, providerID, subjectID string) (LoginIdentity, error)
	LinkLoginIdentity(ctx context.Context, platformUserID string, identity Identity) error
	ListLoginMethods(ctx context.Context, platformUserID string) ([]LoginMethod, error)
	RemoveLoginIdentity(ctx context.Context, platformUserID, providerID, subjectID string) error
	CreateLocalUser(ctx context.Context, username, displayName, email, passwordHash string, mustChangePassword bool) (PlatformUser, error)
	LookupLocalCredential(ctx context.Context, username string) (LocalCredential, PlatformUser, error)
	LocalCredentialForUser(ctx context.Context, platformUserID string) (LocalCredential, error)
	SetLocalPassword(ctx context.Context, platformUserID, passwordHash string, mustChangePassword bool) error
	HasUsableSystemAdmin(ctx context.Context) (bool, error)
	ResolveSessionUser(ctx context.Context, platformUserID string) (SessionUser, error)
	SetSystemAdmin(ctx context.Context, platformUserID string, enabled bool) error
	IsSystemAdmin(ctx context.Context, platformUserID string) (bool, error)
	ListPlatformUsers(ctx context.Context, request MemberPageRequest) (MemberPage, error)
	UpdatePlatformUserProfile(ctx context.Context, platformUserID, displayName string) error
	UpdatePlatformUserAccess(ctx context.Context, platformUserID, status string, systemAdmin bool) error
	CreateTenant(ctx context.Context, tenantID, displayName, initialAdminPlatformUserID string) error
	TenantStatus(ctx context.Context, tenantID string) (string, error)
	SetTenantStatus(ctx context.Context, tenantID, status string) error
	ListTenantModelGrants(ctx context.Context, tenantID string) ([]TenantModelGrant, error)
	ReplaceTenantModelGrants(ctx context.Context, tenantID string, grants []TenantModelGrant) error
	GrantMembership(ctx context.Context, tenantID, platformUserID string, role Role) error
	SetTenantMembership(ctx context.Context, tenantID, platformUserID string, role Role, status string) error
	SetConversationContentAudit(ctx context.Context, tenantID, platformUserID string, enabled bool) error
	ListTenantMemberships(ctx context.Context, platformUserID string) ([]TenantMembership, error)
	ListTenantMembers(ctx context.Context, tenantID string, request MemberPageRequest) (MemberPage, error)
	ListTenantMemberCandidates(ctx context.Context, tenantID string, request MemberPageRequest) (MemberPage, error)
	RoleFor(ctx context.Context, tenantID, platformUserID string) (Role, error)
	ListTenantSummaries(ctx context.Context, platformUserID string, includeAll bool) ([]TenantSummary, error)
	ResolveChannelIdentity(ctx context.Context, tenantID string, channel channels.Channel, bindingID, externalUserID, trustedEnterpriseID string) (ChannelIdentity, bool, error)
	LinkChannelIdentity(ctx context.Context, identity ChannelIdentity) error
	UnlinkChannelIdentity(ctx context.Context, identity ChannelIdentity) error
	ListChannelIdentities(ctx context.Context, tenantID, platformUserID string) ([]ChannelIdentity, error)
}

// PostgresIdentityStore is the durable canonical identity store.
type PostgresIdentityStore struct {
	database *sql.DB
}

func NewPostgresIdentityStore(database *sql.DB) (*PostgresIdentityStore, error) {
	if database == nil {
		return nil, errors.New("identity database is required")
	}
	return &PostgresIdentityStore{database: database}, nil
}

func (s *PostgresIdentityStore) UpsertLoginProvider(ctx context.Context, descriptor ProviderDescriptor, enterpriseID string) error {
	enterpriseID = strings.TrimSpace(enterpriseID)
	if strings.TrimSpace(descriptor.ProviderID) == "" || strings.TrimSpace(descriptor.DisplayName) == "" || enterpriseID == "" {
		return errors.New("login provider metadata is incomplete")
	}
	switch descriptor.Type {
	case ProviderWeCom, ProviderFeishu, ProviderOIDC, ProviderMock:
	default:
		return fmt.Errorf("unsupported login provider type %q", descriptor.Type)
	}
	_, err := s.database.ExecContext(ctx, `
INSERT INTO login_providers (provider_id, provider_type, display_name, enterprise_id, enabled)
VALUES ($1,$2,$3,$4,TRUE)
ON CONFLICT (provider_id) DO UPDATE SET
    provider_type=EXCLUDED.provider_type,
    display_name=EXCLUDED.display_name,
    enterprise_id=EXCLUDED.enterprise_id,
    enabled=TRUE`, descriptor.ProviderID, string(descriptor.Type), descriptor.DisplayName, enterpriseID)
	if err != nil {
		return fmt.Errorf("upsert login provider: %w", err)
	}
	return nil
}

func (s *PostgresIdentityStore) ResolveLoginIdentity(ctx context.Context, external Identity) (PlatformUser, error) {
	if err := validateExternalIdentity(external); err != nil {
		return PlatformUser{}, err
	}
	tx, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return PlatformUser{}, fmt.Errorf("begin login identity transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", advisoryLockKey(external.ProviderID, external.SubjectID)); err != nil {
		return PlatformUser{}, fmt.Errorf("lock login identity: %w", err)
	}
	var providerType, enterpriseID string
	var enabled bool
	if err := tx.QueryRowContext(ctx, `
SELECT provider_type, enterprise_id, enabled
FROM login_providers WHERE provider_id=$1`, external.ProviderID).
		Scan(&providerType, &enterpriseID, &enabled); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return PlatformUser{}, errors.New("login provider is not registered")
		}
		return PlatformUser{}, fmt.Errorf("read login provider: %w", err)
	}
	if !enabled || providerType != string(external.ProviderType) || enterpriseID != external.EnterpriseID {
		return PlatformUser{}, errors.New("login identity does not match the registered provider boundary")
	}

	var user PlatformUser
	err = tx.QueryRowContext(ctx, `
SELECT p.platform_user_id, p.display_name, p.email, p.status, p.first_seen_at, p.last_login_at
FROM login_identities li
JOIN platform_users p ON p.platform_user_id = li.platform_user_id
WHERE li.provider_id=$1 AND li.subject_id=$2`, external.ProviderID, external.SubjectID).
		Scan(&user.PlatformUserID, &user.DisplayName, &user.Email, &user.Status, &user.FirstSeenAt, &user.LastLoginAt)
	if err == nil {
		if err := tx.QueryRowContext(ctx, `
UPDATE platform_users SET
    email=COALESCE(NULLIF($2,''), email),
    last_login_at=NOW()
WHERE platform_user_id=$1
RETURNING platform_user_id, display_name, email, status, first_seen_at, last_login_at`, user.PlatformUserID, strings.TrimSpace(external.Email)).
			Scan(&user.PlatformUserID, &user.DisplayName, &user.Email, &user.Status, &user.FirstSeenAt, &user.LastLoginAt); err != nil {
			return PlatformUser{}, fmt.Errorf("refresh platform user login: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE login_identities SET
    display_name=COALESCE(NULLIF($3,''), display_name),
    email=COALESCE(NULLIF($4,''), email),
    last_login_at=NOW()
WHERE provider_id=$1 AND subject_id=$2`, external.ProviderID, external.SubjectID, strings.TrimSpace(external.DisplayName), strings.TrimSpace(external.Email)); err != nil {
			return PlatformUser{}, fmt.Errorf("refresh login identity: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return PlatformUser{}, fmt.Errorf("commit login identity: %w", err)
		}
		return user, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return PlatformUser{}, fmt.Errorf("lookup login identity: %w", err)
	}

	user.PlatformUserID = uuid.NewString()
	err = tx.QueryRowContext(ctx, `
INSERT INTO platform_users (platform_user_id, display_name, email, status)
VALUES ($1,$2,$3,'active')
RETURNING platform_user_id, display_name, email, status, first_seen_at, last_login_at`, user.PlatformUserID, strings.TrimSpace(external.DisplayName), strings.TrimSpace(external.Email)).
		Scan(&user.PlatformUserID, &user.DisplayName, &user.Email, &user.Status, &user.FirstSeenAt, &user.LastLoginAt)
	if err != nil {
		return PlatformUser{}, fmt.Errorf("create platform user: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO login_identities (provider_id, subject_id, platform_user_id, display_name, email)
VALUES ($1,$2,$3,$4,$5)`, external.ProviderID, external.SubjectID, user.PlatformUserID, strings.TrimSpace(external.DisplayName), strings.TrimSpace(external.Email)); err != nil {
		return PlatformUser{}, fmt.Errorf("create login identity: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return PlatformUser{}, fmt.Errorf("commit new login identity: %w", err)
	}
	return user, nil
}

func (s *PostgresIdentityStore) LookupLoginIdentity(ctx context.Context, providerID, subjectID string) (LoginIdentity, error) {
	var login LoginIdentity
	err := s.database.QueryRowContext(ctx, `
SELECT provider_id, subject_id, platform_user_id, display_name, email, last_login_at
FROM login_identities WHERE provider_id=$1 AND subject_id=$2`, strings.TrimSpace(providerID), strings.TrimSpace(subjectID)).
		Scan(&login.ProviderID, &login.SubjectID, &login.PlatformUserID, &login.DisplayName, &login.Email, &login.LastLoginAt)
	if errors.Is(err, sql.ErrNoRows) {
		return LoginIdentity{}, ErrPlatformUserNotFound
	}
	if err != nil {
		return LoginIdentity{}, fmt.Errorf("lookup login identity: %w", err)
	}
	return login, nil
}

func (s *PostgresIdentityStore) LinkLoginIdentity(ctx context.Context, platformUserID string, external Identity) error {
	platformUserID = strings.TrimSpace(platformUserID)
	if platformUserID == "" {
		return ErrPlatformUserNotFound
	}
	if external.ProviderType == ProviderLocal {
		return errors.New("local login identities are provisioned by administrators")
	}
	if err := validateExternalIdentity(external); err != nil {
		return err
	}
	tx, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin login identity link: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "login-identities:"+platformUserID); err != nil {
		return fmt.Errorf("lock login identities: %w", err)
	}
	var userExists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM platform_users WHERE platform_user_id=$1 AND status='active')`, platformUserID).Scan(&userExists); err != nil {
		return fmt.Errorf("check platform user: %w", err)
	}
	if !userExists {
		return ErrPlatformUserNotFound
	}
	var providerType, enterpriseID string
	if err := tx.QueryRowContext(ctx, `SELECT provider_type, enterprise_id FROM login_providers WHERE provider_id=$1 AND enabled=TRUE`, external.ProviderID).Scan(&providerType, &enterpriseID); errors.Is(err, sql.ErrNoRows) {
		return errors.New("login provider is not configured")
	} else if err != nil {
		return fmt.Errorf("read login provider: %w", err)
	}
	if ProviderType(providerType) != external.ProviderType || enterpriseID != external.EnterpriseID {
		return errors.New("login identity does not match the registered provider boundary")
	}
	var existingUserID string
	err = tx.QueryRowContext(ctx, `SELECT platform_user_id FROM login_identities WHERE provider_id=$1 AND subject_id=$2`, external.ProviderID, external.SubjectID).Scan(&existingUserID)
	if err == nil && existingUserID != platformUserID {
		return ErrLoginIdentityConflict
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read existing login identity: %w", err)
	}
	if err == nil {
		if _, err := tx.ExecContext(ctx, `
UPDATE login_identities
SET display_name=COALESCE(NULLIF($3,''), display_name),
    email=COALESCE(NULLIF($4,''), email),
    last_login_at=NOW()
WHERE provider_id=$1 AND subject_id=$2`, external.ProviderID, external.SubjectID, strings.TrimSpace(external.DisplayName), strings.TrimSpace(external.Email)); err != nil {
			return fmt.Errorf("refresh linked login identity: %w", err)
		}
	} else if errors.Is(err, sql.ErrNoRows) {
		if _, err := tx.ExecContext(ctx, `INSERT INTO login_identities (provider_id, subject_id, platform_user_id, display_name, email) VALUES ($1,$2,$3,$4,$5)`, external.ProviderID, external.SubjectID, platformUserID, strings.TrimSpace(external.DisplayName), strings.TrimSpace(external.Email)); err != nil {
			return fmt.Errorf("link login identity: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit login identity link: %w", err)
	}
	return nil
}

func (s *PostgresIdentityStore) ListLoginMethods(ctx context.Context, platformUserID string) ([]LoginMethod, error) {
	rows, err := s.database.QueryContext(ctx, `
SELECT li.provider_id, lp.provider_type, lp.display_name, li.subject_id, li.created_at
FROM login_identities li JOIN login_providers lp ON lp.provider_id=li.provider_id
WHERE li.platform_user_id=$1 ORDER BY li.created_at, li.provider_id`, strings.TrimSpace(platformUserID))
	if err != nil {
		return nil, fmt.Errorf("list login methods: %w", err)
	}
	defer rows.Close()
	methods := make([]LoginMethod, 0)
	for rows.Next() {
		var method LoginMethod
		if err := rows.Scan(&method.ProviderID, &method.ProviderType, &method.DisplayName, &method.SubjectID, &method.LinkedAt); err != nil {
			return nil, fmt.Errorf("scan login method: %w", err)
		}
		methods = append(methods, method)
	}
	return methods, rows.Err()
}

func (s *PostgresIdentityStore) LatestLoginAtForProvider(ctx context.Context, providerID string) (time.Time, bool, error) {
	providerID = strings.TrimSpace(providerID)
	if providerID == "" {
		return time.Time{}, false, errors.New("login provider ID is required")
	}
	var latest sql.NullTime
	if err := s.database.QueryRowContext(ctx, `
SELECT MAX(last_login_at)
FROM login_identities
WHERE provider_id=$1`, providerID).Scan(&latest); err != nil {
		return time.Time{}, false, fmt.Errorf("read latest provider login: %w", err)
	}
	if !latest.Valid {
		return time.Time{}, false, nil
	}
	return latest.Time.UTC(), true, nil
}

func (s *PostgresIdentityStore) RemoveLoginIdentity(ctx context.Context, platformUserID, providerID, subjectID string) error {
	platformUserID, providerID, subjectID = strings.TrimSpace(platformUserID), strings.TrimSpace(providerID), strings.TrimSpace(subjectID)
	tx, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin login identity removal: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "login-identities:"+platformUserID); err != nil {
		return fmt.Errorf("lock login identities: %w", err)
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM login_identities WHERE platform_user_id=$1`, platformUserID).Scan(&count); err != nil {
		return fmt.Errorf("count login identities: %w", err)
	}
	if count <= 1 {
		return ErrLastLoginIdentity
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM login_identities WHERE platform_user_id=$1 AND provider_id=$2 AND subject_id=$3`, platformUserID, providerID, subjectID)
	if err != nil {
		return fmt.Errorf("remove login identity: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrPlatformUserNotFound
	}
	if providerID == "local" {
		if _, err := tx.ExecContext(ctx, `DELETE FROM local_credentials WHERE platform_user_id=$1`, platformUserID); err != nil {
			return fmt.Errorf("remove local credential: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit login identity removal: %w", err)
	}
	return nil
}

func (s *PostgresIdentityStore) CreateLocalUser(ctx context.Context, username, displayName, email, passwordHash string, mustChangePassword bool) (PlatformUser, error) {
	username, err := NormalizeLocalUsername(username)
	if err != nil {
		return PlatformUser{}, err
	}
	if strings.TrimSpace(passwordHash) == "" {
		return PlatformUser{}, errors.New("password hash is required")
	}
	if strings.TrimSpace(displayName) == "" {
		displayName = username
	}
	now := time.Now().UTC()
	user := PlatformUser{PlatformUserID: uuid.NewString(), DisplayName: strings.TrimSpace(displayName), Email: strings.TrimSpace(email), Status: "active", FirstSeenAt: now, LastLoginAt: now}
	tx, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return PlatformUser{}, fmt.Errorf("begin local user creation: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO login_providers (provider_id, provider_type, display_name, enterprise_id, enabled)
VALUES ('local','local','本地账号','local',TRUE)
ON CONFLICT (provider_id) DO NOTHING`); err != nil {
		return PlatformUser{}, fmt.Errorf("ensure local provider: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO platform_users (platform_user_id, display_name, email, status, first_seen_at, last_login_at) VALUES ($1,$2,$3,'active',$4,$4)`, user.PlatformUserID, user.DisplayName, user.Email, now); err != nil {
		return PlatformUser{}, fmt.Errorf("create local platform user: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO login_identities (provider_id, subject_id, platform_user_id, display_name, email, created_at, last_login_at) VALUES ('local',$1,$2,$3,$4,$5,$5)`, username, user.PlatformUserID, user.DisplayName, user.Email, now); err != nil {
		return PlatformUser{}, fmt.Errorf("create local login identity: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO local_credentials (platform_user_id, username, password_hash, must_change_password) VALUES ($1,$2,$3,$4)`, user.PlatformUserID, username, passwordHash, mustChangePassword); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			return PlatformUser{}, ErrLocalUsernameTaken
		}
		return PlatformUser{}, fmt.Errorf("create local credential: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return PlatformUser{}, fmt.Errorf("commit local user creation: %w", err)
	}
	return user, nil
}

func (s *PostgresIdentityStore) LookupLocalCredential(ctx context.Context, username string) (LocalCredential, PlatformUser, error) {
	username, err := NormalizeLocalUsername(username)
	if err != nil {
		return LocalCredential{}, PlatformUser{}, ErrLocalCredentialNotFound
	}
	var credential LocalCredential
	var user PlatformUser
	err = s.database.QueryRowContext(ctx, `
SELECT c.platform_user_id, c.username, c.password_hash, c.must_change_password,
       p.display_name, p.email, p.status, p.first_seen_at, p.last_login_at
FROM local_credentials c
JOIN platform_users p ON p.platform_user_id=c.platform_user_id
WHERE c.username=$1`, username).Scan(&credential.PlatformUserID, &credential.Username, &credential.PasswordHash, &credential.MustChangePassword, &user.DisplayName, &user.Email, &user.Status, &user.FirstSeenAt, &user.LastLoginAt)
	if errors.Is(err, sql.ErrNoRows) {
		return LocalCredential{}, PlatformUser{}, ErrLocalCredentialNotFound
	}
	if err != nil {
		return LocalCredential{}, PlatformUser{}, fmt.Errorf("lookup local credential: %w", err)
	}
	user.PlatformUserID = credential.PlatformUserID
	return credential, user, nil
}

func (s *PostgresIdentityStore) LocalCredentialForUser(ctx context.Context, platformUserID string) (LocalCredential, error) {
	var credential LocalCredential
	err := s.database.QueryRowContext(ctx, `SELECT platform_user_id, username, password_hash, must_change_password FROM local_credentials WHERE platform_user_id=$1`, strings.TrimSpace(platformUserID)).Scan(&credential.PlatformUserID, &credential.Username, &credential.PasswordHash, &credential.MustChangePassword)
	if errors.Is(err, sql.ErrNoRows) {
		return LocalCredential{}, ErrLocalCredentialNotFound
	}
	if err != nil {
		return LocalCredential{}, fmt.Errorf("lookup local credential for user: %w", err)
	}
	return credential, nil
}

func (s *PostgresIdentityStore) SetLocalPassword(ctx context.Context, platformUserID, passwordHash string, mustChangePassword bool) error {
	if strings.TrimSpace(passwordHash) == "" {
		return errors.New("password hash is required")
	}
	result, err := s.database.ExecContext(ctx, `UPDATE local_credentials SET password_hash=$2, must_change_password=$3, credential_version=credential_version+1, updated_at=NOW() WHERE platform_user_id=$1`, strings.TrimSpace(platformUserID), passwordHash, mustChangePassword)
	if err != nil {
		return fmt.Errorf("update local password: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("read local password update: %w", err)
	} else if affected == 0 {
		return ErrLocalCredentialNotFound
	}
	return nil
}

func (s *PostgresIdentityStore) HasUsableSystemAdmin(ctx context.Context) (bool, error) {
	var exists bool
	if err := s.database.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM system_admins sa JOIN platform_users p ON p.platform_user_id=sa.platform_user_id WHERE p.status='active')`).Scan(&exists); err != nil {
		return false, fmt.Errorf("check usable system administrator: %w", err)
	}
	return exists, nil
}

func (s *PostgresIdentityStore) ResolveSessionUser(ctx context.Context, platformUserID string) (SessionUser, error) {
	platformUserID = strings.TrimSpace(platformUserID)
	principal := SessionUser{Tenants: make([]TenantRole, 0)}
	var status string
	if err := s.database.QueryRowContext(ctx, `
SELECT p.platform_user_id, p.display_name, p.email, p.status,
       EXISTS(SELECT 1 FROM system_admins sa WHERE sa.platform_user_id=p.platform_user_id),
       COALESCE((SELECT must_change_password FROM local_credentials c WHERE c.platform_user_id=p.platform_user_id), FALSE)
FROM platform_users p WHERE p.platform_user_id=$1`, platformUserID).Scan(&principal.PlatformUserID, &principal.DisplayName, &principal.Email, &status, &principal.IsSystemAdmin, &principal.MustChangePassword); errors.Is(err, sql.ErrNoRows) {
		return SessionUser{}, ErrPlatformUserNotFound
	} else if err != nil {
		return SessionUser{}, fmt.Errorf("resolve session user: %w", err)
	}
	if status != "active" {
		return SessionUser{}, ErrPlatformUserSuspended
	}
	rows, err := s.database.QueryContext(ctx, `
SELECT tm.tenant_id, COALESCE(NULLIF(t.display_name,''), t.id), tm.role, tm.status, tm.conversation_content_audit
FROM tenant_members tm JOIN tenants t ON t.id=tm.tenant_id
WHERE tm.platform_user_id=$1 AND tm.status='active'
ORDER BY t.display_name, tm.tenant_id`, platformUserID)
	if err != nil {
		return SessionUser{}, fmt.Errorf("resolve session memberships: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var role TenantRole
		if err := rows.Scan(&role.TenantID, &role.DisplayName, &role.Role, &role.Status, &role.ConversationContentAudit); err != nil {
			return SessionUser{}, fmt.Errorf("scan session membership: %w", err)
		}
		principal.Tenants = append(principal.Tenants, role)
	}
	if err := rows.Err(); err != nil {
		return SessionUser{}, err
	}
	if len(principal.Tenants) == 1 {
		principal.Role = principal.Tenants[0].Role
	}
	return principal, nil
}

func (s *PostgresIdentityStore) SetSystemAdmin(ctx context.Context, platformUserID string, enabled bool) error {
	platformUserID = strings.TrimSpace(platformUserID)
	if platformUserID == "" {
		return errors.New("platform user ID is required")
	}
	tx, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin system administrator update: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('system-admins', 0))`); err != nil {
		return fmt.Errorf("lock system administrators: %w", err)
	}
	var userStatus string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM platform_users WHERE platform_user_id=$1`, platformUserID).Scan(&userStatus); errors.Is(err, sql.ErrNoRows) {
		return ErrPlatformUserNotFound
	} else if err != nil {
		return fmt.Errorf("read platform user: %w", err)
	}
	if enabled {
		if userStatus != "active" {
			return errors.New("a suspended user cannot be a system administrator")
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO system_admins (platform_user_id) VALUES ($1) ON CONFLICT DO NOTHING`, platformUserID); err != nil {
			return fmt.Errorf("grant system admin: %w", err)
		}
	} else {
		var isAdmin bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM system_admins WHERE platform_user_id=$1)`, platformUserID).Scan(&isAdmin); err != nil {
			return fmt.Errorf("read system administrator: %w", err)
		}
		if isAdmin {
			var otherUsable int
			if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM system_admins sa
JOIN platform_users p ON p.platform_user_id=sa.platform_user_id
WHERE sa.platform_user_id<>$1 AND p.status='active'`, platformUserID).Scan(&otherUsable); err != nil {
				return fmt.Errorf("count usable system administrators: %w", err)
			}
			if otherUsable == 0 {
				return ErrLastSystemAdmin
			}
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM system_admins WHERE platform_user_id=$1`, platformUserID); err != nil {
			return fmt.Errorf("revoke system admin: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit system administrator update: %w", err)
	}
	return nil
}

func (s *PostgresIdentityStore) IsSystemAdmin(ctx context.Context, platformUserID string) (bool, error) {
	var exists bool
	if err := s.database.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM system_admins WHERE platform_user_id=$1)`, strings.TrimSpace(platformUserID)).Scan(&exists); err != nil {
		return false, fmt.Errorf("lookup system admin: %w", err)
	}
	return exists, nil
}

func (s *PostgresIdentityStore) ListPlatformUsers(ctx context.Context, request MemberPageRequest) (MemberPage, error) {
	request = normalizeMemberPageRequest(request)
	hasAfter, afterName, afterID := memberAfter(request.After)
	return s.listMemberPage(ctx, request.Limit, `
SELECT p.platform_user_id, p.display_name, p.email, '', p.status,
       EXISTS(SELECT 1 FROM system_admins sa WHERE sa.platform_user_id=p.platform_user_id),
       p.last_login_at, COALESCE(string_agg(DISTINCT lp.display_name, ', '), ''), FALSE
FROM platform_users p
LEFT JOIN login_identities li ON li.platform_user_id=p.platform_user_id
LEFT JOIN login_providers lp ON lp.provider_id=li.provider_id
WHERE ($1='' OR p.platform_user_id ILIKE $2 OR p.display_name ILIKE $2 OR p.email ILIKE $2)
  AND ($3=FALSE OR (p.display_name, p.platform_user_id) > ($4, $5))
GROUP BY p.platform_user_id, p.display_name, p.email, p.status, p.last_login_at
ORDER BY p.display_name, p.platform_user_id
LIMIT $6`, request.Query, "%"+request.Query+"%", hasAfter, afterName, afterID, request.Limit+1)
}

func (s *PostgresIdentityStore) UpdatePlatformUserProfile(ctx context.Context, platformUserID, displayName string) error {
	platformUserID, displayName = strings.TrimSpace(platformUserID), strings.TrimSpace(displayName)
	if platformUserID == "" {
		return ErrPlatformUserNotFound
	}
	if displayName == "" || len([]rune(displayName)) > 80 {
		return errors.New("display name must be between 1 and 80 characters")
	}
	result, err := s.database.ExecContext(ctx, `UPDATE platform_users SET display_name=$2 WHERE platform_user_id=$1`, platformUserID, displayName)
	if err != nil {
		return fmt.Errorf("update platform user profile: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read platform user profile result: %w", err)
	}
	if affected == 0 {
		return ErrPlatformUserNotFound
	}
	return nil
}

func (s *PostgresIdentityStore) UpdatePlatformUserAccess(ctx context.Context, platformUserID, status string, systemAdmin bool) error {
	platformUserID, status = strings.TrimSpace(platformUserID), strings.TrimSpace(status)
	if platformUserID == "" || (status != "active" && status != "suspended") {
		return errors.New("platform user status is invalid")
	}
	tx, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin platform user access update: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('platform-user-access', 0))`); err != nil {
		return fmt.Errorf("lock platform user access: %w", err)
	}
	var currentStatus string
	var currentSystemAdmin bool
	if err := tx.QueryRowContext(ctx, `
SELECT p.status, EXISTS(SELECT 1 FROM system_admins sa WHERE sa.platform_user_id=p.platform_user_id)
FROM platform_users p WHERE p.platform_user_id=$1`, platformUserID).Scan(&currentStatus, &currentSystemAdmin); errors.Is(err, sql.ErrNoRows) {
		return ErrPlatformUserNotFound
	} else if err != nil {
		return fmt.Errorf("read platform user access: %w", err)
	}
	if systemAdmin && status != "active" {
		return errors.New("a suspended user cannot be a system administrator")
	}
	if currentSystemAdmin && (!systemAdmin || status == "suspended") {
		var otherUsable int
		if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM system_admins sa
JOIN platform_users p ON p.platform_user_id=sa.platform_user_id
WHERE sa.platform_user_id<>$1 AND p.status='active'`, platformUserID).Scan(&otherUsable); err != nil {
			return fmt.Errorf("count usable system administrators: %w", err)
		}
		if otherUsable == 0 {
			return ErrLastSystemAdmin
		}
	}
	if currentStatus == "active" && status == "suspended" {
		var blocksTenant bool
		if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1
    FROM tenant_members target
    JOIN tenants t ON t.id=target.tenant_id AND t.status='active'
    WHERE target.platform_user_id=$1 AND target.role='admin' AND target.status='active'
      AND NOT EXISTS (
          SELECT 1 FROM tenant_members other
          JOIN platform_users p ON p.platform_user_id=other.platform_user_id AND p.status='active'
          WHERE other.tenant_id=target.tenant_id
            AND other.platform_user_id<>$1
            AND other.role='admin' AND other.status='active'
      )
)`, platformUserID).Scan(&blocksTenant); err != nil {
			return fmt.Errorf("check tenant administrator continuity: %w", err)
		}
		if blocksTenant {
			return ErrLastTenantAdmin
		}
	}
	result, err := tx.ExecContext(ctx, `UPDATE platform_users SET status=$2 WHERE platform_user_id=$1`, platformUserID, status)
	if err != nil {
		return fmt.Errorf("set platform user status: %w", err)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
		return fmt.Errorf("read platform user status result: %w", rowsErr)
	} else if affected == 0 {
		return ErrPlatformUserNotFound
	}
	if systemAdmin {
		if _, err := tx.ExecContext(ctx, `INSERT INTO system_admins (platform_user_id) VALUES ($1) ON CONFLICT DO NOTHING`, platformUserID); err != nil {
			return fmt.Errorf("grant system admin: %w", err)
		}
	} else {
		if _, err := tx.ExecContext(ctx, `DELETE FROM system_admins WHERE platform_user_id=$1`, platformUserID); err != nil {
			return fmt.Errorf("revoke system admin: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit platform user access update: %w", err)
	}
	return nil
}

func (s *PostgresIdentityStore) CreateTenant(ctx context.Context, tenantID, displayName, initialAdminPlatformUserID string) error {
	tenantID = strings.TrimSpace(tenantID)
	displayName = strings.TrimSpace(displayName)
	initialAdminPlatformUserID = strings.TrimSpace(initialAdminPlatformUserID)
	if tenantID == "" || displayName == "" || initialAdminPlatformUserID == "" {
		return errors.New("tenant ID, display name and initial administrator are required")
	}
	tx, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tenant creation: %w", err)
	}
	defer tx.Rollback()
	var status string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM platform_users WHERE platform_user_id=$1`, initialAdminPlatformUserID).Scan(&status); errors.Is(err, sql.ErrNoRows) {
		return ErrPlatformUserNotFound
	} else if err != nil {
		return fmt.Errorf("read initial tenant administrator: %w", err)
	}
	if status != "active" {
		return errors.New("initial tenant administrator must be active")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO tenants (id, display_name, status) VALUES ($1,$2,'active')`, tenantID, displayName); err != nil {
		return fmt.Errorf("create tenant: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO tenant_members (tenant_id, platform_user_id, role, status) VALUES ($1,$2,'admin','active')`, tenantID, initialAdminPlatformUserID); err != nil {
		return fmt.Errorf("create initial tenant administrator: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tenant creation: %w", err)
	}
	return nil
}

func (s *PostgresIdentityStore) TenantStatus(ctx context.Context, tenantID string) (string, error) {
	var status string
	err := s.database.QueryRowContext(ctx, `SELECT status FROM tenants WHERE id=$1`, strings.TrimSpace(tenantID)).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrTenantNotFound
	}
	if err != nil {
		return "", fmt.Errorf("read tenant status: %w", err)
	}
	return status, nil
}

func (s *PostgresIdentityStore) SetTenantStatus(ctx context.Context, tenantID, status string) error {
	tenantID, status = strings.TrimSpace(tenantID), strings.TrimSpace(status)
	if tenantID == "" || (status != TenantActive && status != TenantSuspended) {
		return errors.New("invalid tenant status")
	}
	tx, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tenant status update: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "tenant-members:"+tenantID); err != nil {
		return fmt.Errorf("lock tenant: %w", err)
	}
	var current string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM tenants WHERE id=$1 FOR UPDATE`, tenantID).Scan(&current); errors.Is(err, sql.ErrNoRows) {
		return ErrTenantNotFound
	} else if err != nil {
		return fmt.Errorf("read tenant status: %w", err)
	}
	if status == TenantActive {
		var admins int
		if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM tenant_members tm
JOIN platform_users p ON p.platform_user_id=tm.platform_user_id AND p.status='active'
WHERE tm.tenant_id=$1 AND tm.role='admin' AND tm.status='active'`, tenantID).Scan(&admins); err != nil {
			return fmt.Errorf("count active tenant administrators: %w", err)
		}
		if admins == 0 {
			return ErrTenantNeedsAdmin
		}
	}
	if current != status {
		if _, err := tx.ExecContext(ctx, `UPDATE tenants SET status=$2 WHERE id=$1`, tenantID, status); err != nil {
			return fmt.Errorf("set tenant status: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tenant status update: %w", err)
	}
	return nil
}

func (s *PostgresIdentityStore) ListTenantModelGrants(ctx context.Context, tenantID string) ([]TenantModelGrant, error) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return nil, errors.New("tenant ID is required")
	}
	var exists bool
	if err := s.database.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tenants WHERE id=$1)`, tenantID).Scan(&exists); err != nil {
		return nil, fmt.Errorf("check tenant model policy tenant: %w", err)
	}
	if !exists {
		return nil, ErrTenantNotFound
	}
	rows, err := s.database.QueryContext(ctx, `
SELECT provider_id, model_name
FROM tenant_model_allowlist
WHERE tenant_id=$1
ORDER BY provider_id, model_name`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list tenant model grants: %w", err)
	}
	defer rows.Close()
	grants := make([]TenantModelGrant, 0)
	for rows.Next() {
		var grant TenantModelGrant
		if err := rows.Scan(&grant.ProviderID, &grant.ModelName); err != nil {
			return nil, fmt.Errorf("scan tenant model grant: %w", err)
		}
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return grants, nil
}

func (s *PostgresIdentityStore) ReplaceTenantModelGrants(ctx context.Context, tenantID string, grants []TenantModelGrant) error {
	tenantID = strings.TrimSpace(tenantID)
	normalized, err := normalizeTenantModelGrants(grants)
	if err != nil {
		return err
	}
	if tenantID == "" {
		return errors.New("tenant ID is required")
	}
	tx, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tenant model policy update: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "tenant-models:"+tenantID); err != nil {
		return fmt.Errorf("lock tenant model policy: %w", err)
	}
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tenants WHERE id=$1)`, tenantID).Scan(&exists); err != nil {
		return fmt.Errorf("check tenant model policy tenant: %w", err)
	}
	if !exists {
		return ErrTenantNotFound
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM tenant_model_allowlist WHERE tenant_id=$1`, tenantID); err != nil {
		return fmt.Errorf("clear tenant model grants: %w", err)
	}
	for _, grant := range normalized {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO tenant_model_allowlist (tenant_id, provider_id, model_name)
VALUES ($1,$2,$3)`, tenantID, grant.ProviderID, grant.ModelName); err != nil {
			return fmt.Errorf("insert tenant model grant: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tenant model policy update: %w", err)
	}
	return nil
}

func normalizeTenantModelGrants(grants []TenantModelGrant) ([]TenantModelGrant, error) {
	result := make([]TenantModelGrant, 0, len(grants))
	seen := make(map[string]struct{}, len(grants))
	for _, grant := range grants {
		grant.ProviderID = strings.TrimSpace(grant.ProviderID)
		grant.ModelName = strings.TrimSpace(grant.ModelName)
		if grant.ProviderID == "" || grant.ModelName == "" {
			return nil, errors.New("tenant model grant requires provider_id and name")
		}
		key := grant.ProviderID + "\x00" + grant.ModelName
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("duplicate tenant model grant %q/%q", grant.ProviderID, grant.ModelName)
		}
		seen[key] = struct{}{}
		result = append(result, grant)
	}
	return result, nil
}

func (s *PostgresIdentityStore) GrantMembership(ctx context.Context, tenantID, platformUserID string, role Role) error {
	return s.SetTenantMembership(ctx, tenantID, platformUserID, role, "active")
}

func (s *PostgresIdentityStore) SetTenantMembership(ctx context.Context, tenantID, platformUserID string, role Role, status string) error {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(platformUserID) == "" {
		return errors.New("membership requires tenant and platform user")
	}
	if role != RoleAdmin && role != RoleMember {
		return errors.New("invalid tenant role")
	}
	if status != "active" && status != "suspended" {
		return errors.New("invalid tenant membership status")
	}
	tx, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tenant membership update: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "tenant-members:"+strings.TrimSpace(tenantID)); err != nil {
		return fmt.Errorf("lock tenant membership: %w", err)
	}
	var userStatus string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM platform_users WHERE platform_user_id=$1`, platformUserID).Scan(&userStatus); errors.Is(err, sql.ErrNoRows) {
		return ErrPlatformUserNotFound
	} else if err != nil {
		return fmt.Errorf("read tenant member user: %w", err)
	}
	if status == "active" && userStatus != "active" {
		return errors.New("a suspended platform user cannot have an active tenant membership")
	}
	var previousRole, previousStatus string
	lookupErr := tx.QueryRowContext(ctx, `SELECT role, status FROM tenant_members WHERE tenant_id=$1 AND platform_user_id=$2`, tenantID, platformUserID).Scan(&previousRole, &previousStatus)
	if lookupErr != nil && !errors.Is(lookupErr, sql.ErrNoRows) {
		return fmt.Errorf("read tenant membership: %w", lookupErr)
	}
	if previousRole == string(RoleAdmin) && previousStatus == "active" && (role != RoleAdmin || status != "active") {
		var tenantActive bool
		if err := tx.QueryRowContext(ctx, `SELECT status='active' FROM tenants WHERE id=$1`, tenantID).Scan(&tenantActive); errors.Is(err, sql.ErrNoRows) {
			return errors.New("tenant does not exist")
		} else if err != nil {
			return fmt.Errorf("read tenant status: %w", err)
		}
		if tenantActive {
			var otherAdmins int
			if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM tenant_members tm
JOIN platform_users p ON p.platform_user_id=tm.platform_user_id AND p.status='active'
WHERE tm.tenant_id=$1 AND tm.platform_user_id<>$2 AND tm.role='admin' AND tm.status='active'`, tenantID, platformUserID).Scan(&otherAdmins); err != nil {
				return fmt.Errorf("count active tenant administrators: %w", err)
			}
			if otherAdmins == 0 {
				return ErrLastTenantAdmin
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO tenant_members (tenant_id, platform_user_id, role, status)
VALUES ($1,$2,$3,$4)
ON CONFLICT (tenant_id, platform_user_id) DO UPDATE SET
    role=EXCLUDED.role,
    status=EXCLUDED.status,
    conversation_content_audit=CASE
        WHEN EXCLUDED.role='admin' AND EXCLUDED.status='active' THEN tenant_members.conversation_content_audit
        ELSE FALSE
    END`, tenantID, platformUserID, string(role), status); err != nil {
		return fmt.Errorf("grant tenant membership: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tenant membership update: %w", err)
	}
	return nil
}

func (s *PostgresIdentityStore) SetConversationContentAudit(ctx context.Context, tenantID, platformUserID string, enabled bool) error {
	tenantID = strings.TrimSpace(tenantID)
	platformUserID = strings.TrimSpace(platformUserID)
	if tenantID == "" || platformUserID == "" {
		return errors.New("tenant and platform user are required")
	}
	result, err := s.database.ExecContext(ctx, `
UPDATE tenant_members
SET conversation_content_audit=$3
WHERE tenant_id=$1 AND platform_user_id=$2 AND role='admin' AND status='active'`, tenantID, platformUserID, enabled)
	if err != nil {
		return fmt.Errorf("set conversation content audit permission: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read conversation content audit update result: %w", err)
	}
	if affected == 0 {
		return errors.New("conversation content audit requires an active tenant administrator")
	}
	return nil
}

func (s *PostgresIdentityStore) ListTenantMemberships(ctx context.Context, platformUserID string) ([]TenantMembership, error) {
	rows, err := s.database.QueryContext(ctx, `
	SELECT tenant_id, platform_user_id, role, status, conversation_content_audit
FROM tenant_members WHERE platform_user_id=$1 ORDER BY tenant_id`, strings.TrimSpace(platformUserID))
	if err != nil {
		return nil, fmt.Errorf("list tenant memberships: %w", err)
	}
	defer rows.Close()
	memberships := make([]TenantMembership, 0)
	for rows.Next() {
		var membership TenantMembership
		var role string
		if err := rows.Scan(&membership.TenantID, &membership.PlatformUserID, &role, &membership.Status, &membership.ConversationContentAudit); err != nil {
			return nil, fmt.Errorf("scan tenant membership: %w", err)
		}
		membership.Role = Role(role)
		memberships = append(memberships, membership)
	}
	return memberships, rows.Err()
}

func (s *PostgresIdentityStore) ListTenantMembers(ctx context.Context, tenantID string, request MemberPageRequest) (MemberPage, error) {
	request = normalizeMemberPageRequest(request)
	hasAfter, afterName, afterID := memberAfter(request.After)
	return s.listMemberPage(ctx, request.Limit, `
SELECT p.platform_user_id, p.display_name, p.email, tm.role, tm.status,
       EXISTS(SELECT 1 FROM system_admins sa WHERE sa.platform_user_id=p.platform_user_id), p.last_login_at,
       COALESCE(string_agg(DISTINCT lp.display_name, ', '), ''), tm.conversation_content_audit
FROM tenant_members tm
JOIN platform_users p ON p.platform_user_id=tm.platform_user_id
LEFT JOIN login_identities li ON li.platform_user_id=p.platform_user_id
LEFT JOIN login_providers lp ON lp.provider_id=li.provider_id
WHERE tm.tenant_id=$1
  AND ($2='' OR p.platform_user_id ILIKE $3 OR p.display_name ILIKE $3 OR p.email ILIKE $3)
  AND ($4=FALSE OR (p.display_name, p.platform_user_id) > ($5, $6))
GROUP BY p.platform_user_id, p.display_name, p.email, tm.role, tm.status, tm.conversation_content_audit, p.last_login_at
ORDER BY p.display_name, p.platform_user_id
LIMIT $7`, tenantID, request.Query, "%"+request.Query+"%", hasAfter, afterName, afterID, request.Limit+1)
}

func (s *PostgresIdentityStore) ListTenantMemberCandidates(ctx context.Context, tenantID string, request MemberPageRequest) (MemberPage, error) {
	request = normalizeMemberPageRequest(request)
	hasAfter, afterName, afterID := memberAfter(request.After)
	return s.listMemberPage(ctx, request.Limit, `
SELECT p.platform_user_id, p.display_name, p.email, '', p.status,
       EXISTS(SELECT 1 FROM system_admins sa WHERE sa.platform_user_id=p.platform_user_id),
       p.last_login_at, COALESCE(string_agg(DISTINCT lp.display_name, ', '), ''), FALSE
FROM platform_users p
LEFT JOIN login_identities li ON li.platform_user_id=p.platform_user_id
LEFT JOIN login_providers lp ON lp.provider_id=li.provider_id
WHERE p.status='active'
  AND NOT EXISTS (SELECT 1 FROM tenant_members tm WHERE tm.tenant_id=$1 AND tm.platform_user_id=p.platform_user_id)
  AND ($2='' OR p.platform_user_id ILIKE $3 OR p.display_name ILIKE $3 OR p.email ILIKE $3)
  AND ($4=FALSE OR (p.display_name, p.platform_user_id) > ($5, $6))
GROUP BY p.platform_user_id, p.display_name, p.email, p.status, p.last_login_at
ORDER BY p.display_name, p.platform_user_id
LIMIT $7`, tenantID, request.Query, "%"+request.Query+"%", hasAfter, afterName, afterID, request.Limit+1)
}

func (s *PostgresIdentityStore) listMemberPage(ctx context.Context, limit int, query string, args ...any) (MemberPage, error) {
	rows, err := s.database.QueryContext(ctx, query, args...)
	if err != nil {
		return MemberPage{}, fmt.Errorf("list members: %w", err)
	}
	defer rows.Close()
	result := make([]MemberSummary, 0)
	for rows.Next() {
		item := MemberSummary{Providers: make([]string, 0)}
		var providers string
		if err := rows.Scan(&item.PlatformUserID, &item.DisplayName, &item.Email, &item.Role, &item.Status, &item.IsSystemAdmin, &item.LastLoginAt, &providers, &item.ConversationContentAudit); err != nil {
			return MemberPage{}, fmt.Errorf("scan member: %w", err)
		}
		if providers != "" {
			item.Providers = strings.Split(providers, ", ")
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return MemberPage{}, err
	}
	page := MemberPage{Members: result}
	if len(page.Members) > limit {
		page.Members = page.Members[:limit]
		last := page.Members[len(page.Members)-1]
		page.Next = &MemberCursor{DisplayName: last.DisplayName, PlatformUserID: last.PlatformUserID}
	}
	return page, nil
}

func normalizeMemberPageRequest(request MemberPageRequest) MemberPageRequest {
	request.Query = strings.TrimSpace(request.Query)
	if request.Limit <= 0 {
		request.Limit = 50
	} else if request.Limit > 200 {
		request.Limit = 200
	}
	return request
}

func memberAfter(cursor *MemberCursor) (bool, string, string) {
	if cursor == nil {
		return false, "", ""
	}
	return true, cursor.DisplayName, cursor.PlatformUserID
}

func (s *PostgresIdentityStore) RoleFor(ctx context.Context, tenantID, platformUserID string) (Role, error) {
	var role string
	err := s.database.QueryRowContext(ctx, `SELECT role FROM tenant_members WHERE tenant_id=$1 AND platform_user_id=$2 AND status='active'`, tenantID, platformUserID).Scan(&role)
	if errors.Is(err, sql.ErrNoRows) {
		return "", errors.New("not a tenant member")
	}
	if err != nil {
		return "", fmt.Errorf("lookup tenant role: %w", err)
	}
	return Role(role), nil
}

func (s *PostgresIdentityStore) ListTenantSummaries(ctx context.Context, platformUserID string, includeAll bool) ([]TenantSummary, error) {
	query := `
SELECT t.id, COALESCE(NULLIF(t.display_name, ''), t.id), COALESCE(tm.role, ''), t.status
FROM tenants t
LEFT JOIN tenant_members tm ON tm.tenant_id=t.id AND tm.platform_user_id=$1 AND tm.status='active'
WHERE $2 OR tm.platform_user_id IS NOT NULL
ORDER BY t.display_name, t.id`
	rows, err := s.database.QueryContext(ctx, query, strings.TrimSpace(platformUserID), includeAll)
	if err != nil {
		return nil, fmt.Errorf("list tenant summaries: %w", err)
	}
	defer rows.Close()
	result := make([]TenantSummary, 0)
	for rows.Next() {
		var summary TenantSummary
		var role string
		if err := rows.Scan(&summary.TenantID, &summary.DisplayName, &role, &summary.Status); err != nil {
			return nil, fmt.Errorf("scan tenant summary: %w", err)
		}
		summary.Role = Role(role)
		result = append(result, summary)
	}
	return result, rows.Err()
}

func (s *PostgresIdentityStore) ResolveChannelIdentity(ctx context.Context, tenantID string, channel channels.Channel, bindingID, externalUserID, trustedEnterpriseID string) (ChannelIdentity, bool, error) {
	base := ChannelIdentity{TenantID: tenantID, Channel: channel, BindingID: bindingID, ExternalUserID: externalUserID}
	var platformUserID sql.NullString
	var linkedAt sql.NullTime
	err := s.database.QueryRowContext(ctx, `
SELECT platform_user_id, linked_at FROM channel_identities
WHERE tenant_id=$1 AND channel_type=$2 AND binding_id=$3 AND external_user_id=$4`,
		tenantID, string(channel), bindingID, externalUserID).Scan(&platformUserID, &linkedAt)
	if err == nil {
		if platformUserID.Valid && strings.TrimSpace(platformUserID.String) != "" {
			base.PlatformUserID = platformUserID.String
			if linkedAt.Valid {
				base.LinkedAt = linkedAt.Time
			}
			return base, true, nil
		}
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return ChannelIdentity{}, false, fmt.Errorf("resolve channel identity: %w", err)
	}
	trustedPlatformUserID := ""
	if channel == channels.WeCom && strings.TrimSpace(trustedEnterpriseID) != "" {
		rows, lookupErr := s.database.QueryContext(ctx, `
SELECT DISTINCT ci.platform_user_id
FROM channel_identities ci
JOIN channel_bindings cb
  ON cb.channel_type=ci.channel_type AND cb.external_binding_id=ci.binding_id
JOIN platform_users p
  ON p.platform_user_id=ci.platform_user_id AND p.status='active'
JOIN tenant_members tm
  ON tm.tenant_id=ci.tenant_id AND tm.platform_user_id=ci.platform_user_id AND tm.status='active'
WHERE ci.tenant_id=$1 AND ci.channel_type='wecom' AND ci.external_user_id=$2
  AND ci.platform_user_id IS NOT NULL AND cb.trusted_enterprise_id=$3
LIMIT 2`, tenantID, strings.TrimSpace(externalUserID), strings.TrimSpace(trustedEnterpriseID))
		if lookupErr != nil {
			return ChannelIdentity{}, false, fmt.Errorf("resolve linked WeCom enterprise identity: %w", lookupErr)
		}
		linkedUsers := make([]string, 0, 2)
		for rows.Next() {
			var linkedUser string
			if scanErr := rows.Scan(&linkedUser); scanErr != nil {
				_ = rows.Close()
				return ChannelIdentity{}, false, fmt.Errorf("scan linked WeCom enterprise identity: %w", scanErr)
			}
			if linkedUser = strings.TrimSpace(linkedUser); linkedUser != "" {
				linkedUsers = append(linkedUsers, linkedUser)
			}
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			_ = rows.Close()
			return ChannelIdentity{}, false, fmt.Errorf("iterate linked WeCom enterprise identities: %w", rowsErr)
		}
		_ = rows.Close()
		if len(linkedUsers) > 1 {
			return ChannelIdentity{}, false, ErrChannelIdentityConflict
		}
		if len(linkedUsers) == 1 {
			trustedPlatformUserID = linkedUsers[0]
		}
	}
	if trustedPlatformUserID == "" && channel == channels.WeCom && strings.TrimSpace(trustedEnterpriseID) != "" {
		err = s.database.QueryRowContext(ctx, `
SELECT li.platform_user_id
FROM login_providers lp
JOIN login_identities li ON li.provider_id=lp.provider_id
JOIN platform_users p ON p.platform_user_id=li.platform_user_id AND p.status='active'
JOIN tenant_members tm ON tm.platform_user_id=li.platform_user_id AND tm.tenant_id=$1 AND tm.status='active'
WHERE lp.provider_type='wecom' AND lp.enterprise_id=$2 AND lp.enabled=TRUE AND li.subject_id=$3
LIMIT 1`, tenantID, strings.TrimSpace(trustedEnterpriseID), strings.TrimSpace(externalUserID)).Scan(&trustedPlatformUserID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return ChannelIdentity{}, false, fmt.Errorf("resolve trusted WeCom identity: %w", err)
		}
	}
	if trustedPlatformUserID == "" {
		_, err = s.database.ExecContext(ctx, `
INSERT INTO channel_identities (tenant_id, channel_type, binding_id, external_user_id, platform_user_id, linked_at)
VALUES ($1,$2,$3,$4,NULL,NULL)
ON CONFLICT (tenant_id, channel_type, binding_id, external_user_id) DO NOTHING`, tenantID, string(channel), bindingID, externalUserID)
		if err != nil {
			return ChannelIdentity{}, false, fmt.Errorf("persist external identity: %w", err)
		}
		return base, false, nil
	}
	_, err = s.database.ExecContext(ctx, `
INSERT INTO channel_identities (tenant_id, channel_type, binding_id, external_user_id, platform_user_id, linked_at)
VALUES ($1,$2,$3,$4,$5,NOW())
ON CONFLICT (tenant_id, channel_type, binding_id, external_user_id) DO UPDATE
SET platform_user_id=EXCLUDED.platform_user_id, linked_at=EXCLUDED.linked_at
WHERE channel_identities.platform_user_id IS NULL`, tenantID, string(channel), bindingID, externalUserID, trustedPlatformUserID)
	if err != nil {
		return ChannelIdentity{}, false, fmt.Errorf("persist trusted external identity: %w", err)
	}
	var resolvedPlatform sql.NullString
	var resolvedLinked sql.NullTime
	if err := s.database.QueryRowContext(ctx, `SELECT platform_user_id, linked_at FROM channel_identities WHERE tenant_id=$1 AND channel_type=$2 AND binding_id=$3 AND external_user_id=$4`, tenantID, string(channel), bindingID, externalUserID).Scan(&resolvedPlatform, &resolvedLinked); err != nil {
		return ChannelIdentity{}, false, fmt.Errorf("read persisted external identity: %w", err)
	}
	if !resolvedPlatform.Valid || strings.TrimSpace(resolvedPlatform.String) == "" {
		return base, false, nil
	}
	base.PlatformUserID = resolvedPlatform.String
	if resolvedLinked.Valid {
		base.LinkedAt = resolvedLinked.Time
	}
	return base, true, nil
}

func (s *PostgresIdentityStore) LinkChannelIdentity(ctx context.Context, identity ChannelIdentity) error {
	if err := validateChannelIdentity(identity); err != nil {
		return err
	}
	tx, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin channel identity link: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	lockKey := advisoryLockKey(identity.TenantID, string(identity.Channel), identity.BindingID, identity.ExternalUserID)
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", lockKey); err != nil {
		return fmt.Errorf("lock channel identity: %w", err)
	}
	var existing sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT platform_user_id FROM channel_identities WHERE tenant_id=$1 AND channel_type=$2 AND binding_id=$3 AND external_user_id=$4`,
		identity.TenantID, string(identity.Channel), identity.BindingID, identity.ExternalUserID).Scan(&existing)
	if err == nil {
		if existing.Valid && existing.String != identity.PlatformUserID {
			return ErrChannelIdentityConflict
		}
		if !existing.Valid {
			if _, err := tx.ExecContext(ctx, `UPDATE channel_identities SET platform_user_id=$5, linked_at=NOW() WHERE tenant_id=$1 AND channel_type=$2 AND binding_id=$3 AND external_user_id=$4`, identity.TenantID, string(identity.Channel), identity.BindingID, identity.ExternalUserID, identity.PlatformUserID); err != nil {
				return fmt.Errorf("link existing external identity: %w", err)
			}
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read channel identity before link: %w", err)
	} else if _, err := tx.ExecContext(ctx, `
INSERT INTO channel_identities (tenant_id, channel_type, binding_id, external_user_id, platform_user_id)
VALUES ($1,$2,$3,$4,$5)`, identity.TenantID, string(identity.Channel), identity.BindingID, identity.ExternalUserID, identity.PlatformUserID); err != nil {
		return fmt.Errorf("link channel identity: %w", err)
	}
	if identity.Channel == channels.WeCom && strings.TrimSpace(identity.TrustedEnterpriseID) != "" {
		var conflict bool
		if err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
  SELECT 1
  FROM channel_bindings cb
  JOIN channel_identities ci
    ON ci.tenant_id=cb.tenant_id AND ci.channel_type=cb.channel_type AND ci.binding_id=cb.external_binding_id
  WHERE cb.tenant_id=$1 AND cb.channel_type='wecom' AND cb.trusted_enterprise_id=$2
    AND ci.external_user_id=$3 AND ci.platform_user_id IS NOT NULL AND ci.platform_user_id <> $4
)`, identity.TenantID, strings.TrimSpace(identity.TrustedEnterpriseID), identity.ExternalUserID, identity.PlatformUserID).Scan(&conflict); err != nil {
			return fmt.Errorf("check WeCom enterprise identity conflict: %w", err)
		}
		if conflict {
			return ErrChannelIdentityConflict
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO channel_identities (tenant_id, channel_type, binding_id, external_user_id, platform_user_id, linked_at)
SELECT cb.tenant_id, 'wecom', cb.external_binding_id, $3, $4, NOW()
FROM channel_bindings cb
WHERE cb.tenant_id=$1 AND cb.channel_type='wecom' AND cb.trusted_enterprise_id=$2
ON CONFLICT (tenant_id, channel_type, binding_id, external_user_id) DO UPDATE
SET platform_user_id=EXCLUDED.platform_user_id, linked_at=EXCLUDED.linked_at
WHERE channel_identities.platform_user_id IS NULL OR channel_identities.platform_user_id=EXCLUDED.platform_user_id`,
			identity.TenantID, strings.TrimSpace(identity.TrustedEnterpriseID), identity.ExternalUserID, identity.PlatformUserID); err != nil {
			return fmt.Errorf("propagate WeCom enterprise identity: %w", err)
		}
	}
	return tx.Commit()
}

// advisoryLockKey builds a PostgreSQL-text-safe, collision-free tuple encoding.
// PostgreSQL text values cannot contain NUL bytes, so do not use "\x00" as a
// separator for advisory-lock inputs. Length prefixes preserve tuple boundaries
// even when individual identity values contain ':' or other punctuation.
func advisoryLockKey(parts ...string) string {
	var builder strings.Builder
	for _, part := range parts {
		builder.WriteString(strconv.Itoa(len(part)))
		builder.WriteByte(':')
		builder.WriteString(part)
	}
	return builder.String()
}

func (s *PostgresIdentityStore) UnlinkChannelIdentity(ctx context.Context, identity ChannelIdentity) error {
	if err := validateChannelIdentity(identity); err != nil {
		return err
	}
	result, err := s.database.ExecContext(ctx, `
UPDATE channel_identities SET platform_user_id=NULL, linked_at=NULL
WHERE tenant_id=$1 AND channel_type=$2 AND binding_id=$3 AND external_user_id=$4 AND platform_user_id=$5`,
		identity.TenantID, string(identity.Channel), identity.BindingID, identity.ExternalUserID, identity.PlatformUserID)
	if err != nil {
		return fmt.Errorf("unlink channel identity: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read unlink result: %w", err)
	}
	if rows == 0 {
		return ErrChannelIdentityNotLinked
	}
	return nil
}

func (s *PostgresIdentityStore) ListChannelIdentities(ctx context.Context, tenantID, platformUserID string) ([]ChannelIdentity, error) {
	rows, err := s.database.QueryContext(ctx, `
SELECT tenant_id, channel_type, binding_id, external_user_id, platform_user_id, linked_at
FROM channel_identities WHERE tenant_id=$1 AND platform_user_id=$2
ORDER BY channel_type, binding_id, external_user_id`, tenantID, platformUserID)
	if err != nil {
		return nil, fmt.Errorf("list channel identities: %w", err)
	}
	defer rows.Close()
	result := make([]ChannelIdentity, 0)
	for rows.Next() {
		var item ChannelIdentity
		var channel string
		if err := rows.Scan(&item.TenantID, &channel, &item.BindingID, &item.ExternalUserID, &item.PlatformUserID, &item.LinkedAt); err != nil {
			return nil, fmt.Errorf("scan channel identity: %w", err)
		}
		item.Channel = channels.Channel(channel)
		result = append(result, item)
	}
	return result, rows.Err()
}

func validateChannelIdentity(identity ChannelIdentity) error {
	if strings.TrimSpace(identity.TenantID) == "" || !identity.Channel.Supported() || identity.Channel == channels.Web || strings.TrimSpace(identity.BindingID) == "" || strings.TrimSpace(identity.ExternalUserID) == "" || strings.TrimSpace(identity.PlatformUserID) == "" {
		return errors.New("channel identity is incomplete")
	}
	return nil
}

func validateExternalIdentity(identity Identity) error {
	if strings.TrimSpace(identity.ProviderID) == "" || strings.TrimSpace(identity.EnterpriseID) == "" || strings.TrimSpace(identity.SubjectID) == "" {
		return errors.New("login identity is incomplete")
	}
	switch identity.ProviderType {
	case ProviderWeCom, ProviderFeishu, ProviderOIDC, ProviderMock:
		return nil
	default:
		return fmt.Errorf("unsupported login provider type %q", identity.ProviderType)
	}
}

var _ IdentityStore = (*PostgresIdentityStore)(nil)
