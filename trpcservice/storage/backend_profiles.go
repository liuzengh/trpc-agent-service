package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

const (
	BackendDomainSession   = "session"
	BackendDomainMemory    = "memory"
	BackendDomainKnowledge = "knowledge"
	BackendDomainArtifact  = "artifact"

	BackendProfileActive   = "active"
	BackendProfileDisabled = "disabled"
)

var (
	ErrBackendProfileNotFound     = errors.New("backend profile not found")
	ErrBackendProfileUnauthorized = errors.New("backend profile is not authorized for tenant")
	ErrBackendProfileUnavailable  = errors.New("backend profile is unavailable")
)

type BackendProfile struct {
	ProfileID     string    `json:"profile_id"`
	DisplayName   string    `json:"display_name"`
	Driver        string    `json:"driver"`
	ConnectionRef string    `json:"connection_ref,omitempty"`
	Status        string    `json:"status"`
	Domains       []string  `json:"domains"`
	CreatedAt     time.Time `json:"created_at,omitempty"`
	UpdatedAt     time.Time `json:"updated_at,omitempty"`
}

type TenantBackendProfile struct {
	ProfileID   string   `json:"profile_id"`
	DisplayName string   `json:"display_name"`
	Driver      string   `json:"driver"`
	Status      string   `json:"status"`
	Domains     []string `json:"domains"`
	Available   bool     `json:"available"`
}

type BackendProfileResolver interface {
	ResolveTenantBackend(context.Context, string, string, string) (config.BackendConfig, error)
}

type BackendProfileStore interface {
	BackendProfileResolver
	ListBackendProfiles(context.Context) ([]BackendProfile, error)
	GetBackendProfile(context.Context, string) (BackendProfile, error)
	UpsertBackendProfile(context.Context, BackendProfile) (BackendProfile, error)
	DeleteBackendProfile(context.Context, string) error
	ListTenantBackendProfiles(context.Context, string) ([]TenantBackendProfile, error)
	ReplaceTenantBackendProfiles(context.Context, string, []string) error
}

func BackendProfileDomains(driver string) []string {
	switch strings.ToLower(strings.TrimSpace(driver)) {
	case "postgres":
		return []string{BackendDomainSession, BackendDomainMemory, BackendDomainArtifact}
	case "redis":
		return []string{BackendDomainSession}
	case "pgvector", "qdrant":
		return []string{BackendDomainKnowledge}
	case "s3", "cos":
		return []string{BackendDomainArtifact}
	case "mem0":
		return []string{BackendDomainMemory}
	default:
		return nil
	}
}

func BackendProfileSupportsDomain(driver, domain string) bool {
	for _, candidate := range BackendProfileDomains(driver) {
		if candidate == domain {
			return true
		}
	}
	return false
}

func NormalizeBackendProfile(profile BackendProfile) (BackendProfile, error) {
	profile.ProfileID = strings.TrimSpace(profile.ProfileID)
	profile.DisplayName = strings.TrimSpace(profile.DisplayName)
	profile.Driver = strings.ToLower(strings.TrimSpace(profile.Driver))
	profile.ConnectionRef = strings.TrimSpace(profile.ConnectionRef)
	profile.Status = strings.ToLower(strings.TrimSpace(profile.Status))
	if profile.Status == "" {
		profile.Status = BackendProfileActive
	}
	if profile.ProfileID == "" || profile.DisplayName == "" {
		return BackendProfile{}, errors.New("backend profile ID and display name are required")
	}
	if strings.ContainsAny(profile.ProfileID, " /\\") {
		return BackendProfile{}, errors.New("backend profile ID contains invalid characters")
	}
	if len(BackendProfileDomains(profile.Driver)) == 0 {
		return BackendProfile{}, fmt.Errorf("unsupported backend profile driver %q", profile.Driver)
	}
	if profile.Status != BackendProfileActive && profile.Status != BackendProfileDisabled {
		return BackendProfile{}, errors.New("backend profile status must be active or disabled")
	}
	if profile.ConnectionRef != "" && !strings.HasPrefix(profile.ConnectionRef, "env:") {
		return BackendProfile{}, errors.New("backend profile connection_ref must use an env: reference")
	}
	switch profile.Driver {
	case "redis", "qdrant", "s3", "cos", "mem0":
		if profile.ConnectionRef == "" {
			return BackendProfile{}, fmt.Errorf("backend profile driver %q requires connection_ref", profile.Driver)
		}
	}
	profile.Domains = BackendProfileDomains(profile.Driver)
	return profile, nil
}

type PostgresBackendProfileStore struct{ database *sql.DB }

func NewPostgresBackendProfileStore(database *sql.DB) (*PostgresBackendProfileStore, error) {
	if database == nil {
		return nil, errors.New("backend profile database is required")
	}
	return &PostgresBackendProfileStore{database: database}, nil
}

func (s *PostgresBackendProfileStore) ListBackendProfiles(ctx context.Context) ([]BackendProfile, error) {
	rows, err := s.database.QueryContext(ctx, `
SELECT profile_id, display_name, driver, connection_ref, status, created_at, updated_at
FROM backend_profiles ORDER BY profile_id`)
	if err != nil {
		return nil, fmt.Errorf("list backend profiles: %w", err)
	}
	defer rows.Close()
	profiles := make([]BackendProfile, 0)
	for rows.Next() {
		profile, err := scanBackendProfile(rows)
		if err != nil {
			return nil, err
		}
		profiles = append(profiles, profile)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list backend profiles: %w", err)
	}
	return profiles, nil
}

func (s *PostgresBackendProfileStore) GetBackendProfile(ctx context.Context, profileID string) (BackendProfile, error) {
	row := s.database.QueryRowContext(ctx, `
SELECT profile_id, display_name, driver, connection_ref, status, created_at, updated_at
FROM backend_profiles WHERE profile_id=$1`, strings.TrimSpace(profileID))
	return scanBackendProfile(row)
}

func (s *PostgresBackendProfileStore) UpsertBackendProfile(ctx context.Context, profile BackendProfile) (BackendProfile, error) {
	profile, err := NormalizeBackendProfile(profile)
	if err != nil {
		return BackendProfile{}, err
	}
	if current, lookupErr := s.GetBackendProfile(ctx, profile.ProfileID); lookupErr == nil {
		if current.Driver != profile.Driver || current.ConnectionRef != profile.ConnectionRef {
			return BackendProfile{}, errors.New("backend profile routing is immutable; create a new profile for a different driver or connection")
		}
	} else if !errors.Is(lookupErr, ErrBackendProfileNotFound) {
		return BackendProfile{}, lookupErr
	}
	row := s.database.QueryRowContext(ctx, `
INSERT INTO backend_profiles (profile_id, display_name, driver, connection_ref, status)
VALUES ($1,$2,$3,$4,$5)
ON CONFLICT (profile_id) DO UPDATE SET
    display_name=EXCLUDED.display_name,
    status=EXCLUDED.status,
    updated_at=NOW()
RETURNING profile_id, display_name, driver, connection_ref, status, created_at, updated_at`,
		profile.ProfileID, profile.DisplayName, profile.Driver, profile.ConnectionRef, profile.Status)
	return scanBackendProfile(row)
}

func (s *PostgresBackendProfileStore) DeleteBackendProfile(ctx context.Context, profileID string) error {
	profileID = strings.TrimSpace(profileID)
	if profileID == "platform-postgres" || profileID == "platform-pgvector" {
		return errors.New("built-in backend profiles cannot be deleted")
	}
	tx, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin backend profile delete: %w", err)
	}
	defer tx.Rollback()
	var assigned int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM tenant_backend_profiles WHERE profile_id=$1`, profileID).Scan(&assigned); err != nil {
		return fmt.Errorf("count backend profile assignments: %w", err)
	}
	if assigned > 0 {
		return errors.New("backend profile is still authorized to a tenant")
	}
	var referenced int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM application_configs
WHERE config_json #>> '{storage,session,profile_id}' = $1
   OR config_json #>> '{storage,memory,profile_id}' = $1
   OR config_json #>> '{storage,knowledge,profile_id}' = $1
   OR config_json #>> '{storage,artifact,profile_id}' = $1`, profileID).Scan(&referenced); err != nil {
		return fmt.Errorf("count backend profile references: %w", err)
	}
	if referenced > 0 {
		return errors.New("backend profile is referenced by application configuration history")
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM backend_profiles WHERE profile_id=$1`, profileID)
	if err != nil {
		return fmt.Errorf("delete backend profile: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read backend profile delete result: %w", err)
	}
	if rows == 0 {
		return ErrBackendProfileNotFound
	}
	return tx.Commit()
}

func (s *PostgresBackendProfileStore) ListTenantBackendProfiles(ctx context.Context, tenantID string) ([]TenantBackendProfile, error) {
	rows, err := s.database.QueryContext(ctx, `
SELECT p.profile_id, p.display_name, p.driver, p.status
FROM tenant_backend_profiles t
JOIN backend_profiles p ON p.profile_id=t.profile_id
WHERE t.tenant_id=$1
ORDER BY p.profile_id`, strings.TrimSpace(tenantID))
	if err != nil {
		return nil, fmt.Errorf("list tenant backend profiles: %w", err)
	}
	defer rows.Close()
	profiles := make([]TenantBackendProfile, 0)
	for rows.Next() {
		var profile TenantBackendProfile
		if err := rows.Scan(&profile.ProfileID, &profile.DisplayName, &profile.Driver, &profile.Status); err != nil {
			return nil, fmt.Errorf("scan tenant backend profile: %w", err)
		}
		profile.Domains = BackendProfileDomains(profile.Driver)
		profile.Available = profile.Status == BackendProfileActive
		profiles = append(profiles, profile)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list tenant backend profiles: %w", err)
	}
	return profiles, nil
}

func (s *PostgresBackendProfileStore) ReplaceTenantBackendProfiles(ctx context.Context, tenantID string, profileIDs []string) error {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return errors.New("tenant ID is required")
	}
	profileIDs = uniqueTrimmed(profileIDs)
	tx, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tenant backend policy update: %w", err)
	}
	defer tx.Rollback()
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tenants WHERE id=$1)`, tenantID).Scan(&exists); err != nil {
		return fmt.Errorf("read tenant: %w", err)
	}
	if !exists {
		return errors.New("tenant not found")
	}
	for _, profileID := range profileIDs {
		var status string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM backend_profiles WHERE profile_id=$1`, profileID).Scan(&status); errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("backend profile %q does not exist", profileID)
		} else if err != nil {
			return fmt.Errorf("read backend profile %q: %w", profileID, err)
		}
		if status != BackendProfileActive {
			return fmt.Errorf("backend profile %q is disabled", profileID)
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM tenant_backend_profiles WHERE tenant_id=$1`, tenantID); err != nil {
		return fmt.Errorf("replace tenant backend policy: %w", err)
	}
	for _, profileID := range profileIDs {
		if _, err := tx.ExecContext(ctx, `INSERT INTO tenant_backend_profiles (tenant_id, profile_id) VALUES ($1,$2)`, tenantID, profileID); err != nil {
			return fmt.Errorf("authorize backend profile %q: %w", profileID, err)
		}
	}
	return tx.Commit()
}

func (s *PostgresBackendProfileStore) ResolveTenantBackend(ctx context.Context, tenantID, domain, profileID string) (config.BackendConfig, error) {
	profileID = strings.TrimSpace(profileID)
	if profileID == "" {
		return config.BackendConfig{}, errors.New("backend profile ID is required")
	}
	var profile BackendProfile
	row := s.database.QueryRowContext(ctx, `
SELECT p.profile_id, p.display_name, p.driver, p.connection_ref, p.status, p.created_at, p.updated_at
FROM tenant_backend_profiles t
JOIN backend_profiles p ON p.profile_id=t.profile_id
WHERE t.tenant_id=$1 AND t.profile_id=$2`, strings.TrimSpace(tenantID), profileID)
	var err error
	profile, err = scanBackendProfile(row)
	if errors.Is(err, ErrBackendProfileNotFound) {
		return config.BackendConfig{}, fmt.Errorf("%w: %s", ErrBackendProfileUnauthorized, profileID)
	}
	if err != nil {
		return config.BackendConfig{}, err
	}
	if profile.Status != BackendProfileActive {
		return config.BackendConfig{}, fmt.Errorf("%w: %s", ErrBackendProfileUnavailable, profileID)
	}
	if !BackendProfileSupportsDomain(profile.Driver, domain) {
		return config.BackendConfig{}, fmt.Errorf("backend profile %q (%s) does not support %s", profileID, profile.Driver, domain)
	}
	return config.BackendConfig{Driver: profile.Driver, ConnectionRef: profile.ConnectionRef}, nil
}

type backendProfileScanner interface{ Scan(...any) error }

func scanBackendProfile(scanner backendProfileScanner) (BackendProfile, error) {
	var profile BackendProfile
	if err := scanner.Scan(&profile.ProfileID, &profile.DisplayName, &profile.Driver, &profile.ConnectionRef, &profile.Status, &profile.CreatedAt, &profile.UpdatedAt); errors.Is(err, sql.ErrNoRows) {
		return BackendProfile{}, ErrBackendProfileNotFound
	} else if err != nil {
		return BackendProfile{}, fmt.Errorf("scan backend profile: %w", err)
	}
	profile.Domains = BackendProfileDomains(profile.Driver)
	return profile, nil
}

func uniqueTrimmed(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

type MemoryBackendProfileStore struct {
	mu          sync.Mutex
	profiles    map[string]BackendProfile
	tenantAllow map[string]map[string]struct{}
}

func NewMemoryBackendProfileStore() *MemoryBackendProfileStore {
	now := time.Now().UTC()
	return &MemoryBackendProfileStore{
		profiles: map[string]BackendProfile{
			"platform-postgres": {ProfileID: "platform-postgres", DisplayName: "平台 PostgreSQL", Driver: "postgres", Status: BackendProfileActive, Domains: BackendProfileDomains("postgres"), CreatedAt: now, UpdatedAt: now},
			"platform-pgvector": {ProfileID: "platform-pgvector", DisplayName: "平台 pgvector", Driver: "pgvector", Status: BackendProfileActive, Domains: BackendProfileDomains("pgvector"), CreatedAt: now, UpdatedAt: now},
		},
		tenantAllow: make(map[string]map[string]struct{}),
	}
}

func (s *MemoryBackendProfileStore) ListBackendProfiles(_ context.Context) ([]BackendProfile, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]BackendProfile, 0, len(s.profiles))
	for _, profile := range s.profiles {
		profile.Domains = append([]string(nil), profile.Domains...)
		result = append(result, profile)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ProfileID < result[j].ProfileID })
	return result, nil
}

func (s *MemoryBackendProfileStore) GetBackendProfile(_ context.Context, profileID string) (BackendProfile, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	profile, ok := s.profiles[strings.TrimSpace(profileID)]
	if !ok {
		return BackendProfile{}, ErrBackendProfileNotFound
	}
	profile.Domains = append([]string(nil), profile.Domains...)
	return profile, nil
}

func (s *MemoryBackendProfileStore) UpsertBackendProfile(_ context.Context, profile BackendProfile) (BackendProfile, error) {
	profile, err := NormalizeBackendProfile(profile)
	if err != nil {
		return BackendProfile{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	if previous, ok := s.profiles[profile.ProfileID]; ok {
		if previous.Driver != profile.Driver || previous.ConnectionRef != profile.ConnectionRef {
			return BackendProfile{}, errors.New("backend profile routing is immutable; create a new profile for a different driver or connection")
		}
		profile.CreatedAt = previous.CreatedAt
	} else {
		profile.CreatedAt = now
	}
	profile.UpdatedAt = now
	s.profiles[profile.ProfileID] = profile
	return profile, nil
}

func (s *MemoryBackendProfileStore) DeleteBackendProfile(_ context.Context, profileID string) error {
	profileID = strings.TrimSpace(profileID)
	if profileID == "platform-postgres" || profileID == "platform-pgvector" {
		return errors.New("built-in backend profiles cannot be deleted")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.profiles[profileID]; !ok {
		return ErrBackendProfileNotFound
	}
	for _, allowed := range s.tenantAllow {
		if _, ok := allowed[profileID]; ok {
			return errors.New("backend profile is still authorized to a tenant")
		}
	}
	delete(s.profiles, profileID)
	return nil
}

func (s *MemoryBackendProfileStore) ListTenantBackendProfiles(_ context.Context, tenantID string) ([]TenantBackendProfile, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	allowed := s.tenantAllow[strings.TrimSpace(tenantID)]
	result := make([]TenantBackendProfile, 0, len(allowed))
	for profileID := range allowed {
		profile, ok := s.profiles[profileID]
		if !ok {
			continue
		}
		result = append(result, TenantBackendProfile{
			ProfileID: profile.ProfileID, DisplayName: profile.DisplayName, Driver: profile.Driver,
			Status: profile.Status, Domains: append([]string(nil), profile.Domains...), Available: profile.Status == BackendProfileActive,
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ProfileID < result[j].ProfileID })
	return result, nil
}

func (s *MemoryBackendProfileStore) ReplaceTenantBackendProfiles(_ context.Context, tenantID string, profileIDs []string) error {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return errors.New("tenant ID is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	allowed := make(map[string]struct{}, len(profileIDs))
	for _, profileID := range uniqueTrimmed(profileIDs) {
		profile, ok := s.profiles[profileID]
		if !ok {
			return fmt.Errorf("backend profile %q does not exist", profileID)
		}
		if profile.Status != BackendProfileActive {
			return fmt.Errorf("backend profile %q is disabled", profileID)
		}
		allowed[profileID] = struct{}{}
	}
	s.tenantAllow[tenantID] = allowed
	return nil
}

func (s *MemoryBackendProfileStore) ResolveTenantBackend(_ context.Context, tenantID, domain, profileID string) (config.BackendConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	profileID = strings.TrimSpace(profileID)
	if _, ok := s.tenantAllow[strings.TrimSpace(tenantID)][profileID]; !ok {
		return config.BackendConfig{}, fmt.Errorf("%w: %s", ErrBackendProfileUnauthorized, profileID)
	}
	profile, ok := s.profiles[profileID]
	if !ok {
		return config.BackendConfig{}, ErrBackendProfileNotFound
	}
	if profile.Status != BackendProfileActive {
		return config.BackendConfig{}, fmt.Errorf("%w: %s", ErrBackendProfileUnavailable, profileID)
	}
	if !BackendProfileSupportsDomain(profile.Driver, domain) {
		return config.BackendConfig{}, fmt.Errorf("backend profile %q (%s) does not support %s", profileID, profile.Driver, domain)
	}
	return config.BackendConfig{Driver: profile.Driver, ConnectionRef: profile.ConnectionRef}, nil
}

var _ BackendProfileStore = (*PostgresBackendProfileStore)(nil)
var _ BackendProfileStore = (*MemoryBackendProfileStore)(nil)
