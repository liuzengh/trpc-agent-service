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

	BackendConnectionNone     = "none"
	BackendConnectionOptional = "optional"
	BackendConnectionRequired = "required"
)

var (
	ErrBackendProfileNotFound        = errors.New("backend profile not found")
	ErrBackendProfileAlreadyExists   = errors.New("backend profile already exists")
	ErrBackendProfileUnauthorized    = errors.New("backend profile is not authorized for tenant")
	ErrBackendProfileUnavailable     = errors.New("backend profile is unavailable")
	ErrMemoryDirectAccessUnsupported = errors.New("configured Memory backend is managed externally")
)

type BackendProfile struct {
	ProfileID     string                    `json:"profile_id"`
	DisplayName   string                    `json:"display_name"`
	Driver        string                    `json:"driver"`
	ConnectionRef string                    `json:"connection_ref,omitempty"`
	Status        string                    `json:"status"`
	Domains       []string                  `json:"domains"`
	Capabilities  BackendDriverCapabilities `json:"capabilities"`
	CreatedAt     time.Time                 `json:"created_at,omitempty"`
	UpdatedAt     time.Time                 `json:"updated_at,omitempty"`
}

type TenantBackendProfile struct {
	ProfileID    string                    `json:"profile_id"`
	DisplayName  string                    `json:"display_name"`
	Driver       string                    `json:"driver"`
	Status       string                    `json:"status"`
	Domains      []string                  `json:"domains"`
	Available    bool                      `json:"available"`
	Capabilities BackendDriverCapabilities `json:"capabilities"`
}

type BackendProfileResolver interface {
	ResolveTenantBackend(context.Context, string, string, string) (config.BackendConfig, error)
}

type BackendProfileStore interface {
	BackendProfileResolver
	ListBackendProfiles(context.Context) ([]BackendProfile, error)
	GetBackendProfile(context.Context, string) (BackendProfile, error)
	CreateBackendProfile(context.Context, BackendProfile) (BackendProfile, error)
	UpdateBackendProfile(context.Context, string, BackendProfileUpdate) (BackendProfile, error)
	DeleteBackendProfile(context.Context, string) error
	ListTenantBackendProfiles(context.Context, string) ([]TenantBackendProfile, error)
	ReplaceTenantBackendProfiles(context.Context, string, []string) error
}

// BackendProfileUpdate contains the only mutable Backend Profile fields.
// Driver, connection and data-domain routing are immutable after creation.
type BackendProfileUpdate struct {
	DisplayName string `json:"display_name"`
	Status      string `json:"status"`
}

type BackendDriverSpec struct {
	Driver           string                    `json:"driver"`
	Domains          []string                  `json:"domains"`
	ConnectionPolicy string                    `json:"connection_policy"`
	Capabilities     BackendDriverCapabilities `json:"capabilities"`
}

// BackendDriverCapabilities exposes operator-relevant differences that affect
// whether a backend is a safe drop-in choice. Keep this contract deliberately
// small: only advertise behavior the platform can verify across its adapters.
type BackendDriverCapabilities struct {
	MultiNode             bool `json:"multi_node"`
	MemoryConsoleBrowsing bool `json:"memory_console_browsing"`
}

var backendDriverSpecs = []BackendDriverSpec{
	{Driver: "inmemory", Domains: []string{BackendDomainSession, BackendDomainMemory, BackendDomainArtifact}, ConnectionPolicy: BackendConnectionNone, Capabilities: BackendDriverCapabilities{MemoryConsoleBrowsing: true}},
	{Driver: "postgres", Domains: []string{BackendDomainSession, BackendDomainMemory, BackendDomainArtifact}, ConnectionPolicy: BackendConnectionOptional, Capabilities: BackendDriverCapabilities{MultiNode: true, MemoryConsoleBrowsing: true}},
	{Driver: "redis", Domains: []string{BackendDomainSession, BackendDomainMemory}, ConnectionPolicy: BackendConnectionRequired, Capabilities: BackendDriverCapabilities{MultiNode: true, MemoryConsoleBrowsing: true}},
	{Driver: "mysql", Domains: []string{BackendDomainSession, BackendDomainMemory}, ConnectionPolicy: BackendConnectionRequired, Capabilities: BackendDriverCapabilities{MultiNode: true, MemoryConsoleBrowsing: true}},
	{Driver: "sqlite", Domains: []string{BackendDomainSession}, ConnectionPolicy: BackendConnectionRequired},
	{Driver: "mongodb", Domains: []string{BackendDomainSession}, ConnectionPolicy: BackendConnectionRequired, Capabilities: BackendDriverCapabilities{MultiNode: true}},
	{Driver: "clickhouse", Domains: []string{BackendDomainSession}, ConnectionPolicy: BackendConnectionRequired, Capabilities: BackendDriverCapabilities{MultiNode: true}},
	{Driver: "pgvector", Domains: []string{BackendDomainKnowledge}, ConnectionPolicy: BackendConnectionOptional, Capabilities: BackendDriverCapabilities{MultiNode: true}},
	{Driver: "qdrant", Domains: []string{BackendDomainKnowledge}, ConnectionPolicy: BackendConnectionRequired, Capabilities: BackendDriverCapabilities{MultiNode: true}},
	{Driver: "elasticsearch", Domains: []string{BackendDomainKnowledge}, ConnectionPolicy: BackendConnectionRequired, Capabilities: BackendDriverCapabilities{MultiNode: true}},
	{Driver: "s3", Domains: []string{BackendDomainArtifact}, ConnectionPolicy: BackendConnectionRequired, Capabilities: BackendDriverCapabilities{MultiNode: true}},
	{Driver: "cos", Domains: []string{BackendDomainArtifact}, ConnectionPolicy: BackendConnectionRequired, Capabilities: BackendDriverCapabilities{MultiNode: true}},
	{Driver: "mem0", Domains: []string{BackendDomainMemory}, ConnectionPolicy: BackendConnectionRequired, Capabilities: BackendDriverCapabilities{MultiNode: true, MemoryConsoleBrowsing: true}},
	{Driver: "chromadb", Domains: []string{BackendDomainMemory}, ConnectionPolicy: BackendConnectionRequired, Capabilities: BackendDriverCapabilities{MultiNode: true, MemoryConsoleBrowsing: true}},
	{Driver: "tencentdb", Domains: []string{BackendDomainMemory}, ConnectionPolicy: BackendConnectionRequired, Capabilities: BackendDriverCapabilities{MultiNode: true}},
}

func BackendDriverCatalog() []BackendDriverSpec {
	result := make([]BackendDriverSpec, 0, len(backendDriverSpecs))
	for _, spec := range backendDriverSpecs {
		spec.Domains = append([]string(nil), spec.Domains...)
		result = append(result, spec)
	}
	return result
}

func BackendDriverDomains(driver string) []string {
	driver = strings.ToLower(strings.TrimSpace(driver))
	for _, spec := range backendDriverSpecs {
		if spec.Driver == driver {
			return append([]string(nil), spec.Domains...)
		}
	}
	return nil
}

func BackendDriverCapabilitiesFor(driver string) BackendDriverCapabilities {
	spec, ok := backendDriverSpec(driver)
	if !ok {
		return BackendDriverCapabilities{}
	}
	return spec.Capabilities
}

func backendDriverSpec(driver string) (BackendDriverSpec, bool) {
	driver = strings.ToLower(strings.TrimSpace(driver))
	for _, spec := range backendDriverSpecs {
		if spec.Driver == driver {
			return spec, true
		}
	}
	return BackendDriverSpec{}, false
}

func backendProfileSupportsDomain(profile BackendProfile, domain string) bool {
	domain = strings.ToLower(strings.TrimSpace(domain))
	for _, candidate := range profile.Domains {
		if candidate == domain {
			return true
		}
	}
	return false
}

func normalizeBackendProfileDomains(driver string, domains []string) ([]string, error) {
	supported := BackendDriverDomains(driver)
	if len(supported) == 0 {
		return nil, fmt.Errorf("unsupported backend profile driver %q", driver)
	}
	selected := make(map[string]struct{}, len(domains))
	for _, domain := range domains {
		domain = strings.ToLower(strings.TrimSpace(domain))
		if domain != "" {
			selected[domain] = struct{}{}
		}
	}
	if len(selected) == 0 {
		return nil, errors.New("backend profile must select at least one data domain")
	}
	allowed := make(map[string]struct{}, len(supported))
	for _, domain := range supported {
		allowed[domain] = struct{}{}
	}
	for domain := range selected {
		if _, ok := allowed[domain]; !ok {
			return nil, fmt.Errorf("backend profile driver %q does not support data domain %q", driver, domain)
		}
	}
	result := make([]string, 0, len(selected))
	for _, domain := range supported {
		if _, ok := selected[domain]; ok {
			result = append(result, domain)
		}
	}
	return result, nil
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
	spec, ok := backendDriverSpec(profile.Driver)
	if !ok {
		return BackendProfile{}, fmt.Errorf("unsupported backend profile driver %q", profile.Driver)
	}
	domains, err := normalizeBackendProfileDomains(profile.Driver, profile.Domains)
	if err != nil {
		return BackendProfile{}, err
	}
	profile.Domains = domains
	if profile.Status != BackendProfileActive && profile.Status != BackendProfileDisabled {
		return BackendProfile{}, errors.New("backend profile status must be active or disabled")
	}
	if profile.ConnectionRef != "" && !strings.HasPrefix(profile.ConnectionRef, "env:") {
		return BackendProfile{}, errors.New("backend profile connection_ref must use an env: reference")
	}
	switch spec.ConnectionPolicy {
	case BackendConnectionRequired:
		if profile.ConnectionRef == "" {
			return BackendProfile{}, fmt.Errorf("backend profile driver %q requires connection_ref", profile.Driver)
		}
	case BackendConnectionNone:
		if profile.ConnectionRef != "" {
			return BackendProfile{}, fmt.Errorf("backend profile driver %q does not accept connection_ref", profile.Driver)
		}
	case BackendConnectionOptional:
		// Either the platform default connection or an explicit env: reference is valid.
	default:
		return BackendProfile{}, fmt.Errorf("backend profile driver %q has invalid connection policy %q", profile.Driver, spec.ConnectionPolicy)
	}
	profile.Capabilities = spec.Capabilities
	return profile, nil
}

func normalizeBackendProfileUpdate(update BackendProfileUpdate) (BackendProfileUpdate, error) {
	update.DisplayName = strings.TrimSpace(update.DisplayName)
	update.Status = strings.ToLower(strings.TrimSpace(update.Status))
	if update.DisplayName == "" {
		return BackendProfileUpdate{}, errors.New("backend profile display name is required")
	}
	if update.Status != BackendProfileActive && update.Status != BackendProfileDisabled {
		return BackendProfileUpdate{}, errors.New("backend profile status must be active or disabled")
	}
	return update, nil
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
SELECT profile_id, display_name, driver, connection_ref, status, array_to_string(domains, ','), created_at, updated_at
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
SELECT profile_id, display_name, driver, connection_ref, status, array_to_string(domains, ','), created_at, updated_at
FROM backend_profiles WHERE profile_id=$1`, strings.TrimSpace(profileID))
	return scanBackendProfile(row)
}

func (s *PostgresBackendProfileStore) CreateBackendProfile(ctx context.Context, profile BackendProfile) (BackendProfile, error) {
	profile, err := NormalizeBackendProfile(profile)
	if err != nil {
		return BackendProfile{}, err
	}
	row := s.database.QueryRowContext(ctx, `
INSERT INTO backend_profiles (profile_id, display_name, driver, connection_ref, status, domains)
VALUES ($1,$2,$3,$4,$5,string_to_array($6, ','))
ON CONFLICT (profile_id) DO NOTHING
RETURNING profile_id, display_name, driver, connection_ref, status, array_to_string(domains, ','), created_at, updated_at`,
		profile.ProfileID, profile.DisplayName, profile.Driver, profile.ConnectionRef, profile.Status, strings.Join(profile.Domains, ","))
	created, err := scanBackendProfile(row)
	if errors.Is(err, ErrBackendProfileNotFound) {
		return BackendProfile{}, fmt.Errorf("%w: %s", ErrBackendProfileAlreadyExists, profile.ProfileID)
	}
	return created, err
}

func (s *PostgresBackendProfileStore) UpdateBackendProfile(ctx context.Context, profileID string, update BackendProfileUpdate) (BackendProfile, error) {
	profileID = strings.TrimSpace(profileID)
	if profileID == "" {
		return BackendProfile{}, errors.New("backend profile ID is required")
	}
	update, err := normalizeBackendProfileUpdate(update)
	if err != nil {
		return BackendProfile{}, err
	}
	row := s.database.QueryRowContext(ctx, `
UPDATE backend_profiles
SET display_name=$2, status=$3, updated_at=NOW()
WHERE profile_id=$1
RETURNING profile_id, display_name, driver, connection_ref, status, array_to_string(domains, ','), created_at, updated_at`,
		profileID, update.DisplayName, update.Status)
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
SELECT p.profile_id, p.display_name, p.driver, p.status, array_to_string(p.domains, ',')
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
		var domains string
		if err := rows.Scan(&profile.ProfileID, &profile.DisplayName, &profile.Driver, &profile.Status, &domains); err != nil {
			return nil, fmt.Errorf("scan tenant backend profile: %w", err)
		}
		profile.Domains = splitBackendDomains(domains)
		profile.Capabilities = BackendDriverCapabilitiesFor(profile.Driver)
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
SELECT p.profile_id, p.display_name, p.driver, p.connection_ref, p.status, array_to_string(p.domains, ','), p.created_at, p.updated_at
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
	if !backendProfileSupportsDomain(profile, domain) {
		return config.BackendConfig{}, fmt.Errorf("backend profile %q is not configured for %s", profileID, domain)
	}
	return config.BackendConfig{Driver: profile.Driver, ConnectionRef: profile.ConnectionRef}, nil
}

type backendProfileScanner interface{ Scan(...any) error }

func scanBackendProfile(scanner backendProfileScanner) (BackendProfile, error) {
	var profile BackendProfile
	var domains string
	if err := scanner.Scan(&profile.ProfileID, &profile.DisplayName, &profile.Driver, &profile.ConnectionRef, &profile.Status, &domains, &profile.CreatedAt, &profile.UpdatedAt); errors.Is(err, sql.ErrNoRows) {
		return BackendProfile{}, ErrBackendProfileNotFound
	} else if err != nil {
		return BackendProfile{}, fmt.Errorf("scan backend profile: %w", err)
	}
	profile.Domains = splitBackendDomains(domains)
	profile.Capabilities = BackendDriverCapabilitiesFor(profile.Driver)
	return profile, nil
}

func splitBackendDomains(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return strings.Split(value, ",")
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
			"platform-postgres": {ProfileID: "platform-postgres", DisplayName: "平台 PostgreSQL", Driver: "postgres", Status: BackendProfileActive, Domains: BackendDriverDomains("postgres"), CreatedAt: now, UpdatedAt: now},
			"platform-pgvector": {ProfileID: "platform-pgvector", DisplayName: "平台 pgvector", Driver: "pgvector", Status: BackendProfileActive, Domains: BackendDriverDomains("pgvector"), CreatedAt: now, UpdatedAt: now},
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
		profile.Capabilities = BackendDriverCapabilitiesFor(profile.Driver)
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
	profile.Capabilities = BackendDriverCapabilitiesFor(profile.Driver)
	return profile, nil
}

func (s *MemoryBackendProfileStore) CreateBackendProfile(_ context.Context, profile BackendProfile) (BackendProfile, error) {
	profile, err := NormalizeBackendProfile(profile)
	if err != nil {
		return BackendProfile{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.profiles[profile.ProfileID]; exists {
		return BackendProfile{}, fmt.Errorf("%w: %s", ErrBackendProfileAlreadyExists, profile.ProfileID)
	}
	now := time.Now().UTC()
	profile.CreatedAt = now
	profile.UpdatedAt = now
	s.profiles[profile.ProfileID] = profile
	return profile, nil
}

func (s *MemoryBackendProfileStore) UpdateBackendProfile(_ context.Context, profileID string, update BackendProfileUpdate) (BackendProfile, error) {
	profileID = strings.TrimSpace(profileID)
	if profileID == "" {
		return BackendProfile{}, errors.New("backend profile ID is required")
	}
	update, err := normalizeBackendProfileUpdate(update)
	if err != nil {
		return BackendProfile{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	profile, ok := s.profiles[profileID]
	if !ok {
		return BackendProfile{}, ErrBackendProfileNotFound
	}
	profile.DisplayName = update.DisplayName
	profile.Status = update.Status
	profile.UpdatedAt = time.Now().UTC()
	s.profiles[profileID] = profile
	profile.Domains = append([]string(nil), profile.Domains...)
	profile.Capabilities = BackendDriverCapabilitiesFor(profile.Driver)
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
			Capabilities: BackendDriverCapabilitiesFor(profile.Driver),
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
	if !backendProfileSupportsDomain(profile, domain) {
		return config.BackendConfig{}, fmt.Errorf("backend profile %q is not configured for %s", profileID, domain)
	}
	return config.BackendConfig{Driver: profile.Driver, ConnectionRef: profile.ConnectionRef}, nil
}

var _ BackendProfileStore = (*PostgresBackendProfileStore)(nil)
var _ BackendProfileStore = (*MemoryBackendProfileStore)(nil)
