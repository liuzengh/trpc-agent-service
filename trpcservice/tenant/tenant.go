// Package tenant defines the immutable multi-tenant catalog used to route
// verified channel bindings to Agent configurations and storage backends.
package tenant

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

const MaxIDBytes = 128

var (
	ErrBindingNotFound           = errors.New("channel binding not found")
	ErrTenantUnavailable         = errors.New("tenant unavailable")
	ErrAgentAppUnavailable       = errors.New("agent app unavailable")
	ErrConfigVersionUnavailable  = errors.New("config version unavailable")
	ErrStorageProfileUnavailable = errors.New("storage profile unavailable")
)

type StorageKind string

const (
	StorageKindInMemory StorageKind = "inmemory"
	StorageKindRedis    StorageKind = "redis"
	StorageKindPostgres StorageKind = "postgres"
	StorageKindMySQL    StorageKind = "mysql"
)

func (k StorageKind) IsSQL() bool {
	return k == StorageKindPostgres || k == StorageKindMySQL
}

const (
	// The pinned SQL modules embed table prefixes (and, for PostgreSQL,
	// schemas) into index names. These limits keep their longest generated
	// index, idx_*_session_summaries_unique_active, within the database
	// identifier limit instead of letting it be silently truncated.
	MaxSQLTablePrefixBytes         = 29
	maxPostgresIndexNamespaceBytes = 27
)

var sqlIdentifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type Tenant struct {
	ID      string `json:"id"`
	Enabled bool   `json:"enabled"`
}

type AgentApp struct {
	TenantID            string `json:"tenant_id"`
	ID                  string `json:"id"`
	Enabled             bool   `json:"enabled"`
	ActiveConfigVersion string `json:"active_config_version"`
}

type ChannelBinding struct {
	ID                string `json:"id"`
	Channel           string `json:"channel"`
	ExternalAccountID string `json:"external_account_id"`
	CredentialRef     string `json:"credential_ref,omitempty"`
	BotIDRef          string `json:"bot_id_ref,omitempty"`
	BotSecretRef      string `json:"bot_secret_ref,omitempty"`
	// ServerURL is an optional provider endpoint override. It is currently
	// consumed only by Telegram bindings so local mocks can be used without
	// changing the default official endpoint.
	ServerURL  string `json:"server_url,omitempty"`
	TenantID   string `json:"tenant_id"`
	AgentAppID string `json:"agent_app_id"`
	Enabled    bool   `json:"enabled"`
}

type ModelConfig struct {
	Name            string
	BaseURL         string
	CredentialRef   string
	RequestTimeout  time.Duration
	MaxOutputTokens int
}

type ConfigVersion struct {
	TenantID         string
	AgentAppID       string
	Version          string
	StorageProfileID string
	Instruction      string
	Model            ModelConfig
}

type StorageProfile struct {
	TenantID      string      `json:"tenant_id"`
	ID            string      `json:"id"`
	Kind          StorageKind `json:"kind"`
	CredentialRef string      `json:"credential_ref,omitempty"`
	KeyPrefix     string      `json:"key_prefix,omitempty"`
	TablePrefix   string      `json:"table_prefix,omitempty"`
	Schema        string      `json:"schema,omitempty"`
	SkipDBInit    bool        `json:"skip_db_init,omitempty"`
	LegacyPrefix  bool        `json:"-"`
}

// NormalizeStorageProfile validates kind-specific fields and returns the
// canonical immutable form used by BackendProvider and backend fingerprints.
func NormalizeStorageProfile(profile StorageProfile) (StorageProfile, error) {
	profile.CredentialRef = strings.TrimSpace(profile.CredentialRef)
	profile.KeyPrefix = strings.TrimSpace(profile.KeyPrefix)
	profile.TablePrefix = strings.TrimSpace(profile.TablePrefix)
	profile.Schema = strings.TrimSpace(profile.Schema)
	switch profile.Kind {
	case StorageKindInMemory:
		if profile.CredentialRef != "" || profile.KeyPrefix != "" || profile.TablePrefix != "" || profile.Schema != "" || profile.SkipDBInit {
			return StorageProfile{}, fmt.Errorf("inmemory storage profile %q must not configure backend fields", profile.ID)
		}
	case StorageKindRedis:
		if profile.CredentialRef == "" || strings.Trim(profile.KeyPrefix, ":") == "" {
			return StorageProfile{}, fmt.Errorf("redis storage profile %q requires credential_ref and key_prefix", profile.ID)
		}
		if profile.TablePrefix != "" || profile.Schema != "" || profile.SkipDBInit {
			return StorageProfile{}, fmt.Errorf("redis storage profile %q must not configure SQL fields", profile.ID)
		}
	case StorageKindPostgres, StorageKindMySQL:
		if profile.CredentialRef == "" || profile.TablePrefix == "" {
			return StorageProfile{}, fmt.Errorf("SQL storage profile %q requires credential_ref and table_prefix", profile.ID)
		}
		if profile.KeyPrefix != "" || profile.LegacyPrefix {
			return StorageProfile{}, fmt.Errorf("SQL storage profile %q must not configure Redis fields", profile.ID)
		}
		if !sqlIdentifierPattern.MatchString(profile.TablePrefix) {
			return StorageProfile{}, fmt.Errorf("SQL storage profile %q has invalid table_prefix", profile.ID)
		}
		if !strings.HasSuffix(profile.TablePrefix, "_") {
			profile.TablePrefix += "_"
		}
		if profile.Kind == StorageKindPostgres {
			if profile.Schema == "" {
				profile.Schema = "public"
			}
			if !sqlIdentifierPattern.MatchString(profile.Schema) || len(profile.Schema) > 63 {
				return StorageProfile{}, fmt.Errorf("postgres storage profile %q has invalid schema", profile.ID)
			}
			if len(profile.Schema)+len(profile.TablePrefix) > maxPostgresIndexNamespaceBytes {
				return StorageProfile{}, fmt.Errorf("postgres storage profile %q schema and table_prefix are too long for official indexes", profile.ID)
			}
		} else if profile.Schema != "" {
			return StorageProfile{}, fmt.Errorf("mysql storage profile %q must not configure schema", profile.ID)
		} else if len(profile.TablePrefix) > MaxSQLTablePrefixBytes {
			return StorageProfile{}, fmt.Errorf("mysql storage profile %q table_prefix exceeds %d bytes", profile.ID, MaxSQLTablePrefixBytes)
		}
	default:
		return StorageProfile{}, fmt.Errorf("storage profile %q has unsupported kind", profile.ID)
	}
	return profile, nil
}

type Catalog struct {
	Tenants         []Tenant
	StorageProfiles []StorageProfile
	AgentApps       []AgentApp
	ConfigVersions  []ConfigVersion
	ChannelBindings []ChannelBinding
}

type Repository interface {
	ResolveBinding(context.Context, string, string) (ChannelBinding, error)
	GetTenant(context.Context, string) (Tenant, error)
	GetAgentApp(context.Context, string, string) (AgentApp, error)
	GetConfigVersion(context.Context, string, string, string) (ConfigVersion, error)
	GetStorageProfile(context.Context, string, string) (StorageProfile, error)
	ListActiveStorageProfiles(context.Context) ([]StorageProfile, error)
}

type tenantAppKey struct {
	tenantID string
	appID    string
}

type configKey struct {
	tenantID string
	appID    string
	version  string
}

type profileKey struct {
	tenantID  string
	profileID string
}

type bindingKey struct {
	channel   string
	bindingID string
}

// PresetRepository is an immutable, in-process index built from the startup
// catalog. Returning values instead of pointers prevents request code from
// mutating the published configuration.
type PresetRepository struct {
	tenants        map[string]Tenant
	apps           map[tenantAppKey]AgentApp
	configs        map[configKey]ConfigVersion
	profiles       map[profileKey]StorageProfile
	bindings       map[bindingKey]ChannelBinding
	activeProfiles []StorageProfile
}

func NewPresetRepository(catalog Catalog) (*PresetRepository, error) {
	r := &PresetRepository{
		tenants:  make(map[string]Tenant, len(catalog.Tenants)),
		apps:     make(map[tenantAppKey]AgentApp, len(catalog.AgentApps)),
		configs:  make(map[configKey]ConfigVersion, len(catalog.ConfigVersions)),
		profiles: make(map[profileKey]StorageProfile, len(catalog.StorageProfiles)),
		bindings: make(map[bindingKey]ChannelBinding, len(catalog.ChannelBindings)),
	}
	if len(catalog.Tenants) == 0 || len(catalog.AgentApps) == 0 || len(catalog.ConfigVersions) == 0 || len(catalog.StorageProfiles) == 0 || len(catalog.ChannelBindings) == 0 {
		return nil, errors.New("catalog must contain tenants, agent apps, config versions, storage profiles, and channel bindings")
	}

	for _, current := range catalog.Tenants {
		if err := ValidateID("tenant id", current.ID); err != nil {
			return nil, err
		}
		if _, exists := r.tenants[current.ID]; exists {
			return nil, fmt.Errorf("duplicate tenant id %q", current.ID)
		}
		r.tenants[current.ID] = current
	}

	for _, current := range catalog.StorageProfiles {
		if err := ValidateID("storage profile tenant id", current.TenantID); err != nil {
			return nil, err
		}
		if err := ValidateID("storage profile id", current.ID); err != nil {
			return nil, err
		}
		if _, exists := r.tenants[current.TenantID]; !exists {
			return nil, fmt.Errorf("storage profile %q references unknown tenant", current.ID)
		}
		current, err := NormalizeStorageProfile(current)
		if err != nil {
			return nil, err
		}
		key := profileKey{tenantID: current.TenantID, profileID: current.ID}
		if _, exists := r.profiles[key]; exists {
			return nil, fmt.Errorf("duplicate storage profile %q for tenant %q", current.ID, current.TenantID)
		}
		r.profiles[key] = current
	}

	for _, current := range catalog.AgentApps {
		if err := ValidateID("agent app tenant id", current.TenantID); err != nil {
			return nil, err
		}
		if err := ValidateID("agent app id", current.ID); err != nil {
			return nil, err
		}
		if err := ValidateID("active config version", current.ActiveConfigVersion); err != nil {
			return nil, err
		}
		tenantValue, exists := r.tenants[current.TenantID]
		if !exists {
			return nil, fmt.Errorf("agent app %q references unknown tenant", current.ID)
		}
		if current.Enabled && !tenantValue.Enabled {
			return nil, fmt.Errorf("enabled agent app %q references disabled tenant", current.ID)
		}
		key := tenantAppKey{tenantID: current.TenantID, appID: current.ID}
		if _, exists := r.apps[key]; exists {
			return nil, fmt.Errorf("duplicate agent app %q for tenant %q", current.ID, current.TenantID)
		}
		r.apps[key] = current
	}

	for _, current := range catalog.ConfigVersions {
		if err := ValidateID("config tenant id", current.TenantID); err != nil {
			return nil, err
		}
		if err := ValidateID("config agent app id", current.AgentAppID); err != nil {
			return nil, err
		}
		if err := ValidateID("config version", current.Version); err != nil {
			return nil, err
		}
		if err := ValidateID("config storage profile id", current.StorageProfileID); err != nil {
			return nil, err
		}
		if _, exists := r.apps[tenantAppKey{tenantID: current.TenantID, appID: current.AgentAppID}]; !exists {
			return nil, fmt.Errorf("config version %q references unknown agent app", current.Version)
		}
		if _, exists := r.profiles[profileKey{tenantID: current.TenantID, profileID: current.StorageProfileID}]; !exists {
			return nil, fmt.Errorf("config version %q references unknown storage profile", current.Version)
		}
		if strings.TrimSpace(current.Instruction) == "" || strings.TrimSpace(current.Model.Name) == "" || strings.TrimSpace(current.Model.BaseURL) == "" || strings.TrimSpace(current.Model.CredentialRef) == "" {
			return nil, fmt.Errorf("config version %q has incomplete agent or model configuration", current.Version)
		}
		if current.Model.RequestTimeout <= 0 || current.Model.MaxOutputTokens <= 0 {
			return nil, fmt.Errorf("config version %q model limits must be positive", current.Version)
		}
		key := configKey{tenantID: current.TenantID, appID: current.AgentAppID, version: current.Version}
		if _, exists := r.configs[key]; exists {
			return nil, fmt.Errorf("duplicate config version %q for tenant %q agent app %q", current.Version, current.TenantID, current.AgentAppID)
		}
		r.configs[key] = current
	}

	active := make(map[profileKey]StorageProfile)
	for key, app := range r.apps {
		_, exists := r.configs[configKey{tenantID: key.tenantID, appID: key.appID, version: app.ActiveConfigVersion}]
		if !exists {
			return nil, fmt.Errorf("agent app %q active config version %q does not exist", app.ID, app.ActiveConfigVersion)
		}
	}
	for key, configValue := range r.configs {
		app := r.apps[tenantAppKey{tenantID: key.tenantID, appID: key.appID}]
		if !app.Enabled {
			continue
		}
		profile := r.profiles[profileKey{tenantID: key.tenantID, profileID: configValue.StorageProfileID}]
		active[profileKey{tenantID: profile.TenantID, profileID: profile.ID}] = profile
	}

	bindingIDs := make(map[string]struct{}, len(catalog.ChannelBindings))
	activeAccounts := make(map[string]string, len(catalog.ChannelBindings))
	for _, current := range catalog.ChannelBindings {
		if err := ValidateID("binding id", current.ID); err != nil {
			return nil, err
		}
		if err := ValidateID("binding channel", current.Channel); err != nil {
			return nil, err
		}
		if err := ValidateID("binding external account id", current.ExternalAccountID); err != nil {
			return nil, err
		}
		tenantValue, tenantExists := r.tenants[current.TenantID]
		appValue, appExists := r.apps[tenantAppKey{tenantID: current.TenantID, appID: current.AgentAppID}]
		if !tenantExists || !appExists {
			return nil, fmt.Errorf("binding %q references unknown tenant or agent app", current.ID)
		}
		if current.Enabled && (!tenantValue.Enabled || !appValue.Enabled) {
			return nil, fmt.Errorf("enabled binding %q references a disabled tenant or agent app", current.ID)
		}
		if err := validateChannelBinding(current); err != nil {
			return nil, err
		}
		if current.Enabled {
			accountKey := current.Channel + "\x00" + current.ExternalAccountID
			if existing, exists := activeAccounts[accountKey]; exists {
				return nil, fmt.Errorf("enabled bindings %q and %q reuse the same external account", existing, current.ID)
			}
			activeAccounts[accountKey] = current.ID
		}
		if _, exists := bindingIDs[current.ID]; exists {
			return nil, fmt.Errorf("duplicate binding id %q", current.ID)
		}
		bindingIDs[current.ID] = struct{}{}
		key := bindingKey{channel: current.Channel, bindingID: current.ID}
		r.bindings[key] = current
	}

	r.activeProfiles = make([]StorageProfile, 0, len(active))
	for _, current := range active {
		r.activeProfiles = append(r.activeProfiles, current)
	}
	sort.Slice(r.activeProfiles, func(i, j int) bool {
		if r.activeProfiles[i].TenantID == r.activeProfiles[j].TenantID {
			return r.activeProfiles[i].ID < r.activeProfiles[j].ID
		}
		return r.activeProfiles[i].TenantID < r.activeProfiles[j].TenantID
	})
	return r, nil
}

func validateChannelBinding(current ChannelBinding) error {
	switch current.Channel {
	case "demo":
		if current.CredentialRef != "" || current.BotIDRef != "" || current.BotSecretRef != "" {
			return fmt.Errorf("demo binding %q must not configure IM credentials", current.ID)
		}
	case "telegram":
		if strings.TrimSpace(current.CredentialRef) == "" || current.BotIDRef != "" || current.BotSecretRef != "" {
			return fmt.Errorf("telegram binding %q requires credential_ref only", current.ID)
		}
	case "wecom_aibot":
		if current.CredentialRef != "" || strings.TrimSpace(current.BotIDRef) == "" || strings.TrimSpace(current.BotSecretRef) == "" {
			return fmt.Errorf("wecom_aibot binding %q requires bot_id_ref and bot_secret_ref only", current.ID)
		}
	case "feishu":
		if current.CredentialRef != "" || strings.TrimSpace(current.BotIDRef) == "" || strings.TrimSpace(current.BotSecretRef) == "" {
			return fmt.Errorf("feishu binding %q requires bot_id_ref and bot_secret_ref only", current.ID)
		}
	default:
		return fmt.Errorf("binding %q has unsupported channel %q", current.ID, current.Channel)
	}
	return nil
}

func (r *PresetRepository) ResolveBinding(ctx context.Context, channel, bindingID string) (ChannelBinding, error) {
	if err := contextErr(ctx); err != nil {
		return ChannelBinding{}, err
	}
	current, exists := r.bindings[bindingKey{channel: channel, bindingID: bindingID}]
	if !exists || !current.Enabled {
		return ChannelBinding{}, ErrBindingNotFound
	}
	return current, nil
}

func (r *PresetRepository) GetTenant(ctx context.Context, tenantID string) (Tenant, error) {
	if err := contextErr(ctx); err != nil {
		return Tenant{}, err
	}
	current, exists := r.tenants[tenantID]
	if !exists || !current.Enabled {
		return Tenant{}, ErrTenantUnavailable
	}
	return current, nil
}

func (r *PresetRepository) GetAgentApp(ctx context.Context, tenantID, appID string) (AgentApp, error) {
	if err := contextErr(ctx); err != nil {
		return AgentApp{}, err
	}
	current, exists := r.apps[tenantAppKey{tenantID: tenantID, appID: appID}]
	if !exists || !current.Enabled {
		return AgentApp{}, ErrAgentAppUnavailable
	}
	return current, nil
}

func (r *PresetRepository) GetConfigVersion(ctx context.Context, tenantID, appID, version string) (ConfigVersion, error) {
	if err := contextErr(ctx); err != nil {
		return ConfigVersion{}, err
	}
	current, exists := r.configs[configKey{tenantID: tenantID, appID: appID, version: version}]
	if !exists {
		return ConfigVersion{}, ErrConfigVersionUnavailable
	}
	return current, nil
}

func (r *PresetRepository) GetStorageProfile(ctx context.Context, tenantID, profileID string) (StorageProfile, error) {
	if err := contextErr(ctx); err != nil {
		return StorageProfile{}, err
	}
	current, exists := r.profiles[profileKey{tenantID: tenantID, profileID: profileID}]
	if !exists {
		return StorageProfile{}, ErrStorageProfileUnavailable
	}
	return current, nil
}

func (r *PresetRepository) ListActiveStorageProfiles(ctx context.Context) ([]StorageProfile, error) {
	if err := contextErr(ctx); err != nil {
		return nil, err
	}
	result := make([]StorageProfile, len(r.activeProfiles))
	copy(result, r.activeProfiles)
	return result, nil
}

func AppName(tenantID, appID string) string {
	return "tenant/" + tenantID + "/app/" + appID
}

func ValidateID(field, value string) error {
	if value == "" || len(value) > MaxIDBytes {
		return fmt.Errorf("%s must be between 1 and %d bytes", field, MaxIDBytes)
	}
	for i := 0; i < len(value); i++ {
		current := value[i]
		if (current >= 'a' && current <= 'z') || (current >= 'A' && current <= 'Z') || (current >= '0' && current <= '9') || current == '.' || current == '_' || current == '-' {
			continue
		}
		return fmt.Errorf("%s contains unsupported characters", field)
	}
	return nil
}

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

var _ Repository = (*PresetRepository)(nil)
