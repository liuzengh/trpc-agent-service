// Package config loads the immutable platform catalog and resolves secret
// references without placing plaintext credentials in catalog files.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/persistence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	DefaultRequestTimeout = 60 * time.Second
	DefaultMaxOutput      = 1024
	DefaultRedisPrefix    = "trpc-agent-service:phase1"
	MinIdentitySecretLen  = 32
	MaxCatalogBytes       = 1 << 20
	CatalogSchemaVersion  = 1

	legacyModelCredential = "internal:legacy-model"
	legacyRedisCredential = "internal:legacy-redis"
)

type CredentialResolver interface {
	Resolve(string) (string, error)
}

type envCredentialResolver struct{}

func (envCredentialResolver) Resolve(ref string) (string, error) {
	name, err := envNameFromCredentialRef(ref)
	if err != nil {
		return "", err
	}
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return "", errors.New("referenced credential is missing")
	}
	return value, nil
}

type staticCredentialResolver map[string]string

func (r staticCredentialResolver) Resolve(ref string) (string, error) {
	value := strings.TrimSpace(r[ref])
	if value == "" {
		return "", errors.New("referenced credential is missing")
	}
	return value, nil
}

func NewStaticCredentialResolver(values map[string]string) CredentialResolver {
	copyValues := make(staticCredentialResolver, len(values))
	for key, value := range values {
		copyValues[key] = value
	}
	return copyValues
}

// Config retains the Phase 1 fields so existing callers can construct a
// legacy single-binding runtime. New code consumes Catalog and Credentials.
type Config struct {
	IdentitySecret []byte
	Catalog        tenant.Catalog
	CatalogPath    string
	Messaging      *MessagingConfig
	ControlPlane   *ControlPlaneConfig
	Observability  ObservabilityConfig

	ModelName           string
	ModelBaseURL        string
	ModelAPIKeyEnv      string
	ModelAPIKey         string
	ModelRequestTimeout time.Duration
	ModelMaxOutput      int
	RedisURL            string
	RedisKeyPrefix      string
	BindingID           string
	TenantID            string
	AgentAppID          string
	ConfigVersion       string
	AppName             string

	credentials CredentialResolver
}

func (c Config) String() string {
	return fmt.Sprintf(
		"Config{CatalogPath:%q Tenants:%d AgentApps:%d ConfigVersions:%d StorageProfiles:%d ChannelBindings:%d Messaging:%v ControlPlane:%v ModelName:%q ModelBaseURL:%q ModelAPIKeyEnv:[REDACTED] ModelAPIKey:[REDACTED] ModelRequestTimeout:%s ModelMaxOutput:%d IdentitySecret:[REDACTED] RedisURL:[REDACTED] RedisKeyPrefix:%q BindingID:%q TenantID:%q AgentAppID:%q ConfigVersion:%q AppName:%q}",
		c.CatalogPath,
		len(c.Catalog.Tenants),
		len(c.Catalog.AgentApps),
		len(c.Catalog.ConfigVersions),
		len(c.Catalog.StorageProfiles),
		len(c.Catalog.ChannelBindings),
		c.Messaging,
		c.ControlPlane,
		c.ModelName,
		redactModelURL(c.ModelBaseURL),
		c.ModelRequestTimeout,
		c.ModelMaxOutput,
		c.RedisKeyPrefix,
		c.BindingID,
		c.TenantID,
		c.AgentAppID,
		c.ConfigVersion,
		c.AppName,
	)
}

func (c Config) GoString() string { return c.String() }

// RuntimeCatalog returns a validated immutable catalog and its resolver. It
// also converts old Config literals used by Phase 1 tests into the same model.
func (c Config) RuntimeCatalog() (tenant.Catalog, CredentialResolver, error) {
	catalog := c.Catalog
	resolver := c.credentials
	if len(catalog.Tenants) == 0 {
		var err error
		catalog, resolver, err = legacyCatalog(c)
		if err != nil {
			return tenant.Catalog{}, nil, err
		}
	}
	if resolver == nil {
		resolver = envCredentialResolver{}
	}
	if _, err := tenant.NewPresetRepository(catalog); err != nil {
		return tenant.Catalog{}, nil, fmt.Errorf("validate platform catalog: %w", err)
	}
	if err := validateCatalogModelURLs(catalog); err != nil {
		return tenant.Catalog{}, nil, err
	}
	if err := validateCatalogCredentials(catalog, resolver); err != nil {
		return tenant.Catalog{}, nil, err
	}
	return catalog, resolver, nil
}

// RoutingCatalog validates the immutable catalog without resolving model or
// tenant storage credentials. Gateway uses it to avoid requiring Worker-only
// secrets.
func (c Config) RoutingCatalog() (tenant.Catalog, error) {
	catalog := c.Catalog
	if len(catalog.Tenants) == 0 {
		var err error
		catalog, _, err = legacyCatalog(c)
		if err != nil {
			return tenant.Catalog{}, err
		}
	}
	if _, err := tenant.NewPresetRepository(catalog); err != nil {
		return tenant.Catalog{}, fmt.Errorf("validate platform catalog: %w", err)
	}
	if err := validateCatalogModelURLs(catalog); err != nil {
		return tenant.Catalog{}, err
	}
	return catalog, nil
}

// GatewayCatalog resolves only credentials owned by Gateway adapters. Worker
// startup deliberately uses RuntimeCatalog and never calls this method.
func (c Config) GatewayCatalog() (tenant.Catalog, CredentialResolver, error) {
	catalog, err := c.RoutingCatalog()
	if err != nil {
		return tenant.Catalog{}, nil, err
	}
	resolver := c.credentials
	if resolver == nil {
		resolver = envCredentialResolver{}
	}
	return catalog, resolver, nil
}

func NewCatalogConfig(catalog tenant.Catalog, identitySecret []byte, resolver CredentialResolver) (Config, error) {
	if len(identitySecret) < MinIdentitySecretLen {
		return Config{}, fmt.Errorf("IDENTITY_SECRET must be at least %d bytes", MinIdentitySecretLen)
	}
	if resolver == nil {
		return Config{}, errors.New("credential resolver is required")
	}
	config := Config{
		IdentitySecret: append([]byte(nil), identitySecret...),
		Catalog:        catalog,
		credentials:    resolver,
	}
	if _, _, err := config.RuntimeCatalog(); err != nil {
		return Config{}, err
	}
	return config, nil
}

// Load preserves the existing all-in-one behavior.
func Load() (Config, error) {
	return LoadForRole(RoleServe)
}

// LoadForRole reads either PLATFORM_CONFIG_FILE or the Phase 1 environment
// contract and resolves only the credentials required by the selected role.
func LoadForRole(role Role) (Config, error) {
	if role != RoleGateway && role != RoleWorker && role != RoleServe {
		return Config{}, errors.New("unsupported service role")
	}
	identitySecret, err := requiredEnv("IDENTITY_SECRET")
	if err != nil {
		return Config{}, err
	}
	if len([]byte(identitySecret)) < MinIdentitySecretLen {
		return Config{}, fmt.Errorf("IDENTITY_SECRET must be at least %d bytes", MinIdentitySecretLen)
	}

	if path := strings.TrimSpace(os.Getenv("PLATFORM_CONFIG_FILE")); path != "" {
		catalog, rawMessaging, rawControlPlane, rawObservability, err := loadPlatformFile(path)
		if err != nil {
			return Config{}, err
		}
		config := Config{IdentitySecret: []byte(identitySecret), Catalog: catalog, credentials: envCredentialResolver{}}
		if _, err := config.RoutingCatalog(); err != nil {
			return Config{}, err
		}
		if role != RoleGateway {
			if _, _, err := config.RuntimeCatalog(); err != nil {
				return Config{}, err
			}
		}
		messaging, err := parseMessagingFile(rawMessaging, config.credentials)
		if err != nil {
			return Config{}, err
		}
		if messaging == nil {
			return Config{}, errors.New("platform messaging configuration is required")
		}
		config.Messaging = messaging
		controlPlane, err := parseControlPlaneFile(rawControlPlane, config.credentials, role)
		if err != nil {
			return Config{}, err
		}
		if err := validateControlPlaneIsolation(controlPlane, messaging, catalog, config.credentials, role); err != nil {
			return Config{}, err
		}
		config.ControlPlane = controlPlane
		observability, err := parseObservabilityFile(rawObservability)
		if err != nil {
			return Config{}, err
		}
		config.Observability = observability
		config.CatalogPath = path
		return config, nil
	}
	return loadLegacy([]byte(identitySecret), role)
}

func loadLegacy(identitySecret []byte, role Role) (Config, error) {
	modelName, err := requiredEnv("MODEL_NAME")
	if err != nil {
		return Config{}, err
	}
	baseURL, err := requiredEnv("MODEL_BASE_URL")
	if err != nil {
		return Config{}, err
	}
	keyEnv, err := requiredEnv("MODEL_API_KEY_ENV")
	if err != nil {
		return Config{}, err
	}
	if !validEnvName(keyEnv) {
		return Config{}, errors.New("MODEL_API_KEY_ENV must name a valid environment variable")
	}
	apiKey := ""
	if role != RoleGateway {
		apiKey = strings.TrimSpace(os.Getenv(keyEnv))
		if apiKey == "" {
			return Config{}, errors.New("model api key referenced by MODEL_API_KEY_ENV is missing")
		}
	}
	redisRaw, err := requiredEnv("REDIS_URL")
	if err != nil {
		return Config{}, err
	}
	redisURL, err := NormalizeRedisURL(redisRaw)
	if err != nil {
		return Config{}, err
	}
	timeout, err := durationEnv("MODEL_REQUEST_TIMEOUT", DefaultRequestTimeout)
	if err != nil {
		return Config{}, err
	}
	if timeout <= 0 {
		return Config{}, errors.New("MODEL_REQUEST_TIMEOUT must be positive")
	}
	maxOutput, err := intEnv("MODEL_MAX_OUTPUT_TOKENS", DefaultMaxOutput)
	if err != nil {
		return Config{}, err
	}
	if maxOutput <= 0 {
		return Config{}, errors.New("MODEL_MAX_OUTPUT_TOKENS must be positive")
	}

	config := Config{
		ModelName:           modelName,
		ModelBaseURL:        strings.TrimRight(baseURL, "/"),
		ModelAPIKeyEnv:      keyEnv,
		ModelAPIKey:         apiKey,
		ModelRequestTimeout: timeout,
		ModelMaxOutput:      maxOutput,
		IdentitySecret:      append([]byte(nil), identitySecret...),
		RedisURL:            redisURL,
		RedisKeyPrefix:      envOrDefault("REDIS_KEY_PREFIX", DefaultRedisPrefix),
		BindingID:           "demo-binding",
		TenantID:            "tenant-demo",
		AgentAppID:          "assistant",
		ConfigVersion:       "v1",
		AppName:             tenant.AppName("tenant-demo", "assistant"),
	}
	messaging := legacyMessaging(redisURL, config.RedisKeyPrefix)
	config.Messaging = &messaging
	catalog, resolver, err := legacyCatalog(config)
	if err != nil {
		return Config{}, err
	}
	config.Catalog = catalog
	config.credentials = resolver
	if role == RoleGateway {
		if _, err := config.RoutingCatalog(); err != nil {
			return Config{}, err
		}
	} else {
		if _, _, err := config.RuntimeCatalog(); err != nil {
			return Config{}, err
		}
	}
	observability, err := parseObservabilityFile(nil)
	if err != nil {
		return Config{}, err
	}
	config.Observability = observability
	return config, nil
}

func legacyCatalog(c Config) (tenant.Catalog, CredentialResolver, error) {
	if strings.TrimSpace(c.ModelName) == "" || strings.TrimSpace(c.ModelBaseURL) == "" || strings.TrimSpace(c.ModelAPIKeyEnv) == "" || strings.TrimSpace(c.RedisURL) == "" {
		return tenant.Catalog{}, nil, errors.New("legacy runtime configuration is incomplete")
	}
	if len(c.IdentitySecret) < MinIdentitySecretLen {
		return tenant.Catalog{}, nil, fmt.Errorf("IDENTITY_SECRET must be at least %d bytes", MinIdentitySecretLen)
	}
	if c.ModelRequestTimeout <= 0 || c.ModelMaxOutput <= 0 {
		return tenant.Catalog{}, nil, errors.New("legacy model limits must be positive")
	}
	bindingID := valueOrDefault(c.BindingID, "demo-binding")
	tenantID := valueOrDefault(c.TenantID, "tenant-demo")
	appID := valueOrDefault(c.AgentAppID, "assistant")
	version := valueOrDefault(c.ConfigVersion, "v1")
	prefix := valueOrDefault(c.RedisKeyPrefix, DefaultRedisPrefix)
	catalog := tenant.Catalog{
		Tenants: []tenant.Tenant{{ID: tenantID, Enabled: true}},
		StorageProfiles: []tenant.StorageProfile{{
			TenantID: tenantID, ID: "redis-default", Kind: tenant.StorageKindRedis,
			CredentialRef: legacyRedisCredential, KeyPrefix: prefix, LegacyPrefix: true,
		}},
		AgentApps: []tenant.AgentApp{{
			TenantID: tenantID, ID: appID, Enabled: true, ActiveConfigVersion: version,
		}},
		ConfigVersions: []tenant.ConfigVersion{{
			TenantID: tenantID, AgentAppID: appID, Version: version,
			StorageProfileID: "redis-default", Instruction: "You are a concise assistant. Answer the user's request directly.",
			Model: tenant.ModelConfig{
				Name: c.ModelName, BaseURL: strings.TrimRight(c.ModelBaseURL, "/"),
				CredentialRef: legacyModelCredential, RequestTimeout: c.ModelRequestTimeout,
				MaxOutputTokens: c.ModelMaxOutput,
			},
		}},
		ChannelBindings: []tenant.ChannelBinding{{
			ID: bindingID, Channel: "demo", ExternalAccountID: bindingID,
			TenantID: tenantID, AgentAppID: appID, Enabled: true,
		}},
	}
	resolver := staticCredentialResolver{
		legacyModelCredential: c.ModelAPIKey,
		legacyRedisCredential: c.RedisURL,
	}
	return catalog, resolver, nil
}

type catalogFile struct {
	SchemaVersion   int                      `json:"schema_version"`
	Messaging       *messagingConfigFile     `json:"messaging,omitempty"`
	ControlPlane    *controlPlaneConfigFile  `json:"control_plane,omitempty"`
	Observability   *observabilityConfigFile `json:"observability,omitempty"`
	Tenants         []tenant.Tenant          `json:"tenants"`
	StorageProfiles []tenant.StorageProfile  `json:"storage_profiles"`
	AgentApps       []tenant.AgentApp        `json:"agent_apps"`
	ConfigVersions  []configVersionFile      `json:"config_versions"`
	ChannelBindings []tenant.ChannelBinding  `json:"channel_bindings"`
}

type configVersionFile struct {
	TenantID         string          `json:"tenant_id"`
	AgentAppID       string          `json:"agent_app_id"`
	Version          string          `json:"version"`
	StorageProfileID string          `json:"storage_profile_id"`
	Instruction      string          `json:"instruction"`
	Model            modelConfigFile `json:"model"`
}

type modelConfigFile struct {
	Name            string `json:"name"`
	BaseURL         string `json:"base_url"`
	CredentialRef   string `json:"credential_ref"`
	RequestTimeout  string `json:"request_timeout"`
	MaxOutputTokens int    `json:"max_output_tokens"`
}

func loadCatalogFile(path string) (tenant.Catalog, error) {
	catalog, _, _, _, err := loadPlatformFile(path)
	return catalog, err
}

func loadPlatformFile(path string) (tenant.Catalog, *messagingConfigFile, *controlPlaneConfigFile, *observabilityConfigFile, error) {
	file, err := os.Open(path)
	if err != nil {
		return tenant.Catalog{}, nil, nil, nil, fmt.Errorf("open platform config: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, MaxCatalogBytes+1))
	if err != nil {
		return tenant.Catalog{}, nil, nil, nil, fmt.Errorf("read platform config: %w", err)
	}
	if len(data) > MaxCatalogBytes {
		return tenant.Catalog{}, nil, nil, nil, fmt.Errorf("platform config exceeds %d bytes", MaxCatalogBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var raw catalogFile
	if err := decoder.Decode(&raw); err != nil {
		return tenant.Catalog{}, nil, nil, nil, fmt.Errorf("decode platform config: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return tenant.Catalog{}, nil, nil, nil, errors.New("platform config must contain one JSON object")
	}
	if raw.SchemaVersion != CatalogSchemaVersion {
		return tenant.Catalog{}, nil, nil, nil, errors.New("unsupported platform config schema_version")
	}

	catalog := tenant.Catalog{
		Tenants:         raw.Tenants,
		StorageProfiles: raw.StorageProfiles,
		AgentApps:       raw.AgentApps,
		ChannelBindings: raw.ChannelBindings,
		ConfigVersions:  make([]tenant.ConfigVersion, 0, len(raw.ConfigVersions)),
	}
	for _, current := range raw.ConfigVersions {
		timeout, err := time.ParseDuration(strings.TrimSpace(current.Model.RequestTimeout))
		if err != nil {
			return tenant.Catalog{}, nil, nil, nil, errors.New("platform config contains an invalid model request_timeout")
		}
		if _, err := envNameFromCredentialRef(current.Model.CredentialRef); err != nil {
			return tenant.Catalog{}, nil, nil, nil, err
		}
		catalog.ConfigVersions = append(catalog.ConfigVersions, tenant.ConfigVersion{
			TenantID: current.TenantID, AgentAppID: current.AgentAppID, Version: current.Version,
			StorageProfileID: current.StorageProfileID, Instruction: current.Instruction,
			Model: tenant.ModelConfig{
				Name: current.Model.Name, BaseURL: strings.TrimRight(current.Model.BaseURL, "/"),
				CredentialRef: current.Model.CredentialRef, RequestTimeout: timeout,
				MaxOutputTokens: current.Model.MaxOutputTokens,
			},
		})
	}
	for _, profile := range catalog.StorageProfiles {
		if profile.Kind == tenant.StorageKindRedis || profile.Kind == tenant.StorageKindPostgres || profile.Kind == tenant.StorageKindMySQL {
			if _, err := envNameFromCredentialRef(profile.CredentialRef); err != nil {
				return tenant.Catalog{}, nil, nil, nil, err
			}
		}
	}
	for _, binding := range catalog.ChannelBindings {
		for _, ref := range []string{binding.CredentialRef, binding.BotIDRef, binding.BotSecretRef} {
			if ref == "" {
				continue
			}
			if _, err := envNameFromCredentialRef(ref); err != nil {
				return tenant.Catalog{}, nil, nil, nil, err
			}
		}
	}
	return catalog, raw.Messaging, raw.ControlPlane, raw.Observability, nil
}

func validateCatalogCredentials(catalog tenant.Catalog, resolver CredentialResolver) error {
	enabledTenants := make(map[string]bool, len(catalog.Tenants))
	enabledApps := make(map[string]bool, len(catalog.AgentApps))
	for _, current := range catalog.Tenants {
		enabledTenants[current.ID] = current.Enabled
	}
	for _, current := range catalog.AgentApps {
		enabledApps[current.TenantID+"\x00"+current.ID] = current.Enabled && enabledTenants[current.TenantID]
	}
	profiles := make(map[string]tenant.StorageProfile, len(catalog.StorageProfiles))
	for _, current := range catalog.StorageProfiles {
		profiles[current.TenantID+"\x00"+current.ID] = current
	}
	checkedProfiles := make(map[string]bool)
	for _, current := range catalog.ConfigVersions {
		if !enabledApps[current.TenantID+"\x00"+current.AgentAppID] {
			continue
		}
		if _, err := resolver.Resolve(current.Model.CredentialRef); err != nil {
			return fmt.Errorf("model credential unavailable for enabled config: %w", err)
		}
		profileKey := current.TenantID + "\x00" + current.StorageProfileID
		profile := profiles[profileKey]
		if profile.Kind != tenant.StorageKindInMemory && !checkedProfiles[profileKey] {
			value, err := resolver.Resolve(profile.CredentialRef)
			if err != nil {
				return fmt.Errorf("storage credential unavailable for enabled config: %w", err)
			}
			if profile.Kind == tenant.StorageKindRedis {
				value, err = NormalizeRedisURL(value)
				if err != nil {
					return errors.New("storage credential for enabled config is not a valid Redis URL")
				}
			}
			if _, err := persistence.FingerprintForProfile(profile, value); err != nil {
				return errors.New("storage credential for enabled config is invalid for configured backend")
			}
			checkedProfiles[profileKey] = true
		}
	}
	return nil
}

func validateChannelCredentials(catalog tenant.Catalog, resolver CredentialResolver) error {
	enabledTenants := make(map[string]bool, len(catalog.Tenants))
	enabledApps := make(map[string]bool, len(catalog.AgentApps))
	for _, current := range catalog.Tenants {
		enabledTenants[current.ID] = current.Enabled
	}
	for _, current := range catalog.AgentApps {
		enabledApps[current.TenantID+"\x00"+current.ID] = current.Enabled && enabledTenants[current.TenantID]
	}
	for _, binding := range catalog.ChannelBindings {
		if !binding.Enabled || !enabledApps[binding.TenantID+"\x00"+binding.AgentAppID] {
			continue
		}
		for _, ref := range []string{binding.CredentialRef, binding.BotIDRef, binding.BotSecretRef} {
			if ref == "" {
				continue
			}
			if _, err := resolver.Resolve(ref); err != nil {
				return errors.New("IM credential unavailable for enabled binding")
			}
		}
	}
	return nil
}

func validateCatalogModelURLs(catalog tenant.Catalog) error {
	for _, current := range catalog.ConfigVersions {
		parsed, err := url.Parse(strings.TrimSpace(current.Model.BaseURL))
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			return errors.New("model base_url must be an absolute HTTP(S) URL")
		}
		if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
			return errors.New("model base_url must not contain credentials, query parameters, or fragments")
		}
	}
	return nil
}

func envNameFromCredentialRef(ref string) (string, error) {
	const prefix = "env:"
	if !strings.HasPrefix(ref, prefix) {
		return "", errors.New("credential_ref must use env:<ENV_NAME>")
	}
	name := strings.TrimPrefix(ref, prefix)
	if !validEnvName(name) {
		return "", errors.New("credential_ref contains an invalid environment variable name")
	}
	return name, nil
}

func requiredEnv(name string) (string, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return "", fmt.Errorf("required environment variable %s is missing", name)
	}
	return value, nil
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func durationEnv(name string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", name, err)
	}
	return duration, nil
}

func intEnv(name string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	number, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", name, err)
	}
	return number, nil
}

func NormalizeRedisURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", errors.New("REDIS_URL is empty")
	}
	if !strings.Contains(raw, "://") {
		raw = "redis://" + raw
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("invalid REDIS_URL: %w", err)
	}
	if parsed.Scheme != "redis" && parsed.Scheme != "rediss" {
		return "", errors.New("REDIS_URL must use redis or rediss scheme")
	}
	if parsed.Host == "" || parsed.Hostname() == "" || parsed.Port() == "" {
		return "", errors.New("REDIS_URL must include host and port")
	}
	port, err := strconv.Atoi(parsed.Port())
	if err != nil || port < 1 || port > 65535 {
		return "", errors.New("REDIS_URL has an invalid port")
	}
	if parsed.Path == "" || parsed.Path == "/" {
		parsed.Path = "/0"
	}
	database := strings.TrimPrefix(parsed.Path, "/")
	if strings.Contains(database, "/") {
		return "", errors.New("REDIS_URL database must be a non-negative integer")
	}
	db, err := strconv.Atoi(database)
	if err != nil || db < 0 {
		return "", errors.New("REDIS_URL database must be a non-negative integer")
	}
	if parsed.Fragment != "" {
		return "", errors.New("REDIS_URL must not include a fragment")
	}
	return parsed.String(), nil
}

func redactURLPassword(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.User == nil {
		return raw
	}
	username := parsed.User.Username()
	if _, hasPassword := parsed.User.Password(); hasPassword {
		parsed.User = url.UserPassword(username, "REDACTED")
	}
	return parsed.String()
}

func redactModelURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "[REDACTED]"
	}
	if parsed.User != nil {
		parsed.User = url.User("REDACTED")
	}
	if parsed.RawQuery != "" {
		parsed.RawQuery = "REDACTED"
	}
	parsed.Fragment = ""
	return parsed.String()
}

func validEnvName(name string) bool {
	for index, current := range name {
		if (current >= 'A' && current <= 'Z') || (current >= 'a' && current <= 'z') || current == '_' || (index > 0 && current >= '0' && current <= '9') {
			continue
		}
		return false
	}
	return name != ""
}

func valueOrDefault(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
