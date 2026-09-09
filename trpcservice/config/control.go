package config

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	DefaultControlPrefix         = "trpc-agent-service:phase6"
	DefaultPolicyCacheTTL        = 5 * time.Minute
	DefaultAuditRetention        = 30 * 24 * time.Hour
	DefaultMetricRetention       = 7 * 24 * time.Hour
	DefaultNodeHeartbeatInterval = 10 * time.Second
	DefaultNodeOfflineAfter      = 30 * time.Second
	DefaultNodeAssignmentWait    = 250 * time.Millisecond
	MaxControlPrefixBytes        = 128
	minControlDuration           = time.Millisecond
)

// ControlPlaneConfig owns governance and node-control state. RedisURL and
// AdminToken are resolved secrets and are never serialized or formatted.
type ControlPlaneConfig struct {
	RedisEndpoint               string
	RedisCredentialRef          string
	RedisURL                    string
	LogicalDB                   int
	KeyPrefix                   string
	AdminTokenRef               string
	AdminToken                  string
	PolicyCacheTTL              time.Duration
	AuditRetention              time.Duration
	MetricRetention             time.Duration
	NodeHeartbeatInterval       time.Duration
	NodeOfflineAfter            time.Duration
	NodeAssignmentWaitBackoff   time.Duration
	TraceDigestV2Enabled        bool
	NodeAssignmentEnabled       bool
	DevelopmentAllowSharedRedis bool
}

func (c ControlPlaneConfig) String() string {
	return fmt.Sprintf(
		"ControlPlaneConfig{RedisEndpoint:[REDACTED] RedisCredentialRef:[REDACTED] RedisURL:[REDACTED] LogicalDB:%d KeyPrefix:%q AdminTokenRef:[REDACTED] AdminToken:[REDACTED] PolicyCacheTTL:%s AuditRetention:%s MetricRetention:%s NodeHeartbeatInterval:%s NodeOfflineAfter:%s NodeAssignmentWaitBackoff:%s TraceDigestV2Enabled:%t NodeAssignmentEnabled:%t DevelopmentAllowSharedRedis:%t}",
		c.LogicalDB, c.KeyPrefix, c.PolicyCacheTTL,
		c.AuditRetention, c.MetricRetention, c.NodeHeartbeatInterval,
		c.NodeOfflineAfter, c.NodeAssignmentWaitBackoff,
		c.TraceDigestV2Enabled, c.NodeAssignmentEnabled,
		c.DevelopmentAllowSharedRedis,
	)
}

func (c ControlPlaneConfig) GoString() string { return c.String() }

type controlPlaneConfigFile struct {
	RedisEndpoint               string `json:"redis_endpoint"`
	RedisCredentialRef          string `json:"redis_credential_ref"`
	LogicalDB                   *int   `json:"logical_db"`
	KeyPrefix                   string `json:"key_prefix,omitempty"`
	AdminTokenRef               string `json:"admin_token_ref,omitempty"`
	PolicyCacheTTL              string `json:"policy_cache_ttl,omitempty"`
	AuditRetention              string `json:"audit_retention,omitempty"`
	MetricRetention             string `json:"metric_retention,omitempty"`
	NodeHeartbeatInterval       string `json:"node_heartbeat_interval,omitempty"`
	NodeOfflineAfter            string `json:"node_offline_after,omitempty"`
	NodeAssignmentWaitBackoff   string `json:"node_assignment_wait_backoff,omitempty"`
	TraceDigestV2Enabled        bool   `json:"trace_digest_v2_enabled,omitempty"`
	NodeAssignmentEnabled       bool   `json:"node_assignment_enabled,omitempty"`
	DevelopmentAllowSharedRedis bool   `json:"development_allow_shared_redis,omitempty"`
}

func parseControlPlaneFile(raw *controlPlaneConfigFile, resolver CredentialResolver, role Role) (*ControlPlaneConfig, error) {
	if raw == nil {
		return nil, nil
	}
	if resolver == nil {
		return nil, errors.New("control plane credential resolver is required")
	}
	endpoint, err := normalizeRedisEndpoint(raw.RedisEndpoint)
	if err != nil {
		return nil, errors.New("control plane redis_endpoint is invalid")
	}
	if raw.LogicalDB == nil || *raw.LogicalDB < 0 {
		return nil, errors.New("control plane logical_db must be a non-negative integer")
	}
	if _, err := envNameFromCredentialRef(raw.RedisCredentialRef); err != nil {
		return nil, errors.New("control plane redis_credential_ref must use env:<ENV_NAME>")
	}
	redisSecret, err := resolver.Resolve(raw.RedisCredentialRef)
	if err != nil {
		return nil, errors.New("control plane Redis credential is unavailable")
	}
	redisURL, err := NormalizeRedisURL(redisSecret)
	if err != nil {
		return nil, errors.New("control plane Redis credential is not a valid Redis URL")
	}
	secretEndpoint, secretDB, err := redisLocation(redisURL)
	if err != nil || secretEndpoint != endpoint || secretDB != *raw.LogicalDB {
		return nil, errors.New("control plane Redis credential does not match redis_endpoint and logical_db")
	}
	config := ControlPlaneConfig{
		RedisEndpoint:               endpoint,
		RedisCredentialRef:          raw.RedisCredentialRef,
		RedisURL:                    redisURL,
		LogicalDB:                   *raw.LogicalDB,
		KeyPrefix:                   valueOrDefault(raw.KeyPrefix, DefaultControlPrefix),
		AdminTokenRef:               strings.TrimSpace(raw.AdminTokenRef),
		PolicyCacheTTL:              DefaultPolicyCacheTTL,
		AuditRetention:              DefaultAuditRetention,
		MetricRetention:             DefaultMetricRetention,
		NodeHeartbeatInterval:       DefaultNodeHeartbeatInterval,
		NodeOfflineAfter:            DefaultNodeOfflineAfter,
		NodeAssignmentWaitBackoff:   DefaultNodeAssignmentWait,
		TraceDigestV2Enabled:        raw.TraceDigestV2Enabled,
		NodeAssignmentEnabled:       raw.NodeAssignmentEnabled,
		DevelopmentAllowSharedRedis: raw.DevelopmentAllowSharedRedis,
	}
	if config.AdminTokenRef != "" {
		if _, err := envNameFromCredentialRef(config.AdminTokenRef); err != nil {
			return nil, errors.New("control plane admin_token_ref must use env:<ENV_NAME>")
		}
		if role == RoleGateway || role == RoleServe {
			config.AdminToken, err = resolver.Resolve(config.AdminTokenRef)
			if err != nil {
				return nil, errors.New("control plane admin token is unavailable")
			}
		}
	}
	for _, target := range []struct {
		raw   string
		value *time.Duration
		name  string
	}{
		{raw.PolicyCacheTTL, &config.PolicyCacheTTL, "policy_cache_ttl"},
		{raw.AuditRetention, &config.AuditRetention, "audit_retention"},
		{raw.MetricRetention, &config.MetricRetention, "metric_retention"},
		{raw.NodeHeartbeatInterval, &config.NodeHeartbeatInterval, "node_heartbeat_interval"},
		{raw.NodeOfflineAfter, &config.NodeOfflineAfter, "node_offline_after"},
		{raw.NodeAssignmentWaitBackoff, &config.NodeAssignmentWaitBackoff, "node_assignment_wait_backoff"},
	} {
		parsed, parseErr := parseOptionalDuration(target.raw, *target.value)
		if parseErr != nil {
			return nil, fmt.Errorf("control plane %s is invalid", target.name)
		}
		*target.value = parsed
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &config, nil
}

func (c ControlPlaneConfig) Validate() error {
	if _, err := normalizeRedisEndpoint(c.RedisEndpoint); err != nil {
		return errors.New("control plane redis_endpoint is invalid")
	}
	endpoint, database, err := redisLocation(c.RedisURL)
	if err != nil || endpoint != c.RedisEndpoint || database != c.LogicalDB {
		return errors.New("control plane Redis URL does not match endpoint and logical DB")
	}
	prefix := strings.TrimRight(strings.TrimSpace(c.KeyPrefix), ":")
	if prefix == "" || len(prefix) > MaxControlPrefixBytes {
		return fmt.Errorf("control plane key_prefix must be between 1 and %d bytes", MaxControlPrefixBytes)
	}
	for _, current := range []byte(prefix) {
		if current < 0x21 || current > 0x7e {
			return errors.New("control plane key_prefix must contain printable ASCII without spaces")
		}
	}
	if c.TraceDigestV2Enabled != c.NodeAssignmentEnabled {
		return errors.New("trace_digest_v2_enabled and node_assignment_enabled must have the same value")
	}
	for name, duration := range map[string]time.Duration{
		"policy_cache_ttl":             c.PolicyCacheTTL,
		"audit_retention":              c.AuditRetention,
		"metric_retention":             c.MetricRetention,
		"node_heartbeat_interval":      c.NodeHeartbeatInterval,
		"node_offline_after":           c.NodeOfflineAfter,
		"node_assignment_wait_backoff": c.NodeAssignmentWaitBackoff,
	} {
		if duration < minControlDuration {
			return fmt.Errorf("control plane %s must be positive", name)
		}
	}
	if c.NodeHeartbeatInterval > c.NodeOfflineAfter/3 {
		return errors.New("control plane node_heartbeat_interval must not exceed one third of node_offline_after")
	}
	return nil
}

func validateControlPlaneIsolation(control *ControlPlaneConfig, messaging *MessagingConfig, catalog tenant.Catalog, resolver CredentialResolver, role Role) error {
	if control == nil {
		return nil
	}
	controlPrefix := strings.TrimRight(control.KeyPrefix, ":")
	check := func(kind, rawURL, keyPrefix string) error {
		endpoint, database, err := redisLocation(rawURL)
		if err != nil {
			return fmt.Errorf("%s Redis location is invalid", kind)
		}
		if strings.TrimRight(keyPrefix, ":") == controlPrefix {
			return fmt.Errorf("control plane key_prefix conflicts with %s Redis", kind)
		}
		if endpoint != control.RedisEndpoint {
			return nil
		}
		if !control.DevelopmentAllowSharedRedis {
			return fmt.Errorf("control plane Redis endpoint conflicts with %s Redis", kind)
		}
		if database == control.LogicalDB {
			return fmt.Errorf("control plane logical_db conflicts with %s Redis", kind)
		}
		return nil
	}
	if messaging != nil {
		if err := check("messaging", messaging.RedisURL, messaging.KeyPrefix); err != nil {
			return err
		}
	}
	if role == RoleGateway {
		return nil
	}
	for _, profile := range catalog.StorageProfiles {
		if profile.Kind != tenant.StorageKindRedis {
			continue
		}
		rawURL, err := resolver.Resolve(profile.CredentialRef)
		if err != nil {
			return errors.New("tenant Redis credential is unavailable for control plane isolation validation")
		}
		normalized, err := NormalizeRedisURL(rawURL)
		if err != nil {
			return errors.New("tenant Redis credential is invalid for control plane isolation validation")
		}
		if err := check("tenant storage", normalized, profile.KeyPrefix); err != nil {
			return err
		}
	}
	return nil
}

func normalizeRedisEndpoint(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (parsed.Scheme != "redis" && parsed.Scheme != "rediss") || parsed.Hostname() == "" || parsed.Port() == "" {
		return "", errors.New("invalid Redis endpoint")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return "", errors.New("Redis endpoint must not contain credentials, database, query, or fragment")
	}
	port, err := strconv.Atoi(parsed.Port())
	if err != nil || port < 1 || port > 65535 {
		return "", errors.New("invalid Redis endpoint port")
	}
	return strings.ToLower(parsed.Scheme) + "://" + strings.ToLower(parsed.Host), nil
}

func redisLocation(raw string) (string, int, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", 0, err
	}
	endpoint, err := normalizeRedisEndpoint((&url.URL{Scheme: parsed.Scheme, Host: parsed.Host}).String())
	if err != nil {
		return "", 0, err
	}
	database, err := strconv.Atoi(strings.TrimPrefix(parsed.Path, "/"))
	if err != nil || database < 0 {
		return "", 0, errors.New("invalid Redis logical DB")
	}
	return endpoint, database, nil
}
