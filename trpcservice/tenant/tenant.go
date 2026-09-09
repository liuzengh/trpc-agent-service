// Package tenant models multi-tenant isolation for config, data, tools, and keys.
package tenant

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"
)

var (
	// ErrNotFound means the requested control-plane object does not exist.
	ErrNotFound = errors.New("tenant object not found")
	// ErrInactive means a tenant or channel binding exists but is disabled.
	ErrInactive = errors.New("tenant object is inactive")
)

// Quota defines tenant-level token and request limits.
type Quota struct {
	DailyTokenLimit int64 `json:"daily_token_limit"`
	RatePerMinute   int   `json:"rate_per_minute"`
}

// Policy defines tenant-level governance settings.
type Policy struct {
	RedactPatterns []string `json:"redact_patterns"`
	AuditLevel     string   `json:"audit_level"`
	BudgetCNY      float64  `json:"budget_cny"`
}

// Tenant is the top-level isolation boundary.
type Tenant struct {
	ID       string `json:"tenant_id"`
	Name     string `json:"name"`
	IsActive bool   `json:"is_active"`
	Quota    Quota  `json:"quota"`
	Policy   Policy `json:"policy"`
}

// ModelConfig selects and authenticates a model provider.
type ModelConfig struct {
	Provider  string        `json:"provider"`
	Model     string        `json:"model"`
	APIKeyRef string        `json:"api_key_ref"`
	BaseURL   string        `json:"base_url,omitempty"`
	Timeout   time.Duration `json:"timeout"`
}

// BackendSelection selects shared state services for one Agent application.
type BackendSelection struct {
	Session string `json:"session"`
	Memory  string `json:"memory"`
}

// AgentApp is a versioned Agent application owned by one tenant.
type AgentApp struct {
	ID        string           `json:"app_id"`
	TenantID  string           `json:"tenant_id"`
	AppName   string           `json:"app_name"`
	Model     ModelConfig      `json:"model"`
	Tools     []string         `json:"tools"`
	Backends  BackendSelection `json:"backends"`
	Version   int              `json:"version"`
	IsCurrent bool             `json:"is_current"`
}

// ChannelBinding maps a platform route to one Agent application.
type ChannelBinding struct {
	ID       string            `json:"binding_id"`
	TenantID string            `json:"tenant_id"`
	AppID    string            `json:"app_id"`
	Channel  string            `json:"channel"`
	RouteKey string            `json:"route_key"`
	Config   map[string]string `json:"config"`
	IsActive bool              `json:"is_active"`
}

// Snapshot is the immutable data-plane view used to process one request.
type Snapshot struct {
	Tenant  Tenant
	App     AgentApp
	Binding ChannelBinding
}

// ConfigStore is the authoritative control-plane store.
type ConfigStore interface {
	UpsertTenant(context.Context, Tenant) error
	GetTenant(context.Context, string) (Tenant, error)
	ListTenants(context.Context) ([]Tenant, error)
	DeactivateTenant(context.Context, string) error

	UpsertApp(context.Context, AgentApp) (int, error)
	GetCurrentApp(context.Context, string) (AgentApp, error)
	ListApps(context.Context, string) ([]AgentApp, error)
	BindAppVersion(context.Context, string, int) error

	UpsertBinding(context.Context, ChannelBinding) error
	GetBindingByRoute(context.Context, string, string) (ChannelBinding, error)
	ListBindings(context.Context, string) ([]ChannelBinding, error)
}

// ConfigCache resolves channel routes into immutable snapshots.
type ConfigCache interface {
	ResolveBinding(context.Context, string, string) (Snapshot, error)
	Invalidate(channel, routeKey string)
}

// ResolveSecret resolves an env:VAR_NAME reference without logging its value.
func ResolveSecret(ref string) (string, error) {
	const prefix = "env:"
	if !strings.HasPrefix(ref, prefix) {
		return "", errors.New("secret reference must use env: prefix")
	}
	name := strings.TrimPrefix(ref, prefix)
	if name == "" {
		return "", errors.New("secret environment variable name is empty")
	}
	value, ok := os.LookupEnv(name)
	if !ok || value == "" {
		return "", fmt.Errorf("secret environment variable %q is not set", name)
	}
	return value, nil
}

// Validate checks identifiers and all supported backend/channel selections.
func (t Tenant) Validate() error {
	if err := validateIdentifier("tenant_id", t.ID); err != nil {
		return err
	}
	if strings.TrimSpace(t.Name) == "" {
		return errors.New("tenant name is required")
	}
	if t.Quota.DailyTokenLimit < 0 || t.Quota.RatePerMinute < 0 || t.Policy.BudgetCNY < 0 {
		return errors.New("tenant quota and budget values must be non-negative")
	}
	return nil
}

// Validate checks one Agent application version.
func (a AgentApp) Validate() error {
	if err := validateIdentifier("app_id", a.ID); err != nil {
		return err
	}
	if err := validateIdentifier("tenant_id", a.TenantID); err != nil {
		return err
	}
	if err := validateIdentifier("app_name", a.AppName); err != nil {
		return err
	}
	if a.Model.Provider == "" || a.Model.Model == "" {
		return errors.New("model provider and model are required")
	}
	if a.Model.APIKeyRef != "" && !strings.HasPrefix(a.Model.APIKeyRef, "env:") {
		return errors.New("model API key must be an env: secret reference")
	}
	if a.Model.BaseURL != "" {
		if parsed, err := url.ParseRequestURI(a.Model.BaseURL); err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return errors.New("model base_url must be an absolute URL")
		}
	}
	if a.Backends.Session != "redis" && a.Backends.Session != "mysql" {
		return errors.New("session backend must be redis or mysql")
	}
	if a.Backends.Memory != "pgvector" && a.Backends.Memory != "mem0" {
		return errors.New("memory backend must be pgvector or mem0")
	}
	return nil
}

// Validate checks one channel binding without dereferencing its secrets.
func (b ChannelBinding) Validate() error {
	if err := validateIdentifier("binding_id", b.ID); err != nil {
		return err
	}
	if err := validateIdentifier("tenant_id", b.TenantID); err != nil {
		return err
	}
	if err := validateIdentifier("app_id", b.AppID); err != nil {
		return err
	}
	switch b.Channel {
	case "wecom", "wecombot", "feishu", "ilink", "webui":
		// wecom = self-built app callback (not the required WeCom IM).
		// Real IMs: wecombot (智能机器人) and feishu. webui is the demo channel.
	default:
		return errors.New("channel must be wecom, wecombot, feishu, ilink, or webui")
	}
	if strings.TrimSpace(b.RouteKey) == "" {
		return errors.New("route_key is required")
	}
	for key, value := range b.Config {
		lowerKey := strings.ToLower(key)
		if strings.Contains(lowerKey, "secret") || strings.Contains(lowerKey, "token") || strings.Contains(lowerKey, "key") {
			if !strings.HasPrefix(value, "env:") {
				return fmt.Errorf("binding config %q must be an env: secret reference", key)
			}
		}
	}
	return nil
}

func validateIdentifier(name, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", name)
	}
	if len(value) > 128 {
		return fmt.Errorf("%s is too long", name)
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '-' || r == '_' {
			continue
		}
		return fmt.Errorf("%s contains unsupported character %q", name, r)
	}
	return nil
}
