package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
)

// HTTPAPIConfig controls the diagnostic HTTP channel, not IM callbacks or Admin.
type HTTPAPIConfig struct {
	Enabled    bool
	Principals []HTTPAPIPrincipal
}

// HTTPAPIPrincipal grants an exact tenant, binding-key and user allowlist.
// Tokens identify trusted callers; user_id in JSON is not proof of identity.
type HTTPAPIPrincipal struct {
	Name        string   `json:"name"`
	Token       string   `json:"token"`
	TenantID    string   `json:"tenant_id"`
	BindingKeys []string `json:"binding_keys"`
	UserIDs     []string `json:"user_ids"`
}

func LoadHTTPAPIConfigFromEnv() (HTTPAPIConfig, error) {
	enabled, err := parseBoolEnv("TRPC_AGENT_HTTP_API_ENABLED", false)
	if err != nil {
		return HTTPAPIConfig{}, err
	}
	cfg := HTTPAPIConfig{Enabled: enabled}
	if raw := strings.TrimSpace(os.Getenv("TRPC_AGENT_HTTP_API_PRINCIPALS_JSON")); raw != "" {
		if err := decodeSecurityConfig(raw, &cfg.Principals); err != nil {
			return HTTPAPIConfig{}, errors.New("invalid TRPC_AGENT_HTTP_API_PRINCIPALS_JSON")
		}
	}
	// Single-user tutorial shorthand. It cannot grant arbitrary tenants/users.
	if token := os.Getenv("TRPC_AGENT_HTTP_API_TOKEN"); token != "" {
		if len(cfg.Principals) != 0 {
			return HTTPAPIConfig{}, errors.New("use HTTP_API_TOKEN or HTTP_API_PRINCIPALS_JSON, not both")
		}
		cfg.Principals = []HTTPAPIPrincipal{{Name: "tutorial-local-client", Token: token,
			TenantID: "tutorial-tenant", BindingKeys: []string{"tutorial-http"}, UserIDs: []string{"alice"}}}
	}
	return cfg, cfg.Validate()
}

func (c HTTPAPIConfig) Validate() error {
	if c.Enabled && len(c.Principals) == 0 {
		return errors.New("HTTP API requires at least one scoped principal")
	}
	names, tokens := map[string]bool{}, map[string]bool{}
	for _, p := range c.Principals {
		if !securityIdentifier(p.Name) || !securityIdentifier(p.TenantID) ||
			len(p.Token) < 32 || len(p.Token) > 512 || strings.ContainsAny(p.Token, " \t\r\n") ||
			names[p.Name] || tokens[p.Token] {
			return errors.New("invalid or duplicate HTTP API principal (token needs 32+ characters)")
		}
		if !exactList(p.BindingKeys, 128) || !exactList(p.UserIDs, 128) {
			return errors.New("HTTP API principal requires exact binding_keys and user_ids (no wildcards)")
		}
		names[p.Name], tokens[p.Token] = true, true
	}
	return nil
}

func securityIdentifier(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for i, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			continue
		}
		if i > 0 && (r == '-' || r == '_' || r == '.') {
			continue
		}
		return false
	}
	return true
}

func exactList(values []string, maxLen int) bool {
	if len(values) == 0 {
		return false
	}
	seen := map[string]bool{}
	for _, value := range values {
		if value == "" || len(value) > maxLen || strings.TrimSpace(value) != value ||
			strings.ContainsAny(value, "*\x00\r\n") || seen[value] {
			return false
		}
		seen[value] = true
	}
	return true
}

// Never include raw security configuration or parser excerpts in errors.
func decodeSecurityConfig(raw string, target any) error {
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("expected one JSON value")
	}
	return nil
}
