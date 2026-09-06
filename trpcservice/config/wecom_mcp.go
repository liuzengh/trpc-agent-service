package config

import (
	"errors"
	"os"
	"regexp"
	"strings"
)

// Empty by default: an MCP URL or a binding alone never enables background I/O.
type WeComMCPTarget struct {
	TenantID  string `json:"tenant_id"`
	BindingID string `json:"binding_id"`
}

func LoadWeComMCPTargetsFromEnv() ([]WeComMCPTarget, error) {
	var targets []WeComMCPTarget
	if raw := strings.TrimSpace(os.Getenv("TRPC_AGENT_WECOM_MCP_TARGETS_JSON")); raw != "" {
		if decodeSecurityConfig(raw, &targets) != nil {
			return nil, errors.New("invalid TRPC_AGENT_WECOM_MCP_TARGETS_JSON")
		}
	}
	if len(targets) > 20 {
		return nil, errors.New("too many WeCom MCP receiver bindings")
	}
	pattern := regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	seen := map[WeComMCPTarget]bool{}
	for _, target := range targets {
		if !pattern.MatchString(target.TenantID) || !pattern.MatchString(target.BindingID) || seen[target] {
			return nil, errors.New("invalid or duplicate WeCom MCP receiver binding")
		}
		seen[target] = true
	}
	return targets, nil
}
