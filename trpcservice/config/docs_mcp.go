package config

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
)

type DocsMCPConfig struct {
	Enabled           bool
	Addr, Root, Token string
}

// The embedded reference server is opt-in and owned by Worker/all processes.
// Other roles do not read its credential. It is deliberately loopback-only.
func LoadDocsMCPConfigFromEnv(roles Roles) (DocsMCPConfig, error) {
	var cfg DocsMCPConfig
	if !roles.Worker {
		return cfg, nil
	}
	enabled, err := parseBoolEnv("TRPC_AGENT_DOCS_MCP_ENABLED", false)
	if err != nil {
		return cfg, errors.New("invalid docs MCP enable flag")
	}
	if !enabled {
		return cfg, nil
	}
	cfg.Enabled = true
	cfg.Addr = strings.TrimSpace(os.Getenv("TRPC_AGENT_DOCS_MCP_ADDR"))
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:18090"
	}
	host, port, err := net.SplitHostPort(cfg.Addr)
	portNumber, portErr := strconv.Atoi(port)
	if err != nil || portErr != nil || portNumber < 1 || portNumber > 65535 || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return DocsMCPConfig{}, errors.New("docs MCP must listen on an explicit loopback IP and port")
	}
	cfg.Root = strings.TrimSpace(os.Getenv("TRPC_AGENT_DOCS_MCP_ROOT"))
	if cfg.Root == "" {
		cfg.Root = "."
	}
	var credential struct {
		URL   string `json:"url"`
		Token string `json:"bearer_token"`
	}
	if json.Unmarshal([]byte(os.Getenv("MCP_DOCS_SERVER")), &credential) != nil || credential.URL != "http://"+cfg.Addr+"/mcp" || len(credential.Token) < 32 || len(credential.Token) > 256 || strings.ContainsAny(credential.Token, "\r\n") {
		return DocsMCPConfig{}, errors.New("docs MCP requires matching endpoint and strong bearer token in MCP_DOCS_SERVER")
	}
	cfg.Token = credential.Token
	return cfg, nil
}
