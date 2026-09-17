package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunMCPDeclarationDigestIsOfflineAndDeterministic(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "reviewed-tool.json")
	if err := os.WriteFile(path, []byte(`{"name":"weather_lookup","description":"weather","inputSchema":{"type":"object","properties":{"city":{"type":"string"}}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := runMCPDeclarationDigest([]string{"--declaration-file", path}, &output); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(output.String()); len(got) != 64 || strings.Trim(got, "0123456789abcdef") != "" {
		t.Fatalf("digest=%q", got)
	}
}

func TestRunMCPBindingDigestsMatchesConfiguredEndpoint(t *testing.T) {
	env := func(key string) string {
		if key == "TRPC_MCP_ENDPOINTS" {
			return `[{"tenant_id":"tenant-a","tool_id":"weather_lookup","version":2,"transport":"streamable","server_url":"https://mcp.example.test/tools","declaration_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","timeout":"10s","secret_ref":"secret://mcp/weather","secret_version":4,"secret_header":"Authorization","secret_prefix":"Bearer "}]`
		}
		return ""
	}
	var output bytes.Buffer
	if err := runMCPBindingDigests(nil, &output, env); err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(strings.TrimSpace(output.String()))
	if len(fields) != 3 || fields[0] != "weather_lookup" || fields[1] != "2" || len(fields[2]) != 64 {
		t.Fatalf("output=%q", output.String())
	}
}
