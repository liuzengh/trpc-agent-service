package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestCheckerRejectsUnsafeArgumentsAndConfigWithoutLeaking(t *testing.T) {
	for _, tc := range []struct {
		name, endpoint string
		args           []string
	}{
		{"missing", "", []string{"-env-file", ""}},
		{"wrong host", "https://other.example/mcp?apikey=private-canary", []string{"-env-file", ""}},
		{"bad flag", "", []string{"-apikey", "private-canary"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("WECOM_MCP_URL", tc.endpoint)
			var out bytes.Buffer
			err := run(tc.args, &out)
			if err == nil || strings.Contains(err.Error(), "private-canary") || out.Len() != 0 {
				t.Fatalf("unsafe checker error/output: %v", err)
			}
		})
	}
}
