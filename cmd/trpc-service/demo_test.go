package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestDemoPreviewIsCredentialAndDatabaseFree(t *testing.T) {
	var output bytes.Buffer
	if err := runDemo(context.Background(), nil, &output, func(string) string { t.Fatal("preview read environment"); return "" }); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	for _, item := range []string{"no database writes", "tenant_id=" + demoTenantID, "agent_app_id=" + demoAgentAppID, "model_profile_id=" + demoModelProfileID, "backend_profile_id=" + demoBackendProfileID} {
		if !strings.Contains(text, item) {
			t.Fatalf("preview missing %q: %s", item, text)
		}
	}
}

func TestDemoConfirmRequiresPostgresDSN(t *testing.T) {
	var output bytes.Buffer
	err := runDemo(context.Background(), []string{"--confirm"}, &output, func(string) string { return "" })
	if err == nil || !strings.Contains(err.Error(), "TRPC_POSTGRES_DSN") {
		t.Fatalf("err=%v", err)
	}
}

func TestDemoRejectsUnexpectedArguments(t *testing.T) {
	err := runDemo(context.Background(), []string{"--unknown"}, &bytes.Buffer{}, func(string) string { return "" })
	if err == nil || !strings.Contains(err.Error(), "usage") {
		t.Fatalf("err=%v", err)
	}
}

func TestDemoIdentifiersHaveStableValidFormat(t *testing.T) {
	ids := fixedDemoIDs()
	if err := ids.Validate(); err != nil {
		t.Fatal(err)
	}
	ids.AgentAppID = "invalid"
	if err := ids.Validate(); err == nil {
		t.Fatal("invalid app ID accepted")
	}
}

func TestDemoIsNotARunnableServiceRole(t *testing.T) {
	if err := runRole(context.Background(), func(string) string { return "" }, nil, "demo"); err == nil || err.Error() != "unsupported service role" {
		t.Fatalf("err=%v", err)
	}
}
