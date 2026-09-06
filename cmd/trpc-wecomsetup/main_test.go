package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

func TestSetupRequiresExplicitModeAndAuthorization(t *testing.T) {
	for _, args := range [][]string{{}, {"-mode", "apply", "-binding-file", "test"}, {"-mode", "send", "-authorized", "-binding-file", "test"}, {"-apikey=credential-canary"}} {
		var out bytes.Buffer
		err := run(args, &out)
		if err == nil || out.Len() != 0 || strings.Contains(err.Error(), "credential-canary") {
			t.Fatal("unsafe setup command")
		}
	}
}
func TestSetupAdminCreatesOnceAndNeverOverwrites(t *testing.T) {
	b := controlplane.ChannelBinding{ID: "binding", TenantID: "tenant", AppID: "app", AccountID: "bot", CallbackKey: "route", ChannelType: "wecom_mcp", Status: "active", SecretRef: "env://MCP", Config: json.RawMessage(`{"allowed_chat_ids":["group"],"allowed_user_ids":["human"],"mention_prefix":"@bot","timezone":"UTC","start_at":"2026-09-06T00:00:00Z","dedupe_mode":"fingerprint-v1"}`)}
	created := false
	creates := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret-canary" {
			t.Error("missing Admin auth")
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/admin/channel-bindings/get":
			if !created {
				w.WriteHeader(404)
				return
			}
		case "/admin/channel-bindings":
			created = true
			creates++
			w.WriteHeader(201)
		default:
			t.Error("unexpected Admin route")
		}
		_ = json.NewEncoder(w).Encode(b)
	}))
	defer server.Close()
	for range 2 {
		if err := applyBinding(context.Background(), server.URL, "secret-canary", b); err != nil {
			t.Fatal(err)
		}
	}
	if creates != 1 {
		t.Fatal("created duplicate binding")
	}
	changed := b
	changed.AppID = "different"
	if err := applyBinding(context.Background(), server.URL, "secret-canary", changed); err == nil || creates != 1 {
		t.Fatal("existing binding overwritten")
	}
	if err := applyBinding(context.Background(), "http://example.com", "secret-canary", b); err == nil {
		t.Fatal("Admin token sent to non-loopback address")
	}
}
