package storage

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/memory"
)

func TestExternalMemoryScopesAndAuthenticatesRequests(t *testing.T) {
	var captured map[string]any
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/v1/memories/add" || request.Header.Get("Authorization") != "Bearer opaque-token" {
			t.Errorf("unexpected request path or authorization")
		}
		if err := json.NewDecoder(request.Body).Decode(&captured); err != nil {
			t.Error(err)
		}
		return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(strings.NewReader(""))}, nil
	})}
	service := &ExternalMemory{TenantID: "tenant-a", AppID: "app-a", Endpoint: "https://memory.example", Token: "opaque-token", Client: client}
	err := service.AddMemory(context.Background(), memory.UserKey{AppName: "tenant/tenant-a/app/app-a", UserID: "user"}, "private body", []string{"topic"})
	if err != nil {
		t.Fatal(err)
	}
	if captured["tenant_id"] != "tenant-a" || captured["app_id"] != "app-a" || captured["user_id"] != "user" {
		t.Fatalf("unexpected scope: %#v", captured)
	}
	if err := service.AddMemory(context.Background(), memory.UserKey{AppName: "tenant/tenant-b/app/app-a", UserID: "user"}, "private body", nil); err == nil {
		t.Fatal("cross-tenant app name must be rejected")
	}
}

func TestExternalMemoryErrorsDoNotEchoSecretOrBody(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusBadGateway, Body: io.NopCloser(strings.NewReader("opaque-token private body"))}, nil
	})}
	service := &ExternalMemory{TenantID: "tenant-a", AppID: "app-a", Endpoint: "https://memory.example", Token: "opaque-token", Client: client}
	err := service.AddMemory(context.Background(), memory.UserKey{AppName: "tenant/tenant-a/app/app-a", UserID: "user"}, "private body", nil)
	if err == nil || err.Error() != "storage: external memory service rejected request" {
		t.Fatalf("unsafe error: %v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}
