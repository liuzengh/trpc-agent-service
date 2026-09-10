package vault

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
)

func TestManagerReadsTenantScopedKVAndRedactsFailures(t *testing.T) {
	var token string
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		token = request.Header.Get("X-Vault-Token")
		if request.URL.Path != "/v1/secret/data/t_00000000000000000000000000/secret/model" {
			t.Fatalf("Vault path = %q", request.URL.Path)
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{"data": map[string]any{"data": map[string]string{"value": "managed-secret"}}})
	}))
	defer server.Close()
	manager, err := New(Config{BaseURL: server.URL, Token: "vault-token", Mount: "secret", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	value, err := manager.Read(context.Background(), modelprofile.SecretScope{TenantID: "t_00000000000000000000000000", SecretRef: "secret/model"})
	if err != nil || value.Value() != "managed-secret" || token != "vault-token" {
		t.Fatalf("Vault Read() = %q, %v, token=%q", value.Value(), err, token)
	}
	if value.String() != "<redacted-secret>" {
		t.Fatal("Vault secret was not redacted")
	}
	if _, err := manager.Read(context.Background(), modelprofile.SecretScope{TenantID: "t_00000000000000000000000000", SecretRef: "../other"}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("invalid path error = %v", err)
	}
	if _, err := New(Config{BaseURL: strings.Replace(server.URL, "https://", "http://", 1), Token: "token", Mount: "secret"}); err == nil {
		t.Fatal("Vault manager accepted non-HTTPS endpoint")
	}
	if strings.Contains(value.String(), "managed-secret") {
		t.Fatal("Vault secret leaked in String")
	}
}

func TestManagerReadFailureBoundaries(t *testing.T) {
	validScope := modelprofile.SecretScope{TenantID: "t_00000000000000000000000000", SecretRef: "secret/model"}
	for name, status := range map[string]int{"unauthorized": http.StatusUnauthorized, "forbidden": http.StatusForbidden, "not-found": http.StatusNotFound, "server": http.StatusInternalServerError} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(status) }))
			defer server.Close()
			manager, err := New(Config{BaseURL: server.URL, Token: "token", Mount: "secret", HTTPClient: server.Client()})
			if err != nil {
				t.Fatal(err)
			}
			_, err = manager.Read(context.Background(), validScope)
			if name == "unauthorized" || name == "forbidden" {
				if !errors.Is(err, ErrUnauthorized) {
					t.Fatalf("Read() = %v", err)
				}
			} else if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("Read() = %v", err)
			}
		})
	}
	for name, body := range map[string]string{"malformed": "not-json", "missing-value": "{\"data\":{\"data\":{}}}", "numeric-value": "{\"data\":{\"data\":{\"value\":12}}}", "empty-value": "{\"data\":{\"data\":{\"value\":\"\"}}}"} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { _, _ = writer.Write([]byte(body)) }))
			defer server.Close()
			manager, err := New(Config{BaseURL: server.URL, Token: "token", Mount: "secret", HTTPClient: server.Client()})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := manager.Read(context.Background(), validScope); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("Read() = %v", err)
			}
		})
	}
	manager, err := New(Config{BaseURL: "https://127.0.0.1:1", Token: "token", Mount: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Read(context.Background(), validScope); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("transport Read() = %v", err)
	}
	if _, err := manager.Read(nil, validScope); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil context Read() = %v", err)
	}
	if _, err := manager.Read(context.Background(), modelprofile.SecretScope{}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("invalid scope Read() = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := manager.Read(canceled, validScope); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Read() = %v", err)
	}
	var nilManager *Manager
	if _, err := nilManager.Read(context.Background(), validScope); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("nil manager Read() = %v", err)
	}
	if _, err := (&Manager{baseURL: "https://bad\nurl", mount: "secret", client: http.DefaultClient}).Read(context.Background(), validScope); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("invalid request URL Read() = %v", err)
	}
}

func TestManagerNewValidationBoundaries(t *testing.T) {
	for name, config := range map[string]Config{
		"empty url":     {Token: "token", Mount: "secret"},
		"invalid url":   {BaseURL: "://bad", Token: "token", Mount: "secret"},
		"missing token": {BaseURL: "https://vault.example", Mount: "secret"},
		"missing mount": {BaseURL: "https://vault.example", Token: "token"},
		"empty host":    {BaseURL: "https://", Token: "token", Mount: "secret"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := New(config); !errors.Is(err, ErrInvalid) {
				t.Fatalf("New() = %v", err)
			}
		})
	}
}
