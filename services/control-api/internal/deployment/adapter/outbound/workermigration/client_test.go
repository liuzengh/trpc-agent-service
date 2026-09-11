package workermigration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	executionv1 "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
)

func TestClientUsesFixedPathAndMapsBusy(t *testing.T) {
	busy := false
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != executionv1.BackendMigrationPath || r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
			w.WriteHeader(400)
			return
		}
		var in executionv1.BackendMigrationRequest
		if json.NewDecoder(r.Body).Decode(&in) != nil || in.Source.Password != "source" || in.Target.Password != "target" {
			t.Error("bad wire")
			w.WriteHeader(400)
			return
		}
		if busy {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"code":"BACKEND_MIGRATION_BUSY"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(executionv1.BackendMigrationResponse{MemoryScopesCopied: 4})
	}))
	defer server.Close()
	httpClient := server.Client()
	httpClient.Timeout = time.Second
	client, err := New(httpClient, server.URL)
	if err != nil {
		t.Fatal(err)
	}
	in := executionv1.BackendMigrationRequest{Source: executionv1.BackendMigrationTarget{Password: "source"}, Target: executionv1.BackendMigrationTarget{Password: "target"}}
	out, err := client.Execute(context.Background(), in)
	if err != nil || out.MemoryScopesCopied != 4 {
		t.Fatal(out, err)
	}
	busy = true
	if _, err := client.Execute(context.Background(), in); err != application.ErrBackendMigrationBusy {
		t.Fatal(err)
	}
}
