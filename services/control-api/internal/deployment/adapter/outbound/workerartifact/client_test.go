package workerartifact

import (
	"context"
	"encoding/json"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestArtifactClientFixedPathAndSecretBody(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/artifacts" || r.Method != "POST" {
			t.Error("route")
		}
		var in application.ArtifactBackendRequest
		if json.NewDecoder(r.Body).Decode(&in) != nil || in.Credentials.SecretAccessKey != "secret" {
			t.Error("payload")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"note","version":0,"size_bytes":0}`))
	}))
	defer server.Close()
	h := server.Client()
	h.Timeout = time.Second
	c, err := New(h, server.URL)
	if err != nil {
		t.Fatal(err)
	}
	out, err := c.ExecuteArtifact(context.Background(), application.ArtifactBackendRequest{Credentials: application.ArtifactSecrets{SecretAccessKey: "secret"}})
	if err != nil || out.Name != "note" {
		t.Fatal(err)
	}
}
func TestArtifactClientDoesNotFollowRedirect(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "https://other.invalid")
		w.WriteHeader(307)
	}))
	defer server.Close()
	h := server.Client()
	h.Timeout = time.Second
	c, _ := New(h, server.URL)
	if _, err := c.ExecuteArtifact(context.Background(), application.ArtifactBackendRequest{}); err != application.ErrArtifactUnavailable {
		t.Fatal(err)
	}
}
