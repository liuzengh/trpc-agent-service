package workerknowledge

import (
	"context"
	"encoding/json"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestKnowledgeClientSendsFixedImportOverTLS(t *testing.T) {
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var q application.KnowledgeRequest
		if r.Method != "POST" || r.URL.Path != "/internal/v1/knowledge" || r.URL.RawQuery != "" {
			t.Error("path")
		}
		if json.NewDecoder(r.Body).Decode(&q) != nil || q.Operation != "import" || q.Credentials.QdrantAPIKey != "qdrant" || q.Credentials.EmbeddingAPIKey != "embedding" {
			t.Error("body")
		}
		_, _ = w.Write([]byte(`{"documents":2}`))
	}))
	defer s.Close()
	h := s.Client()
	h.Timeout = time.Second
	c, e := New(h, s.URL)
	if e != nil {
		t.Fatal(e)
	}
	out, e := c.ImportKnowledge(context.Background(), application.KnowledgeRequest{Operation: "import", Credentials: application.KnowledgeSecrets{QdrantAPIKey: "qdrant", EmbeddingAPIKey: "embedding"}})
	if e != nil || out.Documents != 2 {
		t.Fatal(e)
	}
}
func TestKnowledgeClientRejectsRedirect(t *testing.T) {
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "https://other.invalid")
		w.WriteHeader(307)
	}))
	defer s.Close()
	h := s.Client()
	h.Timeout = time.Second
	c, _ := New(h, s.URL)
	if _, e := c.ImportKnowledge(context.Background(), application.KnowledgeRequest{}); e != application.ErrKnowledgeUnavailable {
		t.Fatal(e)
	}
}
