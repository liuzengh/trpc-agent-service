package platform

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestMemoryKnowledgeArtifactAndTraceCompletePublicWorkflow(t *testing.T) {
	runs := make(chan RunnerRequest, 1)
	client := newChannelTestClient(t, capturingRunner{request: runs})
	client.activateApp("app-one", "deploy-one")
	client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-rich"}`, nil, http.StatusCreated, nil)
	client.post("/api/v1/admin/memory/session-rich", `{"key":"preferred_language","value":"Go"}`, nil, http.StatusCreated, nil)
	client.post("/api/v1/admin/knowledge", `{"agent_app_id":"app-one","source":"mentor-notes","content":"Use PostgreSQL for shared control-plane state.","index_status":"retry_pending"}`, nil, http.StatusCreated, nil)

	traceParent := "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
	client.post("/api/v1/chat/sessions/session-rich/messages", `{"input":"Which backend?"}`, map[string]string{"X-Request-ID": "request-rich", "traceparent": traceParent}, http.StatusAccepted, nil)
	select {
	case request := <-runs:
		for _, expected := range []string{"preferred_language=Go", "Use PostgreSQL for shared control-plane state.", "User:\nWhich backend?"} {
			if !strings.Contains(request.Input, expected) {
				t.Fatalf("contextual input %q does not contain %q", request.Input, expected)
			}
		}
		if request.TraceParent != traceParent {
			t.Fatalf("traceparent = %q", request.TraceParent)
		}
	case <-time.After(time.Second):
		t.Fatal("Agent execution did not start")
	}
	if err := waitForChatEvent(client, "session-rich", "run.completed"); err != nil {
		t.Fatal(err)
	}
	trace, found := client.handler.governance.Trace("tenant-one", "", "request-rich")
	if !found {
		t.Fatal("request trace was not persisted")
	}
	spanNames := make(map[string]bool, len(trace.Spans))
	for _, span := range trace.Spans {
		spanNames[span.Name] = true
	}
	for _, expected := range []string{"storage.session_state.read", "storage.memory.read", "storage.knowledge.read"} {
		if !spanNames[expected] {
			t.Fatalf("trace misses %q: %#v", expected, trace.Spans)
		}
	}

	response := client.do(http.MethodGet, "/api/v1/admin/artifacts?session_id=session-rich", "", nil)
	defer response.Body.Close()
	var artifacts struct {
		Items []Artifact `json:"items"`
	}
	if response.StatusCode != http.StatusOK || json.NewDecoder(response.Body).Decode(&artifacts) != nil || len(artifacts.Items) != 1 {
		t.Fatalf("artifacts status=%d items=%#v", response.StatusCode, artifacts.Items)
	}
	artifact := artifacts.Items[0]
	if artifact.RequestID != "request-rich" || artifact.TraceID == "" || artifact.Status != "published" {
		t.Fatalf("artifact correlation = %#v", artifact)
	}

	client.post("/api/v1/auth/switch-tenant", `{"tenant_id":"tenant-two"}`, nil, http.StatusOK, nil)
	response = client.do(http.MethodGet, "/api/v1/admin/artifacts?session_id=session-rich", "", nil)
	defer response.Body.Close()
	artifacts.Items = nil
	if response.StatusCode != http.StatusOK || json.NewDecoder(response.Body).Decode(&artifacts) != nil || len(artifacts.Items) != 0 {
		t.Fatalf("cross-Tenant artifacts status=%d items=%#v", response.StatusCode, artifacts.Items)
	}
	response = client.do(http.MethodGet, "/api/v1/admin/knowledge?app_id=app-one", "", nil)
	defer response.Body.Close()
	var knowledge struct {
		Items []KnowledgeRecord `json:"items"`
	}
	if response.StatusCode != http.StatusOK || json.NewDecoder(response.Body).Decode(&knowledge) != nil || len(knowledge.Items) != 0 {
		t.Fatalf("cross-Tenant knowledge status=%d items=%#v", response.StatusCode, knowledge.Items)
	}
}
