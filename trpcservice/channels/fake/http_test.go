package fake

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	gatewaymemory "github.com/liuzengh/trpc-agent-service/trpcservice/gateway/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	profilememory "github.com/liuzengh/trpc-agent-service/trpcservice/profile/inmemory"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
	sessionmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker/mockmodel"
)

func TestTwoTenantHTTPVerticalSlice(t *testing.T) {
	tasks := gatewaymemory.NewTaskStore()
	sessions := sessionmemory.New()
	models := mockmodel.New()
	bindings := profilememory.NewBindingResolver()
	var snapshots []profile.ExecutionProfileSnapshot
	for _, id := range []string{"tenant-a", "tenant-b"} {
		b := tenant.ExecutionBinding{AgentAppVersion: 1, AgentAppRevision: 1, AgentContentDigest: "digest-" + id, ConfigVersion: 1, PolicyVersion: 1}
		bindings.Put(id, "app", b)
		key := profile.ExecutionProfileKey{TenantID: id, TenantVersion: 1, AgentAppID: "app", AgentAppVersion: 1, AgentAppRevision: 1, ContentDigest: b.AgentContentDigest, ConfigVersion: 1, PolicyVersion: 1}
		snapshots = append(snapshots, profile.ExecutionProfileSnapshot{Key: key, TenantVersion: 1, AgentAppVersion: 1, ContentDigest: b.AgentContentDigest, AgentKind: "llm", Instruction: "help", ModelProfileRef: profile.VersionedRef{ID: "mock", Version: 1}})
	}
	profiles := profilememory.NewResolver(snapshots...)
	executor := worker.DeterministicTestExecutor{Tasks: tasks, Profiles: profiles, Sessions: sessions, Model: models}
	dispatcher := gateway.LocalDispatcher{Tasks: tasks, Bindings: bindings, Executor: executor}
	handler := NewHandler(dispatcher,
		Binding{Locator: "a", Tenant: tenant.Context{TenantID: "tenant-a", TenantVersion: 1, AgentAppID: "app", SubjectID: "subject", Channel: "fake", TrustedSource: "binding:a"}, IdentityKey: []byte("a-key")},
		Binding{Locator: "c", Tenant: tenant.Context{TenantID: "tenant-a", TenantVersion: 1, AgentAppID: "app", SubjectID: "subject", Channel: "fake", TrustedSource: "binding:c"}, IdentityKey: []byte("a-key")},
		Binding{Locator: "b", Tenant: tenant.Context{TenantID: "tenant-b", TenantVersion: 1, AgentAppID: "app", SubjectID: "subject", Channel: "fake", TrustedSource: "binding:b"}, IdentityKey: []byte("b-key")},
	)
	body := []byte(`{"external_message_id":"same-message","external_user_id":"same-user","external_chat_id":"same-chat","text":"hello"}`)
	for _, locator := range []string{"a", "a", "b"} {
		req := httptest.NewRequest(http.MethodPost, "/bindings/"+locator, bytes.NewReader(body))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("%s: status=%d body=%s", locator, rec.Code, rec.Body.String())
		}
	}
	if got := models.Calls("tenant-a", stableID("req", "tenant-a", "fake", "a", "same-message")); got != 1 {
		t.Fatalf("tenant-a calls=%d", got)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/bindings/c", bytes.NewReader(body)))
	if rec.Code != http.StatusAccepted || models.Calls("tenant-a", stableID("req", "tenant-a", "fake", "c", "same-message")) != 1 {
		t.Fatalf("second account status=%d body=%s", rec.Code, rec.Body.String())
	}
	// Verify terminal facts are independently keyed even though external IDs match.
	for _, id := range []string{"tenant-a", "tenant-b"} {
		status, err := findExecution(context.Background(), tasks, id)
		if err != nil {
			t.Fatal(err)
		}
		result, err := sessions.GetTerminalByInputSeq(context.Background(), sessionstore.TerminalKey{SessionKey: sessionstore.SessionKey{TenantID: id, AgentAppID: "app", SessionID: status.Envelope.SessionID}, InputSeq: 1})
		if err != nil {
			t.Fatal(err)
		}
		if result.ResultRef != "mock-result://"+id+"/"+status.Envelope.RequestID {
			t.Fatalf("tenant result mismatch: %#v", result)
		}
	}
	spoof := []byte(`{"tenant_id":"tenant-b","external_message_id":"x","external_user_id":"u","external_chat_id":"c","text":"x"}`)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/bindings/a", bytes.NewReader(spoof)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("spoof status=%d", rec.Code)
	}
	collision := []byte(`{"external_message_id":"same-message","external_user_id":"same-user","external_chat_id":"same-chat","text":"changed"}`)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/bindings/a", bytes.NewReader(collision)))
	if rec.Code != http.StatusConflict {
		t.Fatalf("collision status=%d", rec.Code)
	}
}

func findExecution(ctx context.Context, tasks *gatewaymemory.TaskStore, tenantID string) (gateway.ExecutionStatus, error) {
	// Request IDs are deterministic but tenant-specific; derive the same trusted key.
	accountID := "a"
	if tenantID == "tenant-b" {
		accountID = "b"
	}
	requestID := stableID("req", tenantID, "fake", accountID, "same-message")
	return tasks.GetExecution(ctx, gateway.ExecutionKey{TenantID: tenantID, RequestID: requestID})
}
