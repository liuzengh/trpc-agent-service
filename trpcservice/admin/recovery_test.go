package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	servicelog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/recovery"
	"github.com/liuzengh/trpc-agent-service/trpcservice/repository"
)

type recoveryFixture struct {
	mu    sync.Mutex
	items map[string]recovery.Item
}

func recoveryKey(tenantID string, kind recovery.Kind, id string) string {
	return tenantID + "\x00" + string(kind) + "\x00" + id
}

func (store *recoveryFixture) List(_ context.Context, tenantID string, kind recovery.Kind, statuses []recovery.Status, limit int) ([]recovery.Item, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	allowed := make(map[recovery.Status]bool)
	for _, status := range statuses {
		allowed[status] = true
	}
	result := make([]recovery.Item, 0)
	for _, item := range store.items {
		if item.TenantID == tenantID && item.Kind == kind && allowed[item.Status] && len(result) < limit {
			result = append(result, item)
		}
	}
	return result, nil
}

func (store *recoveryFixture) Redrive(_ context.Context, tenantID string, kind recovery.Kind, id string, expected recovery.Status) (recovery.Item, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	key := recoveryKey(tenantID, kind, id)
	item, ok := store.items[key]
	if !ok {
		return recovery.Item{}, recovery.ErrNotFound
	}
	if item.Status != expected || expected != recovery.StatusDLQ {
		return recovery.Item{}, recovery.ErrConflict
	}
	item.Status, item.Attempts = recovery.StatusRetry, 0
	store.items[key] = item
	return item, nil
}

func (store *recoveryFixture) ResolveOutbox(_ context.Context, tenantID, id string, expected, decision recovery.Status) (recovery.Item, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	key := recoveryKey(tenantID, recovery.KindOutbox, id)
	item, ok := store.items[key]
	if !ok {
		return recovery.Item{}, recovery.ErrNotFound
	}
	if item.Status != expected || expected != recovery.StatusUncertain || (decision != recovery.StatusRetry && decision != recovery.StatusSent) {
		return recovery.Item{}, recovery.ErrConflict
	}
	item.Status = decision
	if decision == recovery.StatusRetry {
		item.Attempts = 0
	}
	store.items[key] = item
	return item, nil
}

func TestRecoveryAPIAuthenticationIsolationCASAndSecretSafety(t *testing.T) {
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	fixture := &recoveryFixture{items: make(map[string]recovery.Item)}
	for _, tenantID := range []string{"tenant-a", "tenant-b"} {
		for _, item := range []recovery.Item{
			{TenantID: tenantID, Kind: recovery.KindInbox, ID: "shared-inbox", AppID: "app", Binding: "binding", Status: recovery.StatusDLQ, Attempts: 5, TraceID: "trace-in", Created: now},
			{TenantID: tenantID, Kind: recovery.KindOutbox, ID: "shared-outbox", AppID: "app", Binding: "binding", Status: recovery.StatusDLQ, Attempts: 5, TraceID: "trace-out", Created: now},
			{TenantID: tenantID, Kind: recovery.KindOutbox, ID: "uncertain", AppID: "app", Binding: "binding", Status: recovery.StatusUncertain, Attempts: 2, TraceID: "trace-unknown", Created: now},
		} {
			fixture.items[recoveryKey(tenantID, item.Kind, item.ID)] = item
		}
	}
	const canary = "recovery-secret-canary"
	redactor := servicelog.NewRedactor(nil, []string{canary})
	audits := audit.NewMemoryStore(redactor)
	service, err := NewService(repository.NewMemoryStore(), WithRecoveryStore(fixture), WithAudit(audits), WithRedactor(redactor))
	if err != nil {
		t.Fatal(err)
	}
	handler, _ := NewHandler(service)
	authenticator, _ := NewAuthenticator([]Credential{{Name: "alice", Token: "token", Tenants: map[string]bool{"tenant-a": true}}})
	secured := authenticator.Wrap(handler)

	path := "/v1/tenants/tenant-a/operations/outbox"
	if response := call(secured, http.MethodGet, path, "", nil); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated=%d", response.Code)
	}
	if response := call(secured, http.MethodGet, "/v1/tenants/tenant-b/operations/outbox", "token", nil); response.Code != http.StatusForbidden {
		t.Fatalf("forbidden=%d", response.Code)
	}
	listed := call(secured, http.MethodGet, path, "token", nil)
	if listed.Code != http.StatusOK || strings.Contains(listed.Body.String(), canary) {
		t.Fatalf("list=%d %s", listed.Code, listed.Body.String())
	}
	var items []recovery.Item
	if err := json.Unmarshal(listed.Body.Bytes(), &items); err != nil || len(items) != 2 {
		t.Fatalf("items=%+v err=%v", items, err)
	}

	body := []byte(`{"id":"shared-outbox","expected_status":"dlq","reason":"manual retry"}`)
	if response := call(secured, http.MethodPost, path+"/redrive", "token", append(append([]byte(nil), body...), []byte(` {}`)...)); response.Code != http.StatusBadRequest {
		t.Fatalf("trailing JSON=%d %s", response.Code, response.Body.String())
	}
	if response := call(secured, http.MethodPost, path+"/redrive", "token", body); response.Code != http.StatusOK {
		t.Fatalf("redrive=%d %s", response.Code, response.Body.String())
	}
	if stale := call(secured, http.MethodPost, path+"/redrive", "token", body); stale.Code != http.StatusConflict {
		t.Fatalf("stale=%d %s", stale.Code, stale.Body.String())
	}
	// Identical tenant-B ids are never touched by the tenant-A operation.
	if fixture.items[recoveryKey("tenant-b", recovery.KindOutbox, "shared-outbox")].Status != recovery.StatusDLQ {
		t.Fatal("cross-tenant mutation")
	}

	unsafeRetry := []byte(`{"id":"uncertain","expected_status":"uncertain","decision":"retry","reason":"provider check","acknowledge_duplicate_risk":false}`)
	if response := call(secured, http.MethodPost, path+"/resolve", "token", unsafeRetry); response.Code != http.StatusBadRequest {
		t.Fatalf("unsafe retry=%d %s", response.Code, response.Body.String())
	}
	confirmed := []byte(`{"id":"uncertain","expected_status":"uncertain","decision":"retry","reason":"` + canary + ` verified duplicate risk","acknowledge_duplicate_risk":true}`)
	response := call(secured, http.MethodPost, path+"/resolve", "token", confirmed)
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), canary) {
		t.Fatalf("resolve=%d %s", response.Code, response.Body.String())
	}
	records := audits.Records("tenant-a")
	if len(records) != 3 { // successful redrive, stale conflict, successful resolve
		t.Fatalf("audit records=%d %+v", len(records), records)
	}
	payload, _ := json.Marshal(records)
	if strings.Contains(string(payload), canary) {
		t.Fatalf("audit leaked secret: %s", payload)
	}
	if records[len(records)-1].Details["actor"] != "alice" || records[len(records)-1].Details["action"] != "message.outbox.resolve" {
		t.Fatalf("audit=%+v", records[len(records)-1])
	}
	if records[len(records)-1].Details["reason_hash"] == "" || records[len(records)-1].Details["reason"] != nil {
		t.Fatalf("unsafe audit reason=%+v", records[len(records)-1].Details)
	}
}

func TestRecoveryAPIDoesNotTreatUnavailableStoreAsClientError(t *testing.T) {
	service, _ := NewService(repository.NewMemoryStore())
	handler, _ := NewHandler(service)
	request := httptest.NewRequest(http.MethodGet, "/v1/tenants/tenant-a/operations/inbox", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusInternalServerError || !strings.Contains(response.Body.String(), "internal server error") {
		t.Fatalf("response=%d %s", response.Code, response.Body.String())
	}
}

var _ recovery.Store = (*recoveryFixture)(nil)
