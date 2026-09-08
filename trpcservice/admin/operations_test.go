package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/toolexec"
)

func TestToolOperationAdminRBACAndNoOutcomeOverride(t *testing.T) {
	repo := controlplane.NewMemoryRepository(controlplane.DefaultBootstrapData())
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(repo)
	journal := toolexec.NewMemoryJournal()
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(journal)
	store := toolexec.NewMemoryOperations()
	op, _, err := store.Reserve(context.Background(), toolexec.Operation{ID: "op-test", TenantID: "tutorial-tenant", AppID: "tutorial-app", UserID: "alice", ToolName: toolexec.WorkItemTool, BusinessKeyHash: "key-hash", InputHash: "input-hash"})
	if err != nil {
		t.Fatal(err)
	}
	operations, _ := toolexec.NewOperations(store, journal, nil, toolexec.NewWorkItems(nil))
	service, _ := New(repo)
	service.WithToolOperations(operations, journal)
	for _, role := range []string{RoleAuditor, RoleOperator} {
		handler, _ := NewHandlerWithPrincipals(service, []Principal{{Name: role, Role: role, Token: testAdminToken, TenantIDs: []string{"tutorial-tenant"}}})
		for _, tc := range []struct {
			endpoint, body string
			status         int
		}{
			{"/admin/tool-operations/get", `{"tenant_id":"tutorial-tenant","operation_id":"op-test"}`, 200},
			{"/admin/tool-operations/get", `{"tenant_id":"foreign","operation_id":"op-test"}`, 403},
			{"/admin/tool-operations/reconcile", `{"tenant_id":"tutorial-tenant","operation_id":"op-test"}`, 200},
			{"/admin/tool-operations/reconcile", `{"tenant_id":"tutorial-tenant","operation_id":"op-test","status":"succeeded"}`, 400},
			{"/admin/tool-operations/reconcile", `{"tenant_id":"tutorial-tenant","operation_id":"op-test","result":{}}`, 400},
		} {
			want := tc.status
			if role == RoleAuditor && tc.endpoint == "/admin/tool-operations/reconcile" && tc.status != 400 {
				want = 403
			}
			if role == RoleAuditor && tc.status == 400 && !bytes.Contains([]byte(tc.body), []byte(`"result"`)) {
				want = 403
			}
			req := httptest.NewRequest(http.MethodPost, tc.endpoint, bytes.NewBufferString(tc.body))
			req.Header.Set("Authorization", "Bearer "+testAdminToken)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != want {
				t.Fatalf("%s %s: %d want=%d body=%s", role, tc.endpoint, rec.Code, want, rec.Body.String())
			}
		}
	}
	stored, _ := store.Get(context.Background(), op.TenantID, op.ID)
	if stored.Status != toolexec.StatusUnknown || json.Valid(stored.Result) {
		t.Fatal("not-found was treated as success or executed")
	}
}
