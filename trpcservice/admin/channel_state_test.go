package admin

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecommcp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

func TestChannelRecoveryRequiresOperatorAndDisabledVersion(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	b := &data.ChannelBindings[0]
	b.ChannelType = wecommcp.ChannelType
	b.SecretRef = "env://MCP"
	now := time.Now().UTC().Truncate(time.Second)
	cfg := wecommcp.BindingConfig{AllowedChatIDs: []string{"group"}, AllowedUserIDs: []string{"human"}, MentionPrefix: "@bot", MentionStyle: "whitespace", Timezone: "UTC", StartAt: now.Add(-time.Hour).Format(time.RFC3339), DedupeMode: "fingerprint-v1"}
	b.Config, _ = json.Marshal(cfg)
	repo := controlplane.NewMemoryRepository(data)
	t.Cleanup(func() { _ = repo.Close() })
	state := wecommcp.NewMemoryStore()
	hash := sha256.Sum256([]byte("group"))
	chat := hex.EncodeToString(hash[:])
	_, _ = state.Checkpoint(context.Background(), wecommcp.PollKey{TenantID: b.TenantID, BindingID: b.ID, ChatHash: chat}, wecommcp.ConfigFingerprint(*b, cfg), cfg.Start())
	service, _ := New(repo)
	service.WithChannelState(state)
	request := func(role, path string, body map[string]any) int {
		t.Helper()
		handler, _ := NewHandlerWithPrincipals(service, []Principal{{Name: role, Role: role, Token: testAdminToken, TenantIDs: []string{b.TenantID}}})
		raw, _ := json.Marshal(body)
		req := httptest.NewRequest("POST", path, bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer "+testAdminToken)
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		return res.Code
	}
	if got := request(RoleAuditor, "/admin/channel-checkpoints/list", map[string]any{"tenant_id": b.TenantID, "binding_id": b.ID}); got != http.StatusOK {
		t.Fatal(got)
	}
	body := map[string]any{"tenant_id": b.TenantID, "binding_id": b.ID, "chat_hash": chat, "expected_version": 1, "expected_binding_version": 1, "action": "resume", "from": now.Add(-time.Minute), "acknowledge_gap": true}
	if got := request(RoleAuditor, "/admin/channel-checkpoints/recover", body); got != 403 {
		t.Fatal("auditor recovered checkpoint")
	}
	if got := request(RoleOperator, "/admin/channel-checkpoints/recover", body); got != 400 {
		t.Fatal("active binding recovered")
	}
	updated, err := repo.UpdateChannelBinding(context.Background(), b.TenantID, b.ID, b.Config, "disabled", 1)
	if err != nil {
		t.Fatal(err)
	}
	body["expected_binding_version"] = updated.Version
	if got := request(RoleOperator, "/admin/channel-checkpoints/recover", body); got != 200 {
		t.Fatalf("authorized recovery: %d", got)
	}
	body["tenant_id"] = "other-tenant"
	if got := request(RoleOperator, "/admin/channel-checkpoints/recover", body); got != 403 {
		t.Fatal("cross-tenant recovery")
	}
}
