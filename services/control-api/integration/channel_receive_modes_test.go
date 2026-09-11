package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	channelapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
)

func testReceiveModeHTTP(t *testing.T, ctx context.Context, router http.Handler, pool *pgxpool.Pool, tenant string, owner, member *http.Cookie) {
	base := "/v1/tenants/" + tenant + "/channel-accounts"
	body := `{"provider":"telegram","provider_account_id":"654321001","name":"Polling fixture","credentials":{"telegram.bot_token":{"action":"replace","value":"TEST_ONLY_POLLING_TOKEN"}}}`
	var created channelapp.CommandResult
	w := deploymentRequest(t, router, "POST", base, owner, "mode-http-create", body, 201, &created)
	if w.Header().Get("X-Channel-Result-Contract") != "receive-modes-v1" || created.Account.Config.ReceiveMode != "long_polling" {
		t.Fatal("default wire")
	}
	path := base + "/" + created.Account.ID
	patch := `{"expected_account_revision":1,"config":{"receive_mode":"webhook"}}`
	deploymentRequest(t, router, "PATCH", path, member, "mode-member", patch, 403, nil)
	var changed channelapp.CommandResult
	deploymentRequest(t, router, "PATCH", path, owner, "mode-http-change", patch, 200, &changed)
	if changed.Account.Revision != 2 || changed.Account.ConnectionRevision != 2 || changed.Account.Enabled {
		t.Fatal("mode mutation must only save disabled config")
	}
	deploymentRequest(t, router, "POST", path+"/enabled", owner, "mode-required", `{"expected_account_revision":2,"enabled":true}`, 422, nil)
	deploymentRequest(t, router, "PATCH", path, owner, "mode-stale", patch, 409, nil)
	legacyBody := `{"provider":"telegram","provider_account_id":"654321002","name":"Legacy fixture","credentials":{"telegram.bot_token":{"action":"replace","value":"TEST_ONLY_LEGACY_TOKEN"},"telegram.webhook_secret":{"action":"replace","value":"TEST_ONLY_LEGACY_SECRET"}}}`
	legacyCall := func(header []string, raw, key string, want int) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest("POST", base, strings.NewReader(raw))
		req.AddCookie(owner)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", key)
		for _, v := range header {
			req.Header.Add("X-Channel-Create-Contract", v)
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != want {
			t.Fatalf("legacy status=%d want=%d", w.Code, want)
		}
		return w
	}
	for i, h := range [][]string{{"invalid"}, {"webhook-v1", "webhook-v1"}} {
		legacyCall(h, legacyBody, fmt.Sprintf("bad-legacy-%d", i), 400)
	}
	w = legacyCall([]string{"webhook-v1"}, legacyBody, "legacy-recovery", 201)
	var legacy channelapp.CommandResult
	if json.Unmarshal(w.Body.Bytes(), &legacy) != nil || legacy.Account.Config.ReceiveMode != "webhook" {
		t.Fatal("legacy pending intent lost")
	}
	// Mimic exactly the historical stored response shape, while retaining its
	// legacy input MAC. Replay must not substitute current account config.
	if _, err := pool.Exec(ctx, `UPDATE channel_command_receipts SET result_jsonb=result_jsonb#-'{account,config,receive_mode}' WHERE tenant_id=$1 AND result_jsonb->'account'->>'account_id'=$2`, tenant, legacy.Account.ID); err != nil {
		t.Fatal(err)
	}
	w = legacyCall([]string{"webhook-v1"}, legacyBody, "legacy-recovery", 201)
	if w.Header().Get("X-Channel-Result-Contract") != "webhook-v1" || strings.Contains(w.Body.String(), "receive_mode") {
		t.Fatal("legacy receipt normalized")
	}
	legacyCall(nil, legacyBody, "legacy-recovery", 409)
	legacyCall([]string{"webhook-v1"}, strings.Replace(legacyBody, `"name":`, `"config":{"receive_mode":"webhook"},"name":`, 1), "bad-config-legacy", 400)
}
