package web_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestAdminCreateBindingOutboundConfig(t *testing.T) {
	mux, pool := adminTestAPI(t)
	ctx := context.Background()

	tenantID := createTenantFor(t, mux, uniqueName("binding-cfg"))
	code, out := doJSON(t, mux, http.MethodPost, "/admin/tenants/"+tenantID+"/apps",
		`{"name":"bot","agent_type":"llm","config":{"prompt":"p"}}`)
	wantCode(t, code, http.StatusCreated, out)
	appID, _ := out["id"].(string)
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM channel_binding WHERE tenant_id = $1`, tenantID)
		_, _ = pool.Exec(ctx, `DELETE FROM agent_app WHERE tenant_id = $1`, tenantID)
		_, _ = pool.Exec(ctx, `DELETE FROM tenant WHERE id = $1`, tenantID)
	})
	code, out = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appID+"/publish", "")
	wantCode(t, code, http.StatusOK, out)

	bindPath := "/admin/apps/" + appID + "/bindings"
	// The webhook-channel cases carry no webhook_path: createBinding refuses a
	// path no route serves, so each one is auto-filled with its own
	// /callback/{channel}/{binding_id} and stays unique. The wecomws cases do
	// supply one — there the path is the routing key.
	for _, tc := range []struct {
		name    string
		body    string
		want    int
		wantMsg string
	}{
		{
			name: "wecom full outbound identity",
			body: `{"channel":"wecom",` +
				`"config":{"corp_id":"corpB","agent_id":2000003,"secret_ref":"secret-b"}}`,
			want: http.StatusCreated,
		},
		{
			name: "wecom empty config falls back to env identity",
			body: `{"channel":"wecom","config":{}}`,
			want: http.StatusCreated,
		},
		{
			name: "wecom without config",
			body: `{"channel":"wecom"}`,
			want: http.StatusCreated,
		},
		{
			name:    "wecom unknown field",
			body:    `{"channel":"wecom","config":{"secret":"plaintext"}}`,
			want:    http.StatusBadRequest,
			wantMsg: "corp_id, agent_id and secret_ref",
		},
		{
			name:    "wecom wrong type",
			body:    `{"channel":"wecom","config":{"agent_id":"nope"}}`,
			want:    http.StatusBadRequest,
			wantMsg: "corp_id, agent_id and secret_ref",
		},
		{
			name: "wxkf full outbound identity",
			body: `{"channel":"wxkf",` +
				`"config":{"corp_id":"corpB","kf_account":"wkBINDING01","secret_ref":"secret-b"}}`,
			want: http.StatusCreated,
		},
		{
			name:    "wxkf unknown field",
			body:    `{"channel":"wxkf","config":{"secret":"plaintext"}}`,
			want:    http.StatusBadRequest,
			wantMsg: "corp_id, kf_account and secret_ref",
		},
		{
			name:    "wxkf wrong type",
			body:    `{"channel":"wxkf","config":{"corp_id":42}}`,
			want:    http.StatusBadRequest,
			wantMsg: "corp_id, kf_account and secret_ref",
		},
		{
			// The gate is per-channel: a channel without an outbound
			// identity schema keeps accepting arbitrary config.
			name: "other channel config untouched",
			body: `{"channel":"mock","config":{"anything":true}}`,
			want: http.StatusCreated,
		},
		{
			name: "wecomws valid shape",
			body: `{"channel":"wecomws","webhook_path":"/wecomws/bot1",` +
				`"config":{"bot_id":"bot1","secret_ref":"ws-secret"}}`,
			want: http.StatusCreated,
		},
		{
			name:    "wecomws malformed path",
			body:    `{"channel":"wecomws","webhook_path":"/callback/wecomws/bot2"}`,
			want:    http.StatusBadRequest,
			wantMsg: "webhook_path must match /wecomws/{bot_id}",
		},
		{
			// The path suffix is the routing key the bot's inbound messages
			// carry: a mismatch resolves every message to the wrong binding
			// and silently blackholes it (WS has no IM redelivery).
			name: "wecomws path bot_id mismatch",
			body: `{"channel":"wecomws","webhook_path":"/wecomws/bot3",` +
				`"config":{"bot_id":"bot4","secret_ref":"ws-secret"}}`,
			want:    http.StatusBadRequest,
			wantMsg: "must equal config bot_id",
		},
		{
			name: "wecomws unknown field",
			body: `{"channel":"wecomws","webhook_path":"/wecomws/bot5",` +
				`"config":{"bot_id":"bot5","secret_ref":"ws-secret","secret":"plaintext"}}`,
			want:    http.StatusBadRequest,
			wantMsg: "only accepts bot_id and secret_ref",
		},
		{
			name: "wecomws callback refs refused",
			body: `{"channel":"wecomws","webhook_path":"/wecomws/bot6","token_ref":"tok",` +
				`"config":{"bot_id":"bot6","secret_ref":"ws-secret"}}`,
			want:    http.StatusBadRequest,
			wantMsg: "must leave token_ref/aeskey_ref empty",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, out := doJSON(t, mux, http.MethodPost, bindPath, tc.body)
			wantCode(t, code, tc.want, out)
			if tc.wantMsg != "" {
				msg, _ := out["error"].(string)
				if !strings.Contains(msg, tc.wantMsg) {
					t.Fatalf("error %q must name the accepted fields", msg)
				}
				return
			}
			// The row must carry the config verbatim: the adapter reads its
			// outbound identity from exactly this jsonb.
			id, _ := out["id"].(string)
			var stored json.RawMessage
			if err := pool.QueryRow(ctx,
				`SELECT COALESCE(config, '{}'::jsonb) FROM channel_binding WHERE id = $1`, id,
			).Scan(&stored); err != nil {
				t.Fatal(err)
			}
			var want map[string]any
			if err := json.Unmarshal(stored, &want); err != nil {
				t.Fatalf("stored config %s: %v", stored, err)
			}
			var sent struct {
				Config map[string]any `json:"config"`
			}
			if err := json.Unmarshal([]byte(tc.body), &sent); err != nil {
				t.Fatal(err)
			}
			if len(want) != len(sent.Config) {
				t.Fatalf("stored config %v, want %v", want, sent.Config)
			}
			for k, v := range sent.Config {
				if got, ok := want[k]; !ok || !jsonEqual(got, v) {
					t.Fatalf("stored config %v, want %v", want, sent.Config)
				}
			}
		})
	}
}

// jsonEqual compares two decoded JSON scalars (numbers arrive as float64).
func jsonEqual(a, b any) bool {
	ax, _ := json.Marshal(a)
	bx, _ := json.Marshal(b)
	return string(ax) == string(bx)
}
