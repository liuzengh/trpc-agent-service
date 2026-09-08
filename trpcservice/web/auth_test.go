package web

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
)

const testAPIToken = "test-http-client-not-a-real-secret-0123456789"

func testAccess(t *testing.T) *APIAccess {
	t.Helper()
	access, err := NewAPIAccess(config.HTTPAPIConfig{Enabled: true, Principals: []config.HTTPAPIPrincipal{{
		Name: "alice-client", Token: testAPIToken, TenantID: "tutorial-tenant",
		BindingKeys: []string{"tutorial-http"}, UserIDs: []string{"alice"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return access
}

func TestHTTPAPIDefaultDisabledAndRoleSurface(t *testing.T) {
	for _, access := range []*APIAccess{nil, {}} {
		handler := NewHandler(&fakeChatService{}, WithAPIAccess(access))
		for _, path := range []string{"/chat", "/inbound", "/callbacks/telegram/unknown", "/admin/tenants/get"} {
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
			if rec.Code != http.StatusNotFound {
				t.Fatalf("%s: %d", path, rec.Code)
			}
		}
	}
	handler := NewHandler(&fakeChatService{}, WithAPIAccess(testAccess(t)), WithSynchronousChat(false))
	for _, tc := range []struct {
		path   string
		status int
	}{
		{"/chat", 404}, {"/inbound", 401}, {"/healthz", 200}, {"/readyz", 200},
	} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if rec.Code != tc.status {
			t.Fatalf("%s: %d", tc.path, rec.Code)
		}
	}
}

func TestHTTPAPIAuthorizesBeforeExecutionOrPersistence(t *testing.T) {
	for _, endpoint := range []string{"/chat", "/inbound"} {
		for _, tc := range []struct {
			name, auth, binding, user, tenant, channel string
			duplicateHeader                            bool
			status                                     int
		}{
			{name: "missing token", status: 401},
			{name: "wrong token", auth: "Bearer wrong", status: 401},
			{name: "basic auth", auth: "Basic " + testAPIToken, status: 401},
			{name: "multiple headers", auth: "Bearer " + testAPIToken, duplicateHeader: true, status: 401},
			{name: "other binding", auth: "Bearer " + testAPIToken, binding: "foreign", status: 403},
			{name: "impersonate user", auth: "Bearer " + testAPIToken, user: "bob", status: 403},
			{name: "binding reassigned tenant", auth: "Bearer " + testAPIToken, tenant: "foreign-tenant", status: 403},
			{name: "forge Telegram input", auth: "Bearer " + testAPIToken, channel: "telegram", status: 403},
			{name: "allowed", auth: "bearer " + testAPIToken},
		} {
			t.Run(endpoint+"/"+tc.name, func(t *testing.T) {
				scope := runtimecontext.TutorialScope()
				if tc.tenant != "" {
					scope.TenantID = tc.tenant
					scope.StorageScope = "t/" + tc.tenant + "/a/" + scope.AppID
				}
				if tc.channel != "" {
					scope.ChannelType = tc.channel
				}
				resolver := fakeRouteResolver{scope: scope}
				journal := gateway.NewMemoryJournal()
				defer func(closer interface{ Close() error }) { _ = closer.Close() }(journal)
				intake, err := gateway.NewIntake(resolver, journal)
				if err != nil {
					t.Fatal(err)
				}
				service := &fakeChatService{}
				handler := NewHandler(service, WithAPIAccess(testAccess(t)), WithRouteResolver(resolver), WithGatewayIntake(intake))
				binding, user := tc.binding, tc.user
				if binding == "" {
					binding = "tutorial-http"
				}
				if user == "" {
					user = "alice"
				}
				body := `{"binding_key":"` + binding + `","user_id":"` + user + `","message_id":"test-message","session_id":"demo","message":"hello"}`
				req := httptest.NewRequest(http.MethodPost, endpoint+"?token="+testAPIToken, bytes.NewBufferString(body))
				req.RemoteAddr = "127.0.0.1:12345" // A tunnel must not grant implicit loopback access.
				req.Header.Set("X-Forwarded-For", "127.0.0.1")
				if tc.auth != "" {
					req.Header.Set("Authorization", tc.auth)
				}
				if tc.duplicateHeader {
					req.Header.Add("Authorization", tc.auth)
				}
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				want := tc.status
				if want == 0 {
					want = 200
					if endpoint == "/inbound" {
						want = 202
					}
				}
				if rec.Code != want {
					t.Fatalf("status=%d want=%d body=%s", rec.Code, want, rec.Body.String())
				}
				if want >= 400 && (len(journal.Tasks()) != 0 || service.input.MessageID != "") {
					t.Fatal("unauthorized side effect")
				}
				if want < 300 && endpoint == "/inbound" && len(journal.Tasks()) != 1 {
					t.Fatal("authorized message not persisted")
				}
				if strings.Contains(rec.Body.String(), testAPIToken) {
					t.Fatal("token leaked into response")
				}
			})
		}
	}
}
