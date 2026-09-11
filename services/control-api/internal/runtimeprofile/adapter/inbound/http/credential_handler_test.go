package httpadapter_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	identityapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

const credentialDraftBody = `{"expected_draft_revision":1,"credential_protocol_version":"v1","config":{"models":{"primary":{"kind":"openai_compatible","model":"test","base_url":"https://model.example.test/v1","capabilities":["chat"]}},"tools":{},"knowledge":{},"storage":{}},"credentials":{"models":{"primary":{"api_key":{"action":"replace","value":"private-http-canary"}}}}}`

func TestCredentialDraftHTTPUsesStrictWriteDTOAndNeverEchoesSecrets(t *testing.T) {
	service := &runtimeProfileServiceStub{}
	router := authenticatedRouter(service, identityapp.IdentityContext{UserID: "usr_1"})
	for _, action := range []string{"replace", "keep", "clear"} {
		t.Run(action, func(t *testing.T) {
			body := credentialDraftBody
			if action != "replace" {
				body = strings.Replace(body, `"action":"replace","value":"private-http-canary"`, `"action":"`+action+`"`, 1)
			}
			recorder := performCredentialJSON(router, http.MethodPut, "/v1/tenants/tnt_1/runtime-profiles/rpf_1/draft", body, "write-"+action)
			if recorder.Code != http.StatusOK {
				t.Fatalf("status/body = %d/%s", recorder.Code, recorder.Body.String())
			}
			cmd := service.saveCommand
			if cmd.ActorUserID != "usr_1" || cmd.TenantID != "tnt_1" || cmd.ProfileID != "rpf_1" || cmd.IdempotencyKey != "write-"+action || cmd.Write.Credentials["models"]["primary"]["api_key"].Action != action {
				t.Fatal("write command boundary lost identity, key, or action")
			}
			if strings.Contains(recorder.Body.String(), "private-http-canary") || strings.Contains(recorder.Body.String(), "credential_id") || strings.Contains(recorder.Body.String(), "config") {
				t.Fatal("write receipt exposed secret or mutable read view")
			}
		})
	}
}
func TestCredentialDraftHTTPRejectsOldDTOInjectionAndMissingIdempotency(t *testing.T) {
	service := &runtimeProfileServiceStub{}
	router := authenticatedRouter(service, identityapp.IdentityContext{UserID: "usr_1"})
	cases := []struct{ body, key string }{
		{credentialDraftBody, ""}, {`{"expected_revision":1,"spec":{}}`, "old"},
		{strings.Replace(credentialDraftBody, `"model":"test"`, `"model":"test","api_key_credential_id":"crd_0123456789abcdef0123456789abcdef"`, 1), "injected"},
		{strings.Replace(credentialDraftBody, `"model":"test"`, `"model":"test","api_key_ref":"legacy"`, 1), "ref"},
		{strings.Replace(credentialDraftBody, `"value":"private-http-canary"`, `"value":"private-http-canary","value":"second-secret"`, 1), "duplicate"},
		{strings.Replace(credentialDraftBody, `"action":"replace"`, `"Action":"replace"`, 1), "case"},
	}
	for _, tt := range cases {
		recorder := performCredentialJSON(router, http.MethodPut, "/v1/tenants/tnt_1/runtime-profiles/rpf_1/draft", tt.body, tt.key)
		if recorder.Code != http.StatusBadRequest {
			t.Errorf("invalid input status=%d", recorder.Code)
		}
		if strings.Contains(recorder.Body.String(), "private-http-canary") || strings.Contains(recorder.Body.String(), "second-secret") {
			t.Fatal("error echoed credential")
		}
	}
	if service.saveCalls != 0 {
		t.Fatal("invalid input reached application")
	}
}
func TestUsedCredentialUpdateHTTPAndErrorMapping(t *testing.T) {
	body := `{"target":{"profile_revision_number":1,"category":"models","resource_name":"primary","purpose_field":"api_key","association_token":"` + strings.Repeat("a", 64) + `"},"action":"replace","expected_credential_revision":1,"value":"private-http-canary"}`
	for _, tt := range []struct {
		err    error
		status int
	}{{nil, 200}, {domain.ErrCredentialInput, 400}, {domain.ErrCredentialAssociation, 409}, {domain.ErrCredentialConflict, 409}, {domain.ErrCredentialUnavailable, 409}, {application.ErrCredentialIdempotencyConflict, 409}, {application.ErrTenantForbidden, 403}} {
		service := &runtimeProfileServiceStub{credentialErr: tt.err}
		router := authenticatedRouter(service, identityapp.IdentityContext{UserID: "usr_1"})
		recorder := performCredentialJSON(router, http.MethodPost, "/v1/tenants/tnt_1/runtime-profiles/rpf_1/credentials/update", body, "rotate-key")
		if recorder.Code != tt.status {
			t.Errorf("error %v status=%d body=%s", tt.err, recorder.Code, recorder.Body.String())
		}
		if service.updateCommand.IdempotencyKey != "rotate-key" || service.updateCommand.ActorUserID != "usr_1" {
			t.Fatal("update command identity/key lost")
		}
		if strings.Contains(recorder.Body.String(), "private-http-canary") {
			t.Fatal("update response echoed secret")
		}
	}
}
func TestCredentialReadCreateContainsEmptyConfigNotInternalSpec(t *testing.T) {
	service := &runtimeProfileServiceStub{createResult: application.CreateRuntimeProfileResult{Profile: domain.RuntimeProfile{ID: "rpf_1", TenantID: "tnt_1"}, Draft: domain.ProfileDraft{ProfileID: "rpf_1", TenantID: "tnt_1", Revision: 1, Spec: json.RawMessage(`{}`)}}}
	router := authenticatedRouter(service, identityapp.IdentityContext{UserID: "usr_1"})
	r := performJSON(router, http.MethodPost, "/v1/tenants/tnt_1/runtime-profiles", `{"name":"test"}`)
	var body struct {
		Draft map[string]json.RawMessage `json:"draft"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if r.Code != 201 || body.Draft["config"] == nil || body.Draft["spec"] != nil {
		t.Fatalf("create response=%s", r.Body.String())
	}
	var config map[string]map[string]any
	if err := json.Unmarshal(body.Draft["config"], &config); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"models", "tools", "knowledge", "storage"} {
		if config[key] == nil || len(config[key]) != 0 {
			t.Fatal("initial config collections must be empty objects")
		}
	}
}
func performCredentialJSON(router http.Handler, method, path, body, key string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	return w
}

func TestCredentialActionRouteRejectsOtherActionSuffixes(t *testing.T) {
	service := &runtimeProfileServiceStub{}
	router := authenticatedRouter(service, identityapp.IdentityContext{UserID: "usr_1"})
	body := `{"target":{"profile_revision_number":1,"category":"models","resource_name":"primary","purpose_field":"api_key","association_token":"` + strings.Repeat("a", 64) + `"},"action":"clear","expected_credential_revision":1}`
	for _, suffix := range []string{"credentials:delete", "credentialsother", "credentials/updateextra"} {
		r := performCredentialJSON(router, http.MethodPost, "/v1/tenants/tnt_1/runtime-profiles/rpf_1/"+suffix, body, "key")
		if r.Code != http.StatusNotFound {
			t.Errorf("unknown action suffix %s returned %d", suffix, r.Code)
		}
	}
	if service.updateCalls != 0 {
		t.Fatal("unknown route dispatched a credential mutation")
	}
	r := performCredentialJSON(router, http.MethodPost, "/v1/tenants/tnt_1/runtime-profiles/rpf_1/credentials/update", body, "clear-key")
	if r.Code != http.StatusOK || service.updateCommand.Update.Action != "clear" || service.updateCommand.Update.Value != nil {
		t.Fatal("valid clear was not passed through without value")
	}
}

func TestCredentialHTTPReadsBoundedJSONWithoutDecoderBypass(t *testing.T) {
	service := &runtimeProfileServiceStub{}
	router := authenticatedRouter(service, identityapp.IdentityContext{UserID: "usr_1"})
	for _, ct := range []string{"text/plain", "application/json-invalid", ""} {
		r := httptest.NewRequest(http.MethodPut, "/v1/tenants/tnt_1/runtime-profiles/rpf_1/draft", strings.NewReader(credentialDraftBody))
		r.Header.Set("Content-Type", ct)
		r.Header.Set("Idempotency-Key", "bounded-key")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		if w.Code != 400 {
			t.Errorf("unsupported Content-Type %q accepted", ct)
		}
	}
	r := performCredentialJSON(router, http.MethodPut, "/v1/tenants/tnt_1/runtime-profiles/rpf_1/draft", credentialDraftBody+strings.Repeat(" ", domain.MaxDocumentBytes), "large")
	if r.Code != 400 || service.saveCalls != 0 {
		t.Fatal("oversized secret-bearing request crossed the HTTP boundary")
	}
}

func TestCredentialUpdateHTTPRejectsInvalidTargetBeforeApplication(t *testing.T) {
	valid := `{"target":{"profile_revision_number":1,"category":"models","resource_name":"primary","purpose_field":"api_key","association_token":"` + strings.Repeat("a", 64) + `"},"action":"clear","expected_credential_revision":1}`
	cases := []struct{ name, body string }{
		{"missing category", strings.Replace(valid, `"category":"models",`, "", 1)},
		{"unknown category", strings.Replace(valid, `"category":"models"`, `"category":"secrets"`, 1)},
		{"missing purpose", strings.Replace(valid, `"purpose_field":"api_key",`, "", 1)},
		{"unknown purpose", strings.Replace(valid, `"purpose_field":"api_key"`, `"purpose_field":"password"`, 1)},
		{"non-hex token", strings.Replace(valid, strings.Repeat("a", 64), strings.Repeat("z", 64), 1)},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			service := &runtimeProfileServiceStub{}
			router := authenticatedRouter(service, identityapp.IdentityContext{UserID: "usr_1"})
			r := performCredentialJSON(router, http.MethodPost, "/v1/tenants/tnt_1/runtime-profiles/rpf_1/credentials/update", test.body, "invalid-target")
			if r.Code != http.StatusBadRequest || service.updateCalls != 0 {
				t.Fatalf("invalid target crossed input boundary: status=%d application calls=%d", r.Code, service.updateCalls)
			}
		})
	}
}
