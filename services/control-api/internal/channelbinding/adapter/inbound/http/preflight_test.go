package httpadapter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	channelv1 "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
	identityapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
)

type preflightServiceFake struct {
	creates, gets    int
	actor            application.Actor
	account, id, key string
	input            channelv1.PreflightCreateRequest
	err              error
}

func (s *preflightServiceFake) Create(_ context.Context, a application.Actor, account, key string, in channelv1.PreflightCreateRequest) (channelv1.PreflightCreated, error) {
	s.creates++
	s.actor = a
	s.account = account
	s.key = key
	s.input = in
	now := time.Date(2026, 9, 6, 4, 0, 0, 0, time.UTC)
	return channelv1.PreflightCreated{PreflightID: "cpf_test", TenantID: a.TenantID, AccountID: account, RequestedAt: now, JobDeadlineAt: now.Add(120 * time.Second), StatusURL: "/v1/tenants/" + a.TenantID + "/channel-accounts/" + account + "/preflights/cpf_test"}, s.err
}
func (s *preflightServiceFake) Get(_ context.Context, a application.Actor, account, id string) (channelv1.PreflightView, error) {
	s.gets++
	s.actor = a
	s.account = account
	s.id = id
	return channelv1.PreflightView{PreflightID: id, AccountID: account, TenantID: a.TenantID, Checks: []channelv1.PreflightCheck{}}, s.err
}
func preflightRouter(s *preflightServiceFake, identity *identityapp.IdentityContext, readers ...PreflightAccountReader) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	if identity != nil {
		r.Use(func(c *gin.Context) {
			c.Request = c.Request.WithContext(identityapp.WithIdentity(c.Request.Context(), *identity))
		})
	}
	NewPreflightHandler(s, readers...).Register(r)
	return r
}

const preflightCreateBody = `{"expected_account_revision":7,"expected_connection_revision":4,"expected_bot_token_version":2}`
const preflightPublicBase = "/v1/tenants/tnt_test/channel-accounts/cha_test/preflights"

func preflightRequest(r http.Handler, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "create-preflight-1")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}
func TestPreflightCreateReturnsImmutableAcceptedReceipt(t *testing.T) {
	s := &preflightServiceFake{}
	r := preflightRouter(s, &identityapp.IdentityContext{UserID: "usr_owner"})
	w := preflightRequest(r, http.MethodPost, preflightPublicBase, preflightCreateBody)
	if w.Code != 202 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body)
	}
	if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Retry-After") != "2" || w.Header().Get("Location") != preflightPublicBase+"/cpf_test" {
		t.Fatalf("headers=%v", w.Header())
	}
	var got channelv1.PreflightCreated
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || got.PreflightID != "cpf_test" {
		t.Fatalf("result=%+v err=%v", got, err)
	}
	if s.creates != 1 || s.actor != (application.Actor{TenantID: "tnt_test", UserID: "usr_owner"}) || s.account != "cha_test" || s.key != "create-preflight-1" || s.input.ExpectedConnectionRevision != 4 {
		t.Fatalf("service=%+v", s)
	}
}

type preflightAccountReaderFake struct {
	calls int
	err   error
}

func (s *preflightAccountReaderFake) GetAccount(context.Context, application.Actor, string) (application.AccountDetails, error) {
	s.calls++
	return application.AccountDetails{}, s.err
}
func TestPreflightPublicAuthenticationAndVisibilityPrecedeDecode(t *testing.T) {
	for _, tc := range []struct {
		name      string
		identity  *identityapp.IdentityContext
		readerErr error
		want      int
	}{
		{name: "unauthenticated", want: 401},
		{name: "restricted", identity: &identityapp.IdentityContext{UserID: "usr_owner", Restricted: true}, want: 403},
		{name: "other tenant account", identity: &identityapp.IdentityContext{UserID: "usr_owner"}, readerErr: application.ErrAccountNotFound, want: 404},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &preflightServiceFake{}
			q := &preflightAccountReaderFake{err: tc.readerErr}
			w := preflightRequest(preflightRouter(s, tc.identity, q), http.MethodPost, preflightPublicBase, "not-json")
			if w.Code != tc.want || s.creates != 0 || w.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("status=%d calls=%d headers=%v", w.Code, s.creates, w.Header())
			}
		})
	}
}
func TestPreflightPublicRejectsInvalidInputBeforeApplication(t *testing.T) {
	for _, tc := range []struct {
		name, path, body, key, contentType string
		want                               int
	}{
		{name: "extra url", body: `{"expected_account_revision":7,"expected_connection_revision":4,"expected_bot_token_version":2,"url":"https://example.org"}`, want: 400},
		{name: "duplicate key", body: `{"expected_account_revision":7,"expected_account_revision":7,"expected_connection_revision":4,"expected_bot_token_version":2}`, want: 400},
		{name: "tail json", body: preflightCreateBody + `{}`, want: 400},
		{name: "fraction", body: `{"expected_account_revision":7.5,"expected_connection_revision":4,"expected_bot_token_version":2}`, want: 400},
		{name: "unsafe integer", body: `{"expected_account_revision":9007199254740992,"expected_connection_revision":4,"expected_bot_token_version":2}`, want: 400},
		{name: "missing version", body: `{"expected_account_revision":7,"expected_connection_revision":4}`, want: 400},
		{name: "query", path: preflightPublicBase + "?url=x", body: preflightCreateBody, want: 400},
		{name: "missing key", body: preflightCreateBody, key: "-missing", want: 400},
		{name: "invalid key", body: preflightCreateBody, key: "contains space", want: 400},
		{name: "wrong media", body: preflightCreateBody, contentType: "text/plain", want: 400},
		{name: "oversized", body: strings.Repeat(" ", 4097), want: 413},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &preflightServiceFake{}
			r := preflightRouter(s, &identityapp.IdentityContext{UserID: "usr_owner"})
			path := tc.path
			if path == "" {
				path = preflightPublicBase
			}
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(tc.body))
			media := tc.contentType
			if media == "" {
				media = "application/json"
			}
			req.Header.Set("Content-Type", media)
			key := tc.key
			if key == "" {
				key = "fixture-key"
			}
			if key != "-missing" {
				req.Header.Set("Idempotency-Key", key)
			}
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			if w.Code != tc.want || s.creates != 0 {
				t.Fatalf("status=%d calls=%d body=%s", w.Code, s.creates, w.Body)
			}
		})
	}
}
func TestPreflightPublicErrorMappingNeverLeaksDependencyText(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want int
		code string
	}{
		{"member create", application.ErrPermissionDenied, 403, "CHANNEL_PERMISSION_DENIED"},
		{"hidden account", application.ErrAccountNotFound, 404, "CHANNEL_ACCOUNT_NOT_FOUND"},
		{"missing preflight", &domain.Error{Code: "CHANNEL_PREFLIGHT_NOT_FOUND"}, 404, "CHANNEL_PREFLIGHT_NOT_FOUND"},
		{"rate", &domain.Error{Code: "CHANNEL_PREFLIGHT_RATE_LIMITED"}, 429, "CHANNEL_PREFLIGHT_RATE_LIMITED"},
		{"provider", &domain.Error{Code: "CHANNEL_PREFLIGHT_PROVIDER_UNSUPPORTED"}, 422, "CHANNEL_PREFLIGHT_PROVIDER_UNSUPPORTED"},
		{"connection consent", &domain.Error{Code: "CHANNEL_PREFLIGHT_CONNECTION_PROBE_CONFIRMATION_REQUIRED"}, 422, "CHANNEL_PREFLIGHT_CONNECTION_PROBE_CONFIRMATION_REQUIRED"},
		{"revision", &domain.Error{Code: domain.RevisionConflict}, 409, domain.RevisionConflict},
		{"dependency", errors.New("SECRET_PROVIDER_RAW_ERROR"), 503, "CHANNEL_DEPENDENCY_UNAVAILABLE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &preflightServiceFake{err: tc.err}
			w := preflightRequest(preflightRouter(s, &identityapp.IdentityContext{UserID: "usr_test"}), http.MethodPost, preflightPublicBase, preflightCreateBody)
			if w.Code != tc.want || !strings.Contains(w.Body.String(), tc.code) || strings.Contains(w.Body.String(), "SECRET_PROVIDER_RAW_ERROR") {
				t.Fatalf("status=%d body=%s", w.Code, w.Body)
			}
			if tc.want == 429 && w.Header().Get("Retry-After") == "" {
				t.Fatal("no retry-after")
			}
		})
	}
}
func TestPreflightReadPassesTenantAccountAndTaskToAuthorizedQuery(t *testing.T) {
	s := &preflightServiceFake{}
	r := preflightRouter(s, &identityapp.IdentityContext{UserID: "usr_member"})
	w := preflightRequest(r, http.MethodGet, preflightPublicBase+"/cpf_test", "")
	if w.Code != 200 || s.gets != 1 || s.id != "cpf_test" || s.actor.UserID != "usr_member" || w.Header().Get("Cache-Control") != "no-store" || !strings.HasPrefix(w.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("status=%d service=%+v headers=%v", w.Code, s, w.Header())
	}
	for _, path := range []string{preflightPublicBase + "/cpf_test?latest=true", preflightPublicBase + "/_invalid"} {
		if got := preflightRequest(r, http.MethodGet, path, ""); got.Code != 400 {
			t.Fatalf("path=%s status=%d", path, got.Code)
		}
	}
	if got := preflightRequest(r, http.MethodGet, preflightPublicBase+"/cpf_test", `{}`); got.Code != 400 {
		t.Fatalf("GET body status=%d", got.Code)
	}
}

func TestPreflightHiddenAccountReturnsNotFoundBeforeInputChecks(t *testing.T) {
	for _, tc := range []struct{ name, method, path, body string }{
		{"create-valid", "POST", preflightPublicBase, preflightCreateBody},
		{"create-malformed", "POST", preflightPublicBase, "{malformed"},
		{"create-query", "POST", preflightPublicBase + "?extra=true", preflightCreateBody},
		{"get-valid", "GET", preflightPublicBase + "/cpf_test", ""},
		{"get-invalid-id", "GET", preflightPublicBase + "/_invalid", ""},
		{"get-body", "GET", preflightPublicBase + "/cpf_test", "{malformed"},
		{"get-query", "GET", preflightPublicBase + "/cpf_test?extra=true", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := &preflightServiceFake{}
			reader := &preflightAccountReaderFake{err: application.ErrPermissionDenied}
			response := preflightRequest(preflightRouter(service, &identityapp.IdentityContext{UserID: "usr_outsider"}, reader), tc.method, tc.path, tc.body)
			if response.Code != 404 || !strings.Contains(response.Body.String(), "CHANNEL_ACCOUNT_NOT_FOUND") || service.creates != 0 || service.gets != 0 {
				t.Fatalf("status=%d creates=%d gets=%d body=%s", response.Code, service.creates, service.gets, response.Body)
			}
		})
	}
}

func TestPreflightVisibleAccountPermissionFailureRemainsForbidden(t *testing.T) {
	service := &preflightServiceFake{err: application.ErrPermissionDenied}
	reader := &preflightAccountReaderFake{}
	response := preflightRequest(preflightRouter(service, &identityapp.IdentityContext{UserID: "usr_member"}, reader), "POST", preflightPublicBase, preflightCreateBody)
	if response.Code != 403 || service.creates != 1 || reader.calls != 1 {
		t.Fatalf("status=%d calls=%d visibility=%d", response.Code, service.creates, reader.calls)
	}
}

func TestWeComPreflightCreateTransportsExplicitConsentAndSecretVersion(t *testing.T) {
	s := &preflightServiceFake{}
	r := preflightRouter(s, &identityapp.IdentityContext{UserID: "usr_owner"})
	w := preflightRequest(r, http.MethodPost, preflightPublicBase, `{"expected_account_revision":7,"expected_connection_revision":4,"expected_bot_secret_version":2,"allow_connection_probe":true}`)
	if w.Code != 202 || !s.input.AllowConnectionProbe || s.input.ExpectedBotSecretVersion != 2 || s.input.ExpectedBotTokenVersion != 0 {
		t.Fatal("WeCom DTO", w.Code)
	}
}
