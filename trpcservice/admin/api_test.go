package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/app/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	backendmemory "github.com/XnLemon/trpc-agent-service/trpcservice/backend/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	channelmemory "github.com/XnLemon/trpc-agent-service/trpcservice/channels/inmemory"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelmemory "github.com/XnLemon/trpc-agent-service/trpcservice/model/inmemory"
	storagemysql "github.com/XnLemon/trpc-agent-service/trpcservice/storage/mysql"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	tenantmemory "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/inmemory"
)

type adminAuditWriter struct{ events []audit.Event }

func (w *adminAuditWriter) Append(_ context.Context, event audit.Event) (audit.AppendResult, error) {
	w.events = append(w.events, event)
	return audit.AppendResult{Event: event}, nil
}

type adminAuditFailWriter struct{}

func (adminAuditFailWriter) Append(context.Context, audit.Event) (audit.AppendResult, error) {
	return audit.AppendResult{}, errors.New("audit down")
}

func TestRecordMutationWritesControlPlaneAudit(t *testing.T) {
	w := &adminAuditWriter{}
	h := &Handler{config: Config{AuditWriter: w}}
	previous, next := int64(1), int64(2)
	value := map[string]any{"event": channels.ChangeEvent{EventType: channels.EventConfigurationUpdated, TenantID: "tenant-a", ActorType: "admin", ActorID: "actor", Reason: "change", CorrelationID: "corr", PreviousVersion: previous, NextVersion: next, OccurredAt: time.Now().UTC()}}
	if err := h.recordMutation(context.Background(), Principal{}, "request", value); err != nil {
		t.Fatal(err)
	}
	if len(w.events) != 1 || w.events[0].EventType != audit.EventControlPlaneChanged || w.events[0].TenantID != "tenant-a" {
		t.Fatalf("events = %#v", w.events)
	}
}

func TestRecordMutationAuditsRawResourceMutation(t *testing.T) {
	w := &adminAuditWriter{}
	h := &Handler{config: Config{AuditWriter: w}}
	resource := tenant.Tenant{TenantID: "tenant-a", Version: 1}
	if err := h.recordMutation(context.Background(), Principal{SubjectID: "admin-1"}, "request-raw", &resource); err != nil {
		t.Fatal(err)
	}
	if len(w.events) != 1 || w.events[0].CorrelationID != "request-raw" || w.events[0].PreviousVersion == nil || *w.events[0].PreviousVersion != 0 || *w.events[0].NextVersion != 1 {
		t.Fatalf("raw audit event = %#v", w.events)
	}
}

func TestRecordMutationUsesDraftVersionForRawRevision(t *testing.T) {
	w := &adminAuditWriter{}
	h := &Handler{config: Config{AuditWriter: w}}
	revision := appmodel.Revision{TenantID: "tenant-a", DraftVersion: 3, Revision: 7}
	if err := h.recordMutation(context.Background(), Principal{SubjectID: "admin-1"}, "request-draft", &revision); err != nil {
		t.Fatal(err)
	}
	if len(w.events) != 1 || *w.events[0].PreviousVersion != 2 || *w.events[0].NextVersion != 3 {
		t.Fatalf("draft audit event = %#v", w.events)
	}
}

func TestRecordMutationReflectionAndFailureBranches(t *testing.T) {
	if err := (*Handler)(nil).recordMutation(context.Background(), Principal{}, "req", nil); err != nil {
		t.Fatal(err)
	}
	h := &Handler{config: Config{AuditWriter: &adminAuditWriter{}}}
	for _, value := range []any{map[string]any{"event": nil}, map[string]any{"event": "not-struct"}, (*tenant.Tenant)(nil), 42} {
		if err := h.recordMutation(context.Background(), Principal{}, "req", value); err != nil {
			t.Fatal(err)
		}
	}
	resource := struct {
		TenantID string
		Version  int
	}{TenantID: "tenant-a", Version: 2}
	if err := h.recordMutation(context.Background(), Principal{}, "req", resource); err != nil {
		t.Fatal(err)
	}
	failed := &Handler{config: Config{AuditWriter: adminAuditFailWriter{}}}
	if err := failed.recordMutation(context.Background(), Principal{}, "req", resource); !errors.Is(err, audit.ErrWriteFailed) {
		t.Fatalf("err=%v", err)
	}
}

func TestAdminMapsAuditFailureToServiceUnavailable(t *testing.T) {
	status, code := mapError(audit.ErrWriteFailed)
	if status != http.StatusServiceUnavailable || code != "audit_unavailable" {
		t.Fatalf("status=%d code=%q", status, code)
	}
}

func TestAdminMapsMySQLStorageFailureToServiceUnavailable(t *testing.T) {
	status, code := mapError(storagemysql.ErrStorage)
	if status != http.StatusServiceUnavailable || code != "storage_unavailable" {
		t.Fatalf("status=%d code=%q", status, code)
	}
}

func TestAdminConnectionsRouteUsesNoStoreAndMapsServiceErrors(t *testing.T) {
	handler, _ := testHandler(t)
	service := &adminConnectionsStub{list: []Connection{{BindingID: "binding", Channel: channels.ChannelTelegram, BotID: "123", Ready: true}}}
	handler.config.Connections = service
	request := httptest.NewRequest(http.MethodGet, "/admin/v1/tenants/tenant-a/connections", nil)
	request.Header.Set("Authorization", "Bearer admin-token")
	request.Header.Set("X-Request-ID", "request-connections")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "no-store" || service.listTenant != "tenant-a" {
		t.Fatalf("connection route = status:%d cache:%q tenant:%q body:%s", response.Code, response.Header().Get("Cache-Control"), service.listTenant, response.Body.String())
	}

	service.listErr = ErrConnectionUnavailable
	response = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodGet, "/admin/v1/tenants/tenant-a/connections", nil)
	request.Header.Set("Authorization", "Bearer admin-token")
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), "connections_unavailable") {
		t.Fatalf("connection unavailable route = status:%d body:%s", response.Code, response.Body.String())
	}
}

func testHandler(t *testing.T) (*Handler, *StaticAuthenticator) {
	t.Helper()
	modelCatalog, err := modelprofile.NewProviderCatalog(modelprofile.ProviderSpec{Provider: "openai", Models: []string{"gpt-4o-mini"}, EndpointPolicy: modelprofile.FieldOptional, EndpointSchemes: []string{"https"}, EndpointHosts: []string{"api.openai.com"}, SecretRefPolicy: modelprofile.FieldRequired})
	if err != nil {
		t.Fatal(err)
	}
	backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden, Options: map[string]backend.OptionSpec{}})
	if err != nil {
		t.Fatal(err)
	}
	auth, err := NewStaticAuthenticator("admin-token", []string{"*"})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(Config{Tenants: tenantmemory.NewRepository(), Apps: inmemory.NewRepository(), Models: modelmemory.NewRepository(modelCatalog), Backends: backendmemory.NewRepository(backendCatalog), Bindings: channelmemory.NewRepository(), Authenticator: auth})
	if err != nil {
		t.Fatal(err)
	}
	return handler, auth
}

func TestAdminTenantCreateAndReadUseIndependentPrincipal(t *testing.T) {
	handler, _ := testHandler(t)
	create := httptest.NewRequest(http.MethodPost, "/admin/v1/tenants", strings.NewReader("{\"tenant_key\":\"acme\",\"display_name\":\"Acme\"}"))
	create.Header.Set("Authorization", "Bearer admin-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, create)
	if response.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body=%s", response.Code, response.Body.String())
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	var created struct {
		TenantID  string
		TenantKey string
	}
	if err := json.Unmarshal(envelope["data"], &created); err != nil {
		t.Fatal(err)
	}
	if created.TenantID == "" || created.TenantKey != "acme" {
		t.Fatalf("created tenant = %+v", created)
	}

	// A platform wildcard can continue to access the created tenant. Keep a
	// scoped principal check here to preserve the tenant-admin boundary too.
	scopedAuth, err := NewStaticAuthenticator("admin-token", []string{created.TenantID})
	if err != nil {
		t.Fatal(err)
	}
	handler, err = NewHandler(Config{Tenants: handler.config.Tenants, Apps: handler.config.Apps, Models: handler.config.Models, Backends: handler.config.Backends, Bindings: handler.config.Bindings, Authenticator: scopedAuth})
	if err != nil {
		t.Fatal(err)
	}
	read := httptest.NewRequest(http.MethodGet, "/admin/v1/tenants/"+created.TenantID, nil)
	read.Header.Set("Authorization", "Bearer admin-token")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, read)
	if response.Code != http.StatusOK {
		t.Fatalf("read status = %d, body=%s", response.Code, response.Body.String())
	}

	ordinary := httptest.NewRequest(http.MethodGet, "/admin/v1/tenants/"+created.TenantID, nil)
	ordinary.Header.Set("Authorization", "Bearer chat-token")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, ordinary)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("ordinary token status = %d", response.Code)
	}
}

func TestAdminMeAndCollectionListsUseScopedStablePagination(t *testing.T) {
	handler, _ := testHandler(t)
	create := func(key string) string {
		req := httptest.NewRequest(http.MethodPost, "/admin/v1/tenants", strings.NewReader(`{"tenant_key":"`+key+`","display_name":"`+key+`"}`))
		req.Header.Set("Authorization", "Bearer admin-token")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusCreated {
			t.Fatalf("create tenant status = %d", rec.Code)
		}
		var envelope struct {
			Data struct{ TenantID string } `json:"data"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
			t.Fatal(err)
		}
		return envelope.Data.TenantID
	}
	first := create("first")
	secondValue, err := handler.config.Tenants.Create(context.Background(), tenant.CreateInput{TenantKey: "second", DisplayName: "second"})
	if err != nil {
		t.Fatal(err)
	}
	second := secondValue.TenantID
	hiddenValue, err := handler.config.Tenants.Create(context.Background(), tenant.CreateInput{TenantKey: "hidden", DisplayName: "hidden"})
	if err != nil {
		t.Fatal(err)
	}
	auth, err := NewStaticAuthenticator("admin-token", []string{first, second})
	if err != nil {
		t.Fatal(err)
	}
	handler.config.Authenticator = auth
	req := httptest.NewRequest(http.MethodGet, "/admin/v1/me", nil)
	req.Header.Set("Authorization", "Bearer admin-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), first) || !strings.Contains(rec.Body.String(), second) {
		t.Fatalf("me response = %d %s", rec.Code, rec.Body.String())
	}
	req = httptest.NewRequest(http.MethodGet, "/admin/v1/tenants?limit=1", nil)
	req.Header.Set("Authorization", "Bearer admin-token")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("tenant list = %d %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"items"`) {
		t.Fatalf("tenant list missing items: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), first) && !strings.Contains(rec.Body.String(), second) {
		t.Fatalf("tenant list missing scoped item: %s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), hiddenValue.TenantID) {
		t.Fatalf("tenant list exposed out-of-scope tenant: %s", rec.Body.String())
	}
	// Filtering must happen before query matching and pagination. Otherwise a
	// scoped administrator could discover an unauthorized tenant by searching
	// for its known display name or key.
	req = httptest.NewRequest(http.MethodGet, "/admin/v1/tenants?q=hidden&limit=1", nil)
	req.Header.Set("Authorization", "Bearer admin-token")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("scoped tenant search = %d %s", rec.Code, rec.Body.String())
	}
	var searchEnvelope struct {
		Data struct {
			Items      []struct{ TenantID string }
			NextCursor string
		}
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &searchEnvelope); err != nil {
		t.Fatal(err)
	}
	if len(searchEnvelope.Data.Items) != 0 || searchEnvelope.Data.NextCursor != "" {
		t.Fatalf("scoped tenant search returned unauthorized result: %s", rec.Body.String())
	}
}

func TestAdminGlobalFirstTenantCreationIsSerialized(t *testing.T) {
	handler, _ := testHandler(t)
	const attempts = 16
	statuses := make(chan int, attempts)
	var group sync.WaitGroup
	group.Add(attempts)
	for i := 0; i < attempts; i++ {
		go func(index int) {
			defer group.Done()
			body := "{\"tenant_key\":\"parallel-" + strconv.Itoa(index) + "\",\"display_name\":\"Parallel\"}"
			request := httptest.NewRequest(http.MethodPost, "/admin/v1/tenants", strings.NewReader(body))
			request.Header.Set("Authorization", "Bearer admin-token")
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			statuses <- recorder.Code
		}(i)
	}
	group.Wait()
	close(statuses)
	created := 0
	for status := range statuses {
		if status == http.StatusCreated {
			created++
		} else if status != http.StatusForbidden {
			t.Fatalf("parallel first-tenant status = %d, want 201 or 403", status)
		}
	}
	if created != 1 {
		t.Fatalf("parallel first-tenant creates = %d, want exactly one", created)
	}
}

func TestAdminTenantAndAppMetadataUpdatesUsePathScopeAndExpectedVersion(t *testing.T) {
	handler, _ := testHandler(t)
	createTenant := httptest.NewRequest(http.MethodPost, "/admin/v1/tenants", strings.NewReader(`{"tenant_key":"acme","display_name":"Acme"}`))
	createTenant.Header.Set("Authorization", "Bearer admin-token")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, createTenant)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("tenant status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	var tenantEnvelope struct {
		Data struct {
			TenantID string
			Version  int64
		} `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &tenantEnvelope); err != nil {
		t.Fatal(err)
	}
	scoped, err := NewStaticAuthenticator("admin-token", []string{tenantEnvelope.Data.TenantID})
	if err != nil {
		t.Fatal(err)
	}
	handler, err = NewHandler(Config{Tenants: handler.config.Tenants, Apps: handler.config.Apps, Models: handler.config.Models, Backends: handler.config.Backends, Bindings: handler.config.Bindings, Authenticator: scoped})
	if err != nil {
		t.Fatal(err)
	}
	createApp := httptest.NewRequest(http.MethodPost, "/admin/v1/tenants/"+tenantEnvelope.Data.TenantID+"/apps", strings.NewReader(`{"app_key":"support","display_name":"Support"}`))
	createApp.Header.Set("Authorization", "Bearer admin-token")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, createApp)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("app status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	var appEnvelope struct {
		Data struct {
			AppID   string
			Version int64
		} `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &appEnvelope); err != nil {
		t.Fatal(err)
	}
	patchApp := httptest.NewRequest(http.MethodPatch, "/admin/v1/tenants/"+tenantEnvelope.Data.TenantID+"/apps/"+appEnvelope.Data.AppID, strings.NewReader(`{"expected_version":1,"display_name":"Support v2","description":"updated"}`))
	patchApp.Header.Set("Authorization", "Bearer admin-token")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, patchApp)
	if recorder.Code != http.StatusOK {
		t.Fatalf("patch app status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	stale := httptest.NewRequest(http.MethodPatch, "/admin/v1/tenants/"+tenantEnvelope.Data.TenantID+"/apps/"+appEnvelope.Data.AppID, strings.NewReader(`{"expected_version":1,"display_name":"stale","description":"stale"}`))
	stale.Header.Set("Authorization", "Bearer admin-token")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, stale)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("stale patch status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestAdminInvalidatesOnlyTheMutatedRuntimeScope(t *testing.T) {
	handler, _ := testHandler(t)
	invalidator := &recordingCacheInvalidator{}
	handler.config.CacheInvalidator = invalidator
	tenantValue, err := handler.config.Tenants.Create(context.Background(), tenant.CreateInput{TenantKey: "invalidate", DisplayName: "Invalidate"})
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := NewStaticAuthenticator("admin-token", []string{tenantValue.TenantID})
	if err != nil {
		t.Fatal(err)
	}
	handler.config.Authenticator = authenticator
	app, err := handler.config.Apps.Create(context.Background(), appmodel.CreateInput{TenantID: tenantValue.TenantID, AppKey: "invalidate", DisplayName: "Invalidate"})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPatch, "/admin/v1/tenants/"+tenantValue.TenantID+"/apps/"+app.AppID, strings.NewReader(`{"expected_version":1,"display_name":"Invalidate v2"}`))
	request.Header.Set("Authorization", "Bearer admin-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("app update status = %d, body=%s", response.Code, response.Body.String())
	}
	if changes := invalidator.Changes(); len(changes) != 1 || changes[0] != (CacheInvalidation{TenantID: tenantValue.TenantID, AppID: app.AppID, Kind: CacheInvalidationApp}) {
		t.Fatalf("invalidations = %#v", changes)
	}
}

func TestAdminInvalidationScopeMatchesCommittedResource(t *testing.T) {
	handler, _ := testHandler(t)
	invalidator := &recordingCacheInvalidator{}
	handler.config.CacheInvalidator = invalidator
	const tenantID = "t_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	cases := []struct {
		name   string
		method string
		parts  []string
		want   CacheInvalidation
	}{
		{name: "tenant configuration", method: http.MethodPatch, parts: []string{"tenants", tenantID}, want: CacheInvalidation{TenantID: tenantID, Kind: CacheInvalidationTenant}},
		{name: "tenant disable", method: http.MethodPost, parts: []string{"tenants", tenantID, "status"}, want: CacheInvalidation{TenantID: tenantID, Kind: CacheInvalidationTenant}},
		{name: "app publish", method: http.MethodPost, parts: []string{"tenants", tenantID, "apps", "app-1", "revisions", "2", "publish"}, want: CacheInvalidation{TenantID: tenantID, AppID: "app-1", Kind: CacheInvalidationApp}},
		{name: "app rollback", method: http.MethodPost, parts: []string{"tenants", tenantID, "apps", "app-1", "rollback"}, want: CacheInvalidation{TenantID: tenantID, AppID: "app-1", Kind: CacheInvalidationApp}},
		{name: "model update", method: http.MethodPatch, parts: []string{"tenants", tenantID, "models", "model-1"}, want: CacheInvalidation{TenantID: tenantID, ProfileID: "model-1", Kind: CacheInvalidationModel}},
		{name: "backend disable", method: http.MethodPost, parts: []string{"tenants", tenantID, "backends", "backend-1", "status"}, want: CacheInvalidation{TenantID: tenantID, ProfileID: "backend-1", Kind: CacheInvalidationBackend}},
		{name: "binding update", method: http.MethodPatch, parts: []string{"tenants", tenantID, "bindings", "binding-1"}, want: CacheInvalidation{TenantID: tenantID, BindingID: "binding-1", Kind: CacheInvalidationBinding}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			invalidator.Reset()
			handler.invalidateMutation(test.parts, test.method)
			if changes := invalidator.Changes(); len(changes) != 1 || changes[0] != test.want {
				t.Fatalf("invalidations = %#v, want %#v", changes, test.want)
			}
		})
	}
	for _, parts := range [][]string{{"tenants", tenantID, "models"}, {"tenants", tenantID, "apps", "app-1", "revisions", "2"}} {
		invalidator.Reset()
		handler.invalidateMutation(parts, http.MethodPost)
		if changes := invalidator.Changes(); len(changes) != 0 {
			t.Fatalf("unpublished change invalidated runtime: %#v", changes)
		}
	}
}

type recordingCacheInvalidator struct {
	mu      sync.Mutex
	changes []CacheInvalidation
}

func (invalidator *recordingCacheInvalidator) Invalidate(change CacheInvalidation) {
	invalidator.mu.Lock()
	defer invalidator.mu.Unlock()
	invalidator.changes = append(invalidator.changes, change)
}

func (invalidator *recordingCacheInvalidator) Changes() []CacheInvalidation {
	invalidator.mu.Lock()
	defer invalidator.mu.Unlock()
	return append([]CacheInvalidation(nil), invalidator.changes...)
}

func (invalidator *recordingCacheInvalidator) Reset() {
	invalidator.mu.Lock()
	defer invalidator.mu.Unlock()
	invalidator.changes = nil
}

func TestDecodeBodyPreservesProviderOptionKeys(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/admin/v1/tenants/t_01ARZ3NDEKTSV4RRFFQ69G5FAV/models", strings.NewReader(`{"profile_key":"support","display_name":"Support","configuration":{"provider":"openai","model":"gpt-4o-mini","secret_ref":"env/key","options":{"x_custom_option":"keep"}}}`))
	var input modelprofile.CreateInput
	if err := decodeBody(request, &input); err != nil {
		t.Fatal(err)
	}
	if input.Configuration.Options["x_custom_option"] != "keep" {
		t.Fatalf("provider option key was normalized: %#v", input.Configuration.Options)
	}
	var profile modelprofile.CreateInput
	profileRequest := httptest.NewRequest(http.MethodPost, "/admin/v1/tenants/t_01ARZ3NDEKTSV4RRFFQ69G5FAV/models", strings.NewReader(`{"profile_key":"support-model","display_name":"Support"}`))
	if err := decodeBody(profileRequest, &profile); err != nil || profile.ProfileKey != "support-model" {
		t.Fatalf("profile_key decode = %q, %v", profile.ProfileKey, err)
	}
}

func TestAdminMalformedBodyMapsToBadRequest(t *testing.T) {
	handler, _ := testHandler(t)
	request := httptest.NewRequest(http.MethodPost, "/admin/v1/tenants", strings.NewReader(`{`))
	request.Header.Set("Authorization", "Bearer admin-token")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("malformed body status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `"error":"invalid_request"`) {
		t.Fatalf("malformed body category = %s", recorder.Body.String())
	}
}

func TestAdminEmptyBodyMapsToBadRequest(t *testing.T) {
	handler, _ := testHandler(t)
	request := httptest.NewRequest(http.MethodPost, "/admin/v1/tenants", nil)
	request.Header.Set("Authorization", "Bearer admin-token")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "\"error\":\"invalid_request\"") {
		t.Fatalf("empty body status/category = %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestAdminMapsMalformedBodiesAcrossWriteRoutes(t *testing.T) {
	handler, _ := testHandler(t)
	created, err := handler.config.Tenants.Create(context.Background(), tenant.CreateInput{TenantKey: "malformed-routes", DisplayName: "Malformed Routes"})
	if err != nil {
		t.Fatal(err)
	}
	handler.config.Authenticator, err = NewStaticAuthenticator("admin-token", []string{created.TenantID})
	if err != nil {
		t.Fatal(err)
	}
	base := "/admin/v1/tenants/" + created.TenantID
	routes := []struct{ method, path string }{
		{http.MethodPost, base + "/status"}, {http.MethodPost, base + "/apps"},
		{http.MethodPost, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/status"},
		{http.MethodPost, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/revisions"},
		{http.MethodPatch, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/revisions/1"},
		{http.MethodPost, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/revisions/1/publish"},
		{http.MethodPost, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/rollback"},
		{http.MethodPost, base + "/models"}, {http.MethodPatch, base + "/models/model_01ARZ3NDEKTSV4RRFFQ69G5FAV"}, {http.MethodPost, base + "/models/model_01ARZ3NDEKTSV4RRFFQ69G5FAV/status"},
		{http.MethodPost, base + "/backends"}, {http.MethodPatch, base + "/backends/backend_01ARZ3NDEKTSV4RRFFQ69G5FAV"}, {http.MethodPost, base + "/backends/backend_01ARZ3NDEKTSV4RRFFQ69G5FAV/status"},
		{http.MethodPost, base + "/bindings"}, {http.MethodPatch, base + "/bindings/binding_01ARZ3NDEKTSV4RRFFQ69G5FAV"}, {http.MethodPost, base + "/bindings/binding_01ARZ3NDEKTSV4RRFFQ69G5FAV/status"},
	}
	for _, route := range routes {
		request := httptest.NewRequest(route.method, route.path, strings.NewReader(`{`))
		request.Header.Set("Authorization", "Bearer admin-token")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusBadRequest {
			t.Errorf("%s %s status = %d, body=%s", route.method, route.path, recorder.Code, recorder.Body.String())
		}
	}
}

func TestAdminRejectsMalformedRevisionAndExtraRouteSegments(t *testing.T) {
	handler, _ := testHandler(t)
	created, err := handler.config.Tenants.Create(context.Background(), tenant.CreateInput{TenantKey: "route-boundary", DisplayName: "Route Boundary"})
	if err != nil {
		t.Fatal(err)
	}
	handler.config.Authenticator, err = NewStaticAuthenticator("admin-token", []string{created.TenantID})
	if err != nil {
		t.Fatal(err)
	}
	base := "/admin/v1/tenants/" + created.TenantID + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	cases := []string{
		base + "/status/extra",
		base + "/rollback/extra",
		base + "/revisions/not-a-number/publish",
		base + "/revisions/1/publish/extra",
	}
	for _, path := range cases {
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
		request.Header.Set("Authorization", "Bearer admin-token")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusNotFound && recorder.Code != http.StatusBadRequest {
			t.Errorf("%s status = %d, body=%s", path, recorder.Code, recorder.Body.String())
		}
	}
	request := httptest.NewRequest(http.MethodPost, base+"/revisions/not-a-number/publish", strings.NewReader(`{}`))
	request.Header.Set("Authorization", "Bearer admin-token")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), `"error":"invalid_request"`) {
		t.Fatalf("malformed revision response = %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestAdminRouteSurfaceDispatchesEveryControlPlaneOperation(t *testing.T) {
	handler, _ := testHandler(t)
	created, err := handler.config.Tenants.Create(context.Background(), tenant.CreateInput{TenantKey: "route-test", DisplayName: "Route Test"})
	if err != nil {
		t.Fatal(err)
	}
	scoped, err := NewStaticAuthenticator("admin-token", []string{created.TenantID})
	if err != nil {
		t.Fatal(err)
	}
	handler.config.Authenticator = scoped
	base := "/admin/v1/tenants/" + created.TenantID
	routes := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodPatch, base, `{}`},
		{http.MethodPost, base + "/status", `{}`},
		{http.MethodPost, base + "/apps", `{}`},
		{http.MethodGet, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV", ``},
		{http.MethodPatch, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV", `{}`},
		{http.MethodPost, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/status", `{}`},
		{http.MethodPost, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/revisions", `{}`},
		{http.MethodPatch, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/revisions/1", `{}`},
		{http.MethodPost, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/revisions/1/publish", `{}`},
		{http.MethodPost, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/rollback", `{}`},
		{http.MethodPost, base + "/models", `{}`},
		{http.MethodGet, base + "/models/model_01ARZ3NDEKTSV4RRFFQ69G5FAV", ``},
		{http.MethodPatch, base + "/models/model_01ARZ3NDEKTSV4RRFFQ69G5FAV", `{}`},
		{http.MethodPost, base + "/models/model_01ARZ3NDEKTSV4RRFFQ69G5FAV/status", `{}`},
		{http.MethodPost, base + "/backends", `{}`},
		{http.MethodGet, base + "/backends/backend_01ARZ3NDEKTSV4RRFFQ69G5FAV", ``},
		{http.MethodPatch, base + "/backends/backend_01ARZ3NDEKTSV4RRFFQ69G5FAV", `{}`},
		{http.MethodPost, base + "/backends/backend_01ARZ3NDEKTSV4RRFFQ69G5FAV/status", `{}`},
		{http.MethodPost, base + "/bindings", `{}`},
		{http.MethodGet, base + "/bindings/binding_01ARZ3NDEKTSV4RRFFQ69G5FAV", ``},
		{http.MethodPatch, base + "/bindings/binding_01ARZ3NDEKTSV4RRFFQ69G5FAV", `{}`},
		{http.MethodPost, base + "/bindings/binding_01ARZ3NDEKTSV4RRFFQ69G5FAV/status", `{}`},
	}
	for _, route := range routes {
		request := httptest.NewRequest(route.method, route.path, strings.NewReader(route.body))
		request.Header.Set("Authorization", "Bearer admin-token")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code == http.StatusInternalServerError {
			t.Fatalf("%s %s returned 500: %s", route.method, route.path, recorder.Body.String())
		}
	}
}

func TestAdminHappyPathCoversResourceMutations(t *testing.T) {
	fixture := newAdminMutationFixture(t)
	assertAdminModelMutation(t, fixture)
	assertAdminBackendMutation(t, fixture)
	app := createAndPublishAdminRevision(t, fixture)
	assertAdminBindingMutation(t, fixture, app)
}

func TestAdminCanaryRouteBuildsTenantScopedMutation(t *testing.T) {
	fixture := newAdminMutationFixture(t)
	appRoot := createAndPublishAdminRevision(t, fixture)
	stored, err := fixture.handler.config.Apps.Get(context.Background(), fixture.root.TenantID, appRoot.AppID)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := fixture.handler.config.Apps.CreateDraft(context.Background(), appmodel.CreateDraftInput{
		TenantID: fixture.root.TenantID, AppID: appRoot.AppID, ExpectedAppVersion: stored.Version, Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1,
		Configuration: appmodel.DraftConfiguration{Instruction: "candidate", ModelProfileID: "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV", Runtime: appmodel.DefaultRuntimePolicy()},
	})
	if err != nil {
		t.Fatal(err)
	}
	stored, err = fixture.handler.config.Apps.Get(context.Background(), fixture.root.TenantID, appRoot.AppID)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, err = fixture.handler.config.Apps.Publish(context.Background(), appmodel.PublishInput{
		TenantID: fixture.root.TenantID, AppID: appRoot.AppID, Revision: candidate.Revision, ExpectedAppVersion: stored.Version, ExpectedDraftVersion: candidate.DraftVersion, TenantActive: true,
		Metadata: appmodel.ChangeMetadata{ActorType: "admin", ActorID: fixture.principal.SubjectID, Reason: "publish candidate", CorrelationID: "admin-canary-publish"},
	})
	if err != nil {
		t.Fatal(err)
	}
	stored, err = fixture.handler.config.Apps.Get(context.Background(), fixture.root.TenantID, appRoot.AppID)
	if err != nil {
		t.Fatal(err)
	}
	stableRevision := int64(1)
	body := "{\"expected_app_version\":" + strconv.FormatInt(stored.Version, 10) + ",\"candidate_revision\":" + strconv.FormatInt(stableRevision, 10) + ",\"reason\":\"start canary\",\"correlation_id\":\"admin-canary\"}"
	request := fixture.request(http.MethodPost, body)
	status, value, err := fixture.handler.apps(context.Background(), request, fixture.principal, fixture.root.TenantID, []string{appRoot.AppID, "canary"})
	if status != http.StatusOK || value == nil || err != nil {
		t.Fatalf("canary route = status %d value %#v err %v", status, value, err)
	}
	selected, err := fixture.handler.config.Apps.Get(context.Background(), fixture.root.TenantID, appRoot.AppID)
	if err != nil || selected.CanaryRevision == nil || *selected.CanaryRevision != stableRevision {
		t.Fatalf("canary selection = app=%+v err=%v", selected, err)
	}
	clearBody := "{\"expected_app_version\":" + strconv.FormatInt(selected.Version, 10) + ",\"reason\":\"clear canary\",\"correlation_id\":\"admin-canary-clear\"}"
	status, value, err = fixture.handler.apps(context.Background(), fixture.request(http.MethodPost, clearBody), fixture.principal, fixture.root.TenantID, []string{appRoot.AppID, "canary"})
	if status != http.StatusOK || value == nil || err != nil {
		t.Fatalf("canary clear route = status %d value %#v err %v", status, value, err)
	}
}

func TestAdminCanaryRouteRejectsMalformedAndUnsupportedRequests(t *testing.T) {
	fixture := newAdminMutationFixture(t)
	app := createAndPublishAdminRevision(t, fixture)
	tests := []struct {
		name     string
		method   string
		body     string
		tenantID string
		parts    []string
		wantErr  error
	}{
		{name: "wrong method", method: http.MethodPatch, body: `{}`, tenantID: fixture.root.TenantID, parts: []string{app.AppID, "canary"}, wantErr: errNotFound},
		{name: "malformed body", method: http.MethodPost, body: `{`, tenantID: fixture.root.TenantID, parts: []string{app.AppID, "canary"}, wantErr: errInvalidRequest},
		{name: "unknown tenant", method: http.MethodPost, body: `{}`, tenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAV", parts: []string{app.AppID, "canary"}, wantErr: tenant.ErrNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			status, _, err := fixture.handler.apps(context.Background(), fixture.request(tc.method, tc.body), fixture.principal, tc.tenantID, tc.parts)
			if status != 0 || !errors.Is(err, tc.wantErr) {
				t.Fatalf("canary route status=%d err=%v, want %v", status, err, tc.wantErr)
			}
		})
	}
}

type adminMutationFixture struct {
	handler   *Handler
	root      *tenant.Tenant
	principal Principal
}

func newAdminMutationFixture(t *testing.T) adminMutationFixture {
	t.Helper()
	handler, _ := testHandler(t)
	root, err := handler.config.Tenants.Create(context.Background(), tenant.CreateInput{TenantKey: "happy", DisplayName: "Happy"})
	if err != nil {
		t.Fatal(err)
	}
	return adminMutationFixture{
		handler:   handler,
		root:      root,
		principal: Principal{SubjectID: "admin", TenantScopes: map[string]struct{}{root.TenantID: {}}},
	}
}

func (fixture adminMutationFixture) request(method, body string) *http.Request {
	return httptest.NewRequest(method, "/admin/v1", strings.NewReader(body))
}

func assertAdminModelMutation(t *testing.T, fixture adminMutationFixture) {
	t.Helper()
	modelBody := "{\"profile_key\":\"primary\",\"display_name\":\"Primary\",\"reason\":\"create\",\"correlation_id\":\"happy-1\",\"configuration\":{\"provider\":\"openai\",\"model\":\"gpt-4o-mini\",\"secret_ref\":\"env/key\"}}"
	var decodedModel modelprofile.CreateInput
	if err := decodeBody(fixture.request(http.MethodPost, modelBody), &decodedModel); err != nil || decodedModel.Configuration.SecretRef == "" {
		t.Fatalf("decoded model input = %+v, normalized=%#v, err=%v", decodedModel, normalizeKeys(map[string]any{"configuration": map[string]any{"secret_ref": "env/key"}}), err)
	}
	status, modelValue, err := fixture.handler.models(context.Background(), fixture.request(http.MethodPost, modelBody), fixture.principal, fixture.root.TenantID, nil)
	if err != nil || status != http.StatusCreated {
		t.Fatalf("model create = %d, %v", status, err)
	}
	modelID := modelValue.(map[string]any)["profile"].(*modelprofile.Profile).ProfileID
	if status, _, err := fixture.handler.models(context.Background(), fixture.request(http.MethodGet, ""), fixture.principal, fixture.root.TenantID, []string{modelID}); err != nil || status != http.StatusOK {
		t.Fatalf("model get = %d, %v", status, err)
	}
}

func assertAdminBackendMutation(t *testing.T, fixture adminMutationFixture) {
	t.Helper()
	backendBody := "{\"profile_key\":\"primary\",\"display_name\":\"Primary\",\"reason\":\"create\",\"correlation_id\":\"happy-2\",\"bindings\":[{\"capability\":\"session\",\"provider\":\"inmemory\"}]}"
	status, backendValue, err := fixture.handler.backends(context.Background(), fixture.request(http.MethodPost, backendBody), fixture.principal, fixture.root.TenantID, nil)
	if err != nil || status != http.StatusCreated {
		t.Fatalf("backend create = %d, %v", status, err)
	}
	backendID := backendValue.(map[string]any)["profile"].(*backend.Profile).ProfileID
	if status, _, err := fixture.handler.backends(context.Background(), fixture.request(http.MethodGet, ""), fixture.principal, fixture.root.TenantID, []string{backendID}); err != nil || status != http.StatusOK {
		t.Fatalf("backend get = %d, %v", status, err)
	}
}

func createAndPublishAdminRevision(t *testing.T, fixture adminMutationFixture) *appmodel.App {
	t.Helper()
	appRoot, err := fixture.handler.config.Apps.Create(context.Background(), appmodel.CreateInput{TenantID: fixture.root.TenantID, AppKey: "support", DisplayName: "Support"})
	if err != nil {
		t.Fatal(err)
	}
	draftBody := "{\"expected_app_version\":1,\"kind\":\"llm\",\"schema_version\":1,\"configuration\":{\"instruction\":\"answer\",\"model_profile_id\":\"mp_01ARZ3NDEKTSV4RRFFQ69G5FAV\"}}"
	status, draftValue, err := fixture.handler.revisions(context.Background(), fixture.request(http.MethodPost, draftBody), fixture.principal, fixture.root.TenantID, appRoot.AppID, nil)
	if err != nil || status != http.StatusCreated {
		t.Fatalf("draft create = %d, %v", status, err)
	}
	draft := draftValue.(*appmodel.Revision)
	updateBody := "{\"expected_app_version\":1,\"expected_draft_version\":1,\"configuration\":{\"instruction\":\"answer updated\",\"model_profile_id\":\"mp_01ARZ3NDEKTSV4RRFFQ69G5FAV\"}}"
	if status, _, err := fixture.handler.revisions(context.Background(), fixture.request(http.MethodPatch, updateBody), fixture.principal, fixture.root.TenantID, appRoot.AppID, []string{strconv.FormatInt(draft.Revision, 10)}); err != nil || status != http.StatusOK {
		t.Fatalf("draft update = %d, %v", status, err)
	}
	publishBody := "{\"expected_app_version\":1,\"expected_draft_version\":2,\"reason\":\"publish\",\"correlation_id\":\"happy-publish\"}"
	if status, _, err := fixture.handler.revisions(context.Background(), fixture.request(http.MethodPost, publishBody), fixture.principal, fixture.root.TenantID, appRoot.AppID, []string{strconv.FormatInt(draft.Revision, 10), "publish"}); err != nil || status != http.StatusOK {
		t.Fatalf("draft publish = %d, %v", status, err)
	}
	return appRoot
}

func assertAdminBindingMutation(t *testing.T, fixture adminMutationFixture, app *appmodel.App) {
	t.Helper()
	bindingBody := "{\"binding_key\":\"primary\",\"channel\":\"wecom\",\"provider_account_id\":\"corp\",\"public_route_key_digest\":\"0000000000000000000000000000000000000000000000000000000000000000\",\"app_id\":\"" + app.AppID + "\",\"secret_ref\":\"secret/corp\",\"reason\":\"create\",\"correlation_id\":\"happy-3\",\"protocol\":{\"wecom\":{\"corp_id\":\"corp\"}}}"
	status, bindingValue, err := fixture.handler.bindings(context.Background(), fixture.request(http.MethodPost, bindingBody), fixture.principal, fixture.root.TenantID, nil)
	if err != nil || status != http.StatusCreated {
		t.Fatalf("binding create = %d, %v", status, err)
	}
	bindingID := bindingValue.(map[string]any)["binding"].(*channels.Binding).BindingID
	if status, _, err := fixture.handler.bindings(context.Background(), fixture.request(http.MethodGet, ""), fixture.principal, fixture.root.TenantID, []string{bindingID}); err != nil || status != http.StatusOK {
		t.Fatalf("binding get = %d, %v", status, err)
	}
}

func TestAdminErrorMappingCategories(t *testing.T) {
	cases := []struct {
		err    error
		status int
	}{
		{ErrConnectionUnavailable, http.StatusServiceUnavailable}, {ErrAgentNotReady, http.StatusConflict}, {ErrConnectionFailed, http.StatusBadGateway},
		{ErrUnauthenticated, http.StatusUnauthorized}, {ErrForbidden, http.StatusForbidden}, {errNotFound, http.StatusNotFound},
		{tenant.ErrConflict, http.StatusConflict}, {appmodel.ErrConflict, http.StatusConflict}, {modelprofile.ErrConflict, http.StatusConflict},
		{backend.ErrConflict, http.StatusConflict}, {channels.ErrConflict, http.StatusConflict}, {postgres.ErrStorage, http.StatusServiceUnavailable},
		{tenant.ErrInvalid, http.StatusBadRequest}, {appmodel.ErrInvalidTransition, http.StatusBadRequest}, {modelprofile.ErrDisabled, http.StatusBadRequest},
		{backend.ErrInvalidTransition, http.StatusBadRequest}, {channels.ErrDisabled, http.StatusBadRequest}, {errInvalidRequest, http.StatusBadRequest},
		{tenant.ErrDuplicateKey, http.StatusConflict}, {appmodel.ErrDuplicateKey, http.StatusConflict}, {modelprofile.ErrDuplicateKey, http.StatusConflict},
		{backend.ErrDuplicateKey, http.StatusConflict}, {channels.ErrDuplicateKey, http.StatusConflict},
	}
	for _, tc := range cases {
		status, _ := mapError(tc.err)
		if status != tc.status {
			t.Errorf("mapError(%v) = %d, want %d", tc.err, status, tc.status)
		}
	}
	for _, err := range []error{nil, errors.New("unexpected")} {
		if status, _ := mapError(err); status != http.StatusInternalServerError {
			t.Errorf("mapError(%v) status = %d", err, status)
		}
	}
}

func TestAdminHandlerRejectsInvalidConfigurationAndPaths(t *testing.T) {
	if _, err := NewHandler(Config{}); err == nil {
		t.Fatal("NewHandler accepted an empty configuration")
	}
	handler, _ := testHandler(t)
	for _, request := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/other", nil),
		httptest.NewRequest(http.MethodGet, "/admin/v1/tenants", nil),
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusNotFound && recorder.Code != http.StatusUnauthorized {
			t.Fatalf("unexpected status for invalid admin request: %d", recorder.Code)
		}
	}
}

func TestAdminRejectsUnsupportedMethodsAndRouteShapes(t *testing.T) {
	handler, _ := testHandler(t)
	created, err := handler.config.Tenants.Create(context.Background(), tenant.CreateInput{TenantKey: "method-test", DisplayName: "Method Test"})
	if err != nil {
		t.Fatal(err)
	}
	handler.config.Authenticator, err = NewStaticAuthenticator("admin-token", []string{created.TenantID})
	if err != nil {
		t.Fatal(err)
	}
	base := "/admin/v1/tenants/" + created.TenantID
	cases := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/admin/v1/tenants"},
		{http.MethodDelete, base},
		{http.MethodGet, base + "/status"},
		{http.MethodGet, base + "/status/extra"},
		{http.MethodGet, base + "/apps"},
		{http.MethodDelete, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		{http.MethodGet, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/status"},
		{http.MethodGet, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/revisions"},
		{http.MethodGet, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/revisions/1"},
		{http.MethodGet, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/revisions/1/publish"},
		{http.MethodGet, base + "/apps/app_01ARZ3NDEKTSV4RRFFQ69G5FAV/rollback"},
		{http.MethodGet, base + "/models"},
		{http.MethodDelete, base + "/models/model_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		{http.MethodGet, base + "/models/model_01ARZ3NDEKTSV4RRFFQ69G5FAV/status"},
		{http.MethodGet, base + "/backends"},
		{http.MethodDelete, base + "/backends/backend_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		{http.MethodGet, base + "/backends/backend_01ARZ3NDEKTSV4RRFFQ69G5FAV/status"},
		{http.MethodGet, base + "/bindings"},
		{http.MethodDelete, base + "/bindings/binding_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		{http.MethodGet, base + "/bindings/binding_01ARZ3NDEKTSV4RRFFQ69G5FAV/status"},
	}
	for _, tc := range cases {
		request := httptest.NewRequest(tc.method, tc.path, strings.NewReader(`{}`))
		request.Header.Set("Authorization", "Bearer admin-token")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code == http.StatusInternalServerError {
			t.Errorf("%s %s returned 500: %s", tc.method, tc.path, recorder.Body.String())
		}
	}
}

func TestAdminNormalizationAndBodyBoundaries(t *testing.T) {
	if err := decodeBody(nil, &struct{}{}); !errors.Is(err, errInvalidRequest) {
		t.Fatalf("nil request error = %v", err)
	}
	if got := normalizeKeys([]any{map[string]any{"reason": "why", "correlation_id": "corr"}}); got == nil {
		t.Fatal("normalizeKeys returned nil")
	}
	if toExported("x_unknown_key") != "x_unknown_key" {
		t.Fatal("unknown keys must remain unchanged")
	}
}

func TestGlobalAdminListsAndReadsAllTenants(t *testing.T) {
	handler, _ := testHandler(t)
	request := httptest.NewRequest(http.MethodPost, "/admin/v1/tenants", strings.NewReader(`{"tenant_key":"first","display_name":"First"}`))
	request.Header.Set("Authorization", "Bearer admin-token")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("first tenant status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	second, err := handler.config.Tenants.Create(context.Background(), tenant.CreateInput{TenantKey: "second", DisplayName: "Second"})
	if err != nil {
		t.Fatal(err)
	}
	list := httptest.NewRequest(http.MethodGet, "/admin/v1/tenants", nil)
	list.Header.Set("Authorization", "Bearer admin-token")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, list)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), second.TenantID) {
		t.Fatalf("global tenant list = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	me := httptest.NewRequest(http.MethodGet, "/admin/v1/me", nil)
	me.Header.Set("Authorization", "Bearer admin-token")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, me)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"tenant_scopes":["*"]`) {
		t.Fatalf("global principal response = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	read := httptest.NewRequest(http.MethodGet, "/admin/v1/tenants/"+second.TenantID, nil)
	read.Header.Set("Authorization", "Bearer admin-token")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, read)
	if recorder.Code != http.StatusOK {
		t.Fatalf("global tenant read = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	app := httptest.NewRequest(http.MethodPost, "/admin/v1/tenants/"+second.TenantID+"/apps", strings.NewReader(`{"app_key":"support","display_name":"Support"}`))
	app.Header.Set("Authorization", "Bearer admin-token")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, app)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("global app create = %d, body=%s", recorder.Code, recorder.Body.String())
	}
}
