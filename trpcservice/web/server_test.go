package web

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/adminauth"
	"github.com/cyl6/trpc-agent-service/trpcservice/channels"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/coordination"
	"github.com/cyl6/trpc-agent-service/trpcservice/metrics"
	"github.com/cyl6/trpc-agent-service/trpcservice/queue"
	"github.com/cyl6/trpc-agent-service/trpcservice/store"
	"github.com/cyl6/trpc-agent-service/trpcservice/tenant"
	"github.com/cyl6/trpc-agent-service/trpcservice/tenant/governance"
	"github.com/cyl6/trpc-agent-service/trpcservice/tooloperation"
	"github.com/cyl6/trpc-agent-service/trpcservice/worker"
)

type captureDispatcher struct {
	tasks    chan worker.Task
	readyErr error
}

type outboxOperatorDispatcher struct {
	captureDispatcher
	operations []queue.UncertainOutbox
	resolved   queue.ResolveOutboxRequest
	resolveErr error
}

type toolOperationDirect struct {
	operations []tooloperation.Record
	resolved   tooloperation.ResolveRequest
	resolveErr error
}

func (*toolOperationDirect) Process(context.Context, worker.Task) (worker.Result, error) {
	return worker.Result{}, nil
}

func (d *toolOperationDirect) ListUnknownToolOperations(
	context.Context, string, int,
) ([]tooloperation.Record, error) {
	return append([]tooloperation.Record(nil), d.operations...), nil
}

func (d *toolOperationDirect) ResolveToolOperation(
	_ context.Context, request tooloperation.ResolveRequest,
) (*tooloperation.Record, error) {
	d.resolved = request
	if d.resolveErr != nil {
		return nil, d.resolveErr
	}
	return &tooloperation.Record{
		TenantID: request.TenantID, OperationKey: request.OperationKey,
		State: tooloperation.StateRetryableNotApplied,
	}, nil
}

func (d *outboxOperatorDispatcher) ListUncertainOutbox(context.Context, int) ([]queue.UncertainOutbox, error) {
	return append([]queue.UncertainOutbox(nil), d.operations...), nil
}

func (d *outboxOperatorDispatcher) ResolveOutbox(_ context.Context, request queue.ResolveOutboxRequest) error {
	d.resolved = request
	return d.resolveErr
}

const gatewayConfigYAML = `
server: {address: ":0", admin_token_env: ADMIN_TOKEN}
coordination: {backend: inmemory}
telemetry: {service_name: test}
tenants:
  - tenant_id: tenant-a
    version: v1
    enabled: true
    app: {name: assistant, agent_name: chat-agent}
    model: {provider: mock, name: mock}
    tools: {allow: [calculator]}
    channels:
      - {type: telegram, binding_id: tenant-a-hook, enabled: true, token_env: TG_TOKEN, signing_secret_env: TG_SECRET}
    data:
      session: {type: inmemory}
      memory: {type: inmemory}
      summary: {type: inmemory}
      artifact: {type: inmemory}
      knowledge: {type: disabled}
      audit_log: {type: stdout}
    audit: {enabled: true, sink: stdout}
    budget: {requests_per_minute: 10, max_input_chars: 1000}
`

func (d *captureDispatcher) Submit(_ context.Context, task worker.Task) error {
	d.tasks <- task
	return nil
}

func (d *captureDispatcher) Ready(context.Context) error { return d.readyErr }

func gatewayConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg, err := config.Decode(strings.NewReader(gatewayConfigYAML))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestScopedViewerCanReadGlobalViewsWithoutCrossTenantDisclosure(t *testing.T) {
	cfg := gatewayConfig(t)
	other := cfg.Tenants[0]
	other.TenantID = "tenant-b"
	other.Version = "v2"
	other.App.Name = "assistant-b"
	other.App.AgentName = "chat-agent-b"
	other.Channels = append([]config.ChannelConfig(nil), other.Channels...)
	other.Channels[0].BindingID = "tenant-b-hook"
	cfg.Tenants = append(cfg.Tenants, other)
	registry, err := tenant.NewRegistry(cfg)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(registry, channels.NewRegistry(), &captureDispatcher{tasks: make(chan worker.Task, 1)}, nil, metrics.NewMetrics(), "", "", cfg)
	server.SetAdminAuthenticator(adminauth.NewStaticAuthenticator(map[string]adminauth.Principal{
		"scoped-viewer": {Subject: "viewer", Roles: []string{"viewer"}, Tenants: []string{"tenant-a"}},
	}))
	request := httptest.NewRequest(http.MethodGet, "/admin/v1/tenants", nil)
	request.Header.Set("Authorization", "Bearer scoped-viewer")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "tenant-a") || strings.Contains(response.Body.String(), "tenant-b") {
		t.Fatalf("scoped tenant list status=%d body=%s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodGet, "/admin/v1/health/dependencies", nil)
	request.Header.Set("Authorization", "Bearer scoped-viewer")
	response = httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("scoped dependency view status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestReadinessChecksDispatcherAndDrainState(t *testing.T) {
	dispatch := &captureDispatcher{tasks: make(chan worker.Task, 1)}
	server := NewServer(nil, nil, dispatch, nil, metrics.NewMetrics(), "", "", nil)
	request := func() *httptest.ResponseRecorder {
		res := httptest.NewRecorder()
		server.Handler().ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		return res
	}
	if res := request(); res.Code != http.StatusOK || !strings.Contains(res.Body.String(), `"status":"ready"`) {
		t.Fatalf("ready status=%d body=%s", res.Code, res.Body.String())
	}
	dispatch.readyErr = errors.New("database endpoint with secret detail")
	if res := request(); res.Code != http.StatusServiceUnavailable || strings.Contains(res.Body.String(), "secret detail") {
		t.Fatalf("dependency failure status=%d body=%s", res.Code, res.Body.String())
	}
	dispatch.readyErr = nil
	server.SetDraining()
	if res := request(); res.Code != http.StatusServiceUnavailable {
		t.Fatalf("draining status=%d body=%s", res.Code, res.Body.String())
	}
}

func TestReloadRejectsStaticConfigurationChanges(t *testing.T) {
	cases := map[string]string{
		"server": strings.Replace(gatewayConfigYAML, `address: ":0"`, `address: ":9999"`, 1),
		"queue":  strings.Replace(gatewayConfigYAML, "coordination:", "queue: {backend: memory}\ncoordination:", 1),
	}
	for name, changed := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv("ADMIN_TOKEN", "admin-test-token")
			cfg := gatewayConfig(t)
			registry, err := tenant.NewRegistry(cfg)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(changed), 0o600); err != nil {
				t.Fatal(err)
			}
			server := NewServer(registry, channels.NewRegistry(), nil, nil, metrics.NewMetrics(), "ADMIN_TOKEN", path, cfg)
			req := httptest.NewRequest(http.MethodPost, "/admin/v1/reload", nil)
			req.Header.Set("Authorization", "Bearer admin-test-token")
			res := httptest.NewRecorder()
			server.Handler().ServeHTTP(res, req)
			if res.Code != http.StatusConflict {
				t.Fatalf("static reload status=%d body=%s", res.Code, res.Body.String())
			}
		})
	}
}

func TestPublicErrorMapsStableSafeResponses(t *testing.T) {
	cases := []struct {
		err     error
		status  int
		code    string
		message string
	}{
		{governance.ErrUserDenied, http.StatusForbidden, "permission_denied", "当前用户或操作未获授权"},
		{governance.ErrRateLimited, http.StatusTooManyRequests, "rate_limited", "请求过于频繁，请稍后重试"},
		{governance.ErrBudgetExceeded, http.StatusTooManyRequests, "budget_exceeded", "当前租户预算已用尽"},
		{governance.ErrInputTooLarge, http.StatusRequestEntityTooLarge, "input_too_large", "消息内容超过当前租户限制"},
		{coordination.ErrClaimInProgress, http.StatusConflict, "request_in_progress", "同一消息正在处理中，请稍后重试"},
		{worker.ErrTenantFrozen, http.StatusServiceUnavailable, "tenant_maintenance", "当前租户正在进行数据维护，请稍后重试"},
		{context.DeadlineExceeded, http.StatusGatewayTimeout, "model_timeout", "模型调用超时，请稍后重试"},
		{context.Canceled, http.StatusRequestTimeout, "request_canceled", "请求已取消"},
		{errors.New("provider echoed fixture credential"), http.StatusBadGateway, "model_request_failed", "模型调用失败，请检查 API Key、模型 ID 和请求地址"},
	}
	for _, test := range cases {
		status, code, message := publicError(test.err)
		if status != test.status || code != test.code || message != test.message {
			t.Fatalf("publicError(%v)=(%d,%q,%q)", test.err, status, code, message)
		}
		if strings.Contains(message, "fixture credential") {
			t.Fatalf("provider detail leaked: %q", message)
		}
	}
}

func TestAdminCanInspectAndResolveUnknownOutboxWithoutPayloadDisclosure(t *testing.T) {
	t.Setenv("ADMIN_TOKEN", "admin-test-token")
	dispatch := &outboxOperatorDispatcher{
		captureDispatcher: captureDispatcher{tasks: make(chan worker.Task, 1)},
		operations: []queue.UncertainOutbox{{
			OutboxID: "6c5d3d8d-d0a0-4d52-9220-b280e5dbe42d", OperationKey: "op_v1_safe",
			TenantID: "tenant-a", ChannelType: "telegram", BindingID: "binding-a",
			PartIndex: 1, PartCount: 3, AttemptNo: 2, StateVersion: 7,
			ErrorType: "network_unknown",
		}},
	}
	server := NewServer(nil, nil, dispatch, nil, metrics.NewMetrics(), "ADMIN_TOKEN", "", nil)

	listRequest := httptest.NewRequest(http.MethodGet, "/admin/v1/outbox/uncertain?limit=10", nil)
	listRequest.Header.Set("Authorization", "Bearer admin-test-token")
	listResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(listResponse, listRequest)
	if listResponse.Code != http.StatusOK || !strings.Contains(listResponse.Body.String(), "op_v1_safe") {
		t.Fatalf("list status=%d body=%s", listResponse.Code, listResponse.Body.String())
	}
	for _, forbidden := range []string{"payload", "trace_carrier", "target", "message text"} {
		if strings.Contains(listResponse.Body.String(), forbidden) {
			t.Fatalf("uncertain response disclosed %q: %s", forbidden, listResponse.Body.String())
		}
	}

	body := `{"resolution_id":"77f596fa-4a9c-4bbd-b154-92c0aa6385dd","expected_version":7,"expected_attempt":2,"action":"retry","reason":"provider console could not confirm delivery"}`
	resolveRequest := httptest.NewRequest(http.MethodPost, "/admin/v1/outbox/6c5d3d8d-d0a0-4d52-9220-b280e5dbe42d/resolve", strings.NewReader(body))
	resolveRequest.Header.Set("Authorization", "Bearer admin-test-token")
	resolveResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(resolveResponse, resolveRequest)
	if resolveResponse.Code != http.StatusOK {
		t.Fatalf("resolve status=%d body=%s", resolveResponse.Code, resolveResponse.Body.String())
	}
	if dispatch.resolved.Action != store.ResolveRetry || dispatch.resolved.Actor != "admin_api" || dispatch.resolved.ExpectedVersion != 7 {
		t.Fatalf("resolve request = %+v", dispatch.resolved)
	}
}

func TestAdminOutboxResolutionRejectsStaleCASAndNonDurableBackend(t *testing.T) {
	t.Setenv("ADMIN_TOKEN", "admin-test-token")
	body := `{"resolution_id":"77f596fa-4a9c-4bbd-b154-92c0aa6385dd","expected_version":7,"expected_attempt":2,"action":"cancel","reason":"operator canceled partial reply"}`
	path := "/admin/v1/outbox/6c5d3d8d-d0a0-4d52-9220-b280e5dbe42d/resolve"

	stale := &outboxOperatorDispatcher{
		captureDispatcher: captureDispatcher{tasks: make(chan worker.Task, 1)},
		resolveErr:        store.ErrInvalidTransition,
	}
	server := NewServer(nil, nil, stale, nil, metrics.NewMetrics(), "ADMIN_TOKEN", "", nil)
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer admin-test-token")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("stale resolution status=%d body=%s", response.Code, response.Body.String())
	}

	nonDurable := &captureDispatcher{tasks: make(chan worker.Task, 1)}
	server = NewServer(nil, nil, nonDurable, nil, metrics.NewMetrics(), "ADMIN_TOKEN", "", nil)
	request = httptest.NewRequest(http.MethodGet, "/admin/v1/outbox/uncertain", nil)
	request.Header.Set("Authorization", "Bearer admin-test-token")
	response = httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusNotImplemented {
		t.Fatalf("non-durable list status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestAdminToolOperationListAndResolutionAreRedactedAndFenced(t *testing.T) {
	t.Setenv("ADMIN_TOKEN", "admin-test-token")
	direct := &toolOperationDirect{operations: []tooloperation.Record{{
		TenantID: "tenant-a", OperationKey: "toolop_safe_opaque",
		PayloadHash: strings.Repeat("a", 64), Metadata: tooloperation.Metadata{ToolName: "calendar_create"},
		State: tooloperation.StateUnknown, StateVersion: 9, AttemptCount: 2,
	}}}
	server := NewServer(nil, nil, nil, direct, metrics.NewMetrics(), "ADMIN_TOKEN", "", nil)

	listRequest := httptest.NewRequest(
		http.MethodGet, "/admin/v1/tool-operations/uncertain?tenant_id=tenant-a&limit=10", nil,
	)
	listRequest.Header.Set("Authorization", "Bearer admin-test-token")
	listResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(listResponse, listRequest)
	if listResponse.Code != http.StatusOK || !strings.Contains(listResponse.Body.String(), "toolop_safe_opaque") {
		t.Fatalf("list status=%d body=%s", listResponse.Code, listResponse.Body.String())
	}
	for _, forbidden := range []string{"private argument", "provider response", "lease owner"} {
		if strings.Contains(listResponse.Body.String(), forbidden) {
			t.Fatalf("tool operation response disclosed %q", forbidden)
		}
	}

	body := `{"tenant_id":"tenant-a","resolution_id":"77f596fa-4a9c-4bbd-b154-92c0aa6385dd","expected_version":9,"action":"retry_not_applied","reason_code":"provider_proved_absent","retry_at":"2099-01-01T00:00:00Z"}`
	resolveRequest := httptest.NewRequest(
		http.MethodPost, "/admin/v1/tool-operations/toolop_safe_opaque/resolve", strings.NewReader(body),
	)
	resolveRequest.Header.Set("Authorization", "Bearer admin-test-token")
	resolveResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(resolveResponse, resolveRequest)
	if resolveResponse.Code != http.StatusOK {
		t.Fatalf("resolve status=%d body=%s", resolveResponse.Code, resolveResponse.Body.String())
	}
	if direct.resolved.OperationKey != "toolop_safe_opaque" ||
		direct.resolved.Action != tooloperation.ResolveRetryNotApplied ||
		direct.resolved.ExpectedVersion != 9 || direct.resolved.ActorHash != tooloperation.HashPayload([]byte("admin_api")) {
		t.Fatalf("resolve request=%+v", direct.resolved)
	}
}

func TestAdminToolOperationResolutionMapsCASAndValidationErrors(t *testing.T) {
	t.Setenv("ADMIN_TOKEN", "admin-test-token")
	body := `{"tenant_id":"tenant-a","resolution_id":"77f596fa-4a9c-4bbd-b154-92c0aa6385dd","expected_version":3,"action":"reject","reason_code":"operator_rejected"}`
	for name, resolveErr := range map[string]error{
		"stale version":          tooloperation.ErrInvalidTransition,
		"already resolved state": tooloperation.ErrUnknownRequiresResolution,
	} {
		t.Run(name, func(t *testing.T) {
			direct := &toolOperationDirect{resolveErr: resolveErr}
			server := NewServer(nil, nil, nil, direct, metrics.NewMetrics(), "ADMIN_TOKEN", "", nil)
			request := httptest.NewRequest(
				http.MethodPost, "/admin/v1/tool-operations/toolop_safe/resolve", strings.NewReader(body),
			)
			request.Header.Set("Authorization", "Bearer admin-test-token")
			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, request)
			if response.Code != http.StatusConflict {
				t.Fatalf("conflict status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}

	direct := &toolOperationDirect{}
	server := NewServer(nil, nil, nil, direct, metrics.NewMetrics(), "ADMIN_TOKEN", "", nil)
	request := httptest.NewRequest(http.MethodGet, "/admin/v1/tool-operations/uncertain", nil)
	request.Header.Set("Authorization", "Bearer admin-test-token")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("missing tenant status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestTenantListOmitsPromptsSecretReferencesAndAllowedUsers(t *testing.T) {
	t.Setenv("ADMIN_TOKEN", "admin-test-token")
	cfg := gatewayConfig(t)
	cfg.Tenants[0].App.Instruction = "private-system-instruction"
	cfg.Tenants[0].Channels[0].AllowedUsers = []string{"private-external-user"}
	registry, err := tenant.NewRegistry(cfg)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(registry, channels.NewRegistry(), nil, nil, metrics.NewMetrics(), "ADMIN_TOKEN", "", cfg)
	req := httptest.NewRequest(http.MethodGet, "/admin/v1/tenants", nil)
	req.Header.Set("Authorization", "Bearer admin-test-token")
	res := httptest.NewRecorder()
	server.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK || !strings.Contains(res.Body.String(), `"tenant_id":"tenant-a"`) {
		t.Fatalf("tenant list status=%d body=%s", res.Code, res.Body.String())
	}
	for _, forbidden := range []string{"private-system-instruction", "private-external-user", "TG_TOKEN", "TG_SECRET"} {
		if strings.Contains(res.Body.String(), forbidden) {
			t.Fatalf("tenant list leaked %q: %s", forbidden, res.Body.String())
		}
	}
}

func TestWebhookDerivesTenantFromVerifiedBinding(t *testing.T) {
	t.Setenv("TG_SECRET", "webhook-secret")
	t.Setenv("TG_TOKEN", "unused-token")
	cfg := gatewayConfig(t)
	registry, err := tenant.NewRegistry(cfg)
	if err != nil {
		t.Fatal(err)
	}
	dispatch := &captureDispatcher{tasks: make(chan worker.Task, 1)}
	channelRegistry := channels.NewRegistry(channels.NewTelegram(nil))
	server := NewServer(registry, channelRegistry, dispatch, nil, metrics.NewMetrics(), "ADMIN_TOKEN", "", nil)
	body := []byte(`{"tenant_id":"attacker-selected","update_id":42,"message":{"message_id":3,"date":1700000000,"text":"hello","from":{"id":7},"chat":{"id":9,"type":"private"}}}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/telegram/tenant-a-hook", bytes.NewReader(body))
	req.Header.Set("X-Telegram-Bot-Api-Secret-Token", "webhook-secret")
	res := httptest.NewRecorder()
	server.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
	}
	select {
	case task := <-dispatch.tasks:
		if task.Tenant.TenantID != "tenant-a" || task.Message.TenantID != "tenant-a" {
			t.Fatalf("untrusted tenant was accepted: %+v", task)
		}
		if task.Message.ExternalMessageID != "42" || !task.Deliver {
			t.Fatalf("unexpected normalized task: %+v", task)
		}
	case <-time.After(time.Second):
		t.Fatal("no task dispatched")
	}
}

func TestWebhookRejectsInvalidSignatureBeforeDispatch(t *testing.T) {
	t.Setenv("TG_SECRET", "webhook-secret")
	t.Setenv("TG_TOKEN", "unused-token")
	cfg := gatewayConfig(t)
	registry, _ := tenant.NewRegistry(cfg)
	dispatch := &captureDispatcher{tasks: make(chan worker.Task, 1)}
	server := NewServer(registry, channels.NewRegistry(channels.NewTelegram(nil)), dispatch, nil, metrics.NewMetrics(), "", "", nil)
	body := []byte(`{"update_id":42,"message":{"message_id":3,"text":"hello","from":{"id":7},"chat":{"id":9,"type":"private"}}}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/telegram/tenant-a-hook", bytes.NewReader(body))
	req.Header.Set("X-Telegram-Bot-Api-Secret-Token", "wrong")
	res := httptest.NewRecorder()
	server.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
	}
	select {
	case task := <-dispatch.tasks:
		t.Fatalf("invalid webhook was dispatched: %+v", task)
	default:
	}
}
