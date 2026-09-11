package web

import (
	"context"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/cyl6/trpc-agent-service/trpcservice"
	"github.com/cyl6/trpc-agent-service/trpcservice/adminauth"
	"github.com/cyl6/trpc-agent-service/trpcservice/budget"
	"github.com/cyl6/trpc-agent-service/trpcservice/channels"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/configcontrol"
	"github.com/cyl6/trpc-agent-service/trpcservice/contentsafety"
	"github.com/cyl6/trpc-agent-service/trpcservice/coordination"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
	"github.com/cyl6/trpc-agent-service/trpcservice/health"
	"github.com/cyl6/trpc-agent-service/trpcservice/metrics"
	"github.com/cyl6/trpc-agent-service/trpcservice/queue"
	"github.com/cyl6/trpc-agent-service/trpcservice/store"
	"github.com/cyl6/trpc-agent-service/trpcservice/tenant"
	"github.com/cyl6/trpc-agent-service/trpcservice/tenant/governance"
	"github.com/cyl6/trpc-agent-service/trpcservice/tooloperation"
	"github.com/cyl6/trpc-agent-service/trpcservice/worker"
)

//go:embed static
var staticFiles embed.FS

// indexHTML is the console entry page served at "/" with build metadata
// interpolated. It is read once at package init because it never changes at
// runtime.
var indexHTML = func() []byte {
	data, err := staticFiles.ReadFile("static/index.html")
	if err != nil {
		panic("web: embedded static/index.html missing: " + err.Error())
	}
	return data
}()

const maxWebhookBody = 1 << 20

var tracer = otel.Tracer("trpc-agent-service/web")

type DirectProcessor interface {
	Process(context.Context, worker.Task) (worker.Result, error)
}

type ToolOperationOperator interface {
	ListUnknownToolOperations(context.Context, string, int) ([]tooloperation.Record, error)
	ResolveToolOperation(context.Context, tooloperation.ResolveRequest) (*tooloperation.Record, error)
}

type resolveToolOperationRequest struct {
	TenantID        string                         `json:"tenant_id"`
	ResolutionID    string                         `json:"resolution_id"`
	ExpectedVersion int64                          `json:"expected_version"`
	Action          tooloperation.ResolutionAction `json:"action"`
	ReasonCode      string                         `json:"reason_code"`
	ResultHash      string                         `json:"result_hash,omitempty"`
	ReplayReference string                         `json:"replay_reference,omitempty"`
	RetryAt         time.Time                      `json:"retry_at,omitempty"`
}

type Server struct {
	tenants       *tenant.Registry
	channels      *channels.Registry
	dispatcher    queue.Dispatcher
	direct        DirectProcessor
	metrics       *metrics.Metrics
	adminTokenEnv string
	configPath    string
	staticConfig  *config.Config
	control       *configcontrol.Controller
	adminAuth     adminauth.Authenticator
	adminAudit    func(context.Context, adminauth.Principal, adminauth.Action, string, string, string) error
	health        *health.Registry
	custom        customModelState
	mux           *http.ServeMux
	draining      atomic.Bool
}

func NewServer(
	tenants *tenant.Registry,
	channelRegistry *channels.Registry,
	dispatcher queue.Dispatcher,
	direct DirectProcessor,
	metricsExporter *metrics.Metrics,
	adminTokenEnv, configPath string,
	initialConfig *config.Config,
) *Server {
	if metricsExporter == nil {
		metricsExporter = metrics.NewMetrics()
	}
	s := &Server{
		tenants: tenants, channels: channelRegistry, dispatcher: dispatcher, direct: direct,
		metrics: metricsExporter, adminTokenEnv: adminTokenEnv, configPath: configPath,
		staticConfig: initialConfig,
		mux:          http.NewServeMux(),
	}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCtx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		startOptions := []oteltrace.SpanStartOption{oteltrace.WithNewRoot()}
		if remoteSpan := oteltrace.SpanContextFromContext(remoteCtx); remoteSpan.IsValid() {
			startOptions = append(startOptions, oteltrace.WithLinks(oteltrace.Link{SpanContext: remoteSpan}))
		}
		ctx, span := tracer.Start(r.Context(), "gateway.request", startOptions...)
		span.SetAttributes(attribute.String("http.method", r.Method))
		s.mux.ServeHTTP(w, r.WithContext(ctx))
		span.End()
	})
}

func (s *Server) SetControlPlane(controller *configcontrol.Controller) {
	s.control = controller
}

func (s *Server) SetAdminAuthenticator(authenticator adminauth.Authenticator) {
	s.adminAuth = authenticator
}

func (s *Server) SetAdminAudit(writer func(context.Context, adminauth.Principal, adminauth.Action, string, string, string) error) {
	s.adminAudit = writer
}

func (s *Server) SetHealthRegistry(registry *health.Registry) {
	s.health = registry
}

// SetDraining removes the instance from readiness before HTTP shutdown while
// allowing liveness to remain healthy during the drain window.
func (s *Server) SetDraining() { s.draining.Store(true) }

func (s *Server) routes() {
	s.mux.HandleFunc("GET /{$}", s.index)
	// SPA history-API routes: every entry path renders the same index.html and
	// the client router takes over from there.
	for _, path := range []string{"/login", "/console", "/monitor", "/message/{id}"} {
		s.mux.HandleFunc("GET "+path, s.index)
	}
	s.mux.HandleFunc("GET /static/{asset...}", s.staticAsset)
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	s.mux.HandleFunc("GET /readyz", s.ready)
	s.mux.HandleFunc("POST /-/drain", s.drain)
	s.mux.HandleFunc("GET /admin/v1/health/dependencies", s.requireAdmin(s.listDependencyHealth))
	s.mux.Handle("GET /metrics", s.metrics)
	s.mux.HandleFunc("POST /webhooks/{channel}/{binding}", s.webhook)
	s.mux.HandleFunc("GET /webhooks/{channel}/{binding}", s.webhookURLVerify)
	s.mux.HandleFunc("POST /v1/chat/{tenant}", s.requireAdmin(s.directChat))
	s.mux.HandleFunc("GET /admin/v1/tenants", s.requireAdmin(s.listTenants))
	s.mux.HandleFunc("GET /admin/v1/tenants/{tenant}/revisions", s.requireAdmin(s.listTenantRevisions))
	s.mux.HandleFunc("POST /admin/v1/tenants/{tenant}/revisions", s.requireAdmin(s.createTenantRevision))
	s.mux.HandleFunc("POST /admin/v1/tenants/{tenant}/releases", s.requireAdmin(s.createTenantRelease))
	s.mux.HandleFunc("POST /admin/v1/tenants/{tenant}/releases/{release}/promote", s.requireAdmin(s.promoteTenantRelease))
	s.mux.HandleFunc("GET /admin/v1/tenants/{tenant}/releases/{release}", s.requireAdmin(s.getTenantRelease))
	s.mux.HandleFunc("GET /admin/v1/config/nodes", s.requireAdmin(s.listConfigNodes))
	s.mux.HandleFunc("POST /admin/v1/tenants/custom-model", s.requireAdmin(s.addCustomModel))
	s.mux.HandleFunc("POST /admin/v1/reload", s.requireAdmin(s.reload))
	s.mux.HandleFunc("POST /admin/v1/tenants/{tenant}/rollback", s.requireAdmin(s.rollback))
	s.mux.HandleFunc("GET /admin/v1/outbox/uncertain", s.requireAdmin(s.listUncertainOutbox))
	s.mux.HandleFunc("POST /admin/v1/outbox/{outbox}/resolve", s.requireAdmin(s.resolveOutbox))
	s.mux.HandleFunc("GET /admin/v1/tool-operations/uncertain", s.requireAdmin(s.listUncertainToolOperations))
	s.mux.HandleFunc("POST /admin/v1/tool-operations/{operation}/resolve", s.requireAdmin(s.resolveToolOperation))
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() || s.dispatcher == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
		return
	}
	if s.control != nil && !s.control.Ready() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
		return
	}
	if s.health != nil && !s.health.Ready() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
		return
	}
	if probe, ok := s.dispatcher.(queue.ReadinessProbe); ok {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		err := probe.Ready(ctx)
		cancel()
		if err != nil {
			// Dependency details may contain endpoints. Keep the public probe
			// response stable and export diagnosis through controlled telemetry.
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) drain(w http.ResponseWriter, r *http.Request) {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil || (host != "127.0.0.1" && host != "::1") {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	s.SetDraining()
	writeJSON(w, http.StatusOK, map[string]string{"status": "draining"})
}

func (s *Server) webhook(w http.ResponseWriter, r *http.Request) {
	if s.control != nil && !s.control.Ready() {
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	remoteCtx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	startOptions := []oteltrace.SpanStartOption{oteltrace.WithNewRoot()}
	if remoteSpan := oteltrace.SpanContextFromContext(remoteCtx); remoteSpan.IsValid() {
		// The public callback header is untrusted. Link it for diagnostics, but
		// never let it choose our parent, sampling decision, or baggage.
		startOptions = append(startOptions, oteltrace.WithLinks(oteltrace.Link{SpanContext: remoteSpan}))
	}
	ctx, span := tracer.Start(r.Context(), "im.callback", startOptions...)
	defer span.End()
	channelType := r.PathValue("channel")
	bindingID := r.PathValue("binding")
	binding, err := s.tenants.ResolveBinding(channelType, bindingID)
	if err != nil {
		span.SetStatus(codes.Error, "binding not found")
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	span.SetAttributes(attribute.String("tenant.id", binding.Tenant.TenantID), attribute.String("channel", channelType))
	adapter, err := s.channels.Get(channelType)
	if err != nil {
		http.Error(w, "channel unavailable", http.StatusNotFound)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxWebhookBody))
	if err != nil {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return
	}
	verifyCtx, verifySpan := tracer.Start(ctx, "signature.verify")
	err = adapter.Verify(r.WithContext(verifyCtx), body, binding.Channel)
	verifySpan.End()
	if err != nil {
		span.SetStatus(codes.Error, "invalid_signature")
		s.metrics.Add("im_callbacks_total", "Inbound IM callback attempts.", 1, map[string]string{"tenant": binding.Tenant.TenantID, "channel": channelType, "result": "invalid_signature"})
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	parsed, err := adapter.Parse(body, binding.Channel)
	if errors.Is(err, channels.ErrUnsupportedEvent) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ignored"})
		return
	}
	if err != nil {
		span.SetStatus(codes.Error, "invalid_event")
		http.Error(w, "bad event", http.StatusBadRequest)
		return
	}
	if parsed.Challenge != "" {
		writeJSON(w, http.StatusOK, map[string]string{"challenge": parsed.Challenge})
		return
	}
	for _, msg := range parsed.Messages {
		msg.TenantID = binding.Tenant.TenantID
		msg.BindingID = binding.Channel.BindingID
		msg.Channel = binding.Channel.Type
		_, sessionID := domain.Identity(msg, binding.Tenant.App.Name)
		routed, generation, routeErr := s.tenants.ResolveBindingForSession(
			binding.Tenant.TenantID, sessionID, channelType, bindingID,
		)
		if routeErr != nil {
			http.Error(w, "configuration unavailable", http.StatusServiceUnavailable)
			return
		}
		carrier := propagation.MapCarrier{}
		otel.GetTextMapPropagator().Inject(ctx, carrier)
		task := worker.Task{
			Tenant: routed.Tenant, Binding: routed.Channel, Message: msg,
			ConfigRevision: routed.Tenant.Version, ConfigGeneration: generation,
			Deliver: true, TraceCarrier: map[string]string(carrier),
		}
		if err := s.dispatcher.Submit(ctx, task); err != nil {
			span.SetStatus(codes.Error, "queue_unavailable")
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
	}
	s.metrics.Add("im_callbacks_total", "Inbound IM callback attempts.", 1, map[string]string{"tenant": binding.Tenant.TenantID, "channel": channelType, "result": "accepted"})
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

// webhookURLVerify serves providers that register the callback URL with a GET
// challenge (WeCom echoes a decrypted random string). Only adapters opting in
// via channels.URLVerifier are accepted; others get an explicit 405.
func (s *Server) webhookURLVerify(w http.ResponseWriter, r *http.Request) {
	_, span := tracer.Start(r.Context(), "im.url_verify", oteltrace.WithNewRoot())
	defer span.End()
	channelType := r.PathValue("channel")
	bindingID := r.PathValue("binding")
	binding, err := s.tenants.ResolveBinding(channelType, bindingID)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	span.SetAttributes(attribute.String("tenant.id", binding.Tenant.TenantID), attribute.String("channel", channelType))
	adapter, err := s.channels.Get(channelType)
	if err != nil {
		http.Error(w, "channel unavailable", http.StatusNotFound)
		return
	}
	verifier, ok := adapter.(channels.URLVerifier)
	if !ok {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	echo, err := verifier.VerifyURL(r, binding.Channel)
	if err != nil {
		span.SetStatus(codes.Error, "invalid_signature")
		s.metrics.Add("im_callbacks_total", "Inbound IM callback attempts.", 1, map[string]string{"tenant": binding.Tenant.TenantID, "channel": channelType, "result": "invalid_signature"})
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	s.metrics.Add("im_callbacks_total", "Inbound IM callback attempts.", 1, map[string]string{"tenant": binding.Tenant.TenantID, "channel": channelType, "result": "verified"})
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, echo)
}

type directRequest struct {
	MessageID      string       `json:"message_id"`
	UserID         string       `json:"user_id"`
	ConversationID string       `json:"conversation_id"`
	ThreadID       string       `json:"thread_id,omitempty"`
	Scope          domain.Scope `json:"scope"`
	Text           string       `json:"text"`
}

func (s *Server) directChat(w http.ResponseWriter, r *http.Request) {
	if s.direct == nil {
		http.Error(w, "direct processor unavailable", http.StatusServiceUnavailable)
		return
	}
	if s.control != nil && !s.control.Ready() {
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	tenantConfig, err := s.tenants.Tenant(r.PathValue("tenant"))
	if err != nil {
		http.Error(w, "tenant not found", http.StatusNotFound)
		return
	}
	var request directRequest
	if err := decodeJSONBody(w, r, &request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "请求 JSON 格式无效", "code": "invalid_request"})
		return
	}
	request.MessageID = strings.TrimSpace(request.MessageID)
	request.UserID = strings.TrimSpace(request.UserID)
	request.ConversationID = strings.TrimSpace(request.ConversationID)
	request.ThreadID = strings.TrimSpace(request.ThreadID)
	if request.UserID == "" || strings.TrimSpace(request.Text) == "" {
		http.Error(w, "user_id and text are required", http.StatusBadRequest)
		return
	}
	if request.MessageID == "" {
		request.MessageID = uuid.NewString()
	}
	if request.Scope == "" {
		request.Scope = domain.ScopeDirect
	}
	if request.Scope != domain.ScopeDirect && request.Scope != domain.ScopeGroup {
		http.Error(w, "scope must be direct or group", http.StatusBadRequest)
		return
	}
	if request.ConversationID == "" {
		request.ConversationID = request.UserID
	}
	binding := config.ChannelConfig{Type: "api", BindingID: "admin-api", Enabled: true, MaxMessageLength: 40000}
	message := domain.InboundMessage{
		TenantID: tenantConfig.TenantID, BindingID: binding.BindingID, Channel: binding.Type,
		ExternalMessageID: request.MessageID, ExternalUserID: request.UserID,
		ConversationID: request.ConversationID, ThreadID: request.ThreadID,
		Scope: request.Scope, Text: request.Text, ReceivedAt: time.Now().UTC(),
	}
	_, sessionID := domain.Identity(message, tenantConfig.App.Name)
	routedTenant, generation, routeErr := s.tenants.ResolveForSession(tenantConfig.TenantID, sessionID)
	if routeErr != nil {
		http.Error(w, "tenant not found", http.StatusNotFound)
		return
	}
	result, err := s.direct.Process(r.Context(), worker.Task{
		Tenant: routedTenant, Binding: binding, ConfigRevision: routedTenant.Version,
		ConfigGeneration: generation, Deliver: false, Message: message,
	})
	if err != nil {
		status, code, message := publicError(err)
		writeJSON(w, status, map[string]string{
			"error": message, "code": code, "request_id": result.RequestID,
		})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) listTenants(w http.ResponseWriter, r *http.Request) {
	tenants := s.tenants.List()
	_, customNames := s.custom.list()
	redacted := make([]map[string]any, 0, len(tenants))
	principal, scoped := r.Context().Value(adminPrincipalKey{}).(adminauth.Principal)
	for _, tenantConfig := range tenants {
		if scoped && !principal.Allows(adminauth.ActionView, tenantConfig.TenantID) {
			continue
		}
		channels := make([]map[string]any, 0, len(tenantConfig.Channels))
		for _, binding := range tenantConfig.Channels {
			channels = append(channels, map[string]any{
				"type": binding.Type, "binding_id": binding.BindingID, "enabled": binding.Enabled,
			})
		}
		appName := tenantConfig.App.Name
		if displayName, ok := customNames[tenantConfig.TenantID]; ok {
			appName = displayName
		}
		redacted = append(redacted, map[string]any{
			"tenant_id": tenantConfig.TenantID, "version": tenantConfig.Version, "enabled": tenantConfig.Enabled,
			"app": map[string]any{"name": appName, "agent_name": tenantConfig.App.AgentName},
			"model": map[string]any{
				"provider": tenantConfig.Model.Provider, "name": tenantConfig.Model.Name,
				"variant": tenantConfig.Model.Variant, "streaming": tenantConfig.Model.Streaming,
				"fallback_provider": tenantConfig.Model.FallbackProvider,
			},
			"channels": channels,
			"data": map[string]string{
				"session": tenantConfig.Data.Session.Type, "memory": tenantConfig.Data.Memory.Type,
				"summary": tenantConfig.Data.Summary.Type, "artifact": tenantConfig.Data.Artifact.Type,
				"knowledge": tenantConfig.Data.Knowledge.Type, "audit_log": tenantConfig.Data.AuditLog.Type,
			},
		})
		if s.control != nil {
			if state, ok := s.tenants.ControlState(tenantConfig.TenantID); ok {
				item := redacted[len(redacted)-1]
				item["active_revision"] = state.ActiveRevision
				item["canary_revision"] = state.CanaryRevision
				item["rollout_percent"] = state.RolloutPercent
				item["generation"] = state.Generation
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"tenants": redacted})
}

func (s *Server) reload(w http.ResponseWriter, r *http.Request) {
	s.custom.provision.Lock()
	defer s.custom.provision.Unlock()
	if s.configPath == "" {
		http.Error(w, "config reload is disabled", http.StatusNotImplemented)
		return
	}
	cfg, err := config.LoadStatic(s.configPath)
	if err != nil {
		http.Error(w, "invalid configuration", http.StatusBadRequest)
		return
	}
	if s.staticConfig != nil && (cfg.Server != s.staticConfig.Server || cfg.Coordination != s.staticConfig.Coordination || cfg.Queue != s.staticConfig.Queue || cfg.ControlPlane != s.staticConfig.ControlPlane || cfg.Telemetry != s.staticConfig.Telemetry) {
		http.Error(w, "reload only accepts tenant revisions; restart to change server, queue, coordination, or telemetry", http.StatusConflict)
		return
	}
	// Console-registered custom models are runtime-only state: re-apply them on
	// top of the freshly loaded YAML so a reload never silently drops them.
	customTenants, _ := s.custom.list()
	known := make(map[string]struct{}, len(cfg.Tenants))
	for _, t := range cfg.Tenants {
		known[t.TenantID] = struct{}{}
	}
	for _, t := range customTenants {
		if _, exists := known[t.TenantID]; !exists {
			cfg.Tenants = append(cfg.Tenants, t)
		}
	}
	if s.control != nil {
		expected := map[string]int64{}
		if generation, ok := expectedGeneration(r); ok {
			for _, tenantConfig := range cfg.Tenants {
				expected[tenantConfig.TenantID] = generation
			}
		}
		releases, err := s.control.ImportAndFullRelease(r.Context(), cfg.Tenants, "admin_api", "YAML reload", expected)
		if err != nil {
			writeControlError(w, err)
			return
		}
		status := http.StatusAccepted
		if len(releases) == 0 {
			status = http.StatusOK
		}
		writeJSON(w, status, map[string]any{"status": "pending", "releases": releases})
		return
	}
	if err := s.tenants.Apply(cfg); err != nil {
		http.Error(w, "configuration rejected", http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "reloaded"})
}

func (s *Server) rollback(w http.ResponseWriter, r *http.Request) {
	if s.control != nil {
		request, err := decodeRollbackRequest(w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		generation, _ := expectedGeneration(r)
		if request.ExpectedGeneration > 0 {
			generation = request.ExpectedGeneration
		}
		release, err := s.control.Rollback(r.Context(), r.PathValue("tenant"), request.TargetRevision, "admin_api", request.Reason, generation)
		if err != nil {
			writeControlError(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"status": string(release.Status), "release": release})
		return
	}
	revision, err := s.tenants.Rollback(r.PathValue("tenant"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "rolled_back", "version": revision.Version})
}

func (s *Server) listUncertainOutbox(w http.ResponseWriter, r *http.Request) {
	operator, ok := s.dispatcher.(queue.OutboxOperator)
	if !ok {
		http.Error(w, "durable outbox operations are unavailable", http.StatusNotImplemented)
		return
	}
	limit := 50
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 || parsed > 200 {
			http.Error(w, "limit must be between 1 and 200", http.StatusBadRequest)
			return
		}
		limit = parsed
	}
	operations, err := operator.ListUncertainOutbox(r.Context(), limit)
	if err != nil {
		http.Error(w, "outbox ledger unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"operations": operations})
}

func (s *Server) resolveOutbox(w http.ResponseWriter, r *http.Request) {
	operator, ok := s.dispatcher.(queue.OutboxOperator)
	if !ok {
		http.Error(w, "durable outbox operations are unavailable", http.StatusNotImplemented)
		return
	}
	outboxID := strings.TrimSpace(r.PathValue("outbox"))
	if _, err := uuid.Parse(outboxID); err != nil {
		http.Error(w, "invalid outbox id", http.StatusBadRequest)
		return
	}
	var request queue.ResolveOutboxRequest
	if err := decodeJSONBody(w, r, &request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, err := uuid.Parse(request.ResolutionID); err != nil {
		http.Error(w, "resolution_id must be a UUID", http.StatusBadRequest)
		return
	}
	request.Reason = strings.TrimSpace(request.Reason)
	if request.ExpectedVersion <= 0 || request.ExpectedAttempt <= 0 {
		http.Error(w, "expected_version and expected_attempt must be positive", http.StatusBadRequest)
		return
	}
	if len(request.Reason) == 0 || len(request.Reason) > 512 {
		http.Error(w, "reason must contain 1 to 512 bytes", http.StatusBadRequest)
		return
	}
	switch request.Action {
	case store.ResolveAssumeDelivered, store.ResolveRetry, store.ResolveCancel:
	default:
		http.Error(w, "unsupported resolution action", http.StatusBadRequest)
		return
	}
	request.OutboxID = outboxID
	request.Actor = "admin_api"
	if err := operator.ResolveOutbox(r.Context(), request); err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			http.Error(w, "outbox operation not found", http.StatusNotFound)
		case errors.Is(err, store.ErrInvalidTransition), errors.Is(err, store.ErrOperationConflict):
			http.Error(w, "outbox operation changed; refresh and retry", http.StatusConflict)
		default:
			http.Error(w, "outbox ledger unavailable", http.StatusServiceUnavailable)
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "resolved", "action": request.Action})
}

func (s *Server) listUncertainToolOperations(w http.ResponseWriter, r *http.Request) {
	operator, ok := s.direct.(ToolOperationOperator)
	if !ok {
		http.Error(w, "tool operation ledger is unavailable", http.StatusNotImplemented)
		return
	}
	tenantID := strings.TrimSpace(r.URL.Query().Get("tenant_id"))
	if tenantID == "" {
		http.Error(w, "tenant_id is required", http.StatusBadRequest)
		return
	}
	limit := 50
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 || parsed > 100 {
			http.Error(w, "limit must be between 1 and 100", http.StatusBadRequest)
			return
		}
		limit = parsed
	}
	operations, err := operator.ListUnknownToolOperations(r.Context(), tenantID, limit)
	if err != nil {
		if errors.Is(err, tooloperation.ErrInvalidRequest) {
			http.Error(w, "invalid tenant_id or limit", http.StatusBadRequest)
			return
		}
		http.Error(w, "tool operation ledger unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"operations": operations})
}

func (s *Server) resolveToolOperation(w http.ResponseWriter, r *http.Request) {
	operator, ok := s.direct.(ToolOperationOperator)
	if !ok {
		http.Error(w, "tool operation ledger is unavailable", http.StatusNotImplemented)
		return
	}
	operationKey := strings.TrimSpace(r.PathValue("operation"))
	if operationKey == "" || len(operationKey) > 256 {
		http.Error(w, "invalid operation key", http.StatusBadRequest)
		return
	}
	var request resolveToolOperationRequest
	if err := decodeJSONBody(w, r, &request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	request.TenantID = strings.TrimSpace(request.TenantID)
	request.ReasonCode = strings.TrimSpace(request.ReasonCode)
	if request.TenantID == "" || request.ExpectedVersion <= 0 {
		http.Error(w, "tenant_id and positive expected_version are required", http.StatusBadRequest)
		return
	}
	if _, err := uuid.Parse(request.ResolutionID); err != nil {
		http.Error(w, "resolution_id must be a UUID", http.StatusBadRequest)
		return
	}
	switch request.Action {
	case tooloperation.ResolveConfirm, tooloperation.ResolveRetryNotApplied, tooloperation.ResolveReject:
	default:
		http.Error(w, "unsupported resolution action", http.StatusBadRequest)
		return
	}
	record, err := operator.ResolveToolOperation(r.Context(), tooloperation.ResolveRequest{
		ResolutionID: request.ResolutionID, TenantID: request.TenantID,
		OperationKey: operationKey, ExpectedVersion: request.ExpectedVersion,
		Action: request.Action, ActorHash: tooloperation.HashPayload([]byte("admin_api")),
		ReasonCode: request.ReasonCode, ResultHash: request.ResultHash,
		ReplayReference: request.ReplayReference, RetryAt: request.RetryAt,
	})
	if err != nil {
		switch {
		case errors.Is(err, tooloperation.ErrNotFound):
			http.Error(w, "tool operation not found", http.StatusNotFound)
		case errors.Is(err, tooloperation.ErrInvalidRequest):
			http.Error(w, "invalid tool operation resolution", http.StatusBadRequest)
		case errors.Is(err, tooloperation.ErrConflict),
			errors.Is(err, tooloperation.ErrInvalidTransition),
			errors.Is(err, tooloperation.ErrLeaseLost),
			errors.Is(err, tooloperation.ErrUnknownRequiresResolution):
			http.Error(w, "tool operation changed; refresh and retry", http.StatusConflict)
		default:
			http.Error(w, "tool operation ledger unavailable", http.StatusServiceUnavailable)
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "resolved", "action": request.Action, "operation": record,
	})
}

func (s *Server) requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.adminTokenEnv == "" && s.adminAuth == nil {
			http.Error(w, "admin API disabled", http.StatusNotFound)
			return
		}
		token := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		if token == "" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var principal adminauth.Principal
		if s.adminAuth != nil {
			var err error
			principal, err = s.adminAuth.Authenticate(r.Context(), token)
			if err != nil {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		} else {
			secret, err := config.Secret(s.adminTokenEnv)
			if err != nil {
				http.Error(w, "admin API unavailable", http.StatusServiceUnavailable)
				return
			}
			if len(token) != len(secret) || subtle.ConstantTimeCompare([]byte(token), []byte(secret)) != 1 {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			principal = adminauth.Principal{Subject: "local-session-user", Roles: []string{"admin"}, Tenants: []string{"*"}}
		}
		action := adminauth.ActionView
		if r.Method != http.MethodGet {
			action = adminauth.ActionOperate
		}
		if strings.Contains(r.URL.Path, "/custom-model") || strings.Contains(r.URL.Path, "/security/") {
			action = adminauth.ActionManageSecurity
		}
		tenantID := r.PathValue("tenant")
		operation := r.Method + " " + r.URL.Path
		allowed := principal.Allows(action, tenantID)
		// These endpoints contain no mutation and either return dependency
		// status or filter the tenant collection below.  Let a scoped viewer
		// inspect them without turning an empty URL tenant into an implicit
		// all-tenant grant; global writes and unscoped operational ledgers still
		// require an explicit all-tenant scope.
		if !allowed && action == adminauth.ActionView && tenantID == "" &&
			(r.URL.Path == "/admin/v1/health/dependencies" || r.URL.Path == "/admin/v1/tenants") {
			allowed = principal.AllowsGlobal(action)
		}
		if !allowed {
			if s.adminAudit != nil {
				_ = s.adminAudit(r.Context(), principal, action, tenantID, operation, "forbidden")
			}
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		if s.adminAudit != nil {
			if err := s.adminAudit(r.Context(), principal, action, tenantID, operation, "accepted"); err != nil {
				http.Error(w, "admin audit unavailable", http.StatusServiceUnavailable)
				return
			}
		}
		ctx := context.WithValue(r.Context(), adminPrincipalKey{}, principal)
		r = r.WithContext(ctx)
		next(w, r)
	}
}

type adminPrincipalKey struct{}

func (s *Server) index(w http.ResponseWriter, _ *http.Request) {
	page := strings.ReplaceAll(string(indexHTML), "__VERSION__", trpcservice.Version)
	page = strings.ReplaceAll(page, "__COMMIT__", trpcservice.GitCommit)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = io.WriteString(w, page)
}

// staticAsset serves the embedded console assets. Only whitelisted
// extensions are accepted so the handler can never be tricked into serving
// Go sources or nested paths.
func (s *Server) staticAsset(w http.ResponseWriter, r *http.Request) {
	asset := r.PathValue("asset")
	if asset == "" || strings.Contains(asset, "..") {
		http.NotFound(w, r)
		return
	}
	var contentType string
	switch {
	case strings.HasSuffix(asset, ".css"):
		contentType = "text/css; charset=utf-8"
	case strings.HasSuffix(asset, ".js"):
		contentType = "application/javascript; charset=utf-8"
	case strings.HasSuffix(asset, ".html"):
		contentType = "text/html; charset=utf-8"
	case strings.HasSuffix(asset, ".svg"):
		contentType = "image/svg+xml"
	case strings.HasSuffix(asset, ".png"):
		contentType = "image/png"
	case strings.HasSuffix(asset, ".json"):
		contentType = "application/json"
	default:
		http.NotFound(w, r)
		return
	}
	data, err := staticFiles.ReadFile("static/" + asset)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "public, max-age=300")
	_, _ = w.Write(data)
}

func (s *Server) listDependencyHealth(w http.ResponseWriter, _ *http.Request) {
	if s.health == nil {
		writeJSON(w, http.StatusOK, map[string]any{"status": "unknown", "dependencies": []any{}})
		return
	}
	status := "ready"
	if !s.health.Ready() {
		status = "degraded"
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": status, "dependencies": s.health.Snapshot()})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func publicError(err error) (status int, code, message string) {
	switch {
	case err == nil:
		return http.StatusOK, "", ""
	case errors.Is(err, governance.ErrUserDenied):
		return http.StatusForbidden, "permission_denied", "当前用户或操作未获授权"
	case errors.Is(err, governance.ErrRateLimited):
		return http.StatusTooManyRequests, "rate_limited", "请求过于频繁，请稍后重试"
	case errors.Is(err, governance.ErrRateLimiterUnavailable):
		return http.StatusServiceUnavailable, "rate_limiter_unavailable", "共享限流服务暂时不可用，请稍后重试"
	case errors.Is(err, governance.ErrBudgetExceeded):
		return http.StatusTooManyRequests, "budget_exceeded", "当前租户预算已用尽"
	case errors.Is(err, budget.ErrLedgerUnavailable):
		return http.StatusServiceUnavailable, "budget_ledger_unavailable", "预算账本暂时不可用，请稍后重试"
	case errors.Is(err, governance.ErrInputTooLarge):
		return http.StatusRequestEntityTooLarge, "input_too_large", "消息内容超过当前租户限制"
	case errors.Is(err, coordination.ErrClaimInProgress):
		return http.StatusConflict, "request_in_progress", "同一消息正在处理中，请稍后重试"
	case errors.Is(err, worker.ErrTenantFrozen):
		return http.StatusServiceUnavailable, "tenant_maintenance", "当前租户正在进行数据维护，请稍后重试"
	case errors.Is(err, contentsafety.ErrBlocked):
		return http.StatusForbidden, "content_safety_blocked", "内容安全策略拒绝了该请求"
	case errors.Is(err, contentsafety.ErrUnavailable):
		return http.StatusServiceUnavailable, "content_safety_unavailable", "内容安全服务暂时不可用，请稍后重试"
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout, "model_timeout", "模型调用超时，请稍后重试"
	case errors.Is(err, context.Canceled):
		return http.StatusRequestTimeout, "request_canceled", "请求已取消"
	default:
		// Provider errors may echo credentials, prompts, or upstream response
		// bodies. Return a useful stable hint without forwarding err.Error().
		return http.StatusBadGateway, "model_request_failed", "模型调用失败，请检查 API Key、模型 ID 和请求地址"
	}
}

func (s *Server) String() string {
	return fmt.Sprintf("gateway(config=%s)", s.configPath)
}
