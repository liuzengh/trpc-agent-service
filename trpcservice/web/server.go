package web

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/platform"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// ReadinessGate controls whether the serving health endpoint may report ready.
// It is consulted without exposing dependency details to HTTP clients.
type ReadinessGate interface {
	Ready(context.Context) error
}

type Server struct {
	Runner       platform.Runner
	Store        platform.Store
	Resolver     tenant.TenantResolver
	Readiness    ReadinessGate
	AsyncIngress gateway.WebhookIngress
	adapters     map[string]channels.Adapter
	accepting    atomic.Bool
	draining     atomic.Bool
	readinessMu  sync.RWMutex
	// telemetryMiddlewareField is the optional server-owned HTTP observability
	// middleware. Nil keeps the historical handler chain. Telemetry never
	// changes status codes, response bodies, or routing decisions.
	telemetryMiddlewareField func(http.Handler) http.Handler
}

// SetTelemetryMiddleware attaches the optional observability middleware. It
// must be called before Handler() and is not safe for concurrent use.
func (s *Server) SetTelemetryMiddleware(middleware func(http.Handler) http.Handler) {
	s.telemetryMiddlewareField = middleware
}

func NewServer(store platform.Store, runner platform.Runner) *Server {
	server := &Server{Store: store, Runner: runner, adapters: map[string]channels.Adapter{"web": channels.WebAdapter{}, "telegram": channels.TelegramAdapter{}, "wecom": channels.WeComAdapter{}}}
	server.accepting.Store(true)
	return server
}

// SetReadiness changes the dependency gate used by healthz. The lock keeps
// readiness replacement safe while probes are in flight.
func (s *Server) SetReadiness(gate ReadinessGate) {
	if s == nil {
		return
	}
	s.readinessMu.Lock()
	s.Readiness = gate
	s.readinessMu.Unlock()
}

// SetAccepting controls whether new application requests may enter a handler.
// Health and liveness probes remain available while the server drains.
func (s *Server) SetAccepting(accepting bool) {
	if s == nil {
		return
	}
	s.accepting.Store(accepting)
}

// BeginDraining closes the application ingress gate before HTTP shutdown.
func (s *Server) BeginDraining() {
	if s == nil {
		return
	}
	s.draining.Store(true)
	s.accepting.Store(false)
}

func (s *Server) isAccepting() bool {
	return s != nil && s.accepting.Load() && !s.draining.Load()
}

func (s *Server) isDraining() bool {
	return s != nil && s.draining.Load()
}

func (s *Server) readinessGate() ReadinessGate {
	if s == nil {
		return nil
	}
	s.readinessMu.RLock()
	defer s.readinessMu.RUnlock()
	return s.Readiness
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", s.live)
	mux.HandleFunc("/healthz", s.health)
	mux.HandleFunc("/api/tenants", s.tenants)
	mux.HandleFunc("/api/chat", s.chat)
	mux.HandleFunc("/webhook/", s.webhook)
	return s.applyTelemetryMiddleware(requestLog(mux))
}

// telemetryMiddleware wraps the mux with the optional server-owned
// observability middleware. Nil keeps the historical handler chain; telemetry
// never changes status codes or response bodies.
// applyTelemetryMiddleware resolves the observability middleware lazily per
// request so the middleware may be attached after Handler() is first invoked
// (the production runtime assembles after the listener starts). Telemetry
// never changes status codes or response bodies.
func (s *Server) applyTelemetryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if middleware := s.telemetryMiddlewareField; middleware != nil {
			middleware(next).ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}
func (s *Server) live(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	if s.isDraining() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
		return
	}
	readiness := s.readinessGate()
	if readiness != nil {
		if err := readiness.Ready(r.Context()); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
func (s *Server) tenants(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	if _, err := requirePermission(r.Context(), "tenant.admin"); err != nil {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden"})
		return
	}
	var t platform.Tenant
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&t); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid json"})
		return
	}
	if err := s.Store.SaveTenant(r.Context(), t); err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"tenant_id": t.ID})
}
func (s *Server) chat(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeJSON(w, 405, nil)
		return
	}
	var in struct {
		ID        string `json:"id"`
		UserID    string `json:"user_id"`
		SessionID string `json:"session_id"`
		Text      string `json:"text"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&in); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid json"})
		return
	}
	tc, ok := tenant.FromContext(r.Context())
	if !ok || tc.Validate() != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "verified tenant context is required"})
		return
	}
	if in.ID == "" {
		in.ID = strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	if in.SessionID == "" {
		in.SessionID = channels.SessionID(tc.TenantID, "web", in.UserID, "")
	}
	trace, out, err := s.Runner.Run(r.Context(), platform.Message{ID: in.ID, TenantID: tc.TenantID, BindingID: tc.BindingID, Channel: "web", UserID: in.UserID, SessionID: in.SessionID, Content: in.Text})
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error(), "trace_id": trace})
		return
	}
	writeJSON(w, 200, map[string]string{"trace_id": trace, "session_id": in.SessionID, "reply": out})
}
func (s *Server) webhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeJSON(w, 405, nil)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 3 {
		writeJSON(w, 404, map[string]string{"error": "use /webhook/{channel}/{external_app_id}"})
		return
	}
	if !s.isAccepting() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "webhook request rejected"})
		return
	}
	if s.AsyncIngress != nil {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "cannot read body"})
			return
		}
		result := s.AsyncIngress.Handle(r.Context(), parts[1], parts[2], r, body)
		writeRaw(w, result.Status, result.ContentType, result.Body)
		return
	}
	adapter, ok := s.adapters[parts[1]]
	if !ok {
		writeJSON(w, 404, map[string]string{"error": "unsupported channel"})
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": "cannot read body"})
		return
	}
	if err = adapter.Verify(r, body); err != nil {
		writeJSON(w, 401, map[string]string{"error": err.Error()})
		return
	}
	msg, err := adapter.Parse(body)
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid webhook payload"})
		return
	}
	if s.Resolver == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "tenant resolver is not configured"})
		return
	}
	tc, err := s.Resolver.Resolve(r.Context(), tenant.ResolveRequest{Channel: adapter.Name(), ExternalAppID: parts[2], ExternalUser: msg.UserID, ExternalChat: msg.ChatID, RequestID: msg.ID, MessageID: msg.ID, TraceID: msg.ID})
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "binding resolution failed"})
		return
	}
	msg.TenantID = tc.TenantID
	if msg.ID == "" {
		writeJSON(w, 400, map[string]string{"error": "message id is required"})
		return
	}
	session := channels.SessionID(msg.TenantID, msg.Channel, msg.UserID, msg.ChatID)
	ctx := tenant.WithContext(r.Context(), tc)
	trace, out, err := s.Runner.Run(ctx, platform.Message{ID: msg.ID, TenantID: msg.TenantID, BindingID: tc.BindingID, Channel: msg.Channel, UserID: msg.UserID, SessionID: session, Content: msg.Text})
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error(), "trace_id": trace})
		return
	}
	response, _ := adapter.Reply(msg.ChatID, out)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(response)
}
func requirePermission(ctx context.Context, permission string) (tenant.TenantContext, error) {
	tc, ok := tenant.FromContext(ctx)
	if !ok || tc.Validate() != nil || !tc.HasPermission(permission) {
		return tenant.TenantContext{}, errors.New("permission denied")
	}
	return tc, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}

func writeRaw(w http.ResponseWriter, status int, contentType string, body []byte) {
	if contentType == "" {
		contentType = "application/json"
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(status)
	if len(body) > 0 {
		_, _ = w.Write(body)
	}
}

func requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { next.ServeHTTP(w, r) })
}
