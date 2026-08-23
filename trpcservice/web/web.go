// Package web serves the local-only administration and health API.
package web

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/DocJlm/trpc-agent-service/trpcservice/channels"
	"github.com/DocJlm/trpc-agent-service/trpcservice/config"
	"github.com/DocJlm/trpc-agent-service/trpcservice/store"
	"github.com/DocJlm/trpc-agent-service/trpcservice/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type ReadyStatus struct {
	Ready    bool                              `json:"ready"`
	Role     string                            `json:"role"`
	Channels map[string]channels.ChannelHealth `json:"channels,omitempty"`
}

type ReadyFunc func() ReadyStatus

type AgentRecord struct {
	TenantID         string         `json:"tenant_id"`
	ID               string         `json:"id"`
	Name             string         `json:"name"`
	Versions         map[string]any `json:"versions,omitempty"`
	PublishedVersion string         `json:"published_version,omitempty"`
}

type BindingRecord struct {
	TenantID string                `json:"tenant_id"`
	Binding  tenant.ChannelBinding `json:"binding"`
}

type BackendRecord struct {
	TenantID string                `json:"tenant_id"`
	ID       string                `json:"id"`
	Backend  tenant.BackendProfile `json:"backend"`
}

type Server struct {
	addr  string
	repo  store.Repository
	ready ReadyFunc
	mux   *http.ServeMux

	mu       sync.RWMutex
	tenants  map[string]tenant.Tenant
	agents   map[string]*AgentRecord
	bindings map[string]BindingRecord
	backends map[string]BackendRecord
}

func New(cfg config.Config, repo store.Repository, registry *prometheus.Registry, ready ReadyFunc) *Server {
	server := &Server{
		addr: cfg.HTTPAddr, repo: repo, ready: ready, mux: http.NewServeMux(),
		tenants: make(map[string]tenant.Tenant), agents: make(map[string]*AgentRecord),
		bindings: make(map[string]BindingRecord), backends: make(map[string]BackendRecord),
	}
	for _, item := range cfg.Tenants {
		server.tenants[item.ID] = item
	}
	server.routes(registry)
	return server
}

func (s *Server) routes(registry *prometheus.Registry) {
	s.mux.HandleFunc("GET /healthz", s.health)
	s.mux.HandleFunc("GET /readyz", s.readiness)
	if registry != nil {
		s.mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	}
	s.mux.HandleFunc("GET /api/v1/tenants", s.listTenants)
	s.mux.HandleFunc("POST /api/v1/tenants", s.createTenant)
	s.mux.HandleFunc("POST /api/v1/agents", s.createAgent)
	s.mux.HandleFunc("POST /api/v1/agents/", s.agentAction)
	s.mux.HandleFunc("POST /api/v1/channel-bindings", s.createBinding)
	s.mux.HandleFunc("POST /api/v1/backend-profiles", s.createBackend)
}

func (s *Server) Run(ctx context.Context) error {
	httpServer := &http.Server{
		Addr: s.addr, Handler: s.mux,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second,
	}
	done := make(chan error, 1)
	go func() { done <- httpServer.ListenAndServe() }()
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return httpServer.Shutdown(shutdownCtx)
	}
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	stats, err := s.repo.Stats(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "repository_unavailable", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "store": stats})
}

func (s *Server) readiness(w http.ResponseWriter, _ *http.Request) {
	status := ReadyStatus{Ready: true, Role: "unknown"}
	if s.ready != nil {
		status = s.ready()
	}
	code := http.StatusOK
	if !status.Ready {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, status)
}

func (s *Server) listTenants(w http.ResponseWriter, r *http.Request) {
	items, err := s.repo.ListTenants(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "list_tenants", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) createTenant(w http.ResponseWriter, r *http.Request) {
	var item tenant.Tenant
	if !decodeJSON(w, r, &item) {
		return
	}
	if err := item.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_tenant", err)
		return
	}
	if err := s.repo.SeedTenants(r.Context(), []tenant.Tenant{item}); err != nil {
		writeError(w, http.StatusInternalServerError, "persist_tenant", err)
		return
	}
	s.mu.Lock()
	s.tenants[item.ID] = item
	s.mu.Unlock()
	writeJSON(w, http.StatusCreated, item)
}

func (s *Server) createAgent(w http.ResponseWriter, r *http.Request) {
	var item AgentRecord
	if !decodeJSON(w, r, &item) {
		return
	}
	if item.TenantID == "" || item.ID == "" || item.Name == "" {
		writeError(w, http.StatusBadRequest, "invalid_agent", errors.New("tenant_id, id and name are required"))
		return
	}
	item.Versions = make(map[string]any)
	key := item.TenantID + "|" + item.ID
	if err := s.repo.CreateAgent(r.Context(), item.TenantID, item.ID, item.Name); err != nil {
		writeError(w, http.StatusConflict, "persist_agent", err)
		return
	}
	s.mu.Lock()
	s.agents[key] = &item
	s.mu.Unlock()
	writeJSON(w, http.StatusCreated, item)
}

func (s *Server) agentAction(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/agents/")
	if strings.HasSuffix(path, ":publish") {
		s.publishAgent(w, r, strings.TrimSuffix(path, ":publish"))
		return
	}
	if strings.HasSuffix(path, "/versions") {
		s.createAgentVersion(w, r, strings.TrimSuffix(path, "/versions"))
		return
	}
	writeError(w, http.StatusNotFound, "not_found", errors.New("unknown agent action"))
}

func (s *Server) createAgentVersion(w http.ResponseWriter, r *http.Request, id string) {
	var body struct {
		TenantID string `json:"tenant_id"`
		Version  string `json:"version"`
		Profile  any    `json:"profile"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	if body.Version == "" {
		writeError(w, http.StatusBadRequest, "invalid_version", errors.New("version is required"))
		return
	}
	profile, err := json.Marshal(body.Profile)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_profile", err)
		return
	}
	if err := s.repo.CreateAgentVersion(r.Context(), body.TenantID, id, body.Version, profile); err != nil {
		writeError(w, http.StatusConflict, "persist_agent_version", err)
		return
	}
	key := body.TenantID + "|" + id
	s.mu.Lock()
	defer s.mu.Unlock()
	agent := s.agents[key]
	if agent == nil {
		agent = &AgentRecord{TenantID: body.TenantID, ID: id, Versions: make(map[string]any)}
		s.agents[key] = agent
	}
	agent.Versions[body.Version] = body.Profile
	writeJSON(w, http.StatusCreated, body)
}

func (s *Server) publishAgent(w http.ResponseWriter, r *http.Request, id string) {
	var body struct {
		TenantID string `json:"tenant_id"`
		Version  string `json:"version"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	if err := s.repo.PublishAgent(r.Context(), body.TenantID, id, body.Version); err != nil {
		writeError(w, http.StatusConflict, "persist_publish", err)
		return
	}
	key := body.TenantID + "|" + id
	s.mu.Lock()
	defer s.mu.Unlock()
	agent := s.agents[key]
	if agent == nil {
		agent = &AgentRecord{TenantID: body.TenantID, ID: id, Versions: make(map[string]any)}
		s.agents[key] = agent
	}
	agent.PublishedVersion = body.Version
	writeJSON(w, http.StatusOK, agent)
}

func (s *Server) createBinding(w http.ResponseWriter, r *http.Request) {
	var item BindingRecord
	if !decodeJSON(w, r, &item) {
		return
	}
	if item.TenantID == "" || item.Binding.ID == "" || item.Binding.Type == "" || item.Binding.CredentialRef == "" {
		writeError(w, http.StatusBadRequest, "invalid_binding", errors.New("tenant_id and complete binding are required"))
		return
	}
	if err := s.repo.SaveChannelBinding(r.Context(), item.TenantID, item.Binding); err != nil {
		writeError(w, http.StatusConflict, "persist_binding", err)
		return
	}
	s.mu.Lock()
	s.bindings[item.TenantID+"|"+item.Binding.ID] = item
	s.mu.Unlock()
	writeJSON(w, http.StatusCreated, item)
}

func (s *Server) createBackend(w http.ResponseWriter, r *http.Request) {
	var item BackendRecord
	if !decodeJSON(w, r, &item) {
		return
	}
	if item.TenantID == "" || item.ID == "" {
		writeError(w, http.StatusBadRequest, "invalid_backend", errors.New("tenant_id and id are required"))
		return
	}
	if err := s.repo.SaveBackendProfile(r.Context(), item.TenantID, item.ID, item.Backend); err != nil {
		writeError(w, http.StatusConflict, "persist_backend", err)
		return
	}
	s.mu.Lock()
	s.backends[item.TenantID+"|"+item.ID] = item
	s.mu.Unlock()
	writeJSON(w, http.StatusCreated, item)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, destination any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err)
		return false
	}
	return true
}

func writeError(w http.ResponseWriter, status int, code string, err error) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": err.Error()}})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
