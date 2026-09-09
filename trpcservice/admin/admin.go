// Package admin exposes the tenant management API (proposal doc §7, Admin
// API slice 2): CRUD over tenants and their channel bindings, atomically
// persisted to the YAML config file and hot-applied to the runner registry
// without a restart.
package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Service serializes config mutations: every request clones the current
// config, mutates the clone, then validates + saves + hot-applies it. A
// failure at any stage leaves the running config untouched.
type Service struct {
	mu   sync.Mutex
	path string
	cfg  *config.Config
	reg  *agent.Registry
	aud  *audit.Logger
}

// NewService builds the admin service over the live config and registry.
// path is the YAML file every mutation is persisted to; aud may be nil for
// log-only auditing.
func NewService(path string, cfg *config.Config, reg *agent.Registry, aud *audit.Logger) *Service {
	return &Service{path: path, cfg: cfg, reg: reg, aud: aud}
}

// auditAdmin records one mutation attempt on the governance trail. The
// trace id of the admin request is attached so a mutation can be correlated
// with the spans and log lines it produced.
func (s *Service) auditAdmin(ctx context.Context, tenantID, op string, err error) {
	rec := audit.Record{
		Event: audit.EventAdmin, TenantID: tenantID,
		Decision: audit.DecisionOK, Detail: op,
	}
	if sc := trace.SpanContextFromContext(ctx); sc.HasTraceID() {
		rec.TraceID = sc.TraceID().String()
	}
	if err != nil {
		rec.Decision = audit.DecisionError
		rec.ErrorType = "commit"
		rec.Detail = op + ": " + err.Error()
	}
	s.aud.Log(rec)
}

// Guardrails resolves a tenant's current policy for the gateway, so
// hot-updated policies take effect without rewiring it.
func (s *Service) Guardrails(tenantID string) tenant.Guardrails {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.cfg.Tenants[tenantID]; ok {
		return t.Guardrails
	}
	return tenant.Guardrails{}
}

// WeComBinding resolves a tenant's current binding for channel adapters,
// so hot-updated bindings take effect without rewiring the gateway.
func (s *Service) WeComBinding(tenantID string) (*tenant.WeComBinding, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg.WeComBinding(tenantID)
}

// WeChatKfBinding resolves a tenant's WeChat KF binding for the adapter,
// with the same hot-update semantics as WeComBinding.
func (s *Service) WeChatKfBinding(tenantID string) (*tenant.WeChatKfBinding, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg.WeChatKfBinding(tenantID)
}

// Handler mounts /admin/tenants and /admin/tenants/{id}. Every request runs
// inside an admin.request span so mutations carry a trace id end to end.
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/tenants", s.handleCollection)
	mux.HandleFunc("/admin/tenants/", s.handleItem)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := otel.Tracer(metrics.ServiceName).Start(r.Context(), "admin.request",
			trace.WithAttributes(
				attribute.String("http.method", r.Method),
				attribute.String("http.target", r.URL.Path),
			))
		defer span.End()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Wire DTOs. The same shapes serve requests and responses; responses carry
// masked secrets, and an update that sends back an empty or masked secret
// keeps the stored value.
type modelDTO struct {
	Name    string `json:"name"`
	APIKey  string `json:"api_key"`
	BaseURL string `json:"base_url,omitempty"`
}

type wecomDTO struct {
	CorpID         string `json:"corp_id"`
	CorpSecret     string `json:"corp_secret"`
	AgentID        int    `json:"agent_id"`
	Token          string `json:"token"`
	EncodingAESKey string `json:"encoding_aes_key"`
}

type wechatKfDTO struct {
	CorpID         string `json:"corp_id"`
	Secret         string `json:"secret"`
	Token          string `json:"token"`
	EncodingAESKey string `json:"encoding_aes_key"`
}

type channelsDTO struct {
	WeCom    *wecomDTO    `json:"wecom,omitempty"`
	WeChatKf *wechatKfDTO `json:"wechat_kf,omitempty"`
}

type guardrailsDTO struct {
	MaxInputBytes         int      `json:"max_input_bytes,omitempty"`
	BlockedKeywords       []string `json:"blocked_keywords,omitempty"`
	OutputBlockedKeywords []string `json:"output_blocked_keywords,omitempty"`
}

type tenantDTO struct {
	ID         string         `json:"id"`
	Name       string         `json:"name,omitempty"`
	Model      modelDTO       `json:"model"`
	Channels   channelsDTO    `json:"channels"`
	Guardrails *guardrailsDTO `json:"guardrails,omitempty"`
}

type listResponse struct {
	DefaultTenant string      `json:"default_tenant"`
	Tenants       []tenantDTO `json:"tenants"`
}

func (s *Service) handleCollection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.list(w)
	case http.MethodPost:
		s.create(w, r)
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Service) handleItem(w http.ResponseWriter, r *http.Request) {
	id := strings.Trim(strings.TrimPrefix(r.URL.Path, "/admin/tenants/"), "/")
	if id == "" {
		writeErr(w, http.StatusBadRequest, "tenant id is required")
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.get(w, id)
	case http.MethodPut:
		s.update(w, r, id)
	case http.MethodDelete:
		s.delete(w, r, id)
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Service) list(w http.ResponseWriter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resp := listResponse{DefaultTenant: s.cfg.DefaultTenant, Tenants: []tenantDTO{}}
	for _, id := range sortedIDs(s.cfg) {
		resp.Tenants = append(resp.Tenants, toDTO(s.cfg.Tenants[id], true))
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Service) get(w http.ResponseWriter, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.cfg.Tenants[id]
	if !ok {
		writeErr(w, http.StatusNotFound, fmt.Sprintf("tenant %q not found", id))
		return
	}
	writeJSON(w, http.StatusOK, toDTO(t, true))
}

func (s *Service) create(w http.ResponseWriter, r *http.Request) {
	var p tenantDTO
	if err := decode(r, &p); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if p.ID == "" {
		writeErr(w, http.StatusBadRequest, "id is required")
		return
	}
	if _, dup := s.cfg.Tenants[p.ID]; dup {
		writeErr(w, http.StatusConflict, fmt.Sprintf("tenant %q already exists", p.ID))
		return
	}
	// On create, secrets must be provided in clear: there is nothing to keep.
	t := fromDTO(p)
	if t.Model.APIKey == "" || isMasked(t.Model.APIKey) {
		writeErr(w, http.StatusBadRequest, "model.api_key is required")
		return
	}
	if wcom := t.Channels.WeCom; wcom != nil &&
		(wcom.CorpSecret == "" || wcom.Token == "" || wcom.EncodingAESKey == "") {
		writeErr(w, http.StatusBadRequest, "channels.wecom secrets are required")
		return
	}
	if kf := t.Channels.WeChatKf; kf != nil &&
		(kf.Secret == "" || kf.Token == "" || kf.EncodingAESKey == "") {
		writeErr(w, http.StatusBadRequest, "channels.wechat_kf secrets are required")
		return
	}

	next := cloneConfig(s.cfg)
	next.Tenants[t.ID] = t
	if err := s.commit(next); err != nil {
		s.auditAdmin(r.Context(), t.ID, "create", err)
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	s.auditAdmin(r.Context(), t.ID, "create", nil)
	writeJSON(w, http.StatusCreated, toDTO(t, true))
}

func (s *Service) update(w http.ResponseWriter, r *http.Request, id string) {
	var p tenantDTO
	if err := decode(r, &p); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.cfg.Tenants[id]
	if !ok {
		writeErr(w, http.StatusNotFound, fmt.Sprintf("tenant %q not found", id))
		return
	}

	next := cloneConfig(s.cfg)
	t := next.Tenants[id]
	t.Name = p.Name
	t.Model.Name = p.Model.Name
	t.Model.BaseURL = p.Model.BaseURL
	t.Model.APIKey = keepSecret(old.Model.APIKey, p.Model.APIKey)

	if p.Channels.WeCom == nil {
		t.Channels.WeCom = nil // explicit unbind
	} else {
		nw := &tenant.WeComBinding{CorpID: p.Channels.WeCom.CorpID, AgentID: p.Channels.WeCom.AgentID}
		ow := old.Channels.WeCom
		nw.CorpSecret = keepSecretOld(ow, p.Channels.WeCom.CorpSecret, func(b *tenant.WeComBinding) string { return b.CorpSecret })
		nw.Token = keepSecretOld(ow, p.Channels.WeCom.Token, func(b *tenant.WeComBinding) string { return b.Token })
		nw.EncodingAESKey = keepSecretOld(ow, p.Channels.WeCom.EncodingAESKey, func(b *tenant.WeComBinding) string { return b.EncodingAESKey })
		t.Channels.WeCom = nw
	}

	if p.Channels.WeChatKf == nil {
		t.Channels.WeChatKf = nil // explicit unbind
	} else {
		nk := &tenant.WeChatKfBinding{CorpID: p.Channels.WeChatKf.CorpID}
		ok := old.Channels.WeChatKf
		nk.Secret = keepSecretOld(ok, p.Channels.WeChatKf.Secret, func(b *tenant.WeChatKfBinding) string { return b.Secret })
		nk.Token = keepSecretOld(ok, p.Channels.WeChatKf.Token, func(b *tenant.WeChatKfBinding) string { return b.Token })
		nk.EncodingAESKey = keepSecretOld(ok, p.Channels.WeChatKf.EncodingAESKey, func(b *tenant.WeChatKfBinding) string { return b.EncodingAESKey })
		t.Channels.WeChatKf = nk
	}

	// Guardrails follow the channels semantics: absent means clear.
	if p.Guardrails == nil {
		t.Guardrails = tenant.Guardrails{}
	} else {
		t.Guardrails = tenant.Guardrails{
			MaxInputBytes:         p.Guardrails.MaxInputBytes,
			BlockedKeywords:       append([]string(nil), p.Guardrails.BlockedKeywords...),
			OutputBlockedKeywords: append([]string(nil), p.Guardrails.OutputBlockedKeywords...),
		}
	}

	if err := s.commit(next); err != nil {
		s.auditAdmin(r.Context(), id, "update", err)
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	s.auditAdmin(r.Context(), id, "update", nil)
	writeJSON(w, http.StatusOK, toDTO(t, true))
}

func (s *Service) delete(w http.ResponseWriter, r *http.Request, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.cfg.Tenants[id]; !ok {
		writeErr(w, http.StatusNotFound, fmt.Sprintf("tenant %q not found", id))
		return
	}
	if id == s.cfg.DefaultTenant {
		writeErr(w, http.StatusConflict, "cannot delete the default tenant")
		return
	}
	if len(s.cfg.Tenants) == 1 {
		writeErr(w, http.StatusConflict, "cannot delete the last tenant")
		return
	}
	next := cloneConfig(s.cfg)
	delete(next.Tenants, id)
	if err := s.commit(next); err != nil {
		s.auditAdmin(r.Context(), id, "delete", err)
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	s.auditAdmin(r.Context(), id, "delete", nil)
	w.WriteHeader(http.StatusNoContent)
}

// commit validates, persists, and hot-applies next. The running config is
// only swapped after the registry rebuild succeeds; a rebuild failure also
// rolls the file back on a best-effort basis.
func (s *Service) commit(next *config.Config) error {
	if err := config.Save(s.path, next); err != nil { // Save validates first
		return err
	}
	if err := s.reg.Apply(next); err != nil {
		_ = config.Save(s.path, s.cfg)
		return err
	}
	s.cfg = next
	return nil
}

// keepSecret keeps the stored secret when the incoming value is empty or is
// the masked echo of it (client round-tripped a GET response).
func keepSecret(old, in string) string {
	if in == "" || isMasked(in) {
		return old
	}
	return in
}

// keepSecretOld reads the stored secret from the previous binding of any
// channel type and applies the keepSecret rule to the incoming value.
func keepSecretOld[B any](ob *B, in string, get func(*B) string) string {
	old := ""
	if ob != nil {
		old = get(ob)
	}
	return keepSecret(old, in)
}

func isMasked(v string) bool { return strings.Contains(v, "****") }

// maskSecret keeps the first 3 and last 4 characters of long secrets so an
// operator can tell them apart, and fully hides short ones.
func maskSecret(v string) string {
	if v == "" {
		return ""
	}
	if len(v) <= 8 {
		return "****"
	}
	return v[:3] + "****" + v[len(v)-4:]
}

func toDTO(t *tenant.Context, mask bool) tenantDTO {
	d := tenantDTO{
		ID:   t.ID,
		Name: t.Name,
		Model: modelDTO{
			Name:    t.Model.Name,
			APIKey:  t.Model.APIKey,
			BaseURL: t.Model.BaseURL,
		},
	}
	if mask {
		d.Model.APIKey = maskSecret(d.Model.APIKey)
	}
	if t.Channels.WeCom != nil {
		w := *t.Channels.WeCom
		d.Channels.WeCom = &wecomDTO{
			CorpID: w.CorpID, CorpSecret: w.CorpSecret, AgentID: w.AgentID,
			Token: w.Token, EncodingAESKey: w.EncodingAESKey,
		}
		if mask {
			d.Channels.WeCom.CorpSecret = maskSecret(w.CorpSecret)
			d.Channels.WeCom.Token = maskSecret(w.Token)
			d.Channels.WeCom.EncodingAESKey = maskSecret(w.EncodingAESKey)
		}
	}
	if t.Channels.WeChatKf != nil {
		kf := *t.Channels.WeChatKf
		d.Channels.WeChatKf = &wechatKfDTO{
			CorpID: kf.CorpID, Secret: kf.Secret,
			Token: kf.Token, EncodingAESKey: kf.EncodingAESKey,
		}
		if mask {
			d.Channels.WeChatKf.Secret = maskSecret(kf.Secret)
			d.Channels.WeChatKf.Token = maskSecret(kf.Token)
			d.Channels.WeChatKf.EncodingAESKey = maskSecret(kf.EncodingAESKey)
		}
	}
	g := t.Guardrails
	if g.MaxInputBytes != 0 || len(g.BlockedKeywords) != 0 || len(g.OutputBlockedKeywords) != 0 {
		d.Guardrails = &guardrailsDTO{
			MaxInputBytes:         g.MaxInputBytes,
			BlockedKeywords:       append([]string(nil), g.BlockedKeywords...),
			OutputBlockedKeywords: append([]string(nil), g.OutputBlockedKeywords...),
		}
	}
	return d
}

func fromDTO(p tenantDTO) *tenant.Context {
	t := &tenant.Context{
		ID:   p.ID,
		Name: p.Name,
		Model: tenant.ModelConfig{
			Name:    p.Model.Name,
			APIKey:  p.Model.APIKey,
			BaseURL: p.Model.BaseURL,
		},
	}
	if p.Channels.WeCom != nil {
		t.Channels.WeCom = &tenant.WeComBinding{
			CorpID:         p.Channels.WeCom.CorpID,
			CorpSecret:     p.Channels.WeCom.CorpSecret,
			AgentID:        p.Channels.WeCom.AgentID,
			Token:          p.Channels.WeCom.Token,
			EncodingAESKey: p.Channels.WeCom.EncodingAESKey,
		}
	}
	if p.Channels.WeChatKf != nil {
		t.Channels.WeChatKf = &tenant.WeChatKfBinding{
			CorpID:         p.Channels.WeChatKf.CorpID,
			Secret:         p.Channels.WeChatKf.Secret,
			Token:          p.Channels.WeChatKf.Token,
			EncodingAESKey: p.Channels.WeChatKf.EncodingAESKey,
		}
	}
	if p.Guardrails != nil {
		t.Guardrails = tenant.Guardrails{
			MaxInputBytes:         p.Guardrails.MaxInputBytes,
			BlockedKeywords:       append([]string(nil), p.Guardrails.BlockedKeywords...),
			OutputBlockedKeywords: append([]string(nil), p.Guardrails.OutputBlockedKeywords...),
		}
	}
	return t
}

// cloneConfig deep-copies cfg so mutations never touch the running config
// before commit succeeds.
func cloneConfig(c *config.Config) *config.Config {
	n := &config.Config{
		DefaultTenant: c.DefaultTenant,
		Storage:       c.Storage,   // plain value: backend is not tenant-editable
		Log:           c.Log,       // plain values: observability is not
		Audit:         c.Audit,     // tenant-editable either, but Save
		Telemetry:     c.Telemetry, // validates them, so they must survive
		Tenants:       make(map[string]*tenant.Context, len(c.Tenants)),
	}
	for id, t := range c.Tenants {
		cp := *t
		if t.Channels.WeCom != nil {
			w := *t.Channels.WeCom
			cp.Channels.WeCom = &w
		}
		if t.Channels.WeChatKf != nil {
			kf := *t.Channels.WeChatKf
			cp.Channels.WeChatKf = &kf
		}
		cp.Guardrails.BlockedKeywords = append([]string(nil), t.Guardrails.BlockedKeywords...)
		cp.Guardrails.OutputBlockedKeywords = append([]string(nil), t.Guardrails.OutputBlockedKeywords...)
		n.Tenants[id] = &cp
	}
	return n
}

func sortedIDs(c *config.Config) []string {
	ids := make([]string, 0, len(c.Tenants))
	for id := range c.Tenants {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func decode(r *http.Request, v any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("decode body: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
