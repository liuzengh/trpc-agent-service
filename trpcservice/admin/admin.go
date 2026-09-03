// Package admin exposes the tenant management API (proposal doc §7, Admin
// API slice 2): CRUD over tenants and their channel bindings, atomically
// persisted to the YAML config file and hot-applied to the runner registry
// without a restart.
package admin

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
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
}

// NewService builds the admin service over the live config and registry.
// path is the YAML file every mutation is persisted to.
func NewService(path string, cfg *config.Config, reg *agent.Registry) *Service {
	return &Service{path: path, cfg: cfg, reg: reg}
}

// WeComBinding resolves a tenant's current binding for channel adapters,
// so hot-updated bindings take effect without rewiring the gateway.
func (s *Service) WeComBinding(tenantID string) (*tenant.WeComBinding, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg.WeComBinding(tenantID)
}

// Handler mounts /admin/tenants and /admin/tenants/{id}.
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/tenants", s.handleCollection)
	mux.HandleFunc("/admin/tenants/", s.handleItem)
	return mux
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

type channelsDTO struct {
	WeCom *wecomDTO `json:"wecom,omitempty"`
}

type tenantDTO struct {
	ID       string      `json:"id"`
	Name     string      `json:"name,omitempty"`
	Model    modelDTO    `json:"model"`
	Channels channelsDTO `json:"channels"`
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
		s.delete(w, id)
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

	next := cloneConfig(s.cfg)
	next.Tenants[t.ID] = t
	if err := s.commit(next); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
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

	if err := s.commit(next); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, toDTO(t, true))
}

func (s *Service) delete(w http.ResponseWriter, id string) {
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
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
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

func keepSecretOld(ow *tenant.WeComBinding, in string, get func(*tenant.WeComBinding) string) string {
	old := ""
	if ow != nil {
		old = get(ow)
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
	return t
}

// cloneConfig deep-copies cfg so mutations never touch the running config
// before commit succeeds.
func cloneConfig(c *config.Config) *config.Config {
	n := &config.Config{
		DefaultTenant: c.DefaultTenant,
		Tenants:       make(map[string]*tenant.Context, len(c.Tenants)),
	}
	for id, t := range c.Tenants {
		cp := *t
		if t.Channels.WeCom != nil {
			w := *t.Channels.WeCom
			cp.Channels.WeCom = &w
		}
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
