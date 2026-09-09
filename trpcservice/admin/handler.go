// Package admin exposes the tenant control-plane HTTP API.
package admin

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/Violet2314/trpc-agent-service/trpcservice/storage"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

const maxRequestBody = 1 << 20

// Handler implements the /api/v1 control-plane routes.
type Handler struct {
	store    tenant.ConfigStore
	cache    tenant.ConfigCache
	username string
	password string
	mux      *http.ServeMux
	migrator storage.Migrator
}

// Option configures optional Admin API capabilities.
type Option func(*Handler)

// WithMigrator enables migration control routes.
func WithMigrator(migrator storage.Migrator) Option {
	return func(handler *Handler) {
		handler.migrator = migrator
	}
}

// NewHandler constructs a Basic Auth protected Admin API.
func NewHandler(
	store tenant.ConfigStore,
	cache tenant.ConfigCache,
	username string,
	password string,
	options ...Option,
) (http.Handler, error) {
	if store == nil {
		return nil, errors.New("admin config store is required")
	}
	if username == "" || password == "" {
		return nil, errors.New("admin Basic Auth credentials are required")
	}
	handler := &Handler{
		store:    store,
		cache:    cache,
		username: username,
		password: password,
		mux:      http.NewServeMux(),
	}
	for _, option := range options {
		if option != nil {
			option(handler)
		}
	}
	handler.routes()
	return handler, nil
}

func (h *Handler) routes() {
	h.mux.HandleFunc("POST /api/v1/tenants", h.upsertTenant)
	h.mux.HandleFunc("GET /api/v1/tenants", h.listTenants)
	h.mux.HandleFunc("GET /api/v1/tenants/{tenantID}", h.getTenant)
	h.mux.HandleFunc("PUT /api/v1/tenants/{tenantID}", h.upsertTenant)

	h.mux.HandleFunc("POST /api/v1/tenants/{tenantID}/apps", h.upsertApp)
	h.mux.HandleFunc("GET /api/v1/tenants/{tenantID}/apps", h.listApps)
	h.mux.HandleFunc("PUT /api/v1/apps/{appID}", h.upsertApp)
	h.mux.HandleFunc("POST /api/v1/apps/{appID}/versions/{version}/activate", h.activateApp)

	h.mux.HandleFunc("POST /api/v1/apps/{appID}/bindings", h.upsertBinding)
	h.mux.HandleFunc("GET /api/v1/apps/{appID}/bindings", h.listBindings)
	h.mux.HandleFunc("PUT /api/v1/bindings/{bindingID}", h.upsertBinding)

	if h.migrator != nil {
		h.mux.HandleFunc("POST /api/v1/apps/{appID}/migrations", h.startMigration)
		h.mux.HandleFunc("POST /api/v1/migrations/{migrationID}/advance", h.advanceMigration)
		h.mux.HandleFunc("POST /api/v1/migrations/{migrationID}/rollback", h.rollbackMigration)
		h.mux.HandleFunc("GET /api/v1/migrations/{migrationID}", h.getMigration)
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	username, password, ok := r.BasicAuth()
	if !ok ||
		subtle.ConstantTimeCompare([]byte(username), []byte(h.username)) != 1 ||
		subtle.ConstantTimeCompare([]byte(password), []byte(h.password)) != 1 {
		w.Header().Set("WWW-Authenticate", `Basic realm="trpc-agent-service-admin"`)
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) upsertTenant(w http.ResponseWriter, r *http.Request) {
	var value tenant.Tenant
	if err := decodeJSON(w, r, &value); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if pathID := r.PathValue("tenantID"); pathID != "" {
		if value.ID != "" && value.ID != pathID {
			writeError(w, http.StatusBadRequest, "tenant_id does not match URL")
			return
		}
		value.ID = pathID
	}
	if err := h.store.UpsertTenant(r.Context(), value); err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (h *Handler) getTenant(w http.ResponseWriter, r *http.Request) {
	value, err := h.store.GetTenant(r.Context(), r.PathValue("tenantID"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (h *Handler) listTenants(w http.ResponseWriter, r *http.Request) {
	values, err := h.store.ListTenants(r.Context())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, values)
}

func (h *Handler) upsertApp(w http.ResponseWriter, r *http.Request) {
	var value tenant.AgentApp
	if err := decodeJSON(w, r, &value); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if tenantID := r.PathValue("tenantID"); tenantID != "" {
		if value.TenantID != "" && value.TenantID != tenantID {
			writeError(w, http.StatusBadRequest, "tenant_id does not match URL")
			return
		}
		value.TenantID = tenantID
	}
	if appID := r.PathValue("appID"); appID != "" {
		if value.ID != "" && value.ID != appID {
			writeError(w, http.StatusBadRequest, "app_id does not match URL")
			return
		}
		value.ID = appID
	}
	version, err := h.store.UpsertApp(r.Context(), value)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	value.Version = version
	value.IsCurrent = version == 1
	writeJSON(w, http.StatusOK, value)
}

func (h *Handler) listApps(w http.ResponseWriter, r *http.Request) {
	values, err := h.store.ListApps(r.Context(), r.PathValue("tenantID"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, values)
}

func (h *Handler) activateApp(w http.ResponseWriter, r *http.Request) {
	version, err := strconv.Atoi(r.PathValue("version"))
	if err != nil || version <= 0 {
		writeError(w, http.StatusBadRequest, "version must be a positive integer")
		return
	}
	if err := h.store.BindAppVersion(r.Context(), r.PathValue("appID"), version); err != nil {
		writeStoreError(w, err)
		return
	}
	h.invalidateAppBindings(r, r.PathValue("appID"))
	writeJSON(w, http.StatusOK, map[string]any{
		"app_id":  r.PathValue("appID"),
		"version": version,
		"active":  true,
	})
}

// invalidateAppBindings evicts the cached data-plane snapshots of every
// binding routed to the app so the newly activated version takes effect
// immediately. Other replicas still converge within the cache TTL.
func (h *Handler) invalidateAppBindings(r *http.Request, appID string) {
	if h.cache == nil {
		return
	}
	bindings, err := h.store.ListBindings(r.Context(), appID)
	if err != nil {
		// Activation must not fail because of a listing error; the cache TTL
		// still bounds staleness.
		return
	}
	for _, binding := range bindings {
		h.cache.Invalidate(binding.Channel, binding.RouteKey)
	}
}

func (h *Handler) upsertBinding(w http.ResponseWriter, r *http.Request) {
	var value tenant.ChannelBinding
	if err := decodeJSON(w, r, &value); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if appID := r.PathValue("appID"); appID != "" {
		if value.AppID != "" && value.AppID != appID {
			writeError(w, http.StatusBadRequest, "app_id does not match URL")
			return
		}
		value.AppID = appID
	}
	if bindingID := r.PathValue("bindingID"); bindingID != "" {
		if value.ID != "" && value.ID != bindingID {
			writeError(w, http.StatusBadRequest, "binding_id does not match URL")
			return
		}
		value.ID = bindingID
	}
	if err := h.store.UpsertBinding(r.Context(), value); err != nil {
		writeStoreError(w, err)
		return
	}
	if h.cache != nil {
		h.cache.Invalidate(value.Channel, value.RouteKey)
	}
	writeJSON(w, http.StatusOK, value)
}

func (h *Handler) listBindings(w http.ResponseWriter, r *http.Request) {
	values, err := h.store.ListBindings(r.Context(), r.PathValue("appID"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, values)
}

func (h *Handler) startMigration(w http.ResponseWriter, r *http.Request) {
	var body struct {
		From string `json:"from"`
		To   string `json:"to"`
	}
	if err := decodeJSON(w, r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	id, err := h.migrator.Start(r.Context(), r.PathValue("appID"), body.From, body.To)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"migration_id": id})
}

func (h *Handler) advanceMigration(w http.ResponseWriter, r *http.Request) {
	phase, err := h.migrator.Advance(r.Context(), r.PathValue("migrationID"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]storage.MigrationPhase{"phase": phase})
}

func (h *Handler) rollbackMigration(w http.ResponseWriter, r *http.Request) {
	if err := h.migrator.Rollback(r.Context(), r.PathValue("migrationID")); err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"rolled_back": true})
}

func (h *Handler) getMigration(w http.ResponseWriter, r *http.Request) {
	status, err := h.migrator.Status(r.Context(), r.PathValue("migrationID"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain one JSON value")
	}
	return nil
}

func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, tenant.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, tenant.ErrInactive):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, storage.ErrMigrationConflict):
		writeError(w, http.StatusConflict, err.Error())
	default:
		// Validation errors do not expose secrets and are safe to return.
		if stringsContainValidation(err.Error()) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "internal control-plane error")
	}
}

func stringsContainValidation(message string) bool {
	return len(message) >= len("validate ") && message[:len("validate ")] == "validate "
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
