package admin

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type PrincipalResolver interface {
	Resolve(*http.Request) (Principal, error)
}

type Handler struct {
	Service    Service
	Principals PrincipalResolver
	Catalog    Catalog
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	principal, err := h.Principals.Resolve(r)
	if err != nil {
		writeError(w, ErrUnauthenticated)
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/v1/admin/catalog" {
		if err := authorize(principal, principal.TenantID); err != nil {
			writeError(w, err)
			return
		}
		if h.Catalog == nil {
			writeError(w, ErrForbidden)
			return
		}
		value, getErr := h.Catalog.GetCatalog(r.Context(), principal.TenantID)
		if getErr != nil {
			writeError(w, getErr)
			return
		}
		writeJSON(w, http.StatusOK, value)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) >= 2 && parts[0] == "v1" && parts[1] == "config-releases" {
		h.serveRelease(w, r, principal, parts)
		return
	}
	if len(parts) >= 4 && parts[0] == "v1" && parts[1] == "tenants" && parts[3] == "session-migrations" {
		h.serveSessionMigration(w, r, principal, parts)
		return
	}
	if len(parts) >= 4 && parts[0] == "v1" && parts[1] == "tenants" && parts[3] == "knowledge-migrations" {
		h.serveKnowledgeMigration(w, r, principal, parts)
		return
	}
	if len(parts) >= 4 && parts[0] == "v1" && parts[1] == "tenants" && parts[3] == "memory-migrations" {
		h.serveMemoryMigration(w, r, principal, parts)
		return
	}
	if len(parts) < 4 || parts[0] != "v1" || parts[1] != "tenants" || parts[3] != "configs" {
		http.NotFound(w, r)
		return
	}
	tenantID := parts[2]
	switch {
	case r.Method == http.MethodPost && len(parts) == 5 && parts[4] == "validate":
		payload, err := decodePayload(w, r)
		if err == nil {
			err = h.Service.Validate(r.Context(), principal, tenantID, payload)
		}
		if err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPost && len(parts) == 5 && parts[4] == "publish":
		expected, ok := queryInt64(r, "expected_version")
		if !ok {
			http.Error(w, "invalid expected_version", http.StatusBadRequest)
			return
		}
		payload, err := decodePayload(w, r)
		if err == nil {
			var result config.PublishResult
			result, err = h.Service.Publish(r.Context(), principal, tenantID, expected, payload, metadata(r, principal))
			if err == nil {
				writeJSON(w, http.StatusCreated, result)
				return
			}
		}
		writeError(w, err)
	case r.Method == http.MethodPost && len(parts) == 5 && parts[4] == "stage":
		expected, ok := queryInt64(r, "expected_version")
		if !ok {
			http.Error(w, "invalid expected_version", http.StatusBadRequest)
			return
		}
		payload, err := decodePayload(w, r)
		if err == nil {
			var result config.Snapshot
			result, err = h.Service.Stage(r.Context(), principal, tenantID, expected, payload, metadata(r, principal))
			if err == nil {
				writeJSON(w, http.StatusCreated, result)
				return
			}
		}
		writeError(w, err)
	case r.Method == http.MethodPost && len(parts) == 5 && parts[4] == "rollback":
		expected, ok1 := queryInt64(r, "expected_version")
		target, ok2 := queryInt64(r, "target_version")
		if !ok1 || !ok2 {
			http.Error(w, "invalid version", http.StatusBadRequest)
			return
		}
		result, err := h.Service.Rollback(r.Context(), principal, tenantID, expected, target, metadata(r, principal))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, result)
	case r.Method == http.MethodGet && len(parts) == 4:
		result, err := h.Service.Current(r.Context(), principal, tenantID)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	case r.Method == http.MethodGet && len(parts) == 5:
		version, err := strconv.ParseInt(parts[4], 10, 64)
		if err != nil || version < 1 {
			http.Error(w, "invalid version", http.StatusBadRequest)
			return
		}
		result, err := h.Service.Get(r.Context(), principal, tenantID, version)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	default:
		http.NotFound(w, r)
	}
}

func (h Handler) serveSessionMigration(w http.ResponseWriter, r *http.Request, principal Principal, parts []string) {
	tenantID := parts[2]
	switchInput := func() (SessionMigrationSwitchInput, error) {
		var input SessionMigrationSwitchInput
		if err := decodeJSON(w, r, &input); err != nil {
			return SessionMigrationSwitchInput{}, err
		}
		return input, nil
	}
	controlInput := func() (SessionMigrationControlInput, error) {
		var input SessionMigrationControlInput
		if err := decodeJSON(w, r, &input); err != nil {
			return SessionMigrationControlInput{}, err
		}
		return input, nil
	}
	switch {
	case r.Method == http.MethodGet && len(parts) == 4:
		value, err := h.Service.ListSessionMigrations(r.Context(), principal, tenantID)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, value)
	case r.Method == http.MethodPost && len(parts) == 4:
		var input SessionMigrationCreateInput
		if err := decodeJSON(w, r, &input); err != nil {
			writeError(w, err)
			return
		}
		value, err := h.Service.CreateSessionMigration(r.Context(), principal, tenantID, input, metadata(r, principal))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, value)
	case r.Method == http.MethodGet && len(parts) == 5:
		value, err := h.Service.GetSessionMigration(r.Context(), principal, tenantID, parts[4])
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, value)
	case r.Method == http.MethodGet && len(parts) == 6 && parts[5] == "status":
		value, err := h.Service.GetSessionMigrationStatus(r.Context(), principal, tenantID, parts[4])
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, value)
	case r.Method == http.MethodPost && len(parts) == 6 && parts[5] == "cutover":
		input, err := switchInput()
		if err == nil {
			value, switchErr := h.Service.CutoverSessionMigration(r.Context(), principal, tenantID, parts[4], input, metadata(r, principal))
			if switchErr == nil {
				writeJSON(w, http.StatusOK, value)
				return
			}
			err = switchErr
		}
		writeError(w, err)
	case r.Method == http.MethodPost && len(parts) == 6 && parts[5] == "observe":
		var input SessionMigrationObserveInput
		err := decodeJSON(w, r, &input)
		if err == nil {
			value, observeErr := h.Service.BeginSessionMigrationObserve(r.Context(), principal, tenantID, parts[4], input)
			if observeErr == nil {
				writeJSON(w, http.StatusOK, value)
				return
			}
			err = observeErr
		}
		writeError(w, err)
	case r.Method == http.MethodPost && len(parts) == 6 && parts[5] == "rollback":
		input, err := switchInput()
		if err == nil {
			value, rollbackErr := h.Service.RollbackSessionMigration(r.Context(), principal, tenantID, parts[4], input, metadata(r, principal))
			if rollbackErr == nil {
				writeJSON(w, http.StatusOK, value)
				return
			}
			err = rollbackErr
		}
		writeError(w, err)
	case r.Method == http.MethodPost && len(parts) == 6 && parts[5] == "cleanup":
		input, err := switchInput()
		if err == nil {
			value, cleanupErr := h.Service.CleanupSessionMigration(r.Context(), principal, tenantID, parts[4], input)
			if cleanupErr == nil {
				writeJSON(w, http.StatusOK, value)
				return
			}
			err = cleanupErr
		}
		writeError(w, err)
	case r.Method == http.MethodPost && len(parts) == 6 && parts[5] == "pause":
		input, err := controlInput()
		if err == nil {
			value, controlErr := h.Service.PauseSessionMigration(r.Context(), principal, tenantID, parts[4], input, metadata(r, principal))
			if controlErr == nil {
				writeJSON(w, http.StatusOK, value)
				return
			}
			err = controlErr
		}
		writeError(w, err)
	case r.Method == http.MethodPost && len(parts) == 6 && parts[5] == "resume":
		input, err := controlInput()
		if err == nil {
			value, controlErr := h.Service.ResumeSessionMigration(r.Context(), principal, tenantID, parts[4], input, metadata(r, principal))
			if controlErr == nil {
				writeJSON(w, http.StatusOK, value)
				return
			}
			err = controlErr
		}
		writeError(w, err)
	case r.Method == http.MethodPost && len(parts) == 6 && parts[5] == "abort":
		input, err := controlInput()
		if err == nil {
			value, controlErr := h.Service.AbortSessionMigration(r.Context(), principal, tenantID, parts[4], input, metadata(r, principal))
			if controlErr == nil {
				writeJSON(w, http.StatusOK, value)
				return
			}
			err = controlErr
		}
		writeError(w, err)
	default:
		http.NotFound(w, r)
	}
}

func (h Handler) serveKnowledgeMigration(w http.ResponseWriter, r *http.Request, principal Principal, parts []string) {
	tenantID := parts[2]
	switchInput := func() (KnowledgeMigrationSwitchInput, error) {
		var input KnowledgeMigrationSwitchInput
		if err := decodeJSON(w, r, &input); err != nil {
			return input, err
		}
		return input, nil
	}
	controlInput := func() (KnowledgeMigrationControlInput, error) {
		var input KnowledgeMigrationControlInput
		if err := decodeJSON(w, r, &input); err != nil {
			return input, err
		}
		return input, nil
	}
	switch {
	case r.Method == http.MethodGet && len(parts) == 4:
		value, err := h.Service.ListKnowledgeMigrations(r.Context(), principal, tenantID)
		if err == nil {
			writeJSON(w, http.StatusOK, value)
			return
		}
		writeError(w, err)
	case r.Method == http.MethodPost && len(parts) == 4:
		var input KnowledgeMigrationCreateInput
		if err := decodeJSON(w, r, &input); err != nil {
			writeError(w, err)
			return
		}
		value, err := h.Service.CreateKnowledgeMigration(r.Context(), principal, tenantID, input, metadata(r, principal))
		if err == nil {
			writeJSON(w, http.StatusCreated, value)
			return
		}
		writeError(w, err)
	case r.Method == http.MethodGet && len(parts) == 5:
		value, err := h.Service.GetKnowledgeMigration(r.Context(), principal, tenantID, parts[4])
		if err == nil {
			writeJSON(w, http.StatusOK, value)
			return
		}
		writeError(w, err)
	case r.Method == http.MethodGet && len(parts) == 6 && parts[5] == "status":
		value, err := h.Service.GetKnowledgeMigrationStatus(r.Context(), principal, tenantID, parts[4])
		if err == nil {
			writeJSON(w, http.StatusOK, value)
			return
		}
		writeError(w, err)
	case r.Method == http.MethodPost && len(parts) == 6 && parts[5] == "cutover":
		input, err := switchInput()
		if err != nil {
			writeError(w, err)
			return
		}
		value, err := h.Service.CutoverKnowledgeMigration(r.Context(), principal, tenantID, parts[4], input, metadata(r, principal))
		if err == nil {
			writeJSON(w, http.StatusOK, value)
			return
		}
		writeError(w, err)
	case r.Method == http.MethodPost && len(parts) == 6 && parts[5] == "observe":
		var input KnowledgeMigrationObserveInput
		if err := decodeJSON(w, r, &input); err != nil {
			writeError(w, err)
			return
		}
		value, err := h.Service.BeginKnowledgeMigrationObserve(r.Context(), principal, tenantID, parts[4], input)
		if err == nil {
			writeJSON(w, http.StatusOK, value)
			return
		}
		writeError(w, err)
	case r.Method == http.MethodPost && len(parts) == 6 && parts[5] == "rollback":
		input, err := switchInput()
		if err != nil {
			writeError(w, err)
			return
		}
		value, err := h.Service.RollbackKnowledgeMigration(r.Context(), principal, tenantID, parts[4], input, metadata(r, principal))
		if err == nil {
			writeJSON(w, http.StatusOK, value)
			return
		}
		writeError(w, err)
	case r.Method == http.MethodPost && len(parts) == 6 && parts[5] == "cleanup":
		input, err := switchInput()
		if err != nil {
			writeError(w, err)
			return
		}
		value, err := h.Service.CleanupKnowledgeMigration(r.Context(), principal, tenantID, parts[4], input)
		if err == nil {
			writeJSON(w, http.StatusOK, value)
			return
		}
		writeError(w, err)
	case r.Method == http.MethodPost && len(parts) == 6 && parts[5] == "pause":
		input, err := controlInput()
		if err != nil {
			writeError(w, err)
			return
		}
		value, err := h.Service.PauseKnowledgeMigration(r.Context(), principal, tenantID, parts[4], input, metadata(r, principal))
		if err == nil {
			writeJSON(w, http.StatusOK, value)
			return
		}
		writeError(w, err)
	case r.Method == http.MethodPost && len(parts) == 6 && parts[5] == "resume":
		input, err := controlInput()
		if err != nil {
			writeError(w, err)
			return
		}
		value, err := h.Service.ResumeKnowledgeMigration(r.Context(), principal, tenantID, parts[4], input, metadata(r, principal))
		if err == nil {
			writeJSON(w, http.StatusOK, value)
			return
		}
		writeError(w, err)
	case r.Method == http.MethodPost && len(parts) == 6 && parts[5] == "abort":
		input, err := controlInput()
		if err != nil {
			writeError(w, err)
			return
		}
		value, err := h.Service.AbortKnowledgeMigration(r.Context(), principal, tenantID, parts[4], input, metadata(r, principal))
		if err == nil {
			writeJSON(w, http.StatusOK, value)
			return
		}
		writeError(w, err)
	default:
		http.NotFound(w, r)
	}
}

func (h Handler) serveMemoryMigration(w http.ResponseWriter, r *http.Request, principal Principal, parts []string) {
	tenantID := parts[2]
	controlInput := func() (MemoryMigrationControlInput, error) {
		var input MemoryMigrationControlInput
		if err := decodeJSON(w, r, &input); err != nil {
			return input, err
		}
		return input, nil
	}
	switch {
	case r.Method == http.MethodGet && len(parts) == 4:
		value, err := h.Service.ListMemoryMigrations(r.Context(), principal, tenantID)
		if err == nil {
			writeJSON(w, http.StatusOK, value)
			return
		}
		writeError(w, err)
	case r.Method == http.MethodPost && len(parts) == 4:
		var input MemoryMigrationCreateInput
		if err := decodeJSON(w, r, &input); err != nil {
			writeError(w, err)
			return
		}
		value, err := h.Service.CreateMemoryMigration(r.Context(), principal, tenantID, input, metadata(r, principal))
		if err == nil {
			writeJSON(w, http.StatusCreated, value)
			return
		}
		writeError(w, err)
	case r.Method == http.MethodGet && len(parts) == 5:
		value, err := h.Service.GetMemoryMigration(r.Context(), principal, tenantID, parts[4])
		if err == nil {
			writeJSON(w, http.StatusOK, value)
			return
		}
		writeError(w, err)
	case r.Method == http.MethodPost && len(parts) == 6 && parts[5] == "pause":
		input, err := controlInput()
		if err != nil {
			writeError(w, err)
			return
		}
		value, err := h.Service.PauseMemoryMigration(r.Context(), principal, tenantID, parts[4], input, metadata(r, principal))
		if err == nil {
			writeJSON(w, http.StatusOK, value)
			return
		}
		writeError(w, err)
	case r.Method == http.MethodPost && len(parts) == 6 && parts[5] == "resume":
		input, err := controlInput()
		if err != nil {
			writeError(w, err)
			return
		}
		value, err := h.Service.ResumeMemoryMigration(r.Context(), principal, tenantID, parts[4], input, metadata(r, principal))
		if err == nil {
			writeJSON(w, http.StatusOK, value)
			return
		}
		writeError(w, err)
	case r.Method == http.MethodPost && len(parts) == 6 && parts[5] == "abort":
		input, err := controlInput()
		if err != nil {
			writeError(w, err)
			return
		}
		value, err := h.Service.AbortMemoryMigration(r.Context(), principal, tenantID, parts[4], input, metadata(r, principal))
		if err == nil {
			writeJSON(w, http.StatusOK, value)
			return
		}
		writeError(w, err)
	default:
		http.NotFound(w, r)
	}
}

func (h Handler) serveRelease(w http.ResponseWriter, r *http.Request, principal Principal, parts []string) {
	switch {
	case r.Method == http.MethodPost && len(parts) == 2:
		var input config.ReleaseCreateInput
		if err := decodeJSON(w, r, &input); err != nil {
			writeError(w, err)
			return
		}
		input.Metadata = metadata(r, principal)
		value, err := h.Service.CreateRelease(r.Context(), principal, input)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, value)
	case r.Method == http.MethodGet && len(parts) == 3:
		value, err := h.Service.GetRelease(r.Context(), principal, parts[2])
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, value)
	case r.Method == http.MethodPost && len(parts) == 4 && parts[3] == "rollout":
		expected, ok1 := queryInt64(r, "expected_version")
		percentage, ok2 := queryPercentage(r)
		if !ok1 || !ok2 {
			http.Error(w, "invalid rollout parameters", http.StatusBadRequest)
			return
		}
		value, err := h.Service.UpdateRelease(r.Context(), principal, config.ReleaseUpdateInput{ReleaseID: parts[2], ExpectedVersion: expected, Percentage: percentage, Metadata: metadata(r, principal)})
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, value)
	case r.Method == http.MethodPost && len(parts) == 4 && parts[3] == "rollback":
		expected, ok := queryInt64(r, "expected_version")
		if !ok {
			http.Error(w, "invalid expected_version", http.StatusBadRequest)
			return
		}
		value, err := h.Service.RollbackRelease(r.Context(), principal, config.ReleaseRollbackInput{ReleaseID: parts[2], ExpectedVersion: expected, Metadata: metadata(r, principal)})
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, value)
	default:
		http.NotFound(w, r)
	}
}

func decodePayload(w http.ResponseWriter, r *http.Request) (config.ConfigV1, error) {
	body := http.MaxBytesReader(w, r.Body, 1<<20)
	defer body.Close()
	data, err := io.ReadAll(body)
	if err != nil {
		return config.ConfigV1{}, config.ErrInvalid
	}
	payload, err := config.DecodeV1(data)
	if err != nil {
		return config.ConfigV1{}, config.ErrInvalid
	}
	return payload, nil
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) error {
	body := http.MaxBytesReader(w, r.Body, 1<<20)
	defer body.Close()
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return config.ErrInvalid
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return config.ErrInvalid
	}
	return nil
}
func queryInt64(r *http.Request, key string) (int64, bool) {
	value, err := strconv.ParseInt(r.URL.Query().Get(key), 10, 64)
	return value, err == nil && value > 0
}
func queryPercentage(r *http.Request) (int, bool) {
	value, err := strconv.Atoi(r.URL.Query().Get("percentage"))
	return value, err == nil && value >= 0 && value <= 100
}
func metadata(r *http.Request, p Principal) tenant.ChangeMetadata {
	return tenant.ChangeMetadata{ActorType: "admin", ActorID: p.SubjectID, ReasonCode: r.Header.Get("X-Reason-Code"), ReasonRef: r.Header.Get("X-Reason-Ref"), CorrelationID: r.Header.Get("X-Correlation-ID"), TraceID: r.Header.Get("X-Trace-ID")}
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func writeError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	switch {
	case errors.Is(err, ErrUnauthenticated):
		status = http.StatusUnauthorized
	case errors.Is(err, ErrForbidden), errors.Is(err, config.ErrTenantScope):
		status = http.StatusForbidden
	case errors.Is(err, config.ErrNotFound), errors.Is(err, tenant.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, config.ErrVersionConflict), errors.Is(err, tenant.ErrVersionConflict):
		status = http.StatusConflict
	case errors.Is(err, runtime.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, runtime.ErrTenantScope):
		status = http.StatusForbidden
	case errors.Is(err, runtime.ErrVersionConflict), errors.Is(err, runtime.ErrIdempotencyCollision):
		status = http.StatusConflict
	}
	http.Error(w, http.StatusText(status), status)
}
