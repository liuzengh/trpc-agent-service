package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/adminauth"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/controlplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/governance"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/modelcatalog"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/platformtool"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/worker"
)

func registerControlPlaneRoutes(
	mux *http.ServeMux,
	service *controlplane.Service,
	recorder *controlplane.ExecutionRecorder,
	approvalStore governance.ApprovalStore,
	stores ...reliable.Store,
) {
	mux.HandleFunc("/api/v1/tool-approvals", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if _, ok := requireApprovalPermission(w, r, adminauth.PermissionToolApprovalRead); !ok {
			return
		}
		lister, ok := approvalStore.(governance.ApprovalLister)
		if !ok {
			http.Error(w, "tool approval listing is unavailable", http.StatusServiceUnavailable)
			return
		}
		tenantID, err := approvalTenantQuery(r)
		if err != nil {
			http.Error(w, "tenantId query parameter is required", http.StatusBadRequest)
			return
		}
		if _, err := adminauth.RequireTenant(r.Context(), tenantID); err != nil {
			writeAdminAuthorizationError(w, err)
			return
		}
		limit := 50
		limitValues := r.URL.Query()["limit"]
		if len(limitValues) > 1 {
			http.Error(w, "limit must be provided once", http.StatusBadRequest)
			return
		}
		if len(limitValues) == 1 && strings.TrimSpace(limitValues[0]) != "" {
			raw := strings.TrimSpace(limitValues[0])
			parsed, parseErr := strconv.Atoi(raw)
			if parseErr != nil || parsed < 1 || parsed > 100 {
				http.Error(w, "limit must be between 1 and 100", http.StatusBadRequest)
				return
			}
			limit = parsed
		}
		challenges, err := lister.ListChallenges(r.Context(), tenantID, limit)
		if err != nil {
			writeApprovalError(w, err)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"items": safeApprovalChallengesForTenant(challenges, tenantID)})
	})
	mux.HandleFunc("/api/v1/agent-apps", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var request struct {
			TenantID    string `json:"tenantId"`
			Name        string `json:"name"`
			Description string `json:"description"`
		}
		if !decodeJSON(w, r, &request) {
			return
		}
		actor, ok := requireControlPlaneActor(w, r, request.TenantID)
		if !ok {
			return
		}
		app, err := service.CreateApp(r.Context(), request.TenantID, request.Name, request.Description, actor)
		writeControlResult(w, app, err, http.StatusCreated)
	})
	mux.HandleFunc("/api/v1/agent-versions", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var request struct {
			TenantID string                       `json:"tenantId"`
			AppName  string                       `json:"appName"`
			Snapshot controlplane.VersionSnapshot `json:"snapshot"`
		}
		if !decodeJSON(w, r, &request) {
			return
		}
		actor, ok := requireControlPlaneActor(w, r, request.TenantID)
		if !ok {
			return
		}
		version, err := service.CreateVersion(r.Context(), request.TenantID, request.AppName, actor, request.Snapshot)
		writeControlResult(w, version, err, http.StatusCreated)
	})
	mux.HandleFunc("/api/v1/agent-versions/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		versionID, ok := versionIDFromPublishPath(r.URL.Path)
		if !ok {
			http.NotFound(w, r)
			return
		}
		var request struct {
			TenantID string `json:"tenantId"`
		}
		if !decodeJSON(w, r, &request) {
			return
		}
		actor, ok := requireControlPlaneActor(w, r, request.TenantID)
		if !ok {
			return
		}
		err := service.PublishVersion(r.Context(), request.TenantID, versionID, actor)
		writeControlResult(w, map[string]string{"status": "published"}, err, http.StatusOK)
	})
	mux.HandleFunc("/api/v1/deployments", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var request struct {
			TenantID        string `json:"tenantId"`
			AppName         string `json:"appName"`
			StableVersionID string `json:"stableVersionId"`
			CanaryVersionID string `json:"canaryVersionId"`
			CanaryBPS       int    `json:"canaryBps"`
		}
		if !decodeJSON(w, r, &request) {
			return
		}
		actor, ok := requireControlPlaneActor(w, r, request.TenantID)
		if !ok {
			return
		}
		deployment, err := service.Deploy(r.Context(), request.TenantID, request.AppName, request.StableVersionID, request.CanaryVersionID, request.CanaryBPS, actor)
		writeControlResult(w, deployment, err, http.StatusCreated)
	})
	mux.HandleFunc("/api/v1/execution-reconciliations", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var request struct {
			TenantID    string `json:"tenantId"`
			ExecutionID int64  `json:"executionId"`
			Reason      string `json:"reason"`
			Evidence    string `json:"evidence,omitempty"`
		}
		if !decodeJSON(w, r, &request) {
			return
		}
		actor, ok := requireControlPlaneActor(w, r, request.TenantID)
		if !ok {
			return
		}
		if recorder == nil {
			http.Error(w, "execution reconciliation is unavailable", http.StatusServiceUnavailable)
			return
		}
		err := recorder.ReconcileForRetry(
			r.Context(), request.TenantID, request.ExecutionID, actor,
			request.Reason, request.Evidence,
		)
		writeReconciliationResult(w, err)
	})
	if len(stores) > 0 && stores[0] != nil {
		mux.HandleFunc("/api/v1/outbox-replays", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
				return
			}
			var request struct {
				TenantID string `json:"tenantId"`
				OutboxID int64  `json:"outboxId"`
				Reason   string `json:"reason"`
			}
			if !decodeJSON(w, r, &request) {
				return
			}
			actor, ok := requireControlPlaneActor(w, r, request.TenantID)
			if !ok {
				return
			}
			if request.OutboxID <= 0 || strings.TrimSpace(request.Reason) == "" {
				http.Error(w, "outboxId and reason are required", http.StatusBadRequest)
				return
			}
			// An interface containing a nil *PostgresStore/*MemoryStore is not
			// itself nil. Reject it before the capability assertion/invocation so
			// a bad startup wiring cannot turn an operator request into a panic.
			if isNilReliableStore(stores[0]) {
				http.Error(w, "tenant-scoped outbox replay is unavailable", http.StatusServiceUnavailable)
				return
			}
			replayer, ok := stores[0].(reliable.TenantScopedOutboxReplayer)
			if !ok {
				http.Error(w, "tenant-scoped outbox replay is unavailable", http.StatusServiceUnavailable)
				return
			}
			if err := replayer.ReplayOutboxForTenant(r.Context(), request.TenantID, request.OutboxID, actor, request.Reason); err != nil {
				log.Printf("outbox replay failed: error=%s", telemetry.StableErrorCode(err))
				http.Error(w, "outbox replay unavailable", http.StatusConflict)
				return
			}
			writeControlResult(w, map[string]string{"status": "replayed"}, nil, http.StatusOK)
		})
	}
	mux.HandleFunc("/api/v1/tool-approvals/", func(w http.ResponseWriter, r *http.Request) {
		challengeID, action, ok := approvalPath(r.URL.Path)
		if !ok {
			http.NotFound(w, r)
			return
		}
		if (r.Method != http.MethodGet || action != "") && (r.Method != http.MethodPost || action != "grant") {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		permission := adminauth.PermissionToolApprovalRead
		if action == "grant" {
			permission = adminauth.PermissionToolApprovalGrant
		}
		if _, ok := requireApprovalPermission(w, r, permission); !ok {
			return
		}
		tenantID, err := approvalTenantQuery(r)
		if err != nil {
			http.Error(w, "tenantId query parameter is required", http.StatusBadRequest)
			return
		}
		// Authorize the claimed tenant before reading the challenge. The tenant
		// query is only a lookup hint; the persisted challenge scope remains
		// authoritative and is checked below.
		if _, err := adminauth.RequireTenant(r.Context(), tenantID); err != nil {
			writeAdminAuthorizationError(w, err)
			return
		}
		if approvalStore == nil {
			http.Error(w, "tool approval is unavailable", http.StatusServiceUnavailable)
			return
		}
		inspector, canInspect := approvalStore.(governance.ApprovalInspector)
		if !canInspect {
			http.Error(w, "tool approval inspection is unavailable", http.StatusServiceUnavailable)
			return
		}
		challenge, err := inspector.GetChallenge(r.Context(), challengeID)
		if err != nil {
			writeApprovalError(w, err)
			return
		}
		if challenge.Request.TenantID != tenantID {
			// Do not reveal whether a challenge exists under another tenant.
			http.NotFound(w, r)
			return
		}
		switch {
		case r.Method == http.MethodGet && action == "":
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"challengeId":    challenge.ChallengeID,
				"tenantId":       challenge.Request.TenantID,
				"userId":         challenge.Request.UserID,
				"sessionOwnerId": challenge.Request.SessionOwnerID,
				"sessionId":      challenge.Request.SessionID,
				"toolName":       challenge.Request.ToolName,
				"argsHash":       challenge.Request.ArgsHash,
				"invocationId":   challenge.Request.InvocationID,
				"expiresAt":      challenge.ExpiresAt.UTC(),
			})
		case r.Method == http.MethodPost && action == "grant":
			actor, ok := requireAdminActor(w, r)
			if !ok {
				return
			}
			grant, err := approvalStore.Grant(r.Context(), challengeID, actor)
			if err != nil {
				writeApprovalError(w, err)
				return
			}
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"challengeId": grant.ChallengeID,
				"expiresAt":   grant.ExpiresAt.UTC(),
			})
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func isNilReliableStore(store reliable.Store) bool {
	if store == nil {
		return true
	}
	v := reflect.ValueOf(store)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func safeApprovalChallengesForTenant(challenges []governance.ApprovalChallenge, tenantID string) []map[string]interface{} {
	result := make([]map[string]interface{}, 0, len(challenges))
	for _, challenge := range challenges {
		if tenantID != "" && challenge.Request.TenantID != tenantID {
			continue
		}
		result = append(result, map[string]interface{}{
			"challengeId":    challenge.ChallengeID,
			"tenantId":       challenge.Request.TenantID,
			"userId":         challenge.Request.UserID,
			"sessionOwnerId": challenge.Request.SessionOwnerID,
			"sessionId":      challenge.Request.SessionID,
			"toolName":       challenge.Request.ToolName,
			"argsHash":       challenge.Request.ArgsHash,
			"invocationId":   challenge.Request.InvocationID,
			"expiresAt":      challenge.ExpiresAt.UTC(),
		})
	}
	return result
}

func approvalTenantQuery(r *http.Request) (string, error) {
	if r == nil || r.URL == nil {
		return "", fmt.Errorf("tenantId is required")
	}
	values, ok := r.URL.Query()["tenantId"]
	if !ok || len(values) != 1 || tenant.ValidateTenantID(values[0]) != nil {
		return "", fmt.Errorf("tenantId is invalid")
	}
	return values[0], nil
}

func approvalPath(path string) (challengeID, action string, ok bool) {
	const prefix = "/api/v1/tool-approvals/"
	if !strings.HasPrefix(path, prefix) {
		return "", "", false
	}
	remainder := strings.TrimPrefix(path, prefix)
	parts := strings.Split(remainder, "/")
	if len(parts) < 1 || !validApprovalPathPart(parts[0]) || len(parts) > 2 {
		return "", "", false
	}
	if len(parts) == 2 && parts[1] != "grant" {
		return "", "", false
	}
	return parts[0], func() string {
		if len(parts) == 2 {
			return parts[1]
		}
		return ""
	}(), true
}

func validApprovalPathPart(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z') && !(character >= 'A' && character <= 'Z') &&
			!(character >= '0' && character <= '9') && character != '-' && character != '_' {
			return false
		}
	}
	return true
}

func writeApprovalError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, governance.ErrApprovalNotFound):
		http.Error(w, "approval challenge not found", http.StatusNotFound)
	case errors.Is(err, governance.ErrApprovalAlreadyUsed), errors.Is(err, governance.ErrApprovalInvalid):
		http.Error(w, "approval challenge is no longer valid", http.StatusConflict)
	default:
		log.Printf("tool approval request failed: error=%s", telemetry.StableErrorCode(err))
		http.Error(w, "tool approval unavailable", http.StatusServiceUnavailable)
	}
}

func versionIDFromPublishPath(path string) (string, bool) {
	const prefix, suffix = "/api/v1/agent-versions/", "/publish"
	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, suffix) {
		return "", false
	}
	versionID := strings.TrimSuffix(strings.TrimPrefix(path, prefix), suffix)
	if versionID == "" || len(versionID) > 64 {
		return "", false
	}
	for _, character := range versionID {
		if !(character >= 'a' && character <= 'z') && !(character >= 'A' && character <= 'Z') &&
			!(character >= '0' && character <= '9') && character != '-' && character != '_' {
			return "", false
		}
	}
	return versionID, true
}

func validateVersionSnapshot(
	ctx context.Context,
	tenants tenant.Service,
	toolCatalog worker.ToolResolver,
	runtimeFactories *worker.RuntimeAgentRegistry,
	tenantID string,
	snapshot *controlplane.VersionSnapshot,
) error {
	return validateVersionSnapshotWithCatalog(ctx, tenants, toolCatalog, runtimeFactories, tenantID, snapshot, false)
}

func validateVersionSnapshotWithCatalog(
	ctx context.Context,
	tenants tenant.Service,
	toolCatalog worker.ToolResolver,
	runtimeFactories *worker.RuntimeAgentRegistry,
	tenantID string,
	snapshot *controlplane.VersionSnapshot,
	requireCatalog bool,
) error {
	if snapshot == nil {
		return fmt.Errorf("version snapshot is required")
	}
	if err := snapshot.Validate(); err != nil {
		return err
	}
	if err := runtimeFactories.ValidateRuntimeTypeStrict(snapshot.Agent.Type); err != nil {
		return fmt.Errorf("version agent runtime is not admitted: %w", err)
	}
	snapshot.RuntimeCapabilityFingerprint = runtimeFactories.Fingerprint()
	tenantConfig, err := tenants.GetTenant(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("load tenant for version validation: %w", err)
	}
	var configuredModel *tenant.ModelConfig
	for _, configured := range tenantConfig.Models {
		if configured.Provider == snapshot.Model.Provider && configured.ModelName == snapshot.Model.ModelName {
			copy := configured
			configuredModel = &copy
			break
		}
	}
	if configuredModel == nil {
		return fmt.Errorf("version references a model not configured for this tenant")
	}
	// The immutable snapshot may not substitute a different credential handle
	// for the tenant's operator-selected model binding.
	if snapshot.Model.APIKeyRef != configuredModel.APIKeyRef {
		return fmt.Errorf("version model credential reference does not match tenant binding")
	}
	if configuredModel.APIKeyRef != "" {
		authorizer, ok := tenants.(interface {
			AuthorizeModelSecretRef(context.Context, string, string, string, tenant.SecretRef) error
		})
		if !ok {
			return fmt.Errorf("version model credential binding authorization is unavailable")
		}
		if err := authorizer.AuthorizeModelSecretRef(ctx, tenantID, configuredModel.Provider, configuredModel.ModelName, tenant.SecretRef(configuredModel.APIKeyRef)); err != nil {
			return fmt.Errorf("version model credential binding is not authorized")
		}
	}
	if requireCatalog {
		if err := tenant.ValidateProductionModelCatalog(snapshot.Agent, snapshot.Model); err != nil {
			return fmt.Errorf("version model is not publishable: %w", err)
		}
	}
	if configuredModel.MaxTokens > 0 && snapshot.Model.MaxTokens > configuredModel.MaxTokens {
		return fmt.Errorf("version completion limit exceeds the tenant model limit")
	}
	if profile, ok := modelcatalog.Resolve(snapshot.Model.Provider, snapshot.Model.ModelName); ok {
		if snapshot.ModelCatalogRevision != "" && snapshot.ModelCatalogRevision != profile.Revision {
			return fmt.Errorf("version model catalog revision is not current")
		}
		if snapshot.ModelContextWindow != 0 && snapshot.ModelContextWindow != profile.ContextWindow {
			return fmt.Errorf("version model context window does not match the operator catalog")
		}
		snapshot.ModelCatalogRevision = profile.Revision
		snapshot.ModelContextWindow = profile.ContextWindow
	}
	if err := tenant.ValidatePinnedAgentModelBudget(
		snapshot.Agent,
		snapshot.Model,
		tenantConfig.Budget,
		snapshot.ModelCatalogRevision,
		snapshot.ModelContextWindow,
	); err != nil {
		return err
	}
	for _, toolName := range snapshot.Agent.Tools {
		if !tenantConfig.ToolPolicy.IsAllowed(toolName) {
			return fmt.Errorf("version tool %q is outside the tenant whitelist", toolName)
		}
		if platformtool.IsManagedKnowledgeTool(toolName) &&
			(tenantConfig.Storage.KnowledgeBackend != "qdrant" || tenantConfig.Storage.KnowledgeProfile == "") {
			return fmt.Errorf("version tool %q requires a tenant Qdrant knowledge profile", toolName)
		}
	}
	staticTools := make([]string, 0, len(snapshot.Agent.Tools))
	for _, toolName := range snapshot.Agent.Tools {
		if platformtool.IsFrameworkManagedTool(toolName) {
			continue
		}
		staticTools = append(staticTools, toolName)
	}
	if _, err := toolCatalog.Resolve(staticTools); err != nil {
		return fmt.Errorf("version tools are not executable by this platform: %w", err)
	}
	return nil
}

func requireAdminActor(w http.ResponseWriter, r *http.Request) (string, bool) {
	principal, err := adminauth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAdminAuthorizationError(w, err)
		return "", false
	}
	return principal.ID, true
}

// requireApprovalPermission keeps the route safe even when an embedding
// application forgets to install the outer method/permission middleware.
// Production wiring still applies that middleware before this handler.
func requireApprovalPermission(w http.ResponseWriter, r *http.Request, permission adminauth.Permission) (adminauth.Principal, bool) {
	principal, err := adminauth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAdminAuthorizationError(w, err)
		return adminauth.Principal{}, false
	}
	if !principal.Has(permission) {
		writeAdminAuthorizationError(w, adminauth.ErrForbidden)
		return adminauth.Principal{}, false
	}
	return principal, true
}

func requireControlPlaneActor(w http.ResponseWriter, r *http.Request, tenantID string) (string, bool) {
	principal, err := adminauth.RequireTenant(r.Context(), tenantID)
	if err != nil {
		writeAdminAuthorizationError(w, err)
		return "", false
	}
	return principal.ID, true
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target interface{}) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		http.Error(w, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			http.Error(w, "Request too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "Invalid request", http.StatusBadRequest)
		}
		return false
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		http.Error(w, "Request must contain exactly one JSON value", http.StatusBadRequest)
		return false
	}
	return true
}

func writeControlResult(w http.ResponseWriter, value interface{}, err error, successCode int) {
	if err != nil {
		log.Printf("control-plane request failed: error=%s", telemetry.StableErrorCode(err))
		switch {
		case errors.Is(err, controlplane.ErrInvalidControlPlaneRequest):
			http.Error(w, "invalid control-plane request", http.StatusBadRequest)
		case errors.Is(err, controlplane.ErrControlPlaneNotFound):
			http.Error(w, "control-plane resource not found", http.StatusNotFound)
		case errors.Is(err, controlplane.ErrControlPlaneConflict):
			http.Error(w, "control-plane state conflicts with an existing resource", http.StatusConflict)
		case errors.Is(err, controlplane.ErrTenantInactive):
			// A tenant lifecycle decision is a valid, stable client conflict. It
			// must not be reported as 503, which would invite unsafe retries while
			// the tenant is intentionally suspended.
			http.Error(w, "tenant is not active", http.StatusConflict)
		default:
			// Database and driver errors are deliberately collapsed to avoid
			// leaking topology, SQL, or credential details.
			http.Error(w, "control-plane request unavailable", http.StatusServiceUnavailable)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(successCode)
	_ = json.NewEncoder(w).Encode(value)
}

func writeReconciliationResult(w http.ResponseWriter, err error) {
	if err != nil {
		switch {
		case errors.Is(err, controlplane.ErrExecutionRecordMissing):
			http.Error(w, "execution not found", http.StatusNotFound)
		case errors.Is(err, controlplane.ErrReconciliationConflict),
			errors.Is(err, controlplane.ErrReconciliationNotAllowed),
			errors.Is(err, controlplane.ErrSessionExecutionInProgress),
			errors.Is(err, controlplane.ErrSessionReconciliationRequired):
			http.Error(w, "execution cannot be reconciled", http.StatusConflict)
		case errors.Is(err, controlplane.ErrInvalidReconciliationRequest):
			http.Error(w, "invalid reconciliation request", http.StatusBadRequest)
		default:
			// Database and driver errors are deliberately collapsed to avoid
			// leaking topology or credentials.
			log.Printf("execution reconciliation failed: error=%s", telemetry.StableErrorCode(err))
			http.Error(w, "execution reconciliation unavailable", http.StatusServiceUnavailable)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "safe_to_retry"})
}
