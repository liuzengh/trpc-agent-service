package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

func (h *AdminHandler) handleAdminResource(w http.ResponseWriter, r *http.Request) bool {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/admin/")
	if path == r.URL.Path {
		return false
	}
	trusted := h.trustedRequest(w, r)
	if trusted == nil {
		return true
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 {
		return false
	}
	switch parts[0] {
	case "agent-apps":
		h.handleAgentApps(w, trusted, parts[1:])
		return true
	case "deployments":
		h.handleDeployments(w, trusted, parts[1:])
		return true
	case "run":
		h.handleRoutedRun(w, trusted)
		return true
	case "runtime":
		if len(parts) == 2 && parts[1] == "status" {
			h.handleRuntimeStatus(w, trusted)
			return true
		}
	case "operations":
		if len(parts) == 2 {
			tenant, ok := trustedTenant(trusted)
			if !ok {
				writeError(w, http.StatusUnauthorized, "identity_required", "authenticated identity is required")
				return true
			}
			if parts[1] == "drain" {
				h.handleOperationsDrain(w, r, tenant)
				return true
			}
			if parts[1] == "faults" {
				h.handleOperationsFaults(w, r, tenant)
				return true
			}
		}
	case "capacity":
		trustedTenantContext, ok := trustedTenant(trusted)
		if !ok {
			writeError(w, http.StatusUnauthorized, "identity_required", "authenticated identity is required")
			return true
		}
		h.handleCapacity(w, r, trustedTenantContext, parts[1:])
		return true
	}
	return false
}

func trustedTenant(r *http.Request) (TenantContext, bool) {
	return TenantContextFromContext(r.Context())
}

func canMutate(role Role) bool  { return role == RolePlatformAdmin || role == RoleTenantAdmin }
func canOperate(role Role) bool { return canMutate(role) || role == RoleOperator }

func tenantAllowsPlatformAdmin(tenant TenantContext, tenantID string) bool {
	if tenant.Role != RolePlatformAdmin {
		return false
	}
	return tenantID == "" || tenant.AllowsTenant(tenantID)
}

// tenantCanSee keeps ordinary tenant roles on their active tenant while a
// platform administrator may inspect any tenant explicitly assigned to them.
func tenantCanSee(tenant TenantContext, tenantID string) bool {
	if tenantID == tenant.TenantID && tenant.AllowsTenant(tenantID) {
		return true
	}
	return tenant.Role == RolePlatformAdmin && tenant.AllowsTenant(tenantID)
}

func (h *AdminHandler) handleAgentApps(w http.ResponseWriter, r *http.Request, parts []string) {
	tenant, ok := trustedTenant(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "identity_required", "development identity is required")
		return
	}
	if len(parts) == 0 {
		switch r.Method {
		case http.MethodGet:
			items, err := h.platform.listApps(r.Context(), tenant.TenantID)
			if writeControlPlaneError(w, err) {
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"items": items})
		case http.MethodPost:
			if !canMutate(tenant.Role) {
				writeError(w, http.StatusForbidden, "forbidden", "tenant administrator role is required")
				return
			}
			var request struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			}
			// tenant_id is deliberately ignored: ownership always comes from Tenant Context.
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil || !validResourceID(request.ID) || !validDisplayName(request.Name) {
				writeError(w, http.StatusBadRequest, "invalid_agent_app", "id and name must be valid")
				return
			}
			app := AgentApp{ID: request.ID, TenantID: tenant.TenantID, Name: strings.TrimSpace(request.Name), CreatedAt: time.Now().UTC()}
			created, err := h.platform.createApp(r.Context(), app)
			if writeControlPlaneError(w, err) {
				return
			}
			if !created {
				writeError(w, http.StatusConflict, "agent_app_exists", "Agent App identifier already exists in this tenant")
				return
			}
			writeJSON(w, http.StatusCreated, app)
		default:
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET or POST")
		}
		return
	}
	if len(parts) != 1 || r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET")
		return
	}
	app, exists, err := h.platform.app(r.Context(), tenant.TenantID, parts[0])
	if writeControlPlaneError(w, err) {
		return
	}
	if !exists {
		writeError(w, http.StatusNotFound, "agent_app_not_found", "Agent App was not found")
		return
	}
	writeJSON(w, http.StatusOK, app)
}

func resourceKey(tenantID, id string) string { return tenantID + "\x00" + id }

func (p *SnapshotControlPlane) createApp(ctx context.Context, app AgentApp) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return false, p.persistenceErr
	}
	key := resourceKey(app.TenantID, app.ID)
	if _, exists := p.apps[key]; exists {
		return false, nil
	}
	p.apps[key] = app
	ok := p.persistLockedContext(ctx)
	return ok, p.persistenceErr
}

func (p *SnapshotControlPlane) app(ctx context.Context, tenantID, id string) (AgentApp, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return AgentApp{}, false, p.persistenceErr
	}
	app, ok := p.apps[resourceKey(tenantID, id)]
	return app, ok, nil
}

func (p *SnapshotControlPlane) listApps(ctx context.Context, tenantID string) ([]AgentApp, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return nil, p.persistenceErr
	}
	items := make([]AgentApp, 0)
	for _, app := range p.apps {
		if app.TenantID == tenantID {
			items = append(items, app)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	return items, nil
}

func (h *AdminHandler) handleDeployments(w http.ResponseWriter, r *http.Request, parts []string) {
	tenant, ok := trustedTenant(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "identity_required", "development identity is required")
		return
	}
	if len(parts) == 0 {
		h.handleDeploymentCollection(w, r, tenant)
		return
	}
	deployment, exists, err := h.platform.deployment(r.Context(), tenant.TenantID, parts[0])
	if writeControlPlaneError(w, err) {
		return
	}
	if !exists {
		writeError(w, http.StatusNotFound, "deployment_not_found", "Deployment was not found")
		return
	}
	if len(parts) == 1 {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET")
			return
		}
		writeJSON(w, http.StatusOK, deployment)
		return
	}
	switch parts[1] {
	case "versions":
		h.handleVersions(w, r, tenant, deployment, parts[2:])
	case "transition":
		h.handleTransition(w, r, tenant, deployment)
	case "rollout":
		h.handleDeploymentRollout(w, r, tenant, deployment)
	case "rollback-preview":
		h.handleRollbackPreview(w, r, tenant, deployment)
	case "rollback":
		h.handleRollback(w, r, tenant, deployment)
	default:
		writeError(w, http.StatusNotFound, "not_found", "resource was not found")
	}
}

func (h *AdminHandler) handleDeploymentRollout(w http.ResponseWriter, r *http.Request, tenant TenantContext, deployment Deployment) {
	if r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, deployment)
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET or POST")
		return
	}
	if !canOperate(tenant.Role) {
		writeError(w, http.StatusForbidden, "forbidden", "operator role is required")
		return
	}
	var request struct {
		TargetVersionID string `json:"target_version_id"`
		GrayPercentage  int    `json:"gray_percentage"`
		Confirm         bool   `json:"confirm"`
	}
	if err := decodeStrict(r, &request); err != nil || !request.Confirm {
		writeError(w, http.StatusBadRequest, "rollout_confirmation_required", "target version and confirmation are required")
		return
	}
	if err := h.recordDeploymentOperation(r, tenant, "deployment.rollout.updated"); err != nil {
		writeError(w, http.StatusServiceUnavailable, "audit_unavailable", "audit service is unavailable")
		return
	}
	updated, code, ok, err := h.platform.startRollout(r.Context(), deployment, request.TargetVersionID, request.GrayPercentage)
	if writeControlPlaneError(w, err) {
		return
	}
	if !ok {
		writeError(w, http.StatusConflict, code, "Deployment rollout is not allowed")
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

func (h *AdminHandler) handleRollbackPreview(w http.ResponseWriter, r *http.Request, tenant TenantContext, deployment Deployment) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET")
		return
	}
	preview, code, ok, err := h.platform.rollbackPreview(r.Context(), deployment)
	if writeControlPlaneError(w, err) {
		return
	}
	if !ok {
		writeError(w, http.StatusConflict, code, "Deployment rollback preview is unavailable")
		return
	}
	if runtimeStatus := h.runtime.StatusFor(tenant); len(runtimeStatus) > 0 {
		preview.ActiveExecutions = runtimeStatus[0].Active
	}
	writeJSON(w, http.StatusOK, preview)
}

func (h *AdminHandler) handleRollback(w http.ResponseWriter, r *http.Request, tenant TenantContext, deployment Deployment) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be POST")
		return
	}
	if !canOperate(tenant.Role) {
		writeError(w, http.StatusForbidden, "forbidden", "operator role is required")
		return
	}
	var request struct {
		Confirm bool `json:"confirm"`
	}
	if err := decodeStrict(r, &request); err != nil || !request.Confirm {
		writeError(w, http.StatusBadRequest, "rollback_confirmation_required", "rollback confirmation is required")
		return
	}
	if err := h.recordDeploymentOperation(r, tenant, "deployment.rollback.confirmed"); err != nil {
		writeError(w, http.StatusServiceUnavailable, "audit_unavailable", "audit service is unavailable")
		return
	}
	updated, code, ok, err := h.platform.rollback(r.Context(), deployment)
	if writeControlPlaneError(w, err) {
		return
	}
	if !ok {
		writeError(w, http.StatusConflict, code, "Deployment rollback is not allowed")
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

func (h *AdminHandler) recordDeploymentOperation(r *http.Request, tenant TenantContext, decision string) error {
	requestID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
	if !validIdempotencyKey(requestID) {
		requestID = newRequestID()
	}
	auditCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return h.governance.Record(auditCtx, AuditEvent{
		TenantID: tenant.TenantID, UserID: tenant.UserID, Decision: decision,
		RequestID: requestID, TraceID: newTraceID(), OccurredAt: time.Now().UTC(),
	})
}

func (h *AdminHandler) handleDeploymentCollection(w http.ResponseWriter, r *http.Request, tenant TenantContext) {
	switch r.Method {
	case http.MethodGet:
		items, err := h.platform.listDeployments(r.Context(), tenant.TenantID)
		if writeControlPlaneError(w, err) {
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	case http.MethodPost:
		if !canMutate(tenant.Role) {
			writeError(w, http.StatusForbidden, "forbidden", "tenant administrator role is required")
			return
		}
		var request struct {
			ID         string `json:"id"`
			AgentAppID string `json:"agent_app_id"`
		}
		if err := decodeStrict(r, &request); err != nil || !validResourceID(request.ID) || !validResourceID(request.AgentAppID) {
			writeError(w, http.StatusBadRequest, "invalid_deployment", "id and agent_app_id must be valid")
			return
		}
		if _, exists, err := h.platform.app(r.Context(), tenant.TenantID, request.AgentAppID); err != nil || !exists {
			if writeControlPlaneError(w, err) {
				return
			}
			writeError(w, http.StatusNotFound, "agent_app_not_found", "Agent App was not found")
			return
		}
		deployment := Deployment{ID: request.ID, TenantID: tenant.TenantID, AgentAppID: request.AgentAppID, Status: DeploymentDraft, DesiredReplicas: 1, CreatedAt: time.Now().UTC()}
		created, err := h.platform.createDeployment(r.Context(), deployment)
		if writeControlPlaneError(w, err) {
			return
		}
		if !created {
			writeError(w, http.StatusConflict, "deployment_exists", "Deployment identifier already exists")
			return
		}
		writeJSON(w, http.StatusCreated, deployment)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET or POST")
	}
}

func decodeStrict(r *http.Request, target any) error {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func (h *AdminHandler) handleVersions(w http.ResponseWriter, r *http.Request, tenant TenantContext, deployment Deployment, tail []string) {
	if len(tail) > 0 {
		writeError(w, http.StatusMethodNotAllowed, "immutable_version", "Deployment Versions cannot be modified in place")
		return
	}
	switch r.Method {
	case http.MethodGet:
		items, err := h.platform.listVersions(r.Context(), tenant.TenantID, deployment.ID)
		if writeControlPlaneError(w, err) {
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	case http.MethodPost:
		if !canMutate(tenant.Role) {
			writeError(w, http.StatusForbidden, "forbidden", "tenant administrator role is required")
			return
		}
		idempotencyKey := r.Header.Get("Idempotency-Key")
		if idempotencyKey == "" {
			writeError(w, http.StatusBadRequest, "idempotency_key_required", "Idempotency-Key header is required")
			return
		}
		if !validIdempotencyKey(idempotencyKey) {
			writeError(w, http.StatusBadRequest, "invalid_idempotency_key", "Idempotency-Key must contain 1 to 128 printable ASCII characters")
			return
		}
		var request struct {
			Config map[string]any `json:"config"`
		}
		if err := decodeStrict(r, &request); err != nil || len(request.Config) == 0 {
			writeError(w, http.StatusBadRequest, "invalid_deployment_config", "config must be a non-empty JSON object")
			return
		}
		version, code, ok, err := h.platform.createVersion(r.Context(), deployment, idempotencyKey, request.Config)
		if writeControlPlaneError(w, err) {
			return
		}
		if !ok {
			writeError(w, http.StatusConflict, code, "Idempotency-Key was already used with different configuration")
			return
		}
		writeJSON(w, http.StatusCreated, version)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET or POST")
	}
}

func validIdempotencyKey(key string) bool {
	if len(key) == 0 || len(key) > 128 {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] < 0x20 || key[i] > 0x7e {
			return false
		}
	}
	return true
}

func (h *AdminHandler) handleTransition(w http.ResponseWriter, r *http.Request, tenant TenantContext, deployment Deployment) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be POST")
		return
	}
	if !canOperate(tenant.Role) {
		writeError(w, http.StatusForbidden, "forbidden", "operator role is required")
		return
	}
	var request struct {
		Status    DeploymentStatus `json:"status"`
		VersionID string           `json:"version_id"`
	}
	if err := decodeStrict(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_transition", "status is required")
		return
	}
	updated, code, ok, err := h.platform.transition(r.Context(), deployment, request.Status, request.VersionID)
	if writeControlPlaneError(w, err) {
		return
	}
	if !ok {
		message := "Deployment lifecycle transition is not allowed"
		if code == "agent_app_already_has_active_deployment" {
			message = "Agent App already has an active Deployment"
		}
		writeError(w, http.StatusConflict, code, message)
		return
	}
	if request.Status == DeploymentPaused && updated.VersionID != "" {
		if err := h.runtime.RetireVersion(DeploymentVersionRef{TenantID: updated.TenantID, VersionID: updated.VersionID}); err != nil {
			writeError(w, http.StatusServiceUnavailable, "runtime_close_failed", "deployment runtime could not be retired")
			return
		}
	}
	writeJSON(w, http.StatusOK, updated)
}

func (p *SnapshotControlPlane) createDeployment(ctx context.Context, deployment Deployment) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return false, p.persistenceErr
	}
	key := resourceKey(deployment.TenantID, deployment.ID)
	if _, exists := p.deployments[key]; exists {
		return false, nil
	}
	p.deployments[key] = deployment
	ok := p.persistLockedContext(ctx)
	return ok, p.persistenceErr
}

func (p *SnapshotControlPlane) deployment(ctx context.Context, tenantID, id string) (Deployment, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return Deployment{}, false, p.persistenceErr
	}
	deployment, ok := p.deployments[resourceKey(tenantID, id)]
	return deployment, ok, nil
}

func (p *SnapshotControlPlane) listDeployments(ctx context.Context, tenantID string) ([]Deployment, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return nil, p.persistenceErr
	}
	items := []Deployment{}
	for _, item := range p.deployments {
		if item.TenantID == tenantID {
			items = append(items, item)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	return items, nil
}

func (p *SnapshotControlPlane) createVersion(ctx context.Context, deployment Deployment, idempotencyKey string, config map[string]any) (DeploymentVersion, string, bool, error) {
	canonical, _ := json.Marshal(config)
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return DeploymentVersion{}, "control_plane_unavailable", false, p.persistenceErr
	}
	key := resourceKey(deployment.TenantID, deployment.ID)
	creationKey := versionCreationKey{tenantID: deployment.TenantID, deploymentID: deployment.ID, idempotencyKey: idempotencyKey}
	if creation, exists := p.versionCreations[creationKey]; exists {
		if creation.config != string(canonical) {
			return DeploymentVersion{}, "idempotency_key_reused", false, nil
		}
		version := creation.version
		version.Config = cloneConfig(version.Config)
		return version, "", true, nil
	}
	number := len(p.versions[key]) + 1
	version := DeploymentVersion{ID: fmt.Sprintf("%s-v%d", deployment.ID, number), TenantID: deployment.TenantID, AgentAppID: deployment.AgentAppID, DeploymentID: deployment.ID, Number: number, Config: cloneConfig(config), CreatedAt: time.Now().UTC()}
	p.versions[key] = append(p.versions[key], version)
	p.versionCreations[creationKey] = versionCreation{config: string(canonical), version: version}
	if !p.persistLockedContext(ctx) {
		return DeploymentVersion{}, "control_plane_unavailable", false, p.persistenceErr
	}
	version.Config = cloneConfig(version.Config)
	return version, "", true, nil
}

func cloneConfig(config map[string]any) map[string]any {
	data, _ := json.Marshal(config)
	var result map[string]any
	_ = json.Unmarshal(data, &result)
	return result
}

func (p *SnapshotControlPlane) listVersions(ctx context.Context, tenantID, deploymentID string) ([]DeploymentVersion, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return nil, p.persistenceErr
	}
	source := p.versions[resourceKey(tenantID, deploymentID)]
	items := make([]DeploymentVersion, len(source))
	for i, version := range source {
		items[i] = version
		items[i].Config = cloneConfig(version.Config)
	}
	return items, nil
}

func (p *SnapshotControlPlane) transition(ctx context.Context, deployment Deployment, next DeploymentStatus, versionID string) (Deployment, string, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return Deployment{}, "control_plane_unavailable", false, p.persistenceErr
	}
	key := resourceKey(deployment.TenantID, deployment.ID)
	current := p.deployments[key]
	valid := (current.Status == DeploymentDraft && next == DeploymentPublished) || (current.Status == DeploymentPublished && next == DeploymentActive) || (current.Status == DeploymentActive && next == DeploymentPaused)
	if !valid {
		return Deployment{}, "invalid_deployment_transition", false, nil
	}
	if next == DeploymentActive {
		for _, item := range p.deployments {
			if item.TenantID == current.TenantID && item.AgentAppID == current.AgentAppID && item.ID != current.ID && item.Status == DeploymentActive {
				return Deployment{}, "agent_app_already_has_active_deployment", false, nil
			}
		}
	}
	if next == DeploymentPublished {
		found := false
		for _, version := range p.versions[key] {
			if version.ID == versionID {
				found = true
				break
			}
		}
		if !found {
			return Deployment{}, "deployment_version_not_found", false, nil
		}
		current.VersionID = versionID
	}
	current.Status = next
	p.deployments[key] = current
	if !p.persistLockedContext(ctx) {
		return Deployment{}, "control_plane_unavailable", false, p.persistenceErr
	}
	return current, "", true, nil
}

func (p *SnapshotControlPlane) startRollout(ctx context.Context, deployment Deployment, targetVersionID string, grayPercentage int) (Deployment, string, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return Deployment{}, "control_plane_unavailable", false, p.persistenceErr
	}
	key := resourceKey(deployment.TenantID, deployment.ID)
	current := p.deployments[key]
	if current.Status != DeploymentActive || current.VersionID == "" {
		return Deployment{}, "deployment_not_active", false, nil
	}
	if grayPercentage < 0 || grayPercentage > 100 {
		return Deployment{}, "invalid_gray_percentage", false, nil
	}
	if !p.hasVersionLocked(key, targetVersionID) {
		return Deployment{}, "deployment_version_not_found", false, nil
	}
	if current.CurrentVersionID == "" {
		current.CurrentVersionID = current.VersionID
	}
	if current.PreviousVersionID == "" || current.TargetVersionID != targetVersionID {
		current.PreviousVersionID = current.VersionID
	}
	current.TargetVersionID = targetVersionID
	current.GrayPercentage = grayPercentage
	current.RolloutStatus = DeploymentRolloutInProgress
	if grayPercentage == 100 {
		current.VersionID = targetVersionID
		current.CurrentVersionID = targetVersionID
		current.RolloutStatus = DeploymentRolloutCompleted
	}
	p.deployments[key] = current
	if !p.persistLockedContext(ctx) {
		return Deployment{}, "control_plane_unavailable", false, p.persistenceErr
	}
	return current, "", true, nil
}

func (p *SnapshotControlPlane) rollbackPreview(ctx context.Context, deployment Deployment) (DeploymentRollbackPreview, string, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return DeploymentRollbackPreview{}, "control_plane_unavailable", false, p.persistenceErr
	}
	key := resourceKey(deployment.TenantID, deployment.ID)
	current := p.deployments[key]
	if current.Status != DeploymentActive || current.PreviousVersionID == "" || !p.hasVersionLocked(key, current.PreviousVersionID) {
		return DeploymentRollbackPreview{}, "rollback_version_unavailable", false, nil
	}
	return DeploymentRollbackPreview{
		TenantID: current.TenantID, AgentAppID: current.AgentAppID, DeploymentID: current.ID,
		CurrentVersionID: current.VersionID, PreviousVersionID: current.PreviousVersionID,
		ExpectedResult: "active routing returns to the previous immutable Version",
	}, "", true, nil
}

func (p *SnapshotControlPlane) rollback(ctx context.Context, deployment Deployment) (Deployment, string, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return Deployment{}, "control_plane_unavailable", false, p.persistenceErr
	}
	key := resourceKey(deployment.TenantID, deployment.ID)
	current := p.deployments[key]
	if current.Status != DeploymentActive || current.PreviousVersionID == "" || !p.hasVersionLocked(key, current.PreviousVersionID) {
		return Deployment{}, "rollback_version_unavailable", false, nil
	}
	current.VersionID = current.PreviousVersionID
	current.CurrentVersionID = current.PreviousVersionID
	current.TargetVersionID = current.PreviousVersionID
	current.GrayPercentage = 100
	current.RolloutStatus = DeploymentRolloutCompleted
	p.deployments[key] = current
	if !p.persistLockedContext(ctx) {
		return Deployment{}, "control_plane_unavailable", false, p.persistenceErr
	}
	return current, "", true, nil
}

func (p *SnapshotControlPlane) hasVersionLocked(key, versionID string) bool {
	if versionID == "" {
		return false
	}
	for _, version := range p.versions[key] {
		if version.ID == versionID {
			return true
		}
	}
	return false
}
