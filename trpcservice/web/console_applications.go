package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// applicationRequest is the admin payload for creating or publishing a new
// application version. Version numbers are always derived by the server.
type applicationRequest struct {
	TenantID    string                  `json:"tenant_id"`
	AppCode     string                  `json:"app_code"`
	Status      string                  `json:"status"`
	Instruction string                  `json:"instruction"`
	Model       config.ModelConfig      `json:"model"`
	Tools       config.ToolPolicy       `json:"tools"`
	Storage     config.StoragePolicy    `json:"storage"`
	Governance  config.GovernancePolicy `json:"governance"`
	Audit       config.AuditPolicy      `json:"audit"`
	Channels    []struct {
		Type                string   `json:"type"`
		BindingID           string   `json:"binding_id"`
		CredentialRef       string   `json:"credential_ref"`
		TrustedEnterpriseID string   `json:"trusted_enterprise_id,omitempty"`
		AccessPolicy        string   `json:"access_policy,omitempty"`
		Allowlist           []string `json:"allowlist,omitempty"`
	} `json:"channels"`
}

func (r applicationRequest) toTenantConfig(version uint64) (config.TenantConfig, error) {
	tenantConfig := config.TenantConfig{
		TenantID:      strings.TrimSpace(r.TenantID),
		AppCode:       strings.TrimSpace(r.AppCode),
		Status:        strings.TrimSpace(r.Status),
		ConfigVersion: version,
		Instruction:   strings.TrimSpace(r.Instruction),
		Model:         r.Model,
		Tools:         r.Tools,
		Storage:       r.Storage,
		Governance:    r.Governance,
		Audit:         r.Audit,
		Channels:      make([]config.ChannelBinding, 0, len(r.Channels)),
	}
	if tenantConfig.Status == "" {
		tenantConfig.Status = config.AgentDraft
	}
	for _, binding := range r.Channels {
		accessPolicy := strings.TrimSpace(binding.AccessPolicy)
		if accessPolicy == "" {
			accessPolicy = config.ChannelAccessPublic
		}
		tenantConfig.Channels = append(tenantConfig.Channels, config.ChannelBinding{
			Type:                binding.Type,
			BindingID:           strings.TrimSpace(binding.BindingID),
			CredentialRef:       strings.TrimSpace(binding.CredentialRef),
			TrustedEnterpriseID: strings.TrimSpace(binding.TrustedEnterpriseID),
			AccessPolicy:        accessPolicy,
			Allowlist:           append([]string(nil), binding.Allowlist...),
		})
	}
	return tenantConfig, nil
}

func (c *consoleAPI) applications(writer http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case http.MethodGet:
		c.listApplications(writer, request)
	case http.MethodPost:
		c.createApplication(writer, request)
	default:
		methodNotAllowed(writer, "GET, POST")
	}
}

func (c *consoleAPI) updateApplication(writer http.ResponseWriter, request *http.Request) {
	segments := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
	if len(segments) < 5 || segments[0] != "api" || segments[1] != "v1" || segments[2] != "apps" {
		badRequest(writer, "path must start with /api/v1/apps/{tenant_id}/{app_code}")
		return
	}
	tenantID, appCode := segments[3], segments[4]
	switch {
	case len(segments) == 5:
		c.publishApplicationUpdate(writer, request, tenantID, appCode)
	case len(segments) == 6 && segments[5] == "versions":
		c.applicationVersions(writer, request, tenantID, appCode)
	case len(segments) == 7 && segments[5] == "versions":
		c.getApplicationVersion(writer, request, tenantID, appCode, segments[6])
	case len(segments) == 7 && segments[5] == "rollback":
		c.rollbackApplicationVersion(writer, request, tenantID, appCode, segments[6])
	case len(segments) == 6 && segments[5] == "candidate":
		c.applicationCandidate(writer, request, tenantID, appCode)
	case len(segments) == 7 && segments[5] == "candidate" && segments[6] == "promote":
		c.promoteApplicationCandidate(writer, request, tenantID, appCode)
	case len(segments) == 6 && segments[5] == "rollout":
		c.applicationRollout(writer, request, tenantID, appCode)
	default:
		badRequest(writer, "path must be an application, version, candidate, rollout, or rollback endpoint")
	}
}

func (c *consoleAPI) publishApplicationUpdate(writer http.ResponseWriter, request *http.Request, tenantID, appCode string) {
	if request.Method != http.MethodPut {
		methodNotAllowed(writer, http.MethodPut)
		return
	}
	if !requireTenantWrite(writer, request, tenantID) {
		return
	}
	var body applicationRequest
	if err := decodeJSONBody(request, &body); err != nil {
		badRequest(writer, err.Error())
		return
	}
	version, err := c.nextApplicationVersion(request.Context(), tenantID, appCode)
	if err != nil {
		c.writeApplicationLookupError(writer, err)
		return
	}
	body.TenantID = tenantID
	body.AppCode = appCode
	tenantConfig, err := body.toTenantConfig(version)
	if err != nil {
		badRequest(writer, err.Error())
		return
	}
	c.publishApplication(writer, request, tenantConfig, http.StatusOK)
}

func (c *consoleAPI) applicationVersions(writer http.ResponseWriter, request *http.Request, tenantID, appCode string) {
	switch request.Method {
	case http.MethodGet:
		if !requireTenantRead(writer, request, tenantID) {
			return
		}
		versions, err := c.dependencies.Configurations.ListVersions(request.Context(), tenantID, appCode, 100)
		if err != nil {
			c.writeApplicationLookupError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"versions": versions})
	case http.MethodPost:
		if !requireTenantWrite(writer, request, tenantID) {
			return
		}
		var body applicationRequest
		if err := decodeJSONBody(request, &body); err != nil {
			badRequest(writer, err.Error())
			return
		}
		version, err := c.nextApplicationVersion(request.Context(), tenantID, appCode)
		if err != nil {
			c.writeApplicationLookupError(writer, err)
			return
		}
		body.TenantID, body.AppCode = tenantID, appCode
		tenantConfig, err := body.toTenantConfig(version)
		if err != nil {
			badRequest(writer, err.Error())
			return
		}
		if err := c.validateApplication(request.Context(), tenantConfig); err != nil {
			badRequest(writer, err.Error())
			return
		}
		staged, err := c.dependencies.Configurations.Stage(request.Context(), tenantConfig)
		if err != nil {
			if errors.Is(err, tenant.ErrVersionConflict) {
				conflict(writer, "configuration version conflict: retry from the latest version")
				return
			}
			if errors.Is(err, tenant.ErrRolloutInProgress) {
				conflict(writer, "stop or complete the current rollout before replacing the candidate")
				return
			}
			c.writeApplicationLookupError(writer, err)
			return
		}
		writeJSON(writer, http.StatusCreated, map[string]any{"application": staged})
	default:
		methodNotAllowed(writer, "GET, POST")
	}
}

func (c *consoleAPI) getApplicationVersion(writer http.ResponseWriter, request *http.Request, tenantID, appCode, rawVersion string) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	if !requireTenantRead(writer, request, tenantID) {
		return
	}
	version, err := parseConfigVersion(rawVersion)
	if err != nil {
		badRequest(writer, err.Error())
		return
	}
	snapshot, err := c.dependencies.Configurations.GetVersion(request.Context(), tenantID, appCode, version)
	if err != nil {
		c.writeApplicationLookupError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"application": snapshot})
}

func (c *consoleAPI) rollbackApplicationVersion(writer http.ResponseWriter, request *http.Request, tenantID, appCode, rawVersion string) {
	if request.Method != http.MethodPost {
		methodNotAllowed(writer, http.MethodPost)
		return
	}
	if !requireTenantWrite(writer, request, tenantID) {
		return
	}
	version, err := parseConfigVersion(rawVersion)
	if err != nil {
		badRequest(writer, err.Error())
		return
	}
	previous, err := c.dependencies.Configurations.GetVersion(request.Context(), tenantID, appCode, version)
	if err != nil {
		c.writeApplicationLookupError(writer, err)
		return
	}
	nextVersion, err := c.nextApplicationVersion(request.Context(), tenantID, appCode)
	if err != nil {
		c.writeApplicationLookupError(writer, err)
		return
	}
	tenantConfig := previous.Config
	tenantConfig.ConfigVersion = nextVersion
	c.publishApplication(writer, request, tenantConfig, http.StatusOK)
}

func (c *consoleAPI) nextApplicationVersion(ctx context.Context, tenantID, appCode string) (uint64, error) {
	versions, err := c.dependencies.Configurations.ListVersions(ctx, tenantID, appCode, 1)
	if err != nil {
		return 0, err
	}
	if len(versions) != 1 || versions[0].Config.ConfigVersion == 0 {
		return 0, tenant.ErrNotFound
	}
	return versions[0].Config.ConfigVersion + 1, nil
}

type rolloutGenerationRequest struct {
	ExpectedGeneration uint64 `json:"expected_generation"`
}

type candidateActionRequest struct {
	ExpectedVersion    uint64 `json:"expected_version"`
	ExpectedGeneration uint64 `json:"expected_generation"`
}

func (c *consoleAPI) applicationCandidate(writer http.ResponseWriter, request *http.Request, tenantID, appCode string) {
	switch request.Method {
	case http.MethodGet:
		if !requireTenantRead(writer, request, tenantID) {
			return
		}
		candidate, err := c.dependencies.Configurations.GetCandidate(request.Context(), tenantID, appCode)
		if errors.Is(err, tenant.ErrCandidateNotFound) {
			writeJSON(writer, http.StatusOK, map[string]any{"candidate": nil})
			return
		}
		if err != nil {
			c.writeApplicationLookupError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"candidate": candidate})
	case http.MethodDelete:
		if !requireTenantWrite(writer, request, tenantID) {
			return
		}
		var body candidateActionRequest
		if err := decodeJSONBody(request, &body); err != nil {
			badRequest(writer, err.Error())
			return
		}
		if err := c.dependencies.Configurations.DiscardCandidate(request.Context(), tenantID, appCode, body.ExpectedVersion); err != nil {
			c.writeRolloutError(writer, err)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(writer, "GET, DELETE")
	}
}

func (c *consoleAPI) applicationRollout(writer http.ResponseWriter, request *http.Request, tenantID, appCode string) {
	switch request.Method {
	case http.MethodGet:
		if !requireTenantRead(writer, request, tenantID) {
			return
		}
		rollout, err := c.dependencies.Configurations.GetRollout(request.Context(), tenantID, appCode)
		if errors.Is(err, tenant.ErrRolloutNotFound) {
			writeJSON(writer, http.StatusOK, map[string]any{"rollout": nil})
			return
		}
		if err != nil {
			serverError(writer, "read application rollout", err)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"rollout": rollout})
	case http.MethodPut:
		if !requireTenantWrite(writer, request, tenantID) {
			return
		}
		var update tenant.RolloutUpdate
		if err := decodeJSONBody(request, &update); err != nil {
			badRequest(writer, err.Error())
			return
		}
		for _, platformUserID := range update.TestUserIDs {
			if _, err := c.dependencies.Identities.RoleFor(request.Context(), tenantID, platformUserID); err != nil {
				badRequest(writer, fmt.Sprintf("test user %q is not an active tenant member", platformUserID))
				return
			}
		}
		rollout, err := c.dependencies.Configurations.SetRollout(request.Context(), tenantID, appCode, update)
		if err != nil {
			c.writeRolloutError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"rollout": rollout})
	case http.MethodDelete:
		if !requireTenantWrite(writer, request, tenantID) {
			return
		}
		var body rolloutGenerationRequest
		if err := decodeJSONBody(request, &body); err != nil {
			badRequest(writer, err.Error())
			return
		}
		if err := c.dependencies.Configurations.StopRollout(request.Context(), tenantID, appCode, body.ExpectedGeneration); err != nil {
			c.writeRolloutError(writer, err)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(writer, "GET, PUT, DELETE")
	}
}

func (c *consoleAPI) promoteApplicationCandidate(writer http.ResponseWriter, request *http.Request, tenantID, appCode string) {
	if request.Method != http.MethodPost {
		methodNotAllowed(writer, http.MethodPost)
		return
	}
	if !requireTenantWrite(writer, request, tenantID) {
		return
	}
	var body candidateActionRequest
	if err := decodeJSONBody(request, &body); err != nil {
		badRequest(writer, err.Error())
		return
	}
	promoted, err := c.dependencies.Configurations.PromoteCandidate(request.Context(), tenantID, appCode, body.ExpectedVersion, body.ExpectedGeneration)
	if err != nil {
		c.writeRolloutError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"application": promoted})
}

func (c *consoleAPI) writeRolloutError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, tenant.ErrRolloutConflict):
		conflict(writer, "rollout changed on another node; refresh and retry")
	case errors.Is(err, tenant.ErrVersionConflict):
		conflict(writer, "candidate changed on another node; refresh and retry")
	case errors.Is(err, tenant.ErrRolloutInProgress):
		conflict(writer, "stop the current rollout before discarding the candidate")
	case errors.Is(err, tenant.ErrRolloutNotFound), errors.Is(err, tenant.ErrCandidateNotFound), errors.Is(err, tenant.ErrNotFound):
		notFound(writer, "application rollout or candidate does not exist")
	default:
		badRequest(writer, err.Error())
	}
}

func parseConfigVersion(raw string) (uint64, error) {
	version, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	if err != nil || version == 0 {
		return 0, fmt.Errorf("configuration version must be a positive integer")
	}
	return version, nil
}

func (c *consoleAPI) writeApplicationLookupError(writer http.ResponseWriter, err error) {
	if errors.Is(err, tenant.ErrNotFound) {
		notFound(writer, "tenant application configuration does not exist")
		return
	}
	serverError(writer, "resolve tenant application configuration", err)
}

func (c *consoleAPI) createApplication(writer http.ResponseWriter, request *http.Request) {
	var body applicationRequest
	if err := decodeJSONBody(request, &body); err != nil {
		badRequest(writer, err.Error())
		return
	}
	tenantConfig, err := body.toTenantConfig(1)
	if err != nil {
		badRequest(writer, err.Error())
		return
	}
	if strings.TrimSpace(tenantConfig.TenantID) == "" || strings.TrimSpace(tenantConfig.AppCode) == "" {
		badRequest(writer, "tenant_id and app_code are required")
		return
	}
	if !requireTenantWrite(writer, request, tenantConfig.TenantID) {
		return
	}
	c.publishApplication(writer, request, tenantConfig, http.StatusCreated)
}

func (c *consoleAPI) publishApplication(writer http.ResponseWriter, request *http.Request, tenantConfig config.TenantConfig, successStatus int) {
	if err := c.validateApplication(request.Context(), tenantConfig); err != nil {
		badRequest(writer, err.Error())
		return
	}
	snapshot, err := c.dependencies.Configurations.Publish(request.Context(), tenantConfig)
	if err != nil {
		switch {
		case errors.Is(err, tenant.ErrVersionConflict):
			conflict(writer, "configuration version conflict: a newer version is already active")
		case errors.Is(err, tenant.ErrBindingConflict):
			conflict(writer, "channel binding is already owned by another application")
		case errors.Is(err, tenant.ErrRolloutInProgress):
			conflict(writer, "stop or complete the current rollout before publishing another version")
		default:
			badRequest(writer, err.Error())
		}
		return
	}
	writeJSON(writer, successStatus, map[string]any{"application": snapshot})
}

func (c *consoleAPI) listApplications(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	user, ok := sessionUser(request)
	if !ok {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	tenantID := resolveTenantParam(request)
	if tenantID != "" {
		if !requireTenantRead(writer, request, tenantID) {
			return
		}
		applications, err := c.dependencies.Configurations.ListApplications(request.Context(), tenantID)
		if err != nil {
			serverError(writer, "list applications", err)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"applications": applications})
		return
	}
	applications, err := c.dependencies.Configurations.ListApplications(request.Context(), "")
	if err != nil {
		serverError(writer, "list applications", err)
		return
	}
	visible := make([]tenant.Snapshot, 0, len(applications))
	for _, application := range applications {
		if canAccessTenant(user, application.Config.TenantID) {
			visible = append(visible, application)
		}
	}
	applications = visible
	writeJSON(writer, http.StatusOK, map[string]any{"applications": applications})
}
