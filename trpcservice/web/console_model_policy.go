package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

type tenantModelPolicyResponse struct {
	TenantID string                      `json:"tenant_id"`
	Models   []identity.TenantModelGrant `json:"models"`
}

func (c *consoleAPI) tenantModelPolicy(writer http.ResponseWriter, request *http.Request) {
	if c.dependencies.Identities == nil {
		writeJSON(writer, http.StatusServiceUnavailable, map[string]any{"error": "identity store is not configured"})
		return
	}
	tenantID := resolveTenantParam(request)
	if tenantID == "" {
		badRequest(writer, "tenant is required")
		return
	}
	switch request.Method {
	case http.MethodGet:
		user, ok := sessionUser(request)
		if !ok {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		if !user.IsSystemAdmin && !requireTenantWrite(writer, request, tenantID) {
			return
		}
		grants, err := c.dependencies.Identities.ListTenantModelGrants(request.Context(), tenantID)
		if err != nil {
			c.writeTenantModelPolicyError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, tenantModelPolicyResponse{TenantID: tenantID, Models: grants})
	case http.MethodPut:
		if !requireSystemAdmin(writer, request) {
			return
		}
		var body tenantModelPolicyResponse
		if err := decodeJSONBody(request, &body); err != nil {
			badRequest(writer, err.Error())
			return
		}
		if strings.TrimSpace(body.TenantID) != "" && strings.TrimSpace(body.TenantID) != tenantID {
			badRequest(writer, "tenant_id does not match selected tenant")
			return
		}
		if err := c.validateModelGrantsAgainstCatalog(body.Models); err != nil {
			badRequest(writer, err.Error())
			return
		}
		if err := c.dependencies.Identities.ReplaceTenantModelGrants(request.Context(), tenantID, body.Models); err != nil {
			c.writeTenantModelPolicyError(writer, err)
			return
		}
		grants, err := c.dependencies.Identities.ListTenantModelGrants(request.Context(), tenantID)
		if err != nil {
			c.writeTenantModelPolicyError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, tenantModelPolicyResponse{TenantID: tenantID, Models: grants})
	default:
		methodNotAllowed(writer, http.MethodGet+", "+http.MethodPut)
	}
}

func (c *consoleAPI) validateModelGrantsAgainstCatalog(grants []identity.TenantModelGrant) error {
	c.modelMu.Lock()
	defer c.modelMu.Unlock()
	available := make(map[string]struct{})
	for _, provider := range c.dependencies.System.ModelProviders {
		for _, model := range provider.Models {
			available[provider.ID+"\x00"+model.Name] = struct{}{}
		}
	}
	seen := make(map[string]struct{}, len(grants))
	for _, grant := range grants {
		providerID, modelName := strings.TrimSpace(grant.ProviderID), strings.TrimSpace(grant.ModelName)
		if providerID == "" || modelName == "" {
			return errors.New("model grant requires provider_id and name")
		}
		key := providerID + "\x00" + modelName
		if _, exists := seen[key]; exists {
			return fmt.Errorf("duplicate model grant %q/%q", providerID, modelName)
		}
		if _, exists := available[key]; !exists {
			return fmt.Errorf("model %q/%q is not present in the platform model catalog", providerID, modelName)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func (c *consoleAPI) tenantModelProviders(ctx context.Context, tenantID string) ([]TenantModelProviderInfo, error) {
	if c.dependencies.Identities == nil {
		return nil, errors.New("identity store is not configured")
	}
	grants, err := c.dependencies.Identities.ListTenantModelGrants(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	allowed := make(map[string]struct{}, len(grants))
	for _, grant := range grants {
		allowed[grant.ProviderID+"\x00"+grant.ModelName] = struct{}{}
	}
	c.modelMu.Lock()
	defer c.modelMu.Unlock()
	providers := make([]TenantModelProviderInfo, 0)
	for _, provider := range c.dependencies.System.ModelProviders {
		models := make([]ModelInfo, 0)
		for _, model := range provider.Models {
			if _, ok := allowed[provider.ID+"\x00"+model.Name]; ok {
				models = append(models, ModelInfo{Name: model.Name, Capabilities: model.Capabilities, Source: model.Source})
			}
		}
		if len(models) > 0 {
			providers = append(providers, TenantModelProviderInfo{ID: provider.ID, Type: provider.Type, Models: models})
		}
	}
	return providers, nil
}

func (c *consoleAPI) validateApplicationModelPolicy(ctx context.Context, tenantConfig config.TenantConfig) error {
	if strings.TrimSpace(tenantConfig.Model.ProviderID) == "" && strings.TrimSpace(tenantConfig.Model.Name) == "" && len(tenantConfig.Model.FailoverCandidates) == 0 {
		return nil
	}
	if c.dependencies.Identities == nil {
		return errors.New("identity store is not configured")
	}
	grants, err := c.dependencies.Identities.ListTenantModelGrants(ctx, tenantConfig.TenantID)
	if err != nil {
		return fmt.Errorf("read tenant model policy: %w", err)
	}
	allowed := make(map[string]struct{}, len(grants))
	for _, grant := range grants {
		allowed[grant.ProviderID+"\x00"+grant.ModelName] = struct{}{}
	}
	check := func(location, providerID, modelName string) error {
		if strings.TrimSpace(providerID) == "" && strings.TrimSpace(modelName) == "" {
			return nil
		}
		if _, ok := allowed[strings.TrimSpace(providerID)+"\x00"+strings.TrimSpace(modelName)]; !ok {
			return fmt.Errorf("%s model %q/%q is not authorized for tenant %q", location, providerID, modelName, tenantConfig.TenantID)
		}
		return nil
	}
	if err := check("application", tenantConfig.Model.ProviderID, tenantConfig.Model.Name); err != nil {
		return err
	}
	for index, candidate := range tenantConfig.Model.FailoverCandidates {
		if err := check(fmt.Sprintf("application.model.failover_candidates[%d]", index), candidate.ProviderID, candidate.Name); err != nil {
			return err
		}
	}
	return nil
}

func (c *consoleAPI) validateApplication(ctx context.Context, tenantConfig config.TenantConfig) error {
	if err := c.dependencies.ApplicationValidator.Validate(tenantConfig); err != nil {
		return err
	}
	if err := c.validateApplicationModelPolicy(ctx, tenantConfig); err != nil {
		return err
	}
	if err := c.validateApplicationToolPolicy(ctx, tenantConfig); err != nil {
		return err
	}
	return c.validateApplicationStoragePolicy(ctx, tenantConfig)
}

func (c *consoleAPI) validateApplicationStoragePolicy(ctx context.Context, tenantConfig config.TenantConfig) error {
	if c.dependencies.BackendProfiles == nil {
		return errors.New("backend profile store is not configured")
	}
	for _, item := range []struct {
		domain    string
		profileID string
	}{
		{storage.BackendDomainSession, tenantConfig.Storage.Session.ProfileID},
		{storage.BackendDomainMemory, tenantConfig.Storage.Memory.ProfileID},
		{storage.BackendDomainKnowledge, tenantConfig.Storage.Knowledge.ProfileID},
		{storage.BackendDomainArtifact, tenantConfig.Storage.Artifact.ProfileID},
	} {
		if strings.TrimSpace(item.profileID) == "" && tenantConfig.Status == config.AgentActive {
			return fmt.Errorf("application.storage.%s.profile_id is required for an active application", item.domain)
		}
		if strings.TrimSpace(item.profileID) == "" {
			continue
		}
		if _, err := c.dependencies.BackendProfiles.ResolveTenantBackend(ctx, tenantConfig.TenantID, item.domain, item.profileID); err != nil {
			return fmt.Errorf("application.storage.%s profile %q is unavailable: %w", item.domain, item.profileID, err)
		}
	}
	return nil
}

func (c *consoleAPI) writeTenantModelPolicyError(writer http.ResponseWriter, err error) {
	if errors.Is(err, identity.ErrTenantNotFound) {
		notFound(writer, "tenant does not exist")
		return
	}
	serverError(writer, "tenant model policy", err)
}
