// Package admin validates and mutates control-plane configuration.
package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
var ErrInvalid = errors.New("invalid Admin request")

type Service struct {
	repository controlplane.MutableRepository
	tools      *platformtool.Catalog
}

func New(repository controlplane.Repository, catalogs ...*platformtool.Catalog) (*Service, error) {
	mutable, ok := repository.(controlplane.MutableRepository)
	if !ok {
		return nil, fmt.Errorf("control-plane repository is not mutable")
	}
	catalog := platformtool.DefaultCatalog()
	if len(catalogs) > 0 && catalogs[0] != nil {
		catalog = catalogs[0]
	}
	return &Service{repository: mutable, tools: catalog}, nil
}

func (s *Service) CreateTenant(ctx context.Context, tenant controlplane.Tenant) (controlplane.Tenant, error) {
	if !identifierPattern.MatchString(tenant.ID) || strings.TrimSpace(tenant.DisplayName) == "" {
		return controlplane.Tenant{}, invalidf("tenant ID and display name are invalid")
	}
	if tenant.Status == "" {
		tenant.Status = controlplane.StatusActive
	}
	if tenant.Region == "" || tenant.SecretNamespace == "" {
		return controlplane.Tenant{}, invalidf("tenant region and secret namespace are required")
	}
	if err := normalizeJSON(&tenant.QuotaConfig); err != nil {
		return controlplane.Tenant{}, invalidf("quota config: %v", err)
	}
	if err := normalizeJSON(&tenant.AuditPolicy); err != nil {
		return controlplane.Tenant{}, invalidf("audit policy: %v", err)
	}
	now := time.Now().UTC()
	tenant.Version = 1
	tenant.CreatedAt = now
	tenant.UpdatedAt = now
	if err := s.repository.CreateTenant(ctx, tenant); err != nil {
		return controlplane.Tenant{}, err
	}
	return tenant, nil
}

func (s *Service) CreateAgentApp(ctx context.Context, app controlplane.AgentApp) (controlplane.AgentApp, error) {
	if !identifierPattern.MatchString(app.ID) || !identifierPattern.MatchString(app.TenantID) ||
		strings.TrimSpace(app.Name) == "" {
		return controlplane.AgentApp{}, invalidf("Agent app identity is invalid")
	}
	if app.Status == "" {
		app.Status = controlplane.StatusActive
	}
	if err := normalizeJSON(&app.RolloutPolicy); err != nil {
		return controlplane.AgentApp{}, invalidf("rollout policy: %v", err)
	}
	now := time.Now().UTC()
	app.StableRevisionID = ""
	app.Version = 1
	app.CreatedAt = now
	app.UpdatedAt = now
	if err := s.repository.CreateAgentApp(ctx, app); err != nil {
		return controlplane.AgentApp{}, err
	}
	return app, nil
}

func (s *Service) CreateRevision(
	ctx context.Context,
	revision controlplane.AgentRevision,
) (controlplane.AgentRevision, error) {
	if revision.ID == "" {
		revision.ID = "rev-" + uuid.NewString()
	}
	if !identifierPattern.MatchString(revision.ID) ||
		!identifierPattern.MatchString(revision.TenantID) ||
		!identifierPattern.MatchString(revision.AppID) || revision.RevisionNo <= 0 {
		return controlplane.AgentRevision{}, invalidf("Agent revision identity is invalid")
	}
	if revision.AgentType == "" || revision.CreatedBy == "" {
		return controlplane.AgentRevision{}, invalidf("Agent type and creator are required")
	}
	for name, value := range map[string]*json.RawMessage{
		"agent_config":     &revision.AgentConfig,
		"model_config":     &revision.ModelConfig,
		"tool_policy":      &revision.ToolPolicy,
		"knowledge_config": &revision.KnowledgeConfig,
		"memory_config":    &revision.MemoryConfig,
		"guardrail_config": &revision.GuardrailConfig,
	} {
		if err := normalizeJSON(value); err != nil {
			return controlplane.AgentRevision{}, invalidf("%s: %v", name, err)
		}
	}
	toolPolicy, err := governance.ParseToolPolicy(revision.ToolPolicy)
	if err != nil {
		return controlplane.AgentRevision{}, invalidf("tool_policy: %v", err)
	}
	if _, err := s.tools.Resolve(toolPolicy.AllowedTools); err != nil {
		return controlplane.AgentRevision{}, invalidf("tool_policy: %v", err)
	}
	revision.Checksum = controlplane.RevisionChecksum(revision)
	revision.CreatedAt = time.Now().UTC()
	if err := s.repository.CreateRevision(ctx, revision); err != nil {
		return controlplane.AgentRevision{}, err
	}
	return revision, nil
}

func (s *Service) PublishRevision(
	ctx context.Context,
	tenantID string,
	appID string,
	revisionID string,
	expectedVersion int64,
) (controlplane.AgentApp, error) {
	if expectedVersion <= 0 {
		return controlplane.AgentApp{}, invalidf("expected app version must be positive")
	}
	return s.repository.PublishRevision(ctx, tenantID, appID, revisionID, expectedVersion)
}

func (s *Service) CreateChannelBinding(
	ctx context.Context,
	binding controlplane.ChannelBinding,
) (controlplane.ChannelBinding, error) {
	if binding.ID == "" {
		binding.ID = "binding-" + uuid.NewString()
	}
	if binding.CallbackKey == "" {
		binding.CallbackKey = strings.ReplaceAll(uuid.NewString(), "-", "")
	}
	if !identifierPattern.MatchString(binding.ID) ||
		!identifierPattern.MatchString(binding.TenantID) ||
		!identifierPattern.MatchString(binding.AppID) ||
		!identifierPattern.MatchString(binding.CallbackKey) ||
		binding.ChannelType == "" || binding.AccountID == "" || binding.SecretRef == "" {
		return controlplane.ChannelBinding{}, invalidf("channel binding is incomplete")
	}
	if err := normalizeJSON(&binding.Config); err != nil {
		return controlplane.ChannelBinding{}, invalidf("channel binding config: %v", err)
	}
	if binding.Status == "" {
		binding.Status = controlplane.StatusActive
	}
	now := time.Now().UTC()
	binding.Version = 1
	binding.CreatedAt = now
	binding.UpdatedAt = now
	if err := s.repository.CreateChannelBinding(ctx, binding); err != nil {
		return controlplane.ChannelBinding{}, err
	}
	return binding, nil
}

func (s *Service) CreateBackendBinding(
	ctx context.Context,
	binding controlplane.BackendBinding,
) (controlplane.BackendBinding, error) {
	if binding.ID == "" {
		binding.ID = "backend-" + uuid.NewString()
	}
	if !identifierPattern.MatchString(binding.ID) ||
		!identifierPattern.MatchString(binding.TenantID) ||
		(binding.AppID != "" && !identifierPattern.MatchString(binding.AppID)) ||
		binding.ResourceType == "" || binding.BackendType == "" {
		return controlplane.BackendBinding{}, invalidf("backend binding is incomplete")
	}
	if err := normalizeJSON(&binding.Config); err != nil {
		return controlplane.BackendBinding{}, invalidf("backend binding config: %v", err)
	}
	if binding.IsolationLevel == "" {
		binding.IsolationLevel = "shared"
	}
	if binding.MigrationState == "" {
		binding.MigrationState = "active"
	}
	now := time.Now().UTC()
	binding.Version = 1
	binding.CreatedAt = now
	binding.UpdatedAt = now
	if err := s.repository.CreateBackendBinding(ctx, binding); err != nil {
		return controlplane.BackendBinding{}, err
	}
	return binding, nil
}

func normalizeJSON(value *json.RawMessage) error {
	if len(*value) == 0 {
		*value = json.RawMessage(`{}`)
		return nil
	}
	var decoded any
	if err := json.Unmarshal(*value, &decoded); err != nil {
		return err
	}
	canonical, err := json.Marshal(decoded)
	if err != nil {
		return err
	}
	*value = canonical
	return nil
}

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}
