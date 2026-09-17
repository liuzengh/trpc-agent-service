// Package controlplane resolves immutable execution profiles from the
// authoritative control-plane repositories.
package controlplane

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type TenantReader interface {
	Get(context.Context, string) (tenant.Tenant, error)
}

type AgentReader interface {
	Get(context.Context, string, string) (agentapp.AgentApp, error)
	GetRevision(context.Context, string, string, int64) (agentapp.Revision, error)
}

type ConfigReader interface {
	Get(context.Context, string, int64) (config.Snapshot, error)
}

type ModelProfileReader interface {
	GetModel(context.Context, string, string, int64) (provider.ModelProfileSnapshot, error)
}

// Resolver deliberately reads exact revision/config/model versions. Current
// pointers are used only as status gates; they never replace a version carried
// by the authoritative execution envelope.
type Resolver struct {
	Tenants TenantReader
	Agents  AgentReader
	Configs ConfigReader
	Models  ModelProfileReader
}

func (r Resolver) Resolve(ctx context.Context, key profile.ExecutionProfileKey) (profile.ExecutionProfileSnapshot, error) {
	return r.resolve(ctx, key, false)
}

func (r Resolver) ResolveChild(ctx context.Context, parent profile.ExecutionProfileSnapshot, node agentapp.AgentNodeSpecV1) (profile.ExecutionProfileSnapshot, error) {
	if err := r.verifyParent(ctx, parent, node); err != nil {
		return profile.ExecutionProfileSnapshot{}, err
	}
	key := profile.ExecutionProfileKey{
		TenantID: parent.Key.TenantID, TenantVersion: parent.Key.TenantVersion,
		AgentAppID: node.AgentRef.AgentAppID, AgentAppRevision: node.AgentRef.Revision,
		ContentDigest: node.AgentRef.ContentDigest, ConfigVersion: parent.Key.ConfigVersion,
		PolicyVersion: parent.Key.PolicyVersion,
	}
	return r.resolve(ctx, key, true)
}

func (r Resolver) resolve(ctx context.Context, key profile.ExecutionProfileKey, authorizedChild bool) (profile.ExecutionProfileSnapshot, error) {
	if r.Tenants == nil || r.Agents == nil || r.Configs == nil || r.Models == nil {
		return profile.ExecutionProfileSnapshot{}, runtime.ErrCapabilityUnsupported
	}
	if key.TenantID == "" || key.AgentAppID == "" || key.AgentAppRevision < 1 || key.ContentDigest == "" ||
		key.ConfigVersion < 1 || key.PolicyVersion < 1 || key.TenantVersion < 1 || key.AgentAppVersion < 0 {
		return profile.ExecutionProfileSnapshot{}, runtime.ErrInvalidEnvelope
	}

	root, err := r.Tenants.Get(ctx, key.TenantID)
	if err != nil {
		return profile.ExecutionProfileSnapshot{}, err
	}
	if root.TenantID != key.TenantID {
		return profile.ExecutionProfileSnapshot{}, runtime.ErrTenantScope
	}
	if root.Version < key.TenantVersion {
		return profile.ExecutionProfileSnapshot{}, runtime.ErrVersionMismatch
	}
	if root.Status == tenant.StatusDisabled {
		return profile.ExecutionProfileSnapshot{}, runtime.ErrCancelRequested
	}

	app, err := r.Agents.Get(ctx, key.TenantID, key.AgentAppID)
	if err != nil {
		return profile.ExecutionProfileSnapshot{}, err
	}
	if app.TenantID != key.TenantID || app.AgentAppID != key.AgentAppID {
		return profile.ExecutionProfileSnapshot{}, runtime.ErrTenantScope
	}
	if app.Status == agentapp.StatusDisabled || app.Status == agentapp.StatusDraft {
		return profile.ExecutionProfileSnapshot{}, runtime.ErrCapabilityUnsupported
	}
	if key.AgentAppVersion > 0 && app.Version < key.AgentAppVersion {
		return profile.ExecutionProfileSnapshot{}, runtime.ErrVersionMismatch
	}

	revision, err := r.Agents.GetRevision(ctx, key.TenantID, key.AgentAppID, key.AgentAppRevision)
	if err != nil {
		return profile.ExecutionProfileSnapshot{}, err
	}
	if revision.TenantID != key.TenantID || revision.AgentAppID != key.AgentAppID {
		return profile.ExecutionProfileSnapshot{}, runtime.ErrTenantScope
	}
	if revision.State != agentapp.RevisionPublished || revision.ContentDigest != key.ContentDigest {
		return profile.ExecutionProfileSnapshot{}, runtime.ErrVersionMismatch
	}
	digest, err := revision.ComputeContentDigest()
	if err != nil || digest != revision.ContentDigest {
		return profile.ExecutionProfileSnapshot{}, runtime.ErrInvariantViolation
	}
	revision = agentapp.NormalizeRevision(revision)

	snapshot, err := r.Configs.Get(ctx, key.TenantID, key.ConfigVersion)
	if err != nil {
		return profile.ExecutionProfileSnapshot{}, err
	}
	if snapshot.TenantID != key.TenantID {
		return profile.ExecutionProfileSnapshot{}, runtime.ErrTenantScope
	}
	if snapshot.State != config.StatePublished || snapshot.Payload.PolicyVersion != key.PolicyVersion {
		return profile.ExecutionProfileSnapshot{}, runtime.ErrVersionMismatch
	}
	if !authorizedChild && !configAllowsApp(snapshot.Payload, key.AgentAppID) {
		return profile.ExecutionProfileSnapshot{}, runtime.ErrVersionMismatch
	}
	configDigest, _, err := config.ContentDigest(snapshot.Payload)
	if err != nil || configDigest != snapshot.ContentDigest {
		return profile.ExecutionProfileSnapshot{}, runtime.ErrInvariantViolation
	}

	if revision.AgentKind == agentapp.AgentKindLLM {
		refs := append([]agentapp.VersionedRef{{ID: revision.ModelProfileID, Version: revision.ModelProfileVersion}}, revision.FallbackModelRefs...)
		for _, ref := range refs {
			modelProfile, err := r.Models.GetModel(ctx, key.TenantID, ref.ID, ref.Version)
			if err != nil {
				return profile.ExecutionProfileSnapshot{}, err
			}
			if modelProfile.TenantID != key.TenantID || modelProfile.ProfileID != ref.ID || modelProfile.Version != ref.Version {
				return profile.ExecutionProfileSnapshot{}, runtime.ErrTenantScope
			}
			if modelProfile.Status == "disabled" {
				return profile.ExecutionProfileSnapshot{}, runtime.ErrCapabilityUnsupported
			}
		}
	}

	return project(key, app, revision, snapshot.Payload), nil
}

func (r Resolver) verifyParent(ctx context.Context, parent profile.ExecutionProfileSnapshot, node agentapp.AgentNodeSpecV1) error {
	key := parent.Key
	if key.TenantID == "" || key.AgentAppID == "" || key.AgentAppRevision < 1 || key.ContentDigest == "" ||
		node.AgentRef.AgentAppID == "" || node.AgentRef.Revision < 1 || node.AgentRef.ContentDigest == "" ||
		node.AgentRef.AgentAppID == key.AgentAppID {
		return runtime.ErrVersionMismatch
	}
	stored, err := r.Agents.GetRevision(ctx, key.TenantID, key.AgentAppID, key.AgentAppRevision)
	if err != nil {
		return err
	}
	if stored.State != agentapp.RevisionPublished || stored.ContentDigest != key.ContentDigest {
		return runtime.ErrVersionMismatch
	}
	digest, err := stored.ComputeContentDigest()
	if err != nil || digest != stored.ContentDigest {
		return runtime.ErrInvariantViolation
	}
	for _, expected := range stored.AgentSpec.Nodes {
		if expected.Key == node.Key && expected.AgentRef == node.AgentRef {
			return nil
		}
	}
	return runtime.ErrTenantScope
}

func configAllowsApp(value config.ConfigV1, appID string) bool {
	if value.DefaultAgentAppID == appID {
		return true
	}
	for _, binding := range value.ChannelBindings {
		if binding.AgentAppID == appID {
			return true
		}
	}
	return false
}

func project(key profile.ExecutionProfileKey, app agentapp.AgentApp, revision agentapp.Revision, configuration config.ConfigV1) profile.ExecutionProfileSnapshot {
	tools := make([]profile.VersionedRef, len(revision.ToolRefs))
	for index, ref := range revision.ToolRefs {
		tools[index] = profile.VersionedRef{ID: ref.ID, Version: ref.Version, ContentDigest: ref.ContentDigest}
	}
	knowledge := make([]profile.VersionedRef, len(revision.KnowledgeRefs))
	for index, ref := range revision.KnowledgeRefs {
		knowledge[index] = profile.VersionedRef{ID: ref.ID, Version: ref.Version}
	}
	requirements := make(profile.CapabilitySet)
	bindings := make([]profile.BackendBinding, 0, len(configuration.BackendBindings))
	for _, binding := range configuration.BackendBindings {
		bindings = append(bindings, profile.BackendBinding{Domain: binding.Domain, BackendProfileID: binding.BackendProfileID,
			BackendVersion: binding.BackendVersion, Required: append([]string(nil), binding.Required...)})
		for _, required := range binding.Required {
			requirements[required] = true
		}
	}
	appVersion := key.AgentAppVersion
	if appVersion == 0 {
		appVersion = app.Version
	}
	return profile.ExecutionProfileSnapshot{
		Key: key, TenantVersion: key.TenantVersion, AgentAppVersion: appVersion,
		ContentDigest: revision.ContentDigest, AppName: key.TenantID + "/" + key.AgentAppID,
		AgentKind: revision.AgentKind, AgentSpec: revision.AgentSpec, Description: revision.Description,
		Instruction: revision.Instruction, GlobalInstruction: revision.GlobalInstruction,
		ModelProfileRef:   profile.VersionedRef{ID: revision.ModelProfileID, Version: revision.ModelProfileVersion},
		FallbackModelRefs: modelRefs(revision.FallbackModelRefs),
		ToolRefs:          tools, SkillRefs: append([]profile.SkillRef(nil), revision.SkillRefs...),
		PluginRefs: append([]profile.PluginRef(nil), revision.PluginRefs...), KnowledgeRefs: knowledge,
		GenerationConfig: cloneMap(revision.GenerationConfig), RuntimePolicy: cloneMap(revision.RuntimePolicy),
		ExecutionBudget: profile.ExecutionBudgetV1{MaxLLMCalls: revision.ExecutionBudget.MaxLLMCalls,
			MaxToolCalls: revision.ExecutionBudget.MaxToolCalls, MaxParallelTools: revision.ExecutionBudget.MaxParallelTools,
			ExecutionTimeoutSeconds: revision.ExecutionBudget.ExecutionTimeoutSeconds},
		BackendRequirements: requirements, BackendBindings: bindings,
	}
}

func modelRefs(input []agentapp.VersionedRef) []profile.VersionedRef {
	result := make([]profile.VersionedRef, len(input))
	for index, ref := range input {
		result[index] = profile.VersionedRef{ID: ref.ID, Version: ref.Version}
	}
	return result
}

func cloneMap(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	result := make(map[string]any, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

var _ profile.ExecutionProfileResolver = Resolver{}
var _ profile.ChildExecutionProfileResolver = Resolver{}
