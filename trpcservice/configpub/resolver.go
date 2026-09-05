package configpub

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// AssignmentResolver wraps the existing tenant resolver with the durable
// rollout assignment. The inner resolver stays authoritative for binding,
// tenant and agent identity; this wrapper only decides which immutable config
// revision the request uses. Failures fail closed: a configuration fact read
// error never silently degrades to another revision.
type AssignmentResolver struct {
	Inner tenant.TenantResolver
	Co    *Coordinator
}

// Resolve resolves the tenant context and then pins the assigned config
// version. Tenants without publication state keep the registry version.
func (r AssignmentResolver) Resolve(ctx context.Context, req tenant.ResolveRequest) (tenant.TenantContext, error) {
	if r.Co == nil || r.Inner == nil {
		return tenant.TenantContext{}, ErrInvalidArgument
	}
	tc, err := r.Inner.Resolve(ctx, req)
	if err != nil {
		return tenant.TenantContext{}, err
	}
	state, managed, err := r.Co.RolloutState(ctx, tc.TenantID)
	if err != nil {
		// Configuration fact source unavailable: reject instead of guessing.
		return tenant.TenantContext{}, err
	}
	if !managed {
		return tc, nil
	}
	bucket, err := AssignmentBucket(tc.TenantID)
	if err != nil {
		return tenant.TenantContext{}, err
	}
	version, ok := state.Assign(bucket)
	if !ok {
		return tenant.TenantContext{}, ErrInvalidRollout
	}
	revision, err := r.Co.Snapshot(ctx, tc.TenantID, version)
	if err != nil {
		return tenant.TenantContext{}, err
	}
	if revision.Document.Agent.AgentAppID != tc.AgentAppID {
		return tenant.TenantContext{}, ErrTenantMismatch
	}
	// The assigned revision owns the runtime backend policy as well as the
	// version. This keeps every downstream component on one immutable
	// configuration snapshot for the accepted request.
	tc.ConfigVersion = version
	tc.BackendPolicy = revision.Document.BackendPolicy
	if err := tc.Validate(); err != nil {
		return tenant.TenantContext{}, err
	}
	return tc, nil
}

// AgentRegistry is the content registry the snapshot resolver consults for
// non-revision agent content (name, prompt). Only these content fields are
// read; the release identity and version come from the immutable revision.
type AgentRegistry interface {
	Agent(ctx context.Context, tenantID, id string) (tenant.AgentApp, error)
}

// SnapshotAgentResolver is the worker-side agent resolver: it resolves the
// agent specification for the job's immutable config version and never
// re-reads the active configuration. A missing, unreadable or mismatched
// revision fails closed.
type SnapshotAgentResolver struct {
	Co            *Coordinator
	Registry      AgentRegistry
	ModelProvider string
}

// Resolve implements the worker AgentResolver seam.
func (r SnapshotAgentResolver) Resolve(ctx context.Context, tc tenant.TenantContext, ref queue.AgentRefDTO) (agent.AgentSpec, error) {
	if r.Co == nil {
		return agent.AgentSpec{}, ErrInvalidArgument
	}
	if tc.Validate() != nil {
		return agent.AgentSpec{}, ErrTenantMismatch
	}
	if ref.TenantID != tc.TenantID || ref.AgentAppID != tc.AgentAppID || ref.Version != tc.ConfigVersion {
		return agent.AgentSpec{}, ErrTenantMismatch
	}
	revision, err := r.Co.Snapshot(ctx, tc.TenantID, tc.ConfigVersion)
	if err != nil {
		return agent.AgentSpec{}, err
	}
	doc := revision.Document
	if doc.Agent.AgentAppID != tc.AgentAppID {
		// The revision must pin exactly the agent the request was accepted for.
		return agent.AgentSpec{}, ErrTenantMismatch
	}
	spec := agent.AgentSpec{
		TenantID:       tc.TenantID,
		AgentAppID:     doc.Agent.AgentAppID,
		Version:        revision.Version,
		ModelProvider:  r.ModelProvider,
		ModelConfigRef: doc.Agent.ModelConfigRef,
		ToolPolicyRef:  doc.Agent.ToolPolicyRef,
		GuardrailRef:   doc.Agent.GuardrailRef,
	}
	if r.Registry != nil {
		app, err := r.Registry.Agent(ctx, tc.TenantID, doc.Agent.AgentAppID)
		if err != nil {
			// Content registry unavailable: fail closed, never substitute.
			return agent.AgentSpec{}, classify("registry", err)
		}
		spec.Name = app.Name
		spec.SystemPrompt = app.SystemPrompt
		if spec.ModelConfigRef == "" {
			spec.ModelConfigRef = app.ModelConfigRef
		}
		if spec.ToolPolicyRef == "" {
			spec.ToolPolicyRef = app.ToolPolicyID
		}
		if spec.GuardrailRef == "" {
			spec.GuardrailRef = app.GuardrailRef
		}
	}
	return spec, nil
}
