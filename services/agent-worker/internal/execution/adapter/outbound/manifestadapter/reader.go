package manifestadapter

import (
	"context"
	"errors"

	protocol "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	projection "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/application"
	manifest "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/domain"
	"go.opentelemetry.io/otel/trace"
)

type Reader struct {
	Tracer         trace.Tracer
	Projection     projection.Projection
	ContractDigest string
}

func (r Reader) Resolve(ctx context.Context, route domain.Route) (resolved domain.Plan, resultErr error) {
	ctx, span := telemetrytrace.Start(r.Tracer, ctx, "worker.manifest.resolve")
	defer func() { telemetrytrace.End(span, resultErr) }()
	if r.Projection == nil || !domain.DigestValid(r.ContractDigest) {
		return domain.Plan{}, application.ErrManifestInvalid
	}
	m, err := r.Projection.Read(ctx, route.TenantID, route.ManifestRef)
	if errors.Is(err, manifest.ErrMissing) {
		return domain.Plan{}, application.ErrManifestMissing
	}
	if errors.Is(err, manifest.ErrConflict) {
		return domain.Plan{}, application.ErrManifestInvalid
	}
	if err != nil {
		return domain.Plan{}, application.ErrDependency
	}
	if m.ContentDigest != route.ManifestDigest || m.DeploymentRevisionID != route.DeploymentRevisionID {
		return domain.Plan{}, application.ErrManifestInvalid
	}
	envelope, err := protocol.DecodeRuntimeManifest(m.Envelope)
	if err != nil {
		return domain.Plan{}, application.ErrManifestInvalid
	}
	if envelope.ID != route.ManifestRef || envelope.TenantID != route.TenantID || envelope.DeploymentRevisionID != route.DeploymentRevisionID || envelope.ContentDigest != route.ManifestDigest {
		return domain.Plan{}, application.ErrManifestInvalid
	}
	content, err := protocol.VerifyRuntimeManifest(envelope)
	if err != nil {
		return domain.Plan{}, application.ErrManifestInvalid
	}
	if err = protocol.ValidateWorkerV1(content, r.ContractDigest); err != nil {
		if errors.Is(err, protocol.ErrWorkerV1ContractMismatch) {
			return domain.Plan{}, application.ErrManifestContractMismatch
		}
		return domain.Plan{}, application.ErrManifestUnsupported
	}
	if kind := content.AgentPlan.Nodes[content.AgentPlan.Root].Kind; kind == "sequence" || kind == "loop" || content.AgentPlan.Nodes[content.AgentPlan.Root].Workspace != nil {
		return projectSequence(content, m), nil
	}
	return projectLLM(content, content.AgentPlan.Root, m), nil
}

func projectLLM(content protocol.ManifestContent, nodeID string, m manifest.Publication) domain.Plan {
	node := content.AgentPlan.Nodes[nodeID]
	model := content.Resources.Models[node.ModelResource]
	storage := content.Resources.Storage[content.StorageRoles["session"]]
	use := func(u protocol.CredentialUse) domain.CredentialUse {
		return domain.CredentialUse{CredentialID: u.CredentialID, Purpose: u.Purpose, AudienceDigest: u.AudienceDigest}
	}
	p := domain.Plan{TenantID: m.TenantID, ManifestID: m.ManifestID, ManifestDigest: m.ContentDigest, DeploymentRevisionID: m.DeploymentRevisionID, ProfileID: content.Sources.Profile.ProfileID, ProfileRevision: content.Sources.Profile.RevisionNumber, MaxToolCalls: content.Execution.MaxToolCalls, NodeID: nodeID, Instruction: node.Instruction, ModelEndpoint: model.BaseURL, ModelName: model.Model, MaxRunSeconds: content.Execution.MaxRunSeconds, MaxOutputTokens: content.Execution.MaxOutputTokens, ModelCredential: use(model.Credential), SessionCredential: use(storage.Credential), SessionTarget: domain.StorageTarget{Host: storage.Destination.Host, Port: uint16(storage.Destination.Port), Database: storage.Destination.Database, Username: storage.Destination.Username, SSLMode: storage.Destination.SSLMode}}
	for _, name := range node.ToolResources {
		r := content.Resources.Tools[name]
		t := domain.ToolPlan{Resource: name, ServerURL: r.ServerURL, ToolsetName: r.ToolsetName, ToolName: r.ToolName, AuthKind: r.Auth.Kind, Capability: r.Capability}
		if r.Auth.Credential != nil {
			t.Credential = use(*r.Auth.Credential)
		}
		p.Tools = append(p.Tools, t)
	}
	if storage.Kind == "managed_session" {
		backend := storage.Backend.Clone()
		p.SessionBackend = &backend
	}
	if len(node.KnowledgeResources) == 1 {
		name := node.KnowledgeResources[0]
		r := content.Resources.Knowledge[name]
		p.Knowledge = &domain.KnowledgePlan{Resource: name, Backend: r.Backend.Clone(), Credential: use(*r.Credential), EmbeddingCredential: use(r.Embedding.Credential), EmbeddingModel: r.Embedding.Model, EmbeddingEndpoint: r.Embedding.BaseURL, Dimensions: r.Embedding.Dimensions}
	}
	if node.Artifact != nil {
		r := content.Resources.Storage[node.Artifact.Resource]
		p.Artifact = &domain.ArtifactPlan{Backend: r.Backend.Clone(), AccessKeyID: use(r.Credentials.AccessKeyID), SecretAccessKey: use(r.Credentials.SecretAccessKey)}
	}
	if node.Memory != nil {
		resource := content.Resources.Storage[node.Memory.Resource]
		limit := 0
		if node.Memory.PreloadLimit != nil {
			limit = int(*node.Memory.PreloadLimit)
		}
		p.Memory = &domain.MemoryPlan{AgentID: content.Sources.Agent.AgentID, Backend: resource.Backend.Clone(), Credential: use(resource.Credential), Tools: append([]string(nil), node.Memory.Tools...), PreloadLimit: limit}
	}
	if node.Generation != nil {
		p.Temperature = node.Generation.Temperature
		p.NodeMaxOutputTokens = node.Generation.MaxOutputTokens
	}
	if content.Runtime != nil {
		spec := content.Runtime.Summary
		selected := content.Resources.Models[spec.ModelResource]
		p.Summary = &domain.SummaryPlan{ModelEndpoint: selected.BaseURL, ModelName: selected.Model, ModelCredential: use(selected.Credential), EventThreshold: spec.EventThreshold, AddSessionSummary: node.AddSessionSummary != nil && *node.AddSessionSummary}
	}
	return p
}
