package knowledge

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	serviceqdrant "github.com/liuzengh/trpc-agent-service/trpcservice/storage/knowledge/qdrant"
)

type ConfigSnapshotReader interface {
	Get(context.Context, string, int64) (config.Snapshot, error)
}

type BackendProfileReader interface {
	GetBackend(context.Context, string, string, int64) (provider.BackendProfileSnapshot, error)
}

// KnowledgeProfileReader is the shared immutable profile authority required by
// the runtime: backend routing and embedding selection must be read from the
// same tenant-scoped catalog view.
type KnowledgeProfileReader interface {
	ModelProfileReader
	BackendProfileReader
}

// KnowledgeBackendResolver resolves the exact Qdrant binding carried by an
// immutable tenant Config Snapshot. It never accepts a process-global backend
// endpoint, so two tenants may safely use separate reviewed backend profiles.
type KnowledgeBackendResolver interface {
	ResolveKnowledgeBackend(context.Context, string, int64) (*serviceqdrant.Adapter, error)
}

type BackendAdapterResolver struct {
	Configs  ConfigSnapshotReader
	Backends BackendProfileReader
	Secrets  secrets.Provider
	Subject  string
}

func (r BackendAdapterResolver) ResolveKnowledgeBackend(ctx context.Context, tenantID string, configVersion int64) (*serviceqdrant.Adapter, error) {
	if r.Configs == nil || r.Backends == nil || r.Secrets == nil || tenantID == "" || configVersion < 1 ||
		strings.TrimSpace(r.Subject) != r.Subject || r.Subject == "" {
		return nil, runtime.ErrCapabilityUnsupported
	}
	snapshot, err := r.Configs.Get(ctx, tenantID, configVersion)
	if err != nil {
		return nil, err
	}
	if snapshot.TenantID != tenantID || snapshot.ConfigVersion != configVersion || snapshot.State != config.StatePublished {
		return nil, runtime.ErrVersionMismatch
	}
	binding, ok := knowledgeBinding(snapshot.Payload.BackendBindings)
	if !ok {
		return nil, runtime.ErrCapabilityUnsupported
	}
	backend, err := r.Backends.GetBackend(ctx, tenantID, binding.BackendProfileID, binding.BackendVersion)
	if err != nil {
		return nil, err
	}
	if backend.TenantID != tenantID || backend.ProfileID != binding.BackendProfileID || backend.Version != binding.BackendVersion {
		return nil, runtime.ErrTenantScope
	}
	if backend.Status != "active" || (backend.Provider != "qdrant" && backend.Provider != "qdrant-local") || backend.SchemaVersion != 1 || !backend.Capabilities["tenant_filter"] ||
		backend.CredentialRef.Ref == "" || backend.CredentialRef.Version < 1 {
		return nil, runtime.ErrCapabilityUnsupported
	}
	vectorSize, err := strconv.Atoi(backend.Configuration["vector_size"])
	if err != nil || vectorSize < 1 || vectorSize > 65536 {
		return nil, runtime.ErrInvariantViolation
	}
	timeoutMS, err := strconv.Atoi(backend.Configuration["timeout_ms"])
	if err != nil || timeoutMS < 100 || timeoutMS > 600000 {
		return nil, runtime.ErrInvariantViolation
	}
	grpcPort, err := strconv.Atoi(backend.Configuration["grpc_port"])
	if err != nil || grpcPort < 1 || grpcPort > 65535 {
		return nil, runtime.ErrInvariantViolation
	}
	runtimeEngine := strings.TrimSpace(backend.Configuration["runtime_engine"])
	if runtimeEngine != "native" && runtimeEngine != "sdk" {
		return nil, runtime.ErrInvariantViolation
	}
	endpoint := strings.TrimSpace(backend.Configuration["endpoint"])
	collection := strings.TrimSpace(backend.Configuration["collection"])
	watermark := strings.TrimSpace(backend.Configuration["snapshot_watermark"])
	generation := strings.TrimSpace(backend.Configuration["vector_generation"])
	if endpoint == "" || collection == "" || watermark == "" || generation == "" {
		return nil, runtime.ErrInvariantViolation
	}
	tokens := backendToken{provider: r.Secrets, scope: secrets.Scope{TenantID: tenantID, Subject: r.Subject,
		Purpose: secrets.PurposeBackendConnect, ResourceID: backend.ProfileID, ResourceVersion: backend.Version}, ref: backend.CredentialRef}
	return serviceqdrant.New(serviceqdrant.Config{Endpoint: endpoint, Collection: collection, VectorSize: vectorSize,
		SnapshotWatermark: watermark, VectorGeneration: generation,
		RuntimeEngine: runtimeEngine, GRPCPort: grpcPort,
		AllowInsecureHTTP: backend.Provider == "qdrant-local",
		HTTPClient:        &http.Client{Timeout: time.Duration(timeoutMS) * time.Millisecond}, TokenSource: tokens}, nil)
}

func knowledgeBinding(bindings []config.BackendBinding) (config.BackendBinding, bool) {
	for _, binding := range bindings {
		if binding.Domain != knowledgedriver.Domain {
			continue
		}
		for _, capability := range binding.Required {
			if capability == "tenant_filter" {
				return binding, true
			}
		}
	}
	return config.BackendBinding{}, false
}

type backendToken struct {
	provider secrets.Provider
	scope    secrets.Scope
	ref      secrets.SecretRef
}

func (s backendToken) Token(ctx context.Context) (string, error) {
	if s.provider == nil {
		return "", runtime.ErrBackendUnavailable
	}
	value, err := s.provider.Resolve(ctx, s.scope, s.ref)
	if err != nil {
		return "", err
	}
	defer clearSecret(value.Bytes)
	if value.Version != s.ref.Version || len(value.Bytes) == 0 {
		return "", runtime.ErrVersionMismatch
	}
	token := strings.TrimSpace(string(value.Bytes))
	if token == "" || strings.ContainsAny(token, "\r\n\x00") {
		return "", runtime.ErrVersionMismatch
	}
	return token, nil
}

var _ KnowledgeBackendResolver = BackendAdapterResolver{}
