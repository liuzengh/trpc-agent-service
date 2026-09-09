package runtimeplane

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"

	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/artifactplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/knowledgeplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	openaiembedder "trpc.group/trpc-go/trpc-agent-go/knowledge/embedder/openai"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
)

// Request is the immutable scope and capability set for one cached Worker.
// NeedKnowledge is explicit because the qdrant profile may also be present for
// ingestion while an Agent version does not have the knowledge_search tool.
type Request struct {
	Tenant        *tenant.Tenant
	AgentAppID    string
	NeedKnowledge bool
	NeedArtifact  bool
}

// Lease contains borrowed framework services and an idempotent release hook.
// Worker owns the lease for exactly the same lifetime as its Runner cache entry.
type Lease struct {
	Knowledge knowledge.Knowledge
	Artifact  artifact.Service
	Release   func() error
}

// Resolver is implemented by the Worker-only operator profile catalog. Tests
// may inject a deterministic resolver without opening external connections.
type Resolver interface {
	Acquire(context.Context, Request) (Lease, error)
}

type ProfileResolver struct {
	catalog            *Catalog
	db                 *sql.DB
	knowledgeDecorator KnowledgeDecorator
	artifactDecorator  ArtifactDecorator
}

type KnowledgeDecorator func(context.Context, string, string, string, vectorstore.VectorStore) (vectorstore.VectorStore, error)
type ArtifactDecorator func(context.Context, string, string, artifact.Service) (artifact.Service, error)
type ProfileResolverOption func(*ProfileResolver)

func WithMigrationDecorators(knowledgeDecorator KnowledgeDecorator, artifactDecorator ArtifactDecorator) ProfileResolverOption {
	return func(r *ProfileResolver) {
		r.knowledgeDecorator, r.artifactDecorator = knowledgeDecorator, artifactDecorator
	}
}

func NewProfileResolver(catalog *Catalog, db *sql.DB, options ...ProfileResolverOption) (*ProfileResolver, error) {
	if catalog == nil || catalog.validator == nil || db == nil {
		return nil, ErrDataPlaneUnavailable
	}
	resolver := &ProfileResolver{catalog: catalog, db: db}
	for _, option := range options {
		if option != nil {
			option(resolver)
		}
	}
	return resolver, nil
}

func (r *ProfileResolver) Acquire(ctx context.Context, request Request) (Lease, error) {
	if r == nil || r.catalog == nil || r.db == nil || request.Tenant == nil {
		return Lease{}, ErrDataPlaneUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return Lease{}, err
	}
	if err := tenant.ValidateTenantID(request.Tenant.ID); err != nil {
		return Lease{}, ErrDataPlaneUnavailable
	}
	if err := tenant.ValidateAgentAppName(request.AgentAppID); err != nil {
		return Lease{}, ErrDataPlaneUnavailable
	}
	if err := r.catalog.ValidateTenantStorage(request.Tenant.ID, request.Tenant.Storage); err != nil {
		return Lease{}, err
	}

	var (
		lease     Lease
		closers   []func() error
		closeMu   sync.Mutex
		closeOnce sync.Once
		closeErr  error
	)
	lease.Release = func() error {
		closeOnce.Do(func() {
			closeMu.Lock()
			defer closeMu.Unlock()
			for index := len(closers) - 1; index >= 0; index-- {
				closeErr = errors.Join(closeErr, closers[index]())
			}
			closers = nil
		})
		return closeErr
	}
	cleanup := func(cause error) (Lease, error) {
		return Lease{}, errors.Join(cause, lease.Release())
	}

	if request.NeedKnowledge {
		storage := request.Tenant.Storage
		if storage.KnowledgeBackend != "qdrant" || storage.KnowledgeProfile == "" {
			return cleanup(fmt.Errorf("%w: knowledge profile is required", ErrDataPlaneUnavailable))
		}
		profile, err := r.catalog.resolve(request.Tenant.ID, storage.KnowledgeProfile, "qdrant")
		if err != nil {
			return cleanup(err)
		}
		host, portText, err := net.SplitHostPort(profile.definition.Endpoint)
		if err != nil {
			return cleanup(ErrDataPlaneUnavailable)
		}
		port, err := strconv.Atoi(portText)
		if err != nil {
			return cleanup(ErrDataPlaneUnavailable)
		}
		store, err := knowledgeplane.NewSynchronousQdrantScopedStore(ctx, request.Tenant.ID, request.AgentAppID, knowledgeplane.QdrantConfig{
			Host: host, Port: port, APIKey: profile.apiKey, TLS: profile.definition.TLS,
			AllowInsecure: profile.definition.AllowInsecure, Collection: profile.definition.Collection,
			Dimension: profile.definition.Dimension,
		})
		if err != nil {
			return cleanup(fmt.Errorf("%w: initialize knowledge profile", ErrDataPlaneUnavailable))
		}
		closers = append(closers, store.Close)
		var vectorStore vectorstore.VectorStore = store
		if r.knowledgeDecorator != nil {
			vectorStore, err = r.knowledgeDecorator(ctx, request.Tenant.ID, request.AgentAppID, storage.KnowledgeProfile, store)
			if err != nil {
				return cleanup(err)
			}
			if vectorStore == nil {
				return cleanup(ErrDataPlaneUnavailable)
			}
			closers = append(closers, vectorStore.Close)
		}
		embedder := openaiembedder.New(
			openaiembedder.WithModel(profile.definition.EmbeddingModel),
			openaiembedder.WithDimensions(profile.definition.Dimension),
			openaiembedder.WithAPIKey(profile.embeddingAPIKey),
			openaiembedder.WithBaseURL(profile.definition.EmbeddingEndpoint),
		)
		lease.Knowledge = knowledge.New(
			knowledge.WithVectorStore(vectorStore), knowledge.WithEmbedder(embedder),
		)
	}

	if request.NeedArtifact {
		storage := request.Tenant.Storage
		if storage.ArtifactBackend != "s3" || storage.ArtifactProfile == "" {
			return cleanup(fmt.Errorf("%w: artifact profile is required", ErrDataPlaneUnavailable))
		}
		profile, err := r.catalog.resolve(request.Tenant.ID, storage.ArtifactProfile, "s3")
		if err != nil {
			return cleanup(err)
		}
		objects, err := artifactplane.NewMinIOStore(ctx, artifactplane.MinIOConfig{
			Endpoint: profile.definition.Endpoint, AccessKey: profile.accessKey, SecretKey: profile.secretKey,
			SessionToken: profile.sessionToken, Bucket: profile.definition.Bucket, Region: profile.definition.Region,
			Secure: profile.definition.TLS, AllowInsecure: profile.definition.AllowInsecure,
			CreateBucket: profile.definition.CreateBucket, MaxBytes: profile.definition.MaxBytes,
		})
		if err != nil {
			return cleanup(fmt.Errorf("%w: initialize artifact profile", ErrDataPlaneUnavailable))
		}
		service, err := artifactplane.NewService(request.Tenant.ID, r.db, objects, profile.definition.MaxBytes)
		if err != nil {
			return cleanup(fmt.Errorf("%w: initialize artifact service", ErrDataPlaneUnavailable))
		}
		lease.Artifact = service
		if r.artifactDecorator != nil {
			lease.Artifact, err = r.artifactDecorator(ctx, request.Tenant.ID, storage.ArtifactProfile, service)
			if err != nil {
				return cleanup(err)
			}
			if lease.Artifact == nil {
				return cleanup(ErrDataPlaneUnavailable)
			}
			if closer, ok := lease.Artifact.(interface{ Close() error }); ok {
				closers = append(closers, closer.Close)
			}
		}
	}
	return lease, nil
}

var _ Resolver = (*ProfileResolver)(nil)
