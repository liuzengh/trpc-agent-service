//go:build integration && e2e

package qdrant_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	platformconfig "github.com/liuzengh/trpc-agent-service/trpcservice/config"
	platformknowledge "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
	knowledgeqdrant "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge/qdrant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	frameworkqdrant "trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/qdrant"
	qdrantstorage "trpc.group/trpc-go/trpc-agent-go/storage/qdrant"
)

type migrationConfigResolver struct {
	configs map[string]tenant.AppConfig
}

var _ platformconfig.Resolver = (*migrationConfigResolver)(nil)

func (r *migrationConfigResolver) ResolveAppConfig(_ context.Context, tenantID, appID, version string) (tenant.AppConfig, error) {
	config, ok := r.configs[version]
	if !ok || config.TenantID != tenantID || config.AppID != appID {
		return tenant.AppConfig{}, fmt.Errorf("config %s was not found", version)
	}
	return config.Clone(), nil
}

type emptySecretProvider struct{}

var _ platformsecret.SecretProvider = emptySecretProvider{}

func (emptySecretProvider) ResolveSecret(context.Context, tenant.Scope, tenant.SecretRef) (string, error) {
	return "", nil
}

type fixedQdrantEndpointResolver struct {
	host string
	port int
}

var _ knowledgeqdrant.EndpointResolver = fixedQdrantEndpointResolver{}

func (r fixedQdrantEndpointResolver) ResolveQdrantEndpoint(context.Context, string) (knowledgeqdrant.Endpoint, error) {
	return knowledgeqdrant.Endpoint{Host: r.host, Port: r.port}, nil
}

func TestKnowledgeMigrationCopiesQdrantPointEndToEnd(t *testing.T) {
	host := os.Getenv(qdrantTestHostEnv)
	if host == "" {
		t.Fatalf("%s is required", qdrantTestHostEnv)
	}
	port := 6334
	if value := os.Getenv(qdrantTestPortEnv); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			t.Fatalf("parse qdrant port: %v", err)
		}
		port = parsed
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	id := strconv.FormatInt(time.Now().UnixNano(), 10)
	scope := tenant.Scope{TenantID: "migration-" + id, AppID: "knowledge"}
	sourceProfile := "migration-source-" + id
	targetProfile := "migration-target-" + id
	generation := "generation-" + id
	sourceCollection := "knowledge-" + sourceProfile + "-" + generation
	targetCollection := "knowledge-" + targetProfile + "-" + generation

	createStore := func(collection string) {
		store, err := frameworkqdrant.New(ctx,
			frameworkqdrant.WithHost(host),
			frameworkqdrant.WithPort(port),
			frameworkqdrant.WithCollectionName(collection),
			frameworkqdrant.WithDimension(3),
		)
		if err != nil {
			t.Fatalf("create qdrant collection %s: %v", collection, err)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("close setup qdrant store %s: %v", collection, err)
		}
	}
	createStore(sourceCollection)
	cleanupClient, err := qdrantstorage.NewClient(ctx,
		qdrantstorage.WithHost(host),
		qdrantstorage.WithPort(port),
	)
	if err != nil {
		t.Fatalf("create qdrant cleanup client: %v", err)
	}
	t.Cleanup(func() {
		_ = cleanupClient.DeleteCollection(context.Background(), sourceCollection)
		_ = cleanupClient.DeleteCollection(context.Background(), targetCollection)
		_ = cleanupClient.Close()
	})

	setupStore, err := frameworkqdrant.New(ctx,
		frameworkqdrant.WithHost(host),
		frameworkqdrant.WithPort(port),
		frameworkqdrant.WithCollectionName(sourceCollection),
		frameworkqdrant.WithDimension(3),
	)
	if err != nil {
		t.Fatalf("open source qdrant collection: %v", err)
	}
	document := knowledgeVectorDocument(scope, "kb-migration", "doc-migration", "1", "chunk-migration", generation, "point-migration")
	if err := setupStore.Add(ctx, document, []float64{0.2, 0.3, 0.4}); err != nil {
		t.Fatalf("insert source qdrant point: %v", err)
	}
	if err := setupStore.Close(); err != nil {
		t.Fatalf("close source qdrant store: %v", err)
	}

	backend := func(profile, name string) tenant.BackendRef {
		return tenant.BackendRef{
			Kind:     tenant.BackendVector,
			Provider: "qdrant",
			Name:     name,
			Options: map[string]string{
				"embedding_model":      "migration-embedding",
				"embedding_dimensions": "3",
				"embedding_profile":    profile,
				"index_generation":     generation,
			},
		}
	}
	configs := &migrationConfigResolver{configs: map[string]tenant.AppConfig{
		"v1": {TenantID: scope.TenantID, AppID: scope.AppID, Version: "v1", BackendConfig: tenant.BackendConfig{Knowledge: backend(sourceProfile, "source")}},
		"v2": {TenantID: scope.TenantID, AppID: scope.AppID, Version: "v2", BackendConfig: tenant.BackendConfig{Knowledge: backend(targetProfile, "target")}},
	}}
	record := migration.Record{
		ID:                  "migration-" + id,
		TenantID:            scope.TenantID,
		AppID:               scope.AppID,
		Domain:              migration.DomainKnowledge,
		SourceConfigVersion: "v1",
		TargetConfigVersion: "v2",
		Status:              migration.StatusPending,
	}
	copier, err := knowledgeqdrant.NewMigrationCopier(ctx, configs, emptySecretProvider{}, fixedQdrantEndpointResolver{host: host, port: port}, record)
	if err != nil {
		t.Fatalf("create knowledge migration copier: %v", err)
	}
	defer copier.Close()
	ref := platformknowledge.ChunkRef{
		Scope:           scope,
		ConfigVersion:   "v1",
		KnowledgeBaseID: "kb-migration",
		DocumentID:      "doc-migration",
		DocumentVersion: "1",
		ChunkID:         "chunk-migration",
		IndexGeneration: generation,
	}
	if err := copier.CopyKnowledgeChunk(ctx, ref); err != nil {
		t.Fatalf("copy qdrant point: %v", err)
	}
	if err := copier.VerifyKnowledgeChunk(ctx, ref); err != nil {
		t.Fatalf("verify qdrant point: %v", err)
	}
}
