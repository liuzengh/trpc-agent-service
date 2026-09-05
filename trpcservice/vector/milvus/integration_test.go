//go:build integration

package milvus

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/milvus-io/milvus/client/v2/entity"
	sdk "github.com/milvus-io/milvus/client/v2/milvusclient"

	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
)

func integrationConfig() vector.BackendConfig {
	runID := os.Getenv("P106B_MILVUS_RUN_ID")
	if runID == "" {
		runID = strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return vector.BackendConfig{Mode: vector.BackendMilvus, EndpointRef: "local-milvus-endpoint", Collection: "p106b_milvus_" + runID, CredentialRef: "local-milvus-credential", Model: "synthetic-model", ModelVersion: "v1", Dimension: 4, SchemaVersion: "schema-1", Metric: "cosine", MaxInputBytes: vector.MaxContentBytes, MaxMetadataBytes: vector.MaxMetadataBytes, MaxTopK: vector.MaxTopK, OperationTimeout: 8 * time.Second, ReadinessTimeout: 12 * time.Second}
}

type integrationEndpoint string

func (e integrationEndpoint) Resolve(context.Context, string) (string, error) { return string(e), nil }

type integrationCredentials Credentials

func (c integrationCredentials) Resolve(context.Context, string) (Credentials, error) {
	return Credentials(c), nil
}

func integrationEndpointAndCredentials(t *testing.T) (string, Credentials) {
	t.Helper()
	address := os.Getenv("P106B_MILVUS_ADDRESS")
	if address == "" {
		t.Skip("P106B_MILVUS_ADDRESS is not set; local Milvus integration not run")
	}
	credentials := Credentials{Username: os.Getenv("P106B_MILVUS_USERNAME"), Password: os.Getenv("P106B_MILVUS_PASSWORD"), APIKey: os.Getenv("P106B_MILVUS_API_KEY")}
	if !validCredentials(credentials) {
		t.Skip("synthetic local Milvus credentials are not configured")
	}
	return address, credentials
}

func provisionIntegrationCollection(ctx context.Context, client *sdk.Client, cfg vector.BackendConfig) error {
	// Strong consistency is a fixture-only setting so immediate
	// search-after-write is deterministic without fixed sleeps.
	createOption := sdk.NewCreateCollectionOption(cfg.Collection, expectedSchema(cfg)).WithConsistencyLevel(entity.ClStrong)
	// Ready() validates collection properties, so the owned fixture must
	// provision the same server-owned model/schema/version markers.
	for key, value := range expectedProperties(cfg) {
		createOption = createOption.WithProperty(key, value)
	}
	if err := client.CreateCollection(ctx, createOption); err != nil {
		return err
	}
	task, err := client.CreateIndex(ctx, sdk.NewCreateIndexOption(cfg.Collection, fieldVector, expectedIndex(cfg)).WithIndexName(indexName))
	if err != nil {
		return err
	}
	if err := task.Await(ctx); err != nil {
		return err
	}
	loadTask, err := client.LoadCollection(ctx, sdk.NewLoadCollectionOption(cfg.Collection))
	if err != nil {
		return err
	}
	return loadTask.Await(ctx)
}

func integrationClient(t *testing.T, ctx context.Context, address string, credentials Credentials) *sdk.Client {
	t.Helper()
	client, err := sdk.New(ctx, &sdk.ClientConfig{Address: address, Username: credentials.Username, Password: credentials.Password, APIKey: credentials.APIKey})
	if err != nil {
		t.Fatalf("authenticated local Milvus construction failed: category=unavailable")
	}
	return client
}

func integrationStore(t *testing.T, cfg vector.BackendConfig, address string, credentials Credentials) (*sdk.Client, vector.VectorStore) {
	t.Helper()
	// Bounded construction polling tolerates a slow Milvus proxy without
	// fixed-sleep coordination.
	var fixtureClient *sdk.Client
	for attempt := 0; attempt < 30; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		client, err := sdk.New(ctx, &sdk.ClientConfig{Address: address, Username: credentials.Username, Password: credentials.Password, APIKey: credentials.APIKey})
		cancel()
		if err == nil {
			fixtureClient = client
			break
		}
		time.Sleep(2 * time.Second)
	}
	if fixtureClient == nil {
		t.Fatalf("authenticated local Milvus construction failed: category=unavailable")
	}
	provisionCtx, provisionCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer provisionCancel()
	if err := provisionIntegrationCollection(provisionCtx, fixtureClient, cfg); err != nil {
		_ = fixtureClient.Close(context.Background())
		t.Fatalf("owned collection provision failed: category=unavailable")
	}
	if err := fixtureClient.Close(provisionCtx); err != nil {
		t.Fatalf("fixture client close failed: category=unknown")
	}
	var store vector.VectorStore
	for attempt := 0; attempt < 30; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		candidate, err := New(ctx, cfg, integrationEndpoint(address), integrationCredentials(credentials))
		cancel()
		if err == nil {
			store = candidate
			break
		}
		time.Sleep(2 * time.Second)
	}
	if store == nil {
		t.Fatalf("adapter construction failed: category=unavailable")
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		_ = store.Close(cleanupCtx)
		// The owned collection must be dropped even when earlier steps
		// failed mid-restart; retry the drop with bounded attempts.
		for attempt := 0; attempt < 5; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			client, err := sdk.New(ctx, &sdk.ClientConfig{Address: address, Username: credentials.Username, Password: credentials.Password, APIKey: credentials.APIKey})
			if err == nil {
				err = client.DropCollection(ctx, sdk.NewDropCollectionOption(cfg.Collection))
				_ = client.Close(ctx)
			}
			cancel()
			if err == nil {
				return
			}
			time.Sleep(2 * time.Second)
		}
	})
	return nil, store
}

func integrationRequest(t *testing.T, ctx context.Context, sourceID string, tenantID string, dimension int, model string, schema string) vector.UpsertRequest {
	t.Helper()
	source := vector.SourceDocument{SourceType: vector.SourceTypeMemory, SourceID: sourceID, ProjectionScope: "default", SourceVersion: 1, SourceSequence: 1, Content: "synthetic-content", Model: model, ModelVersion: "v1", Dimension: dimension, SchemaVersion: schema}
	values := make([]float64, dimension)
	values[0] = 1
	request, err := vector.BuildUpsertRequest(ctx, source, vector.Embedding{Values: values, Model: model, ModelVersion: "v1", Dimension: dimension})
	if err != nil {
		t.Fatal(err)
	}
	if request.Ref.TenantID != tenantID {
		t.Fatal("integration request tenant context mismatch")
	}
	return request
}

func deleteRequest(t *testing.T, ctx context.Context, source vector.SourceDocument) vector.DeleteRequest {
	t.Helper()
	source.Deleted = true
	source.Content = ""
	request, err := vector.BuildDeleteRequest(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func TestLocalMilvusCRUDSearchIsolationAndValidation(t *testing.T) {
	address, credentials := integrationEndpointAndCredentials(t)
	cfg := integrationConfig()
	cfg.Collection += "_crud"
	_, store := integrationStore(t, cfg, address, credentials)
	readyCtx, readyCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer readyCancel()
	if err := store.Ready(readyCtx); err != nil {
		t.Fatalf("authenticated local Milvus readiness failed: %v", err)
	}
	ctxA := testContext("tenant-a")
	ctxB := testContext("tenant-b")
	requestA := integrationRequest(t, ctxA, "memory-a", "tenant-a", cfg.Dimension, cfg.Model, cfg.SchemaVersion)
	requestB := integrationRequest(t, ctxB, "memory-b", "tenant-b", cfg.Dimension, cfg.Model, cfg.SchemaVersion)
	if err := store.Upsert(ctxA, requestA); err != nil {
		t.Fatal(err)
	}
	if err := store.Upsert(ctxA, requestA); err != nil {
		t.Fatal(err)
	}
	if err := store.Upsert(ctxB, requestB); err != nil {
		t.Fatal(err)
	}
	results, err := store.Search(ctxA, vector.SearchRequest{Query: []float64{1, 0, 0, 0}, TopK: 5, MinScore: 0})
	if err != nil || len(results) != 1 || results[0].Ref.DocumentID != requestA.Ref.DocumentID {
		t.Fatalf("tenant A search failed: count=%d err=%v", len(results), err)
	}
	results, err = store.Search(ctxB, vector.SearchRequest{Query: []float64{1, 0, 0, 0}, TopK: 5, MinScore: 0})
	if err != nil || len(results) != 1 || results[0].Ref.DocumentID != requestB.Ref.DocumentID {
		t.Fatalf("tenant B search failed: count=%d err=%v", len(results), err)
	}
	if err := store.Upsert(ctxA, requestB); !errors.Is(err, vector.ErrInvalidTenant) {
		t.Fatalf("cross-tenant upsert error = %v", err)
	}
	deleteB := deleteRequest(t, ctxB, vector.SourceDocument{SourceType: vector.SourceTypeMemory, SourceID: "memory-b", ProjectionScope: "default", SourceVersion: 1, SourceSequence: 1, Model: cfg.Model, ModelVersion: "v1", Dimension: cfg.Dimension, SchemaVersion: cfg.SchemaVersion})
	if err := store.Delete(ctxA, deleteB); !errors.Is(err, vector.ErrInvalidTenant) {
		t.Fatalf("cross-tenant delete error = %v", err)
	}
	deleteA := deleteRequest(t, ctxA, vector.SourceDocument{SourceType: vector.SourceTypeMemory, SourceID: "memory-a", ProjectionScope: "default", SourceVersion: 1, SourceSequence: 1, Model: cfg.Model, ModelVersion: "v1", Dimension: cfg.Dimension, SchemaVersion: cfg.SchemaVersion})
	if err := store.Delete(ctxA, deleteA); err != nil {
		t.Fatal(err)
	}
	// A repeated delete reports unknown by contract: the backend no longer
	// matches exactly one row, so success cannot be claimed.
	if err := store.Delete(ctxA, deleteA); !errors.Is(err, vector.ErrUnknown) {
		t.Fatalf("repeated delete error = %v; want unknown", err)
	}
	results, err = store.Search(ctxA, vector.SearchRequest{Query: []float64{1, 0, 0, 0}, TopK: 5, MinScore: 0})
	if err != nil || len(results) != 0 {
		t.Fatalf("deleted document remained searchable: count=%d err=%v", len(results), err)
	}
	wrongDimension := integrationRequest(t, ctxA, "memory-dimension", "tenant-a", cfg.Dimension+1, cfg.Model, cfg.SchemaVersion)
	if err := store.Upsert(ctxA, wrongDimension); !errors.Is(err, vector.ErrInvalidDimension) {
		t.Fatalf("wrong dimension error = %v", err)
	}
	wrongModel := integrationRequest(t, ctxA, "memory-model", "tenant-a", cfg.Dimension, "other-model", cfg.SchemaVersion)
	if err := store.Upsert(ctxA, wrongModel); !errors.Is(err, vector.ErrInvalidModel) {
		t.Fatalf("wrong model error = %v", err)
	}
	wrongSchema := integrationRequest(t, ctxA, "memory-schema", "tenant-a", cfg.Dimension, cfg.Model, "schema-other")
	if err := store.Upsert(ctxA, wrongSchema); !errors.Is(err, vector.ErrInvalidSchema) {
		t.Fatalf("wrong schema error = %v", err)
	}
	missingCfg := cfg
	missingCfg.Collection += "_missing"
	missingStore, err := New(context.Background(), missingCfg, integrationEndpoint(address), integrationCredentials(credentials))
	if err != nil {
		t.Fatal(err)
	}
	if err := missingStore.Ready(context.Background()); err == nil {
		t.Fatal("missing collection was reported ready")
	}
	_ = missingStore.Close(context.Background())
	cancelled, cancel := context.WithCancel(ctxA)
	cancel()
	if _, err := store.Search(cancelled, vector.SearchRequest{Query: []float64{1, 0, 0, 0}, TopK: 1}); !errors.Is(err, vector.ErrCancelled) {
		t.Fatalf("cancelled search error = %v", err)

	}
}
func ownedMilvusContainer(t *testing.T) string {
	t.Helper()
	id := os.Getenv("P106B_MILVUS_CONTAINER_ID")
	owner := os.Getenv("P106B_MILVUS_OWNER")
	if id == "" || owner == "" {
		t.Skip("owned Milvus container identity is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", "inspect", "--format", `{{ index .Config.Labels "p106b.milvus.owner" }}`, id).Output()
	if err != nil || strings.TrimSpace(string(output)) != owner {
		t.Fatal("refusing to operate on a non-owned Milvus container")
	}
	return id
}

func dockerAction(t *testing.T, ctx context.Context, args ...string) {
	t.Helper()
	if err := exec.CommandContext(ctx, "docker", args...).Run(); err != nil {
		t.Fatalf("owned Docker operation failed: category=unavailable")
	}
}

func boundedReady(t *testing.T, store vector.VectorStore, timeout time.Duration) error {
	t.Helper()
	// Deadline-bound polling with bounded attempts and a bounded pause;
	// Milvus standalone needs tens of seconds after a container restart.
	var last error
	for attempt := 0; attempt < 60; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		last = store.Ready(ctx)
		cancel()
		if last == nil {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return last
}

func TestLocalMilvusBoundedRestartAndFactoryReconnect(t *testing.T) {
	address, credentials := integrationEndpointAndCredentials(t)
	cfg := integrationConfig()
	cfg.Collection += "_restart"
	_, store := integrationStore(t, cfg, address, credentials)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := store.Ready(ctx); err != nil {
		t.Fatalf("pre-restart readiness failed: %v", err)
	}
	tenantCtx := testContext("tenant-a")
	request := integrationRequest(t, tenantCtx, "memory-restart", "tenant-a", cfg.Dimension, cfg.Model, cfg.SchemaVersion)
	if err := store.Upsert(tenantCtx, request); err != nil {
		t.Fatal(err)
	}
	if results, err := store.Search(tenantCtx, vector.SearchRequest{Query: []float64{1, 0, 0, 0}, TopK: 1, MinScore: 0}); err != nil || len(results) != 1 {
		t.Fatalf("pre-restart search failed: count=%d err=%v", len(results), err)
	}
	containerID := ownedMilvusContainer(t)
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 15*time.Second)
	dockerAction(t, stopCtx, "stop", "--time", "5", containerID)
	stopCancel()
	downCtx, downCancel := context.WithTimeout(context.Background(), 3*time.Second)
	downErr := store.Ready(downCtx)
	downCancel()
	if downErr == nil {
		t.Fatal("readiness reported success while owned Milvus was stopped")
	}
	startCtx, startCancel := context.WithTimeout(context.Background(), 15*time.Second)
	dockerAction(t, startCtx, "start", containerID)
	startCancel()
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("pre-reconnect close failed: %v", err)
	}
	// Milvus standalone needs tens of seconds to serve after the container
	// start; poll factory construction with bounded attempts.
	var newStore vector.VectorStore
	var constructErr error
	for attempt := 0; attempt < 30; attempt++ {
		constructCtx, constructCancel := context.WithTimeout(context.Background(), 5*time.Second)
		newStore, constructErr = New(constructCtx, cfg, integrationEndpoint(address), integrationCredentials(credentials))
		constructCancel()
		if constructErr == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if constructErr != nil {
		t.Fatalf("factory reconnect construction failed: category=unavailable")
	}
	if err := boundedReady(t, newStore, time.Second); err != nil {
		t.Fatalf("post-restart readiness failed: %v", err)
	}
	if err := newStore.Upsert(tenantCtx, request); err != nil {
		t.Fatal(err)
	}
	if results, err := newStore.Search(tenantCtx, vector.SearchRequest{Query: []float64{1, 0, 0, 0}, TopK: 1, MinScore: 0}); err != nil || len(results) != 1 {
		t.Fatalf("post-restart search failed: count=%d err=%v", len(results), err)
	}
	if err := newStore.Delete(tenantCtx, deleteRequest(t, tenantCtx, vector.SourceDocument{SourceType: vector.SourceTypeMemory, SourceID: "memory-restart", ProjectionScope: "default", SourceVersion: 1, SourceSequence: 1, Model: cfg.Model, ModelVersion: "v1", Dimension: cfg.Dimension, SchemaVersion: cfg.SchemaVersion})); err != nil {
		t.Fatal(err)
	}
	if err := newStore.Close(context.Background()); err != nil {
		t.Fatalf("post-reconnect close failed: %v", err)
	}
}

func TestLocalMilvusRejectsInvalidCredentials(t *testing.T) {
	address, credentials := integrationEndpointAndCredentials(t)
	cfg := integrationConfig()
	cfg.Collection += "_auth"
	wrong := credentials
	wrong.Password = credentials.Password + "-wrong"
	store, err := New(context.Background(), cfg, integrationEndpoint(address), integrationCredentials(wrong))
	if err != nil {
		t.Logf("wrong-credential store construction rejected: category=unavailable")
	} else {
		defer store.Close(context.Background())
		readyCtx, readyCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer readyCancel()
		if err := store.Ready(readyCtx); err == nil {
			t.Fatal("wrong credentials were accepted by local Milvus")
		}
	}
	anonymousCtx, anonymousCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer anonymousCancel()
	anonymous, err := sdk.New(anonymousCtx, &sdk.ClientConfig{Address: address})
	if err != nil {
		t.Logf("anonymous client construction rejected: category=unavailable")
		return
	}
	defer anonymous.Close(anonymousCtx)
	if _, err := anonymous.ListCollections(anonymousCtx, sdk.NewListCollectionOption()); err == nil {
		t.Fatal("unauthenticated connection was accepted by local Milvus")
	}
}
