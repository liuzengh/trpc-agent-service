package artifact_test

import (
	"context"
	"errors"
	"testing"

	serviceartifact "github.com/liuzengh/trpc-agent-service/trpcservice/artifact"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	storageartifact "github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact"
	artifactmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact/inmemory"
	sdkartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
)

type backendReader struct {
	value provider.BackendProfileSnapshot
}

func (r backendReader) GetBackend(context.Context, string, string, int64) (provider.BackendProfileSnapshot, error) {
	return r.value, nil
}

func TestResolverPinsPostgresStoreAndScopesAppName(t *testing.T) {
	snapshot := artifactSnapshot()
	backend := provider.BackendProfileSnapshot{TenantID: "tenant-a", ProfileID: "artifact", Version: 7, Status: "active",
		Provider: "postgres", SchemaVersion: 2, Configuration: map[string]string{"connection_id": "separate"},
		Capabilities: provider.CapabilitySet{"strong_ryw": true}}
	var gotConnection, gotDSN string
	resolver := serviceartifact.Resolver{Profiles: backendReader{value: backend}, PostgresConnections: map[string]string{"separate": "postgres://artifact"},
		BuildPostgres: func(_ context.Context, connectionID, dsn string) (storageartifact.Store, func() error, error) {
			gotConnection, gotDSN = connectionID, dsn
			return artifactmemory.New(), nil, nil
		}}
	service, closeFn, err := resolver.Resolve(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if closeFn != nil || gotConnection != "separate" || gotDSN != "postgres://artifact" {
		t.Fatalf("artifact builder = (%q, %q), close=%v", gotConnection, gotDSN, closeFn != nil)
	}
	info := sdkartifact.SessionInfo{AppName: snapshot.AppName, SessionID: "session-1"}
	if _, err := service.SaveArtifact(context.Background(), info, "note.txt", &sdkartifact.Artifact{Data: []byte("hello"), MimeType: "text/plain"}); err != nil {
		t.Fatal(err)
	}
	loaded, err := service.LoadArtifact(context.Background(), info, "note.txt", nil)
	if err != nil || string(loaded.Data) != "hello" {
		t.Fatalf("loaded artifact = %#v, error = %v", loaded, err)
	}
	if _, err := service.SaveArtifact(context.Background(), sdkartifact.SessionInfo{AppName: "tenant-a/other", SessionID: "session-1"}, "note.txt", &sdkartifact.Artifact{Data: []byte("forged")}); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("cross-app write error = %v, want tenant scope", err)
	}
}

func TestResolverUsesDefaultStoreForLegacySnapshot(t *testing.T) {
	snapshot := artifactSnapshot()
	snapshot.BackendBindings = nil
	resolver := serviceartifact.Resolver{Profiles: backendReader{}, Stores: map[string]storageartifact.Store{"default": artifactmemory.New()}}
	service, closeFn, err := resolver.Resolve(context.Background(), snapshot)
	if err != nil || service == nil || closeFn != nil {
		t.Fatalf("legacy artifact service = %#v, close=%v, error=%v", service, closeFn != nil, err)
	}
}

func artifactSnapshot() profile.ExecutionProfileSnapshot {
	return profile.ExecutionProfileSnapshot{Key: profile.ExecutionProfileKey{TenantID: "tenant-a", AgentAppID: "app", ConfigVersion: 2}, AppName: "tenant-a/app",
		BackendBindings: []profile.BackendBinding{{Domain: "artifact", BackendProfileID: "artifact", BackendVersion: 7, Required: []string{"strong_ryw"}}}}
}
