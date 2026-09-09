package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	artifactmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	memorymemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessionmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type failOnceArtifact struct {
	artifact.Service
	failed bool
}

func (service *failOnceArtifact) SaveArtifact(ctx context.Context, info artifact.SessionInfo, name string, value *artifact.Artifact) (int, error) {
	if !service.failed {
		service.failed = true
		return 0, errors.New("temporary target failure")
	}
	return service.Service.SaveArtifact(ctx, info, name, value)
}

func TestMirroredServicesWriteBothAndReadPrimary(t *testing.T) {
	ctx := context.Background()
	primarySession, targetSession := sessionmemory.NewSessionService(), sessionmemory.NewSessionService()
	sessions := &MirroredSession{Primary: primarySession, Target: targetSession}
	key := session.Key{AppName: "tenant/a/app/b", UserID: "user", SessionID: "session"}
	if _, err := sessions.CreateSession(ctx, key, session.StateMap{"value": []byte("primary")}); err != nil {
		t.Fatal(err)
	}
	if found, err := targetSession.GetSession(ctx, key); err != nil || found == nil {
		t.Fatalf("target session=%v err=%v", found, err)
	}

	primaryMemory, targetMemory := memorymemory.NewMemoryService(), memorymemory.NewMemoryService()
	memories := &MirroredMemory{Primary: primaryMemory, Target: targetMemory}
	user := memory.UserKey{AppName: key.AppName, UserID: key.UserID}
	if err := memories.AddMemory(ctx, user, "remember me", nil); err != nil {
		t.Fatal(err)
	}
	if found, err := targetMemory.ReadMemories(ctx, user, 10); err != nil || len(found) != 1 {
		t.Fatalf("target memories=%d err=%v", len(found), err)
	}

	primaryArtifact, targetArtifact := artifactmemory.NewService(), artifactmemory.NewService()
	artifacts := &MirroredArtifact{Primary: primaryArtifact, Target: targetArtifact}
	info := artifact.SessionInfo{AppName: key.AppName, UserID: key.UserID, SessionID: key.SessionID}
	if revision, err := artifacts.SaveArtifact(ctx, info, "answer.txt", &artifact.Artifact{MimeType: "text/plain", Data: []byte("42")}); err != nil || revision != 0 {
		t.Fatalf("revision=%d err=%v", revision, err)
	}
	if found, err := targetArtifact.LoadArtifact(ctx, info, "answer.txt", nil); err != nil || string(found.Data) != "42" {
		t.Fatalf("target artifact=%v err=%v", found, err)
	}
}

func TestMirroredArtifactRetryRepairsMissingRevision(t *testing.T) {
	ctx := context.Background()
	primary := artifactmemory.NewService()
	target := &failOnceArtifact{Service: artifactmemory.NewService()}
	service := &MirroredArtifact{Primary: primary, Target: target}
	info := artifact.SessionInfo{AppName: "tenant/a/app/b", UserID: "user", SessionID: "session"}
	if _, err := service.SaveArtifact(ctx, info, "answer.txt", &artifact.Artifact{MimeType: "text/plain", Data: []byte("first")}); !errors.Is(err, ErrMirrorWrite) {
		t.Fatalf("first error=%v", err)
	}
	if revision, err := service.SaveArtifact(ctx, info, "answer.txt", &artifact.Artifact{MimeType: "text/plain", Data: []byte("retry")}); err != nil || revision != 1 {
		t.Fatalf("retry revision=%d err=%v", revision, err)
	}
	versions, err := target.ListVersions(ctx, info, "answer.txt")
	if err != nil || len(versions) != 2 || versions[0] != 0 || versions[1] != 1 {
		t.Fatalf("versions=%v err=%v", versions, err)
	}
}

func TestValidateRoutedProfileRequiresMatchingSessionSummaryTargets(t *testing.T) {
	postgres := tenant.BackendConfig{Type: tenant.BackendPostgres}
	profile := tenant.StorageProfile{Session: postgres, Memory: postgres, Summary: postgres, Artifact: postgres, Knowledge: postgres, Audit: postgres}
	if err := ValidateRoutedProfile(profile); err != nil {
		t.Fatal(err)
	}
	target := tenant.BackendConfig{Type: tenant.BackendPostgres, Credential: tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "TARGET_DSN"}}
	profile.Session.MigrationTarget = &target
	if err := ValidateRoutedProfile(profile); err == nil {
		t.Fatal("mismatched summary migration target must fail")
	}
	other := target.Clone()
	profile.Summary.MigrationTarget = &other
	if err := ValidateRoutedProfile(profile); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRoutedProfileAllowsBidirectionalArtifactMigration(t *testing.T) {
	postgres := tenant.BackendConfig{Type: tenant.BackendPostgres}
	profile := tenant.StorageProfile{Session: postgres, Memory: postgres, Summary: postgres, Artifact: postgres, Knowledge: postgres, Audit: postgres}
	s3 := tenant.BackendConfig{Type: tenant.BackendS3, Endpoint: "https://objects.example", Namespace: "artifacts", Credential: tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "S3_CREDENTIAL_JSON"}}
	profile.Artifact.MigrationTarget = &s3
	if err := ValidateRoutedProfile(profile); err != nil {
		t.Fatalf("postgres to S3 rejected: %v", err)
	}
	profile.Artifact = s3
	target := postgres.Clone()
	profile.Artifact.MigrationTarget = &target
	if err := ValidateRoutedProfile(profile); err != nil {
		t.Fatalf("S3 to postgres rejected: %v", err)
	}
}

func TestValidateRoutedProfileRejectsSchemaOnlyBackends(t *testing.T) {
	postgres := tenant.BackendConfig{Type: tenant.BackendPostgres}
	base := tenant.StorageProfile{Session: postgres, Memory: postgres, Summary: postgres, Artifact: postgres, Knowledge: postgres, Audit: postgres}
	tests := []struct {
		name   string
		mutate func(*tenant.StorageProfile)
	}{
		{"redis session", func(profile *tenant.StorageProfile) {
			profile.Session.Type, profile.Summary.Type = tenant.BackendRedis, tenant.BackendRedis
		}},
		{"mysql memory", func(profile *tenant.StorageProfile) { profile.Memory.Type = tenant.BackendMySQL }},
		{"local artifact", func(profile *tenant.StorageProfile) { profile.Artifact.Type = tenant.BackendLocal }},
		{"milvus knowledge", func(profile *tenant.StorageProfile) { profile.Knowledge.Type = tenant.BackendMilvus }},
		{"external audit primary", func(profile *tenant.StorageProfile) { profile.Audit.Type = tenant.BackendExternal }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			profile := base.Clone()
			test.mutate(&profile)
			if err := ValidateRoutedProfile(profile); err == nil {
				t.Fatal("production route accepted a backend without a runtime adapter")
			}
		})
	}
}
