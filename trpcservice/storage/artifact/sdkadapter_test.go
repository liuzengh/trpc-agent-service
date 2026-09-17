package artifact_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact/inmemory"
	sdkartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
)

func identityFor(info sdkartifact.SessionInfo, filename string) string {
	sum := sha256.Sum256([]byte(info.SessionID + "\x00" + filename))
	sourceDigest := hex.EncodeToString(sum[:])
	id, _, _ := artifact.StableIdentity(info.AppName, info.SessionID, 0, sourceDigest)
	return id
}

func TestSDKService_SaveAndLoad(t *testing.T) {
	svc := &artifact.SDKService{Store: inmemory.New()}
	info := sdkartifact.SessionInfo{AppName: "tenant-a", UserID: "u1", SessionID: "sess-1"}

	revision, err := svc.SaveArtifact(context.Background(), info, "out.png", &sdkartifact.Artifact{Data: []byte("hello"), MimeType: "image/png"})
	if err != nil {
		t.Fatalf("SaveArtifact: %v", err)
	}
	if revision != 0 {
		t.Fatalf("expected revision 0, got %d", revision)
	}

	loaded, err := svc.LoadArtifact(context.Background(), info, "out.png", nil)
	if err != nil {
		t.Fatalf("LoadArtifact: %v", err)
	}
	if string(loaded.Data) != "hello" || loaded.MimeType != "image/png" || loaded.Name != "out.png" {
		t.Fatalf("unexpected loaded artifact: %+v", loaded)
	}

	revision2, err := svc.SaveArtifact(context.Background(), info, "out.png", &sdkartifact.Artifact{Data: []byte("hello"), MimeType: "image/png"})
	if err != nil {
		t.Fatalf("second SaveArtifact: %v", err)
	}
	if revision2 != 0 {
		t.Fatalf("expected idempotent revision 0, got %d", revision2)
	}
}

func TestSDKService_StampsUnscannedSentinel(t *testing.T) {
	store := inmemory.New()
	svc := &artifact.SDKService{Store: store}
	info := sdkartifact.SessionInfo{AppName: "tenant-a", UserID: "u1", SessionID: "sess-1"}

	if _, err := svc.SaveArtifact(context.Background(), info, "x.bin", &sdkartifact.Artifact{Data: []byte("raw")}); err != nil {
		t.Fatalf("SaveArtifact: %v", err)
	}
	record, err := store.GetArtifact(context.Background(), "tenant-a", identityFor(info, "x.bin"))
	if err != nil {
		t.Fatalf("GetArtifact: %v", err)
	}
	if record.MalwareScanVersion != artifact.SDKUnscanned || record.DLPVersion != artifact.SDKUnscanned {
		t.Fatalf("expected %q sentinel, got malware=%q dlp=%q", artifact.SDKUnscanned, record.MalwareScanVersion, record.DLPVersion)
	}
}

func TestSDKService_MapTenant(t *testing.T) {
	store := inmemory.New()
	svc := &artifact.SDKService{Store: store, MapTenant: func(appName string) string { return "mapped-" + appName }}
	info := sdkartifact.SessionInfo{AppName: "app", UserID: "u1", SessionID: "sess-1"}

	if _, err := svc.SaveArtifact(context.Background(), info, "f.txt", &sdkartifact.Artifact{Data: []byte("d")}); err != nil {
		t.Fatalf("SaveArtifact: %v", err)
	}
	if _, err := svc.LoadArtifact(context.Background(), info, "f.txt", nil); err != nil {
		t.Fatalf("LoadArtifact with MapTenant: %v", err)
	}
	// The identity must be scoped under the mapped tenant, not the raw AppName.
	id := identityFor(sdkartifact.SessionInfo{AppName: "mapped-app", UserID: "u1", SessionID: "sess-1"}, "f.txt")
	if _, err := store.GetArtifact(context.Background(), "mapped-app", id); err != nil {
		t.Fatalf("expected identity under mapped tenant, got %v", err)
	}
	if _, err := store.GetArtifact(context.Background(), "app", id); !errors.Is(err, runtime.ErrNotFound) {
		t.Fatalf("expected no record under raw AppName, got %v", err)
	}
}

func TestSDKService_Validation(t *testing.T) {
	svc := &artifact.SDKService{Store: inmemory.New()}
	info := sdkartifact.SessionInfo{AppName: "t", UserID: "u", SessionID: "s"}

	if _, err := svc.SaveArtifact(context.Background(), info, "x", &sdkartifact.Artifact{}); !errors.Is(err, runtime.ErrInvalidEnvelope) {
		t.Fatalf("empty data: expected ErrInvalidEnvelope, got %v", err)
	}
	if _, err := svc.SaveArtifact(context.Background(), info, "", &sdkartifact.Artifact{Data: []byte("d")}); !errors.Is(err, runtime.ErrInvalidEnvelope) {
		t.Fatalf("empty filename: expected ErrInvalidEnvelope, got %v", err)
	}
	var nilSvc *artifact.SDKService
	if _, err := nilSvc.SaveArtifact(context.Background(), info, "x", &sdkartifact.Artifact{Data: []byte("d")}); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("nil service: expected ErrCapabilityUnsupported, got %v", err)
	}
}

func TestSDKService_UnsupportedOperations(t *testing.T) {
	svc := &artifact.SDKService{Store: inmemory.New()}
	info := sdkartifact.SessionInfo{AppName: "t", UserID: "u", SessionID: "s"}

	if _, err := svc.ListArtifactKeys(context.Background(), info); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("ListArtifactKeys: expected ErrCapabilityUnsupported, got %v", err)
	}
	if err := svc.DeleteArtifact(context.Background(), info, "x"); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("DeleteArtifact: expected ErrCapabilityUnsupported, got %v", err)
	}
	versions, err := svc.ListVersions(context.Background(), info, "x")
	if err != nil || len(versions) != 1 || versions[0] != 0 {
		t.Fatalf("ListVersions: expected [0], got %v err=%v", versions, err)
	}
}
