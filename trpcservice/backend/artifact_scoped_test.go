package backend

import (
	"context"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/artifact"
	artifactinmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
)

func TestScopedArtifactOverridesCallerApplication(t *testing.T) {
	inner := artifactinmemory.NewService()
	scoped, err := NewScopedArtifact(inner, "tenant-a/assistant")
	if err != nil {
		t.Fatal(err)
	}
	spoofed := artifact.SessionInfo{AppName: "tenant-b/assistant", UserID: "user", SessionID: "session"}
	if _, err := scoped.SaveArtifact(context.Background(), spoofed, "answer.txt", &artifact.Artifact{Data: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if got, err := inner.LoadArtifact(context.Background(), spoofed, "answer.txt", nil); err != nil || got != nil {
		t.Fatalf("artifact escaped scope: got=%v err=%v", got, err)
	}
	forced := spoofed
	forced.AppName = "tenant-a/assistant"
	if got, err := inner.LoadArtifact(context.Background(), forced, "answer.txt", nil); err != nil || got == nil {
		t.Fatalf("artifact missing from forced scope: got=%v err=%v", got, err)
	}
}
