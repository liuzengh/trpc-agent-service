package messaging

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	artifactinmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
)

func TestStageInboundFilesPersistsProviderMediaAsArtifactReferences(t *testing.T) {
	service := artifactinmemory.NewService()
	info := agentartifact.SessionInfo{AppName: "acme/support", UserID: "user-1", SessionID: "session-1"}
	files, err := StageInboundFiles(context.Background(), service, info, "message-1", []channels.ReceivedFile{{
		Name: "../report.txt", Data: []byte("hello attachment"),
	}})
	if err != nil {
		t.Fatalf("StageInboundFiles() error = %v", err)
	}
	if len(files) != 1 || files[0].Name != "report.txt" || files[0].ArtifactName != "input/message-1/00-report.txt" {
		t.Fatalf("files = %#v", files)
	}
	version := files[0].Version
	stored, err := service.LoadArtifact(context.Background(), info, files[0].ArtifactName, &version)
	if err != nil || stored == nil || string(stored.Data) != "hello attachment" {
		t.Fatalf("stored artifact = %#v, error = %v", stored, err)
	}
}
