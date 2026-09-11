package messaging

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
)

// StageInboundFiles converts transient provider media into durable Artifact
// references. All ingress surfaces use this function so Artifact naming,
// limits, MIME detection and rollback are identical.
func StageInboundFiles(ctx context.Context, service agentartifact.Service, info agentartifact.SessionInfo, messageID string, received []channels.ReceivedFile) ([]channels.InboundFile, error) {
	if len(received) == 0 {
		return nil, nil
	}
	if service == nil {
		return nil, fmt.Errorf("artifact service is required for file inputs")
	}
	messageID = strings.TrimSpace(messageID)
	if messageID == "" {
		return nil, fmt.Errorf("message ID is required for file inputs")
	}
	files := make([]channels.InboundFile, 0, len(received))
	for index, input := range received {
		name := safeInputName(input.Name)
		if len(input.Data) == 0 {
			DeleteInboundFiles(ctx, service, info, files)
			return nil, fmt.Errorf("file %q is empty", name)
		}
		if int64(len(input.Data)) > storage.MaxArtifactBytes {
			DeleteInboundFiles(ctx, service, info, files)
			return nil, fmt.Errorf("file %q exceeds the %d byte upload limit", name, storage.MaxArtifactBytes)
		}
		mimeType := strings.TrimSpace(input.MimeType)
		if mimeType == "" || mimeType == "application/octet-stream" {
			mimeType = http.DetectContentType(input.Data)
		}
		artifactName := fmt.Sprintf("input/%s/%02d-%s", messageID, index, name)
		version, err := service.SaveArtifact(ctx, info, artifactName, &agentartifact.Artifact{
			Data: input.Data, MimeType: mimeType, Name: name,
		})
		if err != nil {
			DeleteInboundFiles(ctx, service, info, files)
			return nil, fmt.Errorf("save %q: %w", name, err)
		}
		files = append(files, channels.InboundFile{
			Name: name, MimeType: mimeType, ArtifactName: artifactName,
			Version: version, SizeBytes: int64(len(input.Data)),
		})
	}
	return files, nil
}

// DeleteInboundFiles rolls back staged inputs after an ingress failure.
func DeleteInboundFiles(ctx context.Context, service agentartifact.Service, info agentartifact.SessionInfo, files []channels.InboundFile) {
	if service == nil {
		return
	}
	for _, file := range files {
		_ = service.DeleteArtifact(ctx, info, file.ArtifactName)
	}
}

func safeInputName(filename string) string {
	name := path.Base(strings.ReplaceAll(strings.TrimSpace(filename), "\\", "/"))
	name = strings.ReplaceAll(name, "\x00", "")
	if name == "" || name == "." || name == "/" {
		return "upload"
	}
	return name
}
