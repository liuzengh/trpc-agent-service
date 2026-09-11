package tool

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

const SaveArtifactToolName = "platform.save_artifact"

type SaveArtifactTool struct{}

func NewSaveArtifactTool() *SaveArtifactTool {
	return &SaveArtifactTool{}
}

func (t *SaveArtifactTool) Declaration() *agenttool.Declaration {
	return &agenttool.Declaration{
		Name:        SaveArtifactToolName,
		Description: "Save a file produced for the current user. Use this for reports, code, images or other run outputs. Set user_scope true for files that should persist across sessions.",
		InputSchema: &agenttool.Schema{
			Type:                 "object",
			Required:             []string{"filename"},
			AdditionalProperties: false,
			Properties: map[string]*agenttool.Schema{
				"filename":    {Type: "string", Description: "File name without path separators"},
				"mime_type":   {Type: "string", Description: "IANA MIME type; defaults to text/plain for text and application/octet-stream for binary"},
				"text":        {Type: "string", Description: "UTF-8 text content"},
				"data_base64": {Type: "string", Description: "Raw bytes as standard base64"},
				"name":        {Type: "string", Description: "Optional display name"},
				"user_scope":  {Type: "boolean", Description: "Store in the user: namespace so the file is visible across sessions"},
			},
		},
	}
}

type saveArtifactRequest struct {
	Filename   string `json:"filename"`
	MimeType   string `json:"mime_type"`
	Text       string `json:"text"`
	DataBase64 string `json:"data_base64"`
	Name       string `json:"name"`
	UserScope  bool   `json:"user_scope"`
}

func (t *SaveArtifactTool) Call(ctx context.Context, arguments []byte) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var request saveArtifactRequest
	if len(arguments) > 0 && string(arguments) != "null" {
		if err := json.Unmarshal(arguments, &request); err != nil {
			return nil, fmt.Errorf("decode save artifact arguments: %w", err)
		}
	}
	filename := strings.TrimSpace(request.Filename)
	if filename == "" {
		return nil, fmt.Errorf("filename is required")
	}
	if request.Text != "" && request.DataBase64 != "" {
		return nil, fmt.Errorf("provide either text or data_base64, not both")
	}
	var data []byte
	mimeType := strings.TrimSpace(request.MimeType)
	switch {
	case request.DataBase64 != "":
		decoded, err := base64.StdEncoding.DecodeString(request.DataBase64)
		if err != nil {
			return nil, fmt.Errorf("decode data_base64: %w", err)
		}
		data = decoded
		if mimeType == "" {
			mimeType = "application/octet-stream"
		}
	default:
		if request.Text == "" {
			return nil, fmt.Errorf("text or data_base64 is required")
		}
		data = []byte(request.Text)
		if mimeType == "" {
			mimeType = "text/plain"
		}
	}
	if int64(len(data)) > storage.MaxArtifactBytes {
		return nil, fmt.Errorf("artifact content exceeds limit %d", storage.MaxArtifactBytes)
	}
	if request.UserScope && !strings.HasPrefix(filename, "user:") {
		filename = "user:" + filename
	}
	toolContext, err := agent.NewToolContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("artifact tool context: %w", err)
	}
	version, err := toolContext.SaveArtifact(filename, &agentartifact.Artifact{
		Data:     data,
		MimeType: mimeType,
		Name:     strings.TrimSpace(request.Name),
	})
	if err != nil {
		return nil, err
	}
	recordSavedArtifact(ctx, SavedArtifact{
		Filename: filename, Version: version, MimeType: mimeType, Name: strings.TrimSpace(request.Name),
	})
	return map[string]any{"filename": filename, "version": version, "mime_type": mimeType}, nil
}
