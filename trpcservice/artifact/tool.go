package artifact

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"
)

// SaveTool is the artifact_save builtin: the model (or rather the execution
// it runs in) writes one named file. The execution identity is fixed at
// assembly; the model only supplies name and content.
type SaveTool struct {
	svc   *Service
	scope Scope
}

// Scope is the execution identity the tool was assembled for.
type Scope struct {
	TenantID    string
	ExecutionID string
	SessionPK   int64
}

// Name is the pinned name revisions use.
const Name = "artifact_save"

// NewSaveTool builds the tool.
func NewSaveTool(svc *Service, scope Scope) *SaveTool {
	return &SaveTool{svc: svc, scope: scope}
}

// Declaration implements frameworktool.Tool.
func (t *SaveTool) Declaration() *frameworktool.Declaration {
	return &frameworktool.Declaration{
		Name: Name,
		Description: "Save a text file the user asked for. The file becomes downloadable " +
			"once this reply is delivered; the id is returned for reference.",
		InputSchema: &frameworktool.Schema{
			Type: "object",
			Properties: map[string]*frameworktool.Schema{
				"name":    {Type: "string", Description: "file name, e.g. report.md"},
				"content": {Type: "string", Description: "the whole file content"},
				"mime":    {Type: "string", Description: "optional media type, default text/plain"},
			},
			Required:             []string{"name", "content"},
			AdditionalProperties: false,
		},
	}
}

// Call implements frameworktool.CallableTool.
func (t *SaveTool) Call(ctx context.Context, jsonArgs []byte) (any, error) {
	var args struct {
		Name    string `json:"name"`
		Content string `json:"content"`
		MIME    string `json:"mime"`
	}
	dec := json.NewDecoder(bytes.NewReader(jsonArgs))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&args); err != nil {
		return nil, fmt.Errorf("artifact_save: arguments: %w", err)
	}
	name := strings.TrimSpace(args.Name)
	if name == "" {
		return nil, fmt.Errorf("artifact_save: name is required")
	}
	if strings.ContainsAny(name, "/\\") {
		// The name travels to a header and a Content-Disposition; a path
		// separator in it is either a mistake or an attempt at one.
		return nil, fmt.Errorf("artifact_save: name must not contain path separators")
	}
	if strings.TrimSpace(args.Content) == "" {
		return nil, fmt.Errorf("artifact_save: content is required")
	}
	if args.MIME == "" {
		args.MIME = "text/plain"
	}
	a, err := t.svc.SaveStaged(ctx, t.scope.TenantID, t.scope.ExecutionID, t.scope.SessionPK,
		uuid.NewString(), name, args.MIME, []byte(args.Content))
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"artifact_id": a.PublicID,
		"name":        a.Name,
		"status":      a.Status,
		"note":        "the file is downloadable once this reply has been committed",
	}, nil
}
