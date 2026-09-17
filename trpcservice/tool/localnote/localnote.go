// Package localnote provides the code-owned business tool used by webui-local
// to exercise durable confirmation without an external provider dependency.
package localnote

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	servicetool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

const (
	ID      = "webui_create_note"
	Version = int64(1)

	maximumArguments = 8 << 10
	maximumTitle     = 200
	maximumContent   = 4000
)

type input struct {
	Title   string `json:"title"`
	Content string `json:"content"`
}

type Result struct {
	Status  string `json:"status"`
	NoteID  string `json:"note_id"`
	Title   string `json:"title"`
	Content string `json:"content"`
}

// Registration binds this exact implementation to one tenant. The production
// worker does not register it; only the explicitly local WebUI composition
// root does.
func Registration(tenantID string) servicetool.Registration {
	return servicetool.Registration{TenantID: tenantID, ID: ID, Version: Version, Status: servicetool.StatusActive,
		Build: func(context.Context, servicetool.BuildRequest) (agenttool.CallableTool, error) { return Tool{}, nil }}
}

type Tool struct{}

func (Tool) Declaration() *agenttool.Declaration {
	return &agenttool.Declaration{Name: ID,
		Description: "Create a user-approved local note. This action always requires durable confirmation.",
		InputSchema: &agenttool.Schema{Type: "object", AdditionalProperties: false, Required: []string{"title", "content"},
			Properties: map[string]*agenttool.Schema{
				"title":   {Type: "string", Description: "Short note title"},
				"content": {Type: "string", Description: "Note content"},
			}},
		OutputSchema: &agenttool.Schema{Type: "object", AdditionalProperties: false, Required: []string{"status", "note_id", "title", "content"},
			Properties: map[string]*agenttool.Schema{
				"status":  {Type: "string"},
				"note_id": {Type: "string"},
				"title":   {Type: "string"},
				"content": {Type: "string"},
			}},
	}
}

func (Tool) Call(ctx context.Context, arguments []byte) (any, error) {
	if ctx == nil || len(arguments) == 0 || len(arguments) > maximumArguments {
		return nil, runtime.ErrInvariantViolation
	}
	execution, ok := runtime.ExecutionContextFrom(ctx)
	if !ok || execution.SubjectID == "" || execution.ToolCallID == "" || execution.ArgsDigest == "" {
		return nil, runtime.ErrTenantScope
	}
	canonical, digest, err := governance.CanonicalArguments(arguments)
	if err != nil || digest != execution.ArgsDigest {
		return nil, runtime.ErrVersionMismatch
	}
	decoder := json.NewDecoder(bytes.NewReader(canonical))
	decoder.DisallowUnknownFields()
	var value input
	if err := decoder.Decode(&value); err != nil {
		return nil, runtime.ErrInvariantViolation
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, runtime.ErrInvariantViolation
	}
	value.Title = strings.TrimSpace(value.Title)
	value.Content = strings.TrimSpace(value.Content)
	if value.Title == "" || value.Content == "" || len(value.Title) > maximumTitle || len(value.Content) > maximumContent ||
		strings.ContainsRune(value.Title, 0) || strings.ContainsRune(value.Content, 0) {
		return nil, runtime.ErrInvariantViolation
	}
	digestInput := execution.TenantID + "\x00" + execution.RequestID + "\x00" + execution.ToolCallID + "\x00" + execution.ArgsDigest
	sum := sha256.Sum256([]byte(digestInput))
	return Result{Status: "created", NoteID: "note_" + hex.EncodeToString(sum[:12]), Title: value.Title, Content: value.Content}, nil
}

var _ agenttool.CallableTool = Tool{}
