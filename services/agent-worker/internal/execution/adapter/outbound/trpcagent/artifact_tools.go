package trpcagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	"go.opentelemetry.io/otel/trace"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

var ErrArtifact = errors.New("artifact operation failed")
var ArtifactToolNames = []string{"artifact_save", "artifact_load", "artifact_list", "artifact_delete"}

type ArtifactConfig struct {
	Service  artifact.Service
	MaxBytes int64
}

// ArtifactSessionInfo is shared by Runner and the authenticated file entry.
// HTTP callers supply a Run ID; the ledger, not request JSON, supplies sessionID.
func ArtifactSessionInfo(tenantID, sessionID string) artifact.SessionInfo {
	k := sessionKey(tenantID, sessionID)
	return artifact.SessionInfo{AppName: k.AppName, UserID: k.UserID, SessionID: k.SessionID}
}
func ValidArtifactName(s string) bool {
	return len(s) <= 255 && s != "" && s != "." && s != ".." && utf8.ValidString(s) && !strings.ContainsAny(s, "/\\\x00\r\n") && strings.TrimSpace(s) == s
}

func ValidArtifactMIME(s string) bool {
	return len(s) > 0 && len(s) <= 256 && utf8.ValidString(s) && !strings.ContainsAny(s, "\r\n\x00")
}

type ArtifactResult struct {
	Name      string `json:"name"`
	Version   int    `json:"version"`
	Ref       string `json:"ref"`
	MimeType  string `json:"mime_type"`
	SizeBytes int    `json:"size_bytes"`
	SHA256    string `json:"sha256"`
	Content   []byte `json:"content_base64,omitempty"`
}

func ArtifactMetadata(scope artifact.SessionInfo, name string, version int, a *artifact.Artifact) ArtifactResult {
	sum := sha256.Sum256(a.Data)
	// A reference is an identifier, not a public or bearer download URL.
	return ArtifactResult{Name: name, Version: version, Ref: fmt.Sprintf("artifact:%s:%s:%d", scope.SessionID, name, version), MimeType: a.MimeType, SizeBytes: len(a.Data), SHA256: hex.EncodeToString(sum[:])}
}

type artifactTools struct {
	failed   atomic.Bool
	tracer   trace.Tracer
	maxBytes int64
}
type artifactTool struct {
	name  string
	state *artifactTools
}

func (s *artifactTools) tools() []tool.Tool {
	out := make([]tool.Tool, 0, len(ArtifactToolNames))
	for _, n := range ArtifactToolNames {
		out = append(out, &artifactTool{name: n, state: s})
	}
	return out
}
func (t *artifactTool) Declaration() *tool.Declaration {
	props := map[string]*tool.Schema{}
	required := []string{}
	if t.name != "artifact_list" {
		props["name"] = &tool.Schema{Type: "string", Description: "Single filename, not a path"}
		required = append(required, "name")
	}
	if t.name == "artifact_save" {
		props["content_base64"] = &tool.Schema{Type: "string", Description: "File bytes encoded as base64"}
		props["mime_type"] = &tool.Schema{Type: "string"}
		required = append(required, "content_base64", "mime_type")
	}
	if t.name == "artifact_load" {
		props["version"] = &tool.Schema{Type: "integer", Description: "Zero-based version; omit for latest"}
	}
	return &tool.Declaration{Name: t.name, Description: "Explicit Worker file-service operation on the current Session. Save immediately persists a version; it is not rolled back with a failed model run. Delete hides all versions.", InputSchema: &tool.Schema{Type: "object", Properties: props, Required: required, AdditionalProperties: false}}
}
func (t *artifactTool) Call(ctx context.Context, raw []byte) (any, error) {
	var args struct {
		Name     string `json:"name"`
		Content  []byte `json:"content_base64"`
		MimeType string `json:"mime_type"`
		Version  *int   `json:"version"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(&args) != nil || !errors.Is(dec.Decode(new(any)), io.EOF) || (t.name != "artifact_list" && !ValidArtifactName(args.Name)) || (args.Version != nil && *args.Version < 0) {
		return nil, errors.New("invalid artifact arguments")
	}
	if t.name == "artifact_save" && (int64(len(args.Content)) > t.state.maxBytes || !ValidArtifactMIME(args.MimeType)) {
		return nil, errors.New("invalid artifact bytes or media type")
	}
	cc, err := agent.NewCallbackContext(ctx)
	if err != nil {
		t.state.failed.Store(true)
		return nil, ErrArtifact
	}
	ctx, span := telemetrytrace.Start(t.state.tracer, ctx, "artifact."+strings.TrimPrefix(t.name, "artifact_"))
	cc.Context = ctx
	var result any
	switch t.name {
	case "artifact_save":
		a := &artifact.Artifact{Data: args.Content, MimeType: args.MimeType, Name: args.Name}
		var version int
		version, err = cc.SaveArtifact(args.Name, a)
		if err == nil {
			inv, _ := agent.InvocationFromContext(ctx)
			scope := artifact.SessionInfo{AppName: inv.Session.AppName, UserID: inv.Session.UserID, SessionID: inv.Session.ID}
			result = ArtifactMetadata(scope, args.Name, version, a)
		}
	case "artifact_load":
		version := args.Version
		if version == nil {
			var versions []int
			versions, err = cc.ListArtifactVersions(args.Name)
			if err == nil && len(versions) > 0 {
				v := slices.Max(versions)
				version = &v
			}
		}
		var a *artifact.Artifact
		if err == nil && version != nil {
			a, err = cc.LoadArtifact(args.Name, version)
		}
		if err == nil {
			if a == nil {
				result = map[string]any{"found": false, "name": args.Name}
			} else {
				inv, _ := agent.InvocationFromContext(ctx)
				scope := artifact.SessionInfo{AppName: inv.Session.AppName, UserID: inv.Session.UserID, SessionID: inv.Session.ID}
				value := ArtifactMetadata(scope, args.Name, *version, a)
				value.Content = a.Data
				result = value
			}
		}
	case "artifact_list":
		var keys []string
		keys, err = cc.ListArtifacts()
		if keys == nil {
			keys = []string{}
		}
		result = map[string]any{"keys": keys}
	case "artifact_delete":
		err = cc.DeleteArtifact(args.Name)
		result = map[string]any{"deleted": true, "name": args.Name}
	default:
		err = ErrArtifact
	}
	if err != nil {
		t.state.failed.Store(true)
		telemetrytrace.End(span, ErrArtifact)
		return nil, ErrArtifact
	}
	telemetrytrace.End(span, nil)
	return result, nil
}
