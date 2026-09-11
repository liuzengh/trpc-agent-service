package tool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	platformskill "github.com/liuzengh/trpc-agent-service/trpcservice/skill"
	platformworkspace "github.com/liuzengh/trpc-agent-service/trpcservice/workspace"
)

// frameworkHosted names pinned "go" tools whose implementations are mounted
// by the framework itself when the runner is assembled (today: the skill
// tooling behind llmagent.WithSkills and the workspace execution surface
// behind llmagent.WithCodeExecutor). The pin still needs its binding row —
// that row is the governance record of what the revision attached — but
// assembly neither wraps nor registers anything for these names here.
var frameworkHosted = map[string]struct{}{
	platformskill.Name:     {},
	platformworkspace.Name: {},
}

// Assembly: turning one revision's pinned tool list into live tools.
//
// Reliable mode has exactly one source of tools — `agent_revisions.tools`
// listing (name, version) pins that were resolved through tool_bindings at
// publish time. There is deliberately no fallback to a name-only allowlist
// here: that fallback is how a model would end up holding an ungoverned
// builtin in the middle of an otherwise controlled deployment. If a revision
// carries no pins, the agent has no tools, which is a valid configuration.
type PinnedTool struct {
	Name    string `json:"name"`
	Version uint32 `json:"version"`
}

// RevisionTools is the JSON shape of agent_revisions.tools in reliable mode.
type RevisionTools struct {
	Pinned []PinnedTool `json:"pinned"`
}

// ParseRevisionTools validates the pin list: names are unique, versions are
// positive. A duplicate name is refused rather than deduplicated, because it
// can only come from a publish-time bug and silently picking one version
// would make the manifest hash a lie.
func ParseRevisionTools(raw json.RawMessage) ([]PinnedTool, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var rt RevisionTools
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rt); err != nil {
		return nil, fmt.Errorf("tool: revision tools: %w", err)
	}
	seen := make(map[string]struct{}, len(rt.Pinned))
	for _, p := range rt.Pinned {
		if p.Name == "" {
			return nil, errors.New("tool: revision tools: a pin has an empty name")
		}
		if p.Version == 0 {
			return nil, fmt.Errorf("tool: revision tools: %q has no version", p.Name)
		}
		if _, dup := seen[p.Name]; dup {
			return nil, fmt.Errorf("tool: revision tools: %q is pinned twice", p.Name)
		}
		seen[p.Name] = struct{}{}
	}
	return rt.Pinned, nil
}

// SecretRefs is the shape of tool_bindings.secret_refs: header name to
// reference. Values are references (env:..., file:...), never plaintext, and
// they are resolved through the allowlisted Resolver.
type SecretRefs struct {
	Headers map[string]string `json:"headers,omitempty"`
}

// BuildPinned resolves every pin into a governed tool. Any failure here
// fails the execution before the model is called: a revision that cannot be
// assembled is a publishing bug, and running it with a silently missing tool
// would change the agent's behaviour without changing its manifest.
func BuildPinned(
	ctx context.Context,
	scope controlplane.Scope,
	appID int64,
	raw json.RawMessage,
	gov *Governor,
	resolver *secrets.Resolver,
	extras map[string]frameworktool.CallableTool,
) ([]frameworktool.Tool, error) {
	pinned, err := ParseRevisionTools(raw)
	if err != nil {
		return nil, err
	}
	if len(pinned) == 0 {
		return nil, nil
	}
	if gov == nil {
		return nil, errors.New("tool: pinned tools require a governor; this run did not come through a claim")
	}
	tools := make([]frameworktool.Tool, 0, len(pinned))
	for _, p := range pinned {
		b, err := scope.GetToolBindingByName(ctx, appID, p.Name, p.Version)
		if err != nil {
			return nil, fmt.Errorf("tool: pinned tool %s@%d: %w", p.Name, p.Version, err)
		}
		if b.Status != "active" {
			return nil, fmt.Errorf("tool: pinned tool %s@%d is %s; republish or re-activate it", p.Name, p.Version, b.Status)
		}
		meta := ToolMeta{
			Name:       p.Name,
			ToolID:     b.ToolID,
			Version:    b.Version,
			Kind:       b.Kind,
			RiskLevel:  b.RiskLevel,
			SideEffect: b.SideEffect,
			Idempotent: b.Idempotent,
			Timeout:    time.Duration(b.TimeoutMS) * time.Millisecond,
		}
		switch b.Kind {
		case "go":
			if _, hosted := frameworkHosted[p.Name]; hosted {
				// Mounted by the framework when the runner is assembled; the
				// pin above is what turns it on. Nothing to resolve here.
				continue
			}
			// A name in "extras" is a tool the caller assembled with request-
			// scoped facts (the pinned knowledge bases, the execution identity)
			// that a static builtin registry cannot carry (see
			// knowledge.NewSearchTool). Extras take precedence, and a name in
			// neither is an assembly error rather than a silent omission.
			var callable frameworktool.CallableTool
			if inner, ok := extras[p.Name]; ok {
				callable = inner
			} else if builtinInstance, ok := Builtins()[p.Name]; ok {
				var assertOK bool
				callable, assertOK = builtinInstance.(frameworktool.CallableTool)
				if !assertOK {
					return nil, fmt.Errorf("tool: platform tool %q is not callable", p.Name)
				}
			} else {
				return nil, fmt.Errorf("tool: pinned Go tool %q has no platform implementation", p.Name)
			}
			tools = append(tools, gov.Wrap(callable, meta))
		case "http":
			schema, err := CompileInputSchema(b.InputSchema)
			if err != nil {
				return nil, fmt.Errorf("tool: %s: %w", p.Name, err)
			}
			spec, err := CompileHTTPSpec(b.Spec)
			if err != nil {
				return nil, fmt.Errorf("tool: %s: %w", p.Name, err)
			}
			secretHeaders, err := resolveSecretHeaders(resolver, b.SecretRefs)
			if err != nil {
				return nil, fmt.Errorf("tool: %s: %w", p.Name, err)
			}
			inner := NewHTTPTool(p.Name, "", schema, spec, secretHeaders,
				HTTPPolicy{SideEffect: b.SideEffect, Idempotent: b.Idempotent})
			tools = append(tools, gov.Wrap(inner, meta))
		default:
			return nil, fmt.Errorf("tool: pinned tool %s has unknown kind %q", p.Name, b.Kind)
		}
	}
	return tools, nil
}

// resolveSecretHeaders resolves every header credential through the
// allowlist. A reference outside the allowlist fails assembly rather than
// being skipped: "the tool silently ran without its credential" is the
// failure mode this check exists to prevent (approved plan: 越界拒绝).
func resolveSecretHeaders(resolver *secrets.Resolver, raw json.RawMessage) (map[string]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var refs SecretRefs
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&refs); err != nil {
		return nil, fmt.Errorf("secret_refs: %w", err)
	}
	if len(refs.Headers) == 0 {
		return nil, nil
	}
	if resolver == nil {
		return nil, errors.New("secret_refs are configured but no secret resolver is wired")
	}
	out := make(map[string]string, len(refs.Headers))
	for header, ref := range refs.Headers {
		if header == "" {
			return nil, errors.New("secret_refs: a header name is empty")
		}
		v, err := resolver.Resolve(ref)
		if err != nil {
			return nil, fmt.Errorf("secret ref for header %s: %w", header, err)
		}
		out[header] = v
	}
	return out, nil
}
