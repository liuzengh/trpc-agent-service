// Package tool registers the platform tools a revision may reference, and the
// policies that authorize them.
//
// The registry is static and process-local: its contents are fixed when the
// binary is built, and every Runtime resolves against the same set. It is
// extensible in the sense that matters here — a new tool is a Definition added
// to the builtin list, not a new branch in the resolver — but nothing registers
// a tool at runtime. Dynamic registration would make the set of executable
// tools depend on load order and on whichever tenant published last, and a
// revision is supposed to be reproducible.
package tool

import (
	"errors"
	"fmt"
	"regexp"

	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

var (
	// ErrUnknownTool reports a tool_refs entry that names no registered tool.
	ErrUnknownTool = errors.New("tool: revision references an unknown tool")
	// ErrUnknownPolicy reports a policy_refs entry that names no registered
	// policy.
	ErrUnknownPolicy = errors.New("tool: revision references an unknown policy")
	// ErrDuplicateRef reports a reference list that names the same ref twice.
	ErrDuplicateRef = errors.New("tool: revision repeats a reference")
	// ErrPolicyRequired reports tool_refs with no policy_refs to authorize them.
	ErrPolicyRequired = errors.New("tool: tool refs require at least one policy ref")
	// ErrNotAuthorized reports a tool that some named policy does not allow.
	ErrNotAuthorized = errors.New("tool: tool is not authorized by every policy")
	// ErrInvalidRegistry reports a registry that cannot be built from its
	// definitions. It is a fault in the binary, not in a revision.
	ErrInvalidRegistry = errors.New("tool: invalid registry definition")
)

// functionNamePattern is the name shape a tool may present to a model.
//
// This is the OpenAI function-name constraint, and it is the narrow form that
// every OpenAI-compatible provider accepts: letters, digits, underscore and
// hyphen only, at most 64 characters. A dot is specifically excluded — it is
// legal in this platform's other identifiers, and several providers reject it.
//
// A tool whose name is refused by the provider does not degrade; it fails the
// whole request, including the turns that never wanted the tool. So the check
// happens when the registry is built, not when a model rejects it.
var functionNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// Definition is one registered tool.
//
// Ref is both the identifier a RevisionConfig names and the function name the
// model sees, so there is exactly one string to reason about; NewRegistry
// checks that the constructed tool agrees.
type Definition struct {
	Ref         string
	Description string
	// New returns a fresh instance for one Runtime. Every tool registered here
	// is stateless, so a shared instance would work today — but a Runtime is
	// per (tenant, app, revision) and outlives a request, so a tool that later
	// holds state must not be able to carry it across tenants by accident.
	New func() agenttool.Tool
}

// PolicyDefinition is one registered policy: the set of tool refs it allows.
//
// A policy carries no deny list. Evaluation is deny-first — a tool is refused
// unless every named policy names it — so a deny list would only be able to
// re-refuse something already refused.
type PolicyDefinition struct {
	Ref             string
	Description     string
	AllowedToolRefs []string
}

// Registry is an immutable set of tools and the policies that authorize them.
type Registry struct {
	tools    map[string]Definition
	policies map[string]map[string]struct{}
}

// NewRegistry validates definitions and returns the registry they describe.
//
// Everything checkable is checked here, once, rather than per revision: a
// duplicate ref, a name no provider would accept, a policy that allows a tool
// that does not exist. Each is a fault in the binary that would otherwise
// surface as a confusing per-tenant failure much later.
func NewRegistry(tools []Definition, policies []PolicyDefinition) (*Registry, error) {
	registry := &Registry{
		tools:    make(map[string]Definition, len(tools)),
		policies: make(map[string]map[string]struct{}, len(policies)),
	}
	for _, definition := range tools {
		if !functionNamePattern.MatchString(definition.Ref) {
			return nil, fmt.Errorf(
				"%w: tool ref %q must match %s", ErrInvalidRegistry, definition.Ref, functionNamePattern)
		}
		if _, exists := registry.tools[definition.Ref]; exists {
			return nil, fmt.Errorf(
				"%w: tool ref %q is registered twice", ErrInvalidRegistry, definition.Ref)
		}
		if definition.New == nil {
			return nil, fmt.Errorf(
				"%w: tool ref %q has no constructor", ErrInvalidRegistry, definition.Ref)
		}
		// The declaration is what actually reaches the provider, so the name is
		// verified on a real instance rather than on the ref alone.
		if err := validateDeclaration(definition); err != nil {
			return nil, err
		}
		registry.tools[definition.Ref] = definition
	}
	for _, policy := range policies {
		if policy.Ref == "" {
			return nil, fmt.Errorf("%w: a policy has no ref", ErrInvalidRegistry)
		}
		if _, exists := registry.policies[policy.Ref]; exists {
			return nil, fmt.Errorf(
				"%w: policy ref %q is registered twice", ErrInvalidRegistry, policy.Ref)
		}
		allowed := make(map[string]struct{}, len(policy.AllowedToolRefs))
		for _, ref := range policy.AllowedToolRefs {
			if _, known := registry.tools[ref]; !known {
				return nil, fmt.Errorf(
					"%w: policy %q allows unregistered tool %q",
					ErrInvalidRegistry, policy.Ref, ref)
			}
			allowed[ref] = struct{}{}
		}
		registry.policies[policy.Ref] = allowed
	}
	return registry, nil
}

// validateDeclaration builds one instance and checks that what it publishes
// matches what was registered.
func validateDeclaration(definition Definition) error {
	instance := definition.New()
	if instance == nil {
		return fmt.Errorf(
			"%w: tool ref %q constructed no tool", ErrInvalidRegistry, definition.Ref)
	}
	declaration := instance.Declaration()
	if declaration == nil {
		return fmt.Errorf(
			"%w: tool ref %q has no declaration", ErrInvalidRegistry, definition.Ref)
	}
	if declaration.Name != definition.Ref {
		return fmt.Errorf(
			"%w: tool ref %q declares function name %q",
			ErrInvalidRegistry, definition.Ref, declaration.Name)
	}
	if _, callable := instance.(agenttool.CallableTool); !callable {
		return fmt.Errorf(
			"%w: tool ref %q is not callable", ErrInvalidRegistry, definition.Ref)
	}
	return nil
}

// Resolve returns the tools a revision may execute, in tool_refs order.
//
// It is the whole of the tool-authorization decision, and it fails closed:
// every reference must resolve, no reference may repeat, and a tool is
// executable only when every named policy allows it. Nothing is skipped with a
// warning. A revision that names something the platform cannot honor does not
// get a quietly reduced tool set — it does not build at all, because the
// alternative is an agent that silently behaves as a different agent.
//
// The empty case is preserved exactly: no tool_refs means no tools, and a
// revision written before tools existed resolves to nil.
func (r *Registry) Resolve(toolRefs []string, policyRefs []string) ([]agenttool.Tool, error) {
	if r == nil {
		return nil, fmt.Errorf("%w: registry is nil", ErrInvalidRegistry)
	}
	// Policies are validated first and unconditionally. A revision naming a
	// policy that does not exist is misconfigured whether or not it also names
	// a tool, and accepting it would let a policy be deleted from the binary
	// without anything noticing.
	if err := r.validatePolicyRefs(policyRefs); err != nil {
		return nil, err
	}
	if len(toolRefs) == 0 {
		return nil, nil
	}
	if len(policyRefs) == 0 {
		return nil, fmt.Errorf("%w: %d tool ref(s) named", ErrPolicyRequired, len(toolRefs))
	}
	resolved := make([]agenttool.Tool, 0, len(toolRefs))
	seen := make(map[string]struct{}, len(toolRefs))
	for _, ref := range toolRefs {
		definition, known := r.tools[ref]
		if !known {
			return nil, fmt.Errorf("%w: %q", ErrUnknownTool, ref)
		}
		if _, repeated := seen[ref]; repeated {
			return nil, fmt.Errorf("%w: tool %q", ErrDuplicateRef, ref)
		}
		seen[ref] = struct{}{}
		if err := r.authorize(ref, policyRefs); err != nil {
			return nil, err
		}
		resolved = append(resolved, definition.New())
	}
	return resolved, nil
}

// HasPolicy reports whether ref names a registered policy.
//
// It exists for the security manifest, which validates a tenant's entitled
// policy refs at load time. That check has to happen there and not at first
// use: by design a caller cannot tell "unknown policy" from "not entitled to
// it", so a misspelled ref in the manifest would otherwise be a silent,
// permanent refusal with nothing to read.
func (r *Registry) HasPolicy(ref string) bool {
	if r == nil {
		return false
	}
	_, known := r.policies[ref]
	return known
}

// validatePolicyRefs rejects a policy list that names something unregistered or
// names the same policy twice.
func (r *Registry) validatePolicyRefs(policyRefs []string) error {
	seen := make(map[string]struct{}, len(policyRefs))
	for _, ref := range policyRefs {
		if _, known := r.policies[ref]; !known {
			return fmt.Errorf("%w: %q", ErrUnknownPolicy, ref)
		}
		if _, repeated := seen[ref]; repeated {
			return fmt.Errorf("%w: policy %q", ErrDuplicateRef, ref)
		}
		seen[ref] = struct{}{}
	}
	return nil
}

// authorize requires every named policy to allow this tool. Policies intersect
// rather than union: policy_refs narrows tool_refs, so adding a policy can only
// ever remove a tool from the resolved set.
func (r *Registry) authorize(toolRef string, policyRefs []string) error {
	for _, policyRef := range policyRefs {
		if _, allowed := r.policies[policyRef][toolRef]; !allowed {
			return fmt.Errorf(
				"%w: policy %q does not allow tool %q", ErrNotAuthorized, policyRef, toolRef)
		}
	}
	return nil
}
