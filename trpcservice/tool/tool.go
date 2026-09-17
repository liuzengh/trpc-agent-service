// Package tool registers platform tools and tenant-scoped MCP / function tools.
package tool

import (
	"context"
	"strings"
	"sync"
	"unicode"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

// Status is the lifecycle state of a code-owned tool registration. A
// suspended or disabled registration is never returned to an Agent bundle;
// callers must publish a new exact version to re-enable a changed tool.
type Status string

const (
	StatusActive    Status = "active"
	StatusSuspended Status = "suspended"
	StatusDisabled  Status = "disabled"
)

// BuildFunc constructs a raw tool from a trusted, reviewed implementation.
// It must not be supplied by tenant configuration or remote input. The
// returned tool is immediately wrapped by Resolver with a tenant check and
// then by the governance Guard before it reaches an Agent.
type BuildFunc func(context.Context, BuildRequest) (agenttool.CallableTool, error)

// Registration is immutable once added to a Catalog. TenantID is mandatory:
// a global tool registration would make an accidental cross-tenant lookup
// indistinguishable from an authorization success.
type Registration struct {
	TenantID string
	ID       string
	Version  int64
	// ContentDigest pins behavior that is not represented by the (id, version)
	// tuple, such as a reviewed remote MCP endpoint and declaration. Empty is
	// reserved for legacy code-owned tools whose implementation is the binary.
	ContentDigest string
	Status        Status
	SecretRef     secrets.SecretRef
	Build         BuildFunc
}

type registrationKey struct {
	tenantID string
	id       string
	version  int64
}

// Catalog is a process-local, code-owned immutable tool directory. It is not
// a substitute for the Agent Revision allowlist or PolicySnapshot; those
// checks remain mandatory in the caller and final Guard.
type Catalog struct {
	mu      sync.RWMutex
	entries map[registrationKey]Registration
}

func NewCatalog(registrations ...Registration) (*Catalog, error) {
	catalog := &Catalog{entries: make(map[registrationKey]Registration, len(registrations))}
	for _, registration := range registrations {
		if err := catalog.Register(registration); err != nil {
			return nil, err
		}
	}
	return catalog, nil
}

// Register adds one reviewed exact version. Duplicate keys are rejected even
// when the builders happen to be identical, preserving immutable revisions.
func (c *Catalog) Register(registration Registration) error {
	if c == nil || !validTenantText(registration.TenantID) || !validToolID(registration.ID) || registration.Version < 1 ||
		(registration.ContentDigest != "" && !validContentDigest(registration.ContentDigest)) ||
		(registration.Status != StatusActive && registration.Status != StatusSuspended && registration.Status != StatusDisabled) ||
		registration.Build == nil || !validSecretRef(registration.SecretRef) {
		return runtime.ErrInvariantViolation
	}
	key := registrationKey{tenantID: registration.TenantID, id: registration.ID, version: registration.Version}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[registrationKey]Registration)
	}
	if _, exists := c.entries[key]; exists {
		return runtime.ErrVersionConflict
	}
	c.entries[key] = registration
	return nil
}

func (c *Catalog) resolve(tenantID, id string, version int64) (Registration, error) {
	if c == nil || !validTenantText(tenantID) || !validToolID(id) || version < 1 {
		return Registration{}, runtime.ErrTenantScope
	}
	c.mu.RLock()
	registration, ok := c.entries[registrationKey{tenantID: tenantID, id: id, version: version}]
	c.mu.RUnlock()
	if !ok {
		return Registration{}, runtime.ErrNotFound
	}
	return registration, nil
}

// BuildRequest is the only input a registered implementation receives. Secret
// bytes are resolved lazily at Call time from the trusted runtime execution
// context, never while building a reusable bundle.
type BuildRequest struct {
	TenantID string
	ID       string
	Version  int64
	secret   secrets.SecretRef
	provider secrets.Provider
}

// ResolveSecret returns a short-lived copy owned by the caller. The caller
// must clear value.Bytes immediately after constructing the provider request.
func (r BuildRequest) ResolveSecret(ctx context.Context) (secrets.SecretValue, error) {
	if r.provider == nil || r.secret.Ref == "" || r.secret.Version < 1 {
		return secrets.SecretValue{}, runtime.ErrCapabilityUnsupported
	}
	if ctx == nil {
		return secrets.SecretValue{}, runtime.ErrTenantScope
	}
	execution, ok := runtime.ExecutionContextFrom(ctx)
	if !ok || execution.TenantID != r.TenantID || execution.SubjectID == "" {
		return secrets.SecretValue{}, runtime.ErrTenantScope
	}
	scope := secrets.Scope{TenantID: r.TenantID, Subject: execution.SubjectID, Purpose: secrets.PurposeToolCall,
		ResourceID: r.ID, ResourceVersion: r.Version}
	value, err := r.provider.Resolve(ctx, scope, r.secret)
	if err != nil {
		clear(value.Bytes)
		return secrets.SecretValue{}, err
	}
	if value.Version != r.secret.Version {
		clear(value.Bytes)
		return secrets.SecretValue{}, runtime.ErrVersionMismatch
	}
	return value, nil
}

// Resolver implements agent.ToolResolver. Each ref is looked up using the
// complete (tenant, id, version) key and the returned raw tool is bound to the
// same tenant before the governance wrapper is applied by AgentFactory.
type Resolver struct {
	Catalog *Catalog
	Secrets secrets.Provider
}

func (r Resolver) ResolveTools(ctx context.Context, tenantID string, refs []profile.VersionedRef) ([]agenttool.Tool, error) {
	if r.Catalog == nil || !validTenantText(tenantID) {
		return nil, runtime.ErrCapabilityUnsupported
	}
	if err := contextErr(ctx); err != nil {
		return nil, err
	}
	if len(refs) == 0 {
		return []agenttool.Tool{}, nil
	}
	seen := make(map[profile.VersionedRef]struct{}, len(refs))
	result := make([]agenttool.Tool, len(refs))
	for index, ref := range refs {
		if !validToolID(ref.ID) || ref.Version < 1 || (ref.ContentDigest != "" && !validContentDigest(ref.ContentDigest)) {
			return nil, runtime.ErrInvalidEnvelope
		}
		if _, exists := seen[ref]; exists {
			return nil, runtime.ErrInvariantViolation
		}
		seen[ref] = struct{}{}
		registration, err := r.Catalog.resolve(tenantID, ref.ID, ref.Version)
		if err != nil {
			return nil, err
		}
		if registration.Status != StatusActive {
			return nil, runtime.ErrCapabilityUnsupported
		}
		if registration.ContentDigest != "" && registration.ContentDigest != ref.ContentDigest {
			return nil, runtime.ErrVersionMismatch
		}
		if registration.SecretRef.Ref != "" && r.Secrets == nil {
			return nil, runtime.ErrCapabilityUnsupported
		}
		value, err := registration.Build(ctx, BuildRequest{TenantID: tenantID, ID: ref.ID, Version: ref.Version,
			secret: registration.SecretRef, provider: r.Secrets})
		if err != nil {
			return nil, err
		}
		if value == nil || value.Declaration() == nil || value.Declaration().Name != ref.ID {
			return nil, runtime.ErrCapabilityUnsupported
		}
		result[index] = tenantBoundCallable{tenantID: tenantID, id: ref.ID, inner: value}
		if err := contextErr(ctx); err != nil {
			return nil, err
		}
	}
	return result, nil
}

type tenantBoundCallable struct {
	tenantID string
	id       string
	inner    agenttool.CallableTool
}

func (t tenantBoundCallable) Declaration() *agenttool.Declaration { return t.inner.Declaration() }

func (t tenantBoundCallable) Call(ctx context.Context, args []byte) (any, error) {
	if t.inner == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	if ctx == nil {
		return nil, runtime.ErrTenantScope
	}
	execution, ok := runtime.ExecutionContextFrom(ctx)
	if !ok || execution.TenantID != t.tenantID {
		return nil, runtime.ErrTenantScope
	}
	if declaration := t.inner.Declaration(); declaration == nil || declaration.Name != t.id {
		return nil, runtime.ErrVersionMismatch
	}
	return t.inner.Call(ctx, args)
}

func validTenantText(value string) bool {
	return value != "" && strings.TrimSpace(value) == value && strings.IndexFunc(value, unicode.IsControl) < 0
}

func validToolID(value string) bool {
	if !validTenantText(value) || len(value) > 64 {
		return false
	}
	for _, character := range value {
		if !((character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '_' || character == '-') {
			return false
		}
	}
	return true
}

func validSecretRef(value secrets.SecretRef) bool {
	return (value.Ref == "" && value.Version == 0) || (validTenantText(value.Ref) && value.Version >= 1)
}

func validContentDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
			return false
		}
	}
	return true
}

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return runtime.ErrInvariantViolation
	}
	return ctx.Err()
}

var _ interface {
	ResolveTools(context.Context, string, []profile.VersionedRef) ([]agenttool.Tool, error)
} = Resolver{}
