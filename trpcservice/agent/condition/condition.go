// Package condition provides the small, code-owned Catalog for Graph routing
// conditions. A Revision can select a fixed ID/version, but can never upload a
// Go function, an expression, or a state-path evaluator.
package condition

import (
	"context"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"trpc.group/trpc-go/trpc-agent-go/graph"
)

const (
	// LastResponsePresence selects branch "present" when the immediately
	// preceding graph node left a non-empty textual response, otherwise "empty".
	// It is intentionally state-shape-specific and has no tenant configurable
	// parameters.
	LastResponsePresence = "last_response_presence"
	Version              = int64(1)

	BranchPresent = "present"
	BranchEmpty   = "empty"
)

// Selector is the service-owned projection of graph.ConditionalFunc.
// Returning a branch token rather than a node name forces Factory to map the
// result through the immutable Revision edge list.
type Selector interface {
	Select(context.Context, graph.State) (string, error)
	// Branches lists the complete finite output vocabulary. Factory requires
	// the Revision path map to cover it exactly, so an unreviewed state outcome
	// cannot become an implicit graph exit or a dynamically chosen node.
	Branches() []string
}

// Resolver resolves one reviewed condition for a tenant-scoped runtime build.
type Resolver interface {
	ResolveGraphCondition(context.Context, string, profile.VersionedRef) (Selector, error)
}

// Registry is a static process catalog. It intentionally has no registration
// API exposed to control-plane requests: adding a condition is a code review
// and release operation, not tenant configuration.
type Registry struct{}

// DefaultRegistry returns the platform's reviewed graph-condition catalog.
func DefaultRegistry() Registry { return Registry{} }

func (Registry) ResolveGraphCondition(ctx context.Context, tenantID string, ref profile.VersionedRef) (Selector, error) {
	if ctx == nil || strings.TrimSpace(tenantID) != tenantID || tenantID == "" || ref.ID == "" || ref.Version < 1 ||
		ref.ContentDigest != "" {
		return nil, runtime.ErrInvariantViolation
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ref.ID != LastResponsePresence || ref.Version != Version {
		return nil, runtime.ErrCapabilityUnsupported
	}
	return lastResponsePresence{}, nil
}

type lastResponsePresence struct{}

func (lastResponsePresence) Branches() []string { return []string{BranchPresent, BranchEmpty} }

func (lastResponsePresence) Select(ctx context.Context, state graph.State) (string, error) {
	if ctx == nil {
		return "", runtime.ErrInvariantViolation
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	response, _ := graph.GetStateValue[string](state, graph.StateKeyLastResponse)
	if strings.TrimSpace(response) == "" {
		return BranchEmpty, nil
	}
	return BranchPresent, nil
}

var _ Resolver = Registry{}
var _ Selector = lastResponsePresence{}
