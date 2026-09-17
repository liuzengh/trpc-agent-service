// Package tenant defines the trusted tenant identity carried across service
// boundaries. Runtime configuration versions deliberately live in
// ExecutionBinding rather than TenantContext.
package tenant

import (
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// TenantContext is the server-established security and routing identity.
// Values received from a client or broker must be compared with a value loaded
// from a trusted binding or task store before use.
type TenantContext struct {
	TenantID      string
	TenantVersion int64
	AgentAppID    string
	SubjectID     string
	Channel       string
	TrustedSource string
}

// Context is the short name used by service ports.
type Context = TenantContext

// ExecutionBinding freezes the control-plane versions accepted by
// PrepareDispatch.
type ExecutionBinding struct {
	AgentAppVersion    int64
	AgentAppRevision   int64
	AgentContentDigest string
	ConfigVersion      int64
	PolicyVersion      int64
	// ExecutionBudget comes from the exact published revision accepted by this
	// binding. PrepareDispatch copies it only into the Envelope, the single
	// cross-process execution contract.
	ExecutionBudget runtime.ExecutionBudget
}

var (
	ErrUntrustedSource = errors.New("tenant context has no trusted source")
	ErrInvalidContext  = errors.New("invalid tenant context")
	ErrInvalidBinding  = errors.New("invalid execution binding")
)

// Validate rejects incomplete identities at every external boundary.
func (c TenantContext) Validate() error {
	if c.TrustedSource == "" {
		return ErrUntrustedSource
	}
	if c.TenantID == "" || c.TenantVersion < 1 || c.AgentAppID == "" ||
		c.SubjectID == "" || c.Channel == "" {
		return fmt.Errorf("%w: required identity field is empty", ErrInvalidContext)
	}
	return nil
}

// Validate rejects bindings that could resolve current/latest configuration.
func (b ExecutionBinding) Validate() error {
	if b.AgentAppVersion < 1 || b.AgentAppRevision < 1 ||
		b.AgentContentDigest == "" || b.ConfigVersion < 1 || b.PolicyVersion < 1 {
		return fmt.Errorf("%w: all versions and the digest are required", ErrInvalidBinding)
	}
	if err := b.ExecutionBudget.Validate(); err != nil {
		return fmt.Errorf("%w: execution budget is invalid", ErrInvalidBinding)
	}
	return nil
}
