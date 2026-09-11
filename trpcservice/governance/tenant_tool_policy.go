package governance

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// TenantToolGrantLookup resolves the current tenant grant for one
// platform-managed tool. It is evaluated at invocation time so revocation does
// not depend on republishing an application configuration.
type TenantToolGrantLookup func(context.Context, string, string) (bool, error)

// TenantToolPolicy composes the platform-wide tool safety policy with the
// current tenant grant for platform-managed tools. Framework-injected and
// tenant-defined HTTP/MCP tools remain governed by the base policy only.
type TenantToolPolicy struct {
	base          ToolPolicy
	platformTools map[string]struct{}
	lookup        TenantToolGrantLookup
}

func NewTenantToolPolicy(base ToolPolicy, platformTools []string, lookup TenantToolGrantLookup) (*TenantToolPolicy, error) {
	if base == nil {
		return nil, errors.New("base tool policy is required")
	}
	if lookup == nil {
		return nil, errors.New("tenant tool grant lookup is required")
	}
	managed := make(map[string]struct{}, len(platformTools))
	for _, name := range platformTools {
		name = strings.TrimSpace(name)
		if name != "" {
			managed[name] = struct{}{}
		}
	}
	return &TenantToolPolicy{base: base, platformTools: managed, lookup: lookup}, nil
}

func (p *TenantToolPolicy) Authorize(ctx context.Context, execution ExecutionContext, request ToolRequest) error {
	if err := p.base.Authorize(ctx, execution, request); err != nil {
		return err
	}
	if _, managed := p.platformTools[request.Name]; !managed {
		return nil
	}
	granted, err := p.lookup(ctx, execution.TenantID, request.Name)
	if err != nil {
		return errors.Join(fmt.Errorf("%w: tenant tool policy lookup failed", ErrToolDenied), err)
	}
	if !granted {
		return fmt.Errorf("%w: tool %q is not authorized for tenant %q", ErrToolDenied, request.Name, execution.TenantID)
	}
	return nil
}

var _ ToolPolicy = (*TenantToolPolicy)(nil)
