package config

import (
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// TenantBundle is the atomic unit validated before a tenant configuration is published.
type TenantBundle struct {
	Tenant   tenant.Tenant           `json:"tenant"`
	Agents   []tenant.AgentApp       `json:"agents"`
	Bindings []tenant.ChannelBinding `json:"bindings"`
}

func (b TenantBundle) Validate() error {
	if err := b.Tenant.Validate(); err != nil {
		return fmt.Errorf("tenant: %w", err)
	}
	if len(b.Agents) == 0 {
		return errors.New("at least one agent is required")
	}
	defaultFound := false
	for i, agent := range b.Agents {
		if err := agent.Validate(); err != nil {
			return fmt.Errorf("agent %d: %w", i, err)
		}
		if agent.TenantID != b.Tenant.ID {
			return fmt.Errorf("agent %q belongs to a different tenant", agent.ID)
		}
		if agent.ID == b.Tenant.DefaultAgentID {
			defaultFound = true
		}
	}
	if !defaultFound {
		return errors.New("default_agent_id does not reference an agent in the bundle")
	}
	for i, binding := range b.Bindings {
		if err := binding.Validate(); err != nil {
			return fmt.Errorf("binding %d: %w", i, err)
		}
		if binding.TenantID != b.Tenant.ID {
			return fmt.Errorf("binding %q belongs to a different tenant", binding.ID)
		}
	}
	return nil
}
