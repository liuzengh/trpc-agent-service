package telegramruntime

import (
	"context"

	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
)

// Legacy operation types remain for settlement of pre-upgrade registration
// records. The dual-mode runtime uses a single physical receiver owner instead.
type Operation struct {
	ID, AccountID, InstanceID, InstanceEpoch string
	Revision, Epoch                          int64
}
type Registrations interface {
	State(context.Context, *c.Permit) (string, error)
	Acquire(context.Context, *c.Permit) (Operation, bool, error)
	Check(context.Context, *c.Permit, Operation) error
	BeginCall(context.Context, *c.Permit, Operation) error
	Finish(context.Context, *c.Permit, Operation, string) error
}
