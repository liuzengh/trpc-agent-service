package relay

import (
	"context"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type TenantControlState struct {
	TenantID, Kind, AggregateID, Status string
	Version                             uint64
}

type TenantControlReader interface {
	ReadTenantControl(context.Context, TenantControlEvent) (TenantControlState, error)
}

type TenantControlSink interface {
	ApplyTenantControl(context.Context, TenantControlState) error
}

// MonotonicTenantControlConsumer treats control delivery as an acceleration
// hint and reloads authoritative state before applying it. Per node, an older
// event can never roll a cache or cancellation watermark backwards.
type MonotonicTenantControlConsumer struct {
	Reader TenantControlReader
	Sink   TenantControlSink

	mu         sync.Mutex
	watermarks map[string]uint64
}

func (c *MonotonicTenantControlConsumer) Consume(ctx context.Context, event TenantControlEvent) error {
	if c.Reader == nil || c.Sink == nil || event.TenantID == "" || event.AggregateID == "" || event.Version < 1 {
		return runtime.ErrInvariantViolation
	}
	state, err := c.Reader.ReadTenantControl(ctx, event)
	if err != nil {
		return err
	}
	if state.TenantID != event.TenantID || state.Kind != event.Kind || state.AggregateID != event.AggregateID || state.Version < event.Version {
		return runtime.ErrVersionMismatch
	}
	key := state.TenantID + "\x00" + state.Kind + "\x00" + state.AggregateID
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.watermarks == nil {
		c.watermarks = make(map[string]uint64)
	}
	if c.watermarks[key] >= state.Version {
		return nil
	}
	if err := c.Sink.ApplyTenantControl(ctx, state); err != nil {
		return err
	}
	c.watermarks[key] = state.Version
	return nil
}

func (c *MonotonicTenantControlConsumer) Watermark(tenantID, kind, aggregateID string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.watermarks[tenantID+"\x00"+kind+"\x00"+aggregateID]
}
