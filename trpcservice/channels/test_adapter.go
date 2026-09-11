package channels

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

// TestAdapter records deliveries locally. It is the outbound half of the HTTP
// Test Channel and does not perform network calls.
type TestAdapter struct {
	mu         sync.Mutex
	deliveries []TestDelivery
}

type TestDelivery struct {
	Binding controlplane.ChannelBinding
	Message OutboundMessage
	Receipt DeliveryReceipt
}

func NewTestAdapter() *TestAdapter { return &TestAdapter{} }

func (a *TestAdapter) Type() string { return "http" }

func (a *TestAdapter) Send(
	ctx context.Context,
	binding controlplane.ChannelBinding,
	message OutboundMessage,
) (DeliveryReceipt, error) {
	if err := ctx.Err(); err != nil {
		return DeliveryReceipt{}, context.Cause(ctx)
	}
	if binding.ChannelType != a.Type() || binding.Status != controlplane.StatusActive {
		return DeliveryReceipt{}, fmt.Errorf("HTTP test binding is unavailable")
	}
	receipt := DeliveryReceipt{
		ProviderMessageID: "http-test-" + message.OutboundID,
		SentAt:            time.Now().UTC(),
	}
	a.mu.Lock()
	a.deliveries = append(a.deliveries, TestDelivery{
		Binding: binding,
		Message: message,
		Receipt: receipt,
	})
	a.mu.Unlock()
	return receipt, nil
}

func (a *TestAdapter) Capabilities() Capabilities {
	return Capabilities{MaxTextRunes: 8000}
}

func (a *TestAdapter) Deliveries() []TestDelivery {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]TestDelivery(nil), a.deliveries...)
}

var _ Adapter = (*TestAdapter)(nil)
