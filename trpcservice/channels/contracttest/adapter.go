// Package contracttest defines provider-neutral Channel Adapter acceptance.
package contracttest

import (
	"context"
	"errors"
	"testing"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/delivery"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	messagingmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/inmemory"
)

type DeliveryObservation struct {
	Calls           int
	ClientRequestID string
	Content         []byte
	Target          channel.DeliveryTarget
}

type DeliveryHarness struct {
	Adapter channel.Adapter
	Event   channel.ReplyEvent
	Result  messaging.ResultRecord
	Observe func() DeliveryObservation
}

type DeliveryFactory func(testing.TB) DeliveryHarness

type adapterResolver struct{ adapter channel.Adapter }

func (r adapterResolver) ResolveAdapter(context.Context, string, string) (channel.Adapter, error) {
	return r.adapter, nil
}

// RunDelivery proves that every provider participates in the same durable
// Delivery Ledger boundary. Replaying a sent reply must not call the provider
// twice, and the provider receives only the frozen target/content plus the
// Ledger-derived stable client request ID.
func RunDelivery(t *testing.T, factory DeliveryFactory) {
	t.Helper()
	t.Run("ledger_replay_has_one_provider_effect", func(t *testing.T) {
		harness := factory(t)
		if harness.Adapter == nil || harness.Adapter.ID() == "" || !harness.Adapter.Capabilities().Text || harness.Observe == nil {
			t.Fatal("invalid adapter delivery harness")
		}
		store := messagingmemory.New()
		if err := store.PutResult(context.Background(), harness.Result); err != nil {
			t.Fatal(err)
		}
		service := delivery.Service{Results: store, Ledger: store, Adapters: adapterResolver{adapter: harness.Adapter}, Owner: "contract-owner"}
		if err := service.Deliver(context.Background(), harness.Event); err != nil {
			t.Fatal(err)
		}
		if err := service.Deliver(context.Background(), harness.Event); err != nil {
			t.Fatal(err)
		}
		observation := harness.Observe()
		if observation.Calls != 1 || observation.ClientRequestID == "" || string(observation.Content) != string(harness.Result.Content) || observation.Target != harness.Event.Target {
			t.Fatalf("observation=%#v", observation)
		}
		key := messaging.DeliveryKey{TenantID: harness.Event.TenantID, DeliveryKey: harness.Event.DeliveryKey, SegmentNo: 0}
		record, err := store.GetDelivery(context.Background(), key)
		if err != nil || record.State != messaging.DeliverySent || record.ProviderMessageID == "" || record.ClientRequestID != observation.ClientRequestID {
			t.Fatalf("record=%#v err=%v", record, err)
		}
	})

	t.Run("stale_result_reference_fails_before_provider", func(t *testing.T) {
		harness := factory(t)
		store := messagingmemory.New()
		if err := store.PutResult(context.Background(), harness.Result); err != nil {
			t.Fatal(err)
		}
		harness.Event.ContentRef += "-stale"
		service := delivery.Service{Results: store, Ledger: store, Adapters: adapterResolver{adapter: harness.Adapter}, Owner: "contract-owner"}
		if err := service.Deliver(context.Background(), harness.Event); !errors.Is(err, runtime.ErrVersionMismatch) {
			t.Fatalf("error=%v", err)
		}
		if observation := harness.Observe(); observation.Calls != 0 {
			t.Fatalf("provider calls=%d", observation.Calls)
		}
	})
}
