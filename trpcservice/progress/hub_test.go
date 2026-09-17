package progress

import "testing"

func TestHubDropsOnlySlowSubscriberUpdates(t *testing.T) {
	hub := NewHub()
	slow, cancelSlow := hub.Subscribe("tenant", 1)
	defer cancelSlow()
	fast, cancelFast := hub.Subscribe("tenant", 2)
	defer cancelFast()
	hub.TryPublish(Event{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", Sequence: 1, Kind: RunStarted})
	<-fast
	hub.TryPublish(Event{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", Sequence: 2, Kind: MessageDelta, Content: "a"})
	if got := <-fast; got.Sequence != 2 {
		t.Fatalf("fast=%#v", got)
	}
	if got := <-slow; got.Sequence != 1 {
		t.Fatalf("slow first=%#v", got)
	}
	select {
	case unexpected := <-slow:
		t.Fatalf("slow received overflow=%#v", unexpected)
	default:
	}
}

func TestHubScopesSubscribersToTenant(t *testing.T) {
	hub := NewHub()
	stream, cancel := hub.Subscribe("tenant-a", 1)
	defer cancel()
	hub.TryPublish(Event{SchemaVersion: 1, TenantID: "tenant-b", RequestID: "request", Sequence: 1, Kind: RunStarted})
	select {
	case unexpected := <-stream:
		t.Fatalf("cross-tenant event=%#v", unexpected)
	default:
	}
}
