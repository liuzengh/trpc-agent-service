package webui

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Violet2314/trpc-agent-service/trpcservice/reply"
)

func TestRedisHubCrossNodePublishSubscribe(t *testing.T) {
	server := miniredis.RunT(t)
	clientA := redis.NewClient(&redis.Options{Addr: server.Addr()})
	clientB := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = clientA.Close()
		_ = clientB.Close()
	})
	hubA, err := NewRedisHub(clientA)
	if err != nil {
		t.Fatalf("NewRedisHub(A) error = %v", err)
	}
	hubB, err := NewRedisHub(clientB)
	if err != nil {
		t.Fatalf("NewRedisHub(B) error = %v", err)
	}
	if err := hubA.Ensure("session-a", "owner-a"); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	events, cancel, err := hubB.Subscribe("session-a", "owner-a")
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer cancel()
	if err := hubA.Publish(
		"session-a", "owner-a", reply.Event{Type: "text_delta", Text: "cross-node"},
	); err != nil {
		t.Fatalf("Publish(text) error = %v", err)
	}
	if err := hubA.Publish(
		"session-a", "owner-a", reply.Event{Type: "done"},
	); err != nil {
		t.Fatalf("Publish(done) error = %v", err)
	}
	var received []reply.Event
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	for {
		select {
		case event, ok := <-events:
			if !ok {
				if len(received) != 2 || received[0].Text != "cross-node" {
					t.Fatalf("received events = %#v", received)
				}
				return
			}
			received = append(received, event)
		case <-timeout.C:
			t.Fatalf("timed out waiting for events: %#v", received)
		}
	}
}

func TestRedisHubOwnerIsolation(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	hub, err := NewRedisHub(client)
	if err != nil {
		t.Fatalf("NewRedisHub() error = %v", err)
	}
	if err := hub.Ensure("session-a", "owner-a"); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if err := hub.Ensure("session-a", "owner-b"); err != errStreamForbidden {
		t.Fatalf("Ensure(other owner) error = %v", err)
	}
}
