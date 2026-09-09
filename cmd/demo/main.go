package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
)

type event struct {
	Step   string `json:"step"`
	Result string `json:"result"`
}

func main() {
	ctx := context.Background()
	store := reliable.NewMemoryStore()
	events := make([]event, 0, 8)

	msg := &reliable.InboxMessage{
		TenantID:          "tenant-demo-a",
		ChannelType:       "local-demo",
		ChannelAccountID:  "account-demo",
		AgentApp:          "support-agent",
		ExternalMessageID: "demo-message-1",
		ConversationID:    "conversation-demo",
		ReplyToID:         "reply-demo-1",
		UserID:            "user-demo",
		SessionID:         "session-demo",
		PayloadHash:       strings.Repeat("a", 64),
		Payload:           []byte(`{"content":"故障演示"}`),
	}
	inserted, err := store.EnqueueInbox(ctx, msg)
	if err != nil || !inserted {
		fatal("enqueue", err)
	}
	events = append(events, event{"durable_inbox", fmt.Sprintf("accepted id=%d sequence=%d", msg.ID, msg.SessionSequence)})

	first, err := store.ClaimInbox(ctx, "worker-old", time.Millisecond)
	if err != nil {
		fatal("first claim", err)
	}
	time.Sleep(3 * time.Millisecond)
	second, err := store.ClaimInbox(ctx, "worker-new", 100*time.Millisecond)
	if err != nil {
		fatal("lease takeover", err)
	}
	if _, err := store.CompleteInbox(ctx, first.ID, first.Lease, reliable.OutboxReply{ContentType: "text", Content: "stale"}); !errors.Is(err, reliable.ErrStaleLease) {
		fatal("stale fence rejection", fmt.Errorf("got %v", err))
	}
	events = append(events, event{"stale_fence", "old worker rejected after lease takeover"})
	outbox, err := store.CompleteInbox(ctx, second.ID, second.Lease, reliable.OutboxReply{ContentType: "text", Content: "recovered"})
	if err != nil {
		fatal("complete inbox", err)
	}
	events = append(events, event{"atomic_outbox", fmt.Sprintf("created outbox id=%d", outbox.ID)})

	delivery, err := store.ClaimOutbox(ctx, "delivery-1", time.Millisecond)
	if err != nil {
		fatal("claim outbox", err)
	}
	if err := store.MarkDispatchStarted(ctx, delivery.ID, delivery.Lease); err != nil {
		fatal("dispatch fence", err)
	}
	time.Sleep(3 * time.Millisecond)
	result, err := store.ReapExpired(ctx, 10)
	if err != nil {
		fatal("reap", err)
	}
	if result.OutboxDispatchUnknown != 1 {
		fatal("reconciliation", fmt.Errorf("unknown=%d", result.OutboxDispatchUnknown))
	}
	events = append(events, event{"reconciliation", "provider outcome unknown; automatic resend blocked"})

	enc := json.NewEncoder(os.Stdout)
	for _, item := range events {
		if err := enc.Encode(item); err != nil {
			fatal("encode", err)
		}
	}
}

func fatal(step string, err error) {
	if err == nil {
		err = errors.New("unexpected failure")
	}
	fmt.Fprintf(os.Stderr, "demo %s failed: %v\n", step, err)
	os.Exit(1)
}
