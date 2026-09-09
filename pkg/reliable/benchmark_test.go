package reliable

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// BenchmarkMemoryStoreInboxOutbox measures the deterministic state-machine
// path used by local contract tests. It is intentionally provider-free: the
// result is a reproducible queue baseline, not a production capacity claim.
func BenchmarkMemoryStoreInboxOutbox(b *testing.B) {
	ctx := context.Background()
	store := NewMemoryStore()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := &InboxMessage{
			TenantID:          "tenant-bench",
			ChannelType:       "local",
			ChannelAccountID:  "account-bench",
			AgentApp:          "assistant",
			ExternalMessageID: fmt.Sprintf("bench-%d", i),
			ConversationID:    fmt.Sprintf("conversation-%d", i),
			ReplyToID:         fmt.Sprintf("reply-%d", i),
			UserID:            "user-bench",
			SessionID:         fmt.Sprintf("session-%d", i),
			PayloadHash:       strings.Repeat("a", 64),
			Payload:           []byte(`{"content":"benchmark"}`),
		}
		inserted, err := store.EnqueueInbox(ctx, msg)
		if err != nil || !inserted {
			b.Fatalf("enqueue: inserted=%v err=%v", inserted, err)
		}
		claimed, err := store.ClaimInbox(ctx, "bench-owner", time.Second)
		if err != nil {
			b.Fatalf("claim: %v", err)
		}
		if _, err := store.CompleteInbox(ctx, claimed.ID, claimed.Lease, OutboxReply{
			ContentType: "text",
			Content:     "benchmark-ok",
		}); err != nil {
			b.Fatalf("complete: %v", err)
		}
	}
}
