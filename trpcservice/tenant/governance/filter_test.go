package governance

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"

	"trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestInboundFilterEnforcesMonthlyCostReservation(t *testing.T) {
	filter := NewFilter()
	filter.now = func() time.Time { return time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC) }
	tenant := config.TenantConfig{
		TenantID: "tenant-a",
		Model:    config.ModelConfig{MaxTokens: 1000, OutputPrice: 10},
		Budget:   config.BudgetPolicy{RequestsPerMinute: 10, MaxInputChars: 1000, MonthlyCostUSD: 0.1},
	}
	msg := domain.InboundMessage{ExternalUserID: "u", Text: "hello"}
	if err := filter.CheckInbound(tenant, config.ChannelConfig{}, msg); err != nil {
		t.Fatalf("first reservation failed: %v", err)
	}
	if err := filter.CheckInbound(tenant, config.ChannelConfig{}, msg); err != ErrBudgetExceeded {
		t.Fatalf("expected budget rejection, got %v", err)
	}
}

func TestInboundFilterCountsAttachmentMetadataInInputBudget(t *testing.T) {
	filter := NewFilter()
	tenant := config.TenantConfig{
		TenantID: "tenant-a",
		Model:    config.ModelConfig{MaxTokens: 10},
		Budget:   config.BudgetPolicy{RequestsPerMinute: 10, MaxInputChars: 20},
	}
	msg := domain.InboundMessage{
		TenantID: "tenant-a", ExternalUserID: "user", Text: "ok",
		Attachments: []domain.Attachment{{Type: "file", Name: "a-long-file-name.pdf"}},
	}
	if err := filter.CheckInbound(tenant, config.ChannelConfig{}, msg); !errors.Is(err, ErrInputTooLarge) {
		t.Fatalf("attachment metadata must count toward input budget, got %v", err)
	}
}

func TestInboundFilterBoundsAttachmentCountAndMetadata(t *testing.T) {
	filter := NewFilter()
	tenant := config.TenantConfig{
		TenantID: "tenant-a",
		Model:    config.ModelConfig{MaxTokens: 10},
		Budget:   config.BudgetPolicy{RequestsPerMinute: 10, MaxInputChars: 10000},
	}
	tooMany := domain.InboundMessage{TenantID: "tenant-a", ExternalUserID: "user", Attachments: make([]domain.Attachment, maxAttachmentCount+1)}
	if err := filter.CheckInbound(tenant, config.ChannelConfig{}, tooMany); !errors.Is(err, ErrInputTooLarge) {
		t.Fatalf("attachment count must be bounded, got %v", err)
	}
	longName := domain.InboundMessage{
		TenantID: "tenant-a", ExternalUserID: "user",
		Attachments: []domain.Attachment{{Type: "file", Name: strings.Repeat("名", maxAttachmentNameRunes+1)}},
	}
	if err := filter.CheckInbound(tenant, config.ChannelConfig{}, longName); !errors.Is(err, ErrInputTooLarge) {
		t.Fatalf("attachment name must be bounded, got %v", err)
	}
}

func TestPermissionPolicyRequiresArgumentBoundOneTimeApproval(t *testing.T) {
	store := NewApprovalStore()
	store.now = func() time.Time { return time.Unix(100, 0) }
	policy := config.ToolPolicy{Allow: []string{"danger"}, RequireConfirm: []string{"danger"}}
	check := PermissionPolicy(policy, store)
	rc := RequestContext{TenantID: "t", ConfigVersion: "v1", UserID: "u", SessionID: "s"}
	req := &tool.PermissionRequest{ToolName: "danger", Arguments: []byte(`{"target":"a"}`)}
	decision, err := check(contextWithRequest(context.Background(), rc), req)
	if err != nil || decision.Action != tool.PermissionActionAsk {
		t.Fatalf("initial decision = %+v, %v", decision, err)
	}
	nonce := strings.TrimPrefix(decision.Reason[strings.Index(decision.Reason, "#approve:"):], "#approve:")
	rc.ApprovalNonce = nonce
	changed := &tool.PermissionRequest{ToolName: "danger", Arguments: []byte(`{"target":"b"}`)}
	decision, _ = check(contextWithRequest(context.Background(), rc), changed)
	if decision.Action != tool.PermissionActionAsk {
		t.Fatalf("approval must not authorize changed arguments: %+v", decision)
	}
	// Use the new approval for the changed arguments once.
	nonce = strings.TrimPrefix(decision.Reason[strings.Index(decision.Reason, "#approve:"):], "#approve:")
	rc.ApprovalNonce = nonce
	decision, _ = check(contextWithRequest(context.Background(), rc), changed)
	if decision.Action != tool.PermissionActionAllow {
		t.Fatalf("matching approval should allow: %+v", decision)
	}
	decision, _ = check(contextWithRequest(context.Background(), rc), changed)
	if decision.Action != tool.PermissionActionAsk {
		t.Fatalf("approval must be one-time: %+v", decision)
	}
}

func contextWithRequest(ctx context.Context, rc RequestContext) context.Context {
	return WithRequestContext(ctx, rc)
}

func TestExtractApproval(t *testing.T) {
	clean, nonce := ExtractApproval("please run #approve:abcdefabcdefabcdef now")
	if nonce != "abcdefabcdefabcdef" || clean != "please run now" {
		t.Fatalf("got clean=%q nonce=%q", clean, nonce)
	}
}

func TestApprovalStoreSweepsExpiredEntries(t *testing.T) {
	store := NewApprovalStore()
	now := time.Unix(100, 0)
	store.now = func() time.Time { return now }
	if _, err := store.Issue("expired", time.Second); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	if _, err := store.Issue("current", time.Minute); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.items) != 1 {
		t.Fatalf("expired approvals were not swept: %d entries", len(store.items))
	}
}
