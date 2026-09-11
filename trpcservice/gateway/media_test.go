package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/routing"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type mediaAdapter struct {
	callbackTestAdapter
	kind   string
	edited bool
}

func TestCallbackRateLimitIncludesControlMessagesWithoutDoubleCounting(t *testing.T) {
	for _, mode := range []string{"text", "media", "approval"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			data := controlplane.DefaultBootstrapData()
			data.Tenants[0].QuotaConfig = json.RawMessage(`{"requests_per_minute":1}`)
			repo := controlplane.NewMemoryRepository(data)
			defer func(closer interface{ Close() error }) { _ = closer.Close() }(repo)
			guard, _ := tenant.NewGuard(ctx, repo, config.QuotaConfig{Backend: config.QuotaBackendLocal})
			defer func(closer interface{ Close() error }) { _ = closer.Close() }(guard)
			journal := NewMemoryJournal()
			defer func(closer interface{ Close() error }) { _ = closer.Close() }(journal)
			resolver, _ := routing.NewControlPlaneResolver(repo)
			intake, _ := NewIntake(resolver, journal, WithQuotaGuard(guard))
			kind := "text"
			if mode == "media" {
				kind = "file"
			}
			registry, _ := channels.NewRegistry(mediaAdapter{kind: kind})
			var options []CallbackGatewayOption
			if mode == "approval" {
				options = append(options, WithApprovalDecisionHandler(&approvalDecisionTestHandler{}))
			}
			gateway, _ := NewCallbackGateway(repo, registry, intake, options...)
			if _, err := gateway.Handle(ctx, "http", "tutorial-http", httptest.NewRequest(http.MethodPost, "/callback", nil)); err != nil {
				t.Fatalf("first message counted twice: %v", err)
			}
			if _, err := gateway.Handle(ctx, "http", "tutorial-http", httptest.NewRequest(http.MethodPost, "/callback", nil)); !errors.Is(err, tenant.ErrRateLimited) {
				t.Fatalf("control quota bypass: %v", err)
			}
		})
	}
}

func (a mediaAdapter) Callback(ctx context.Context, binding controlplane.ChannelBinding, r *http.Request) (channels.CallbackResult, error) {
	result, err := a.callbackTestAdapter.Callback(ctx, binding, r)
	result.Messages[0].MessageType = a.kind
	result.Messages[0].Edited = a.edited
	result.Messages[0].Text = "批准 apr_00000000000000000000000000000000 attachment-file-canary"
	return result, err
}

func TestMediaAndEditsDoNotReachApprovalOrAgent(t *testing.T) {
	for _, kind := range []string{"image", "file", "voice", "video", "text"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			repo := controlplane.NewMemoryRepository(controlplane.DefaultBootstrapData())
			defer func(closer interface{ Close() error }) { _ = closer.Close() }(repo)
			journal := NewMemoryJournal()
			defer func(closer interface{ Close() error }) { _ = closer.Close() }(journal)
			resolver, _ := routing.NewControlPlaneResolver(repo)
			intake, _ := NewIntake(resolver, journal)
			registry, _ := channels.NewRegistry(mediaAdapter{kind: kind, edited: kind == "text"})
			approvals := &approvalDecisionTestHandler{}
			gateway, _ := NewCallbackGateway(repo, registry, intake, WithApprovalDecisionHandler(approvals))
			for i := 0; i < 2; i++ {
				if _, err := gateway.Handle(ctx, "http", "tutorial-http", httptest.NewRequest(http.MethodPost, "/callback", nil)); err != nil {
					t.Fatal(err)
				}
			}
			if approvals.calls != 0 || len(journal.Tasks()) != 0 {
				t.Fatal("media/edit was treated as a tool approval or Agent input")
			}
			items, err := journal.ClaimOutbound(ctx, "sender", 10, time.Minute)
			if err != nil || len(items) != 1 || !strings.HasPrefix(items[0].Text, "平台提示") || strings.Contains(items[0].Text, "canary") {
				t.Fatalf("media reply=%+v %v", items, err)
			}
		})
	}
}

func TestEnabledAttachmentIsQueuedWithoutParsingCaptionAsApproval(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	binding := data.ChannelBindings[0]
	binding.ChannelType = "telegram"
	binding.Config = json.RawMessage(`{"attachments_enabled":true}`)
	binding.Version = 3
	data.ChannelBindings[0] = binding
	repo := controlplane.NewMemoryRepository(data)
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(repo)
	journal := NewMemoryJournal()
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(journal)
	resolver, _ := routing.NewControlPlaneResolver(repo)
	intake, _ := NewIntake(resolver, journal)
	approvals := &approvalDecisionTestHandler{}
	registry, _ := channels.NewRegistry(callbackTestAdapter{})
	g, _ := NewCallbackGateway(repo, registry, intake, WithApprovalDecisionHandler(approvals))
	message := channels.InboundEnvelope{OccurredAt: time.Now(), ExternalMessageID: "attachment-id", ExternalUserID: "user", ExternalChatID: "chat", ChatType: "direct", MessageType: "file", Text: "批准 apr_00000000000000000000000000000000", Media: &channels.MediaReference{FileID: "provider-file"}, ReplyTarget: "chat"}
	for i := 0; i < 2; i++ {
		if err := g.acceptVerifiedMessage(context.Background(), binding, message); err != nil {
			t.Fatal(err)
		}
	}
	tasks := journal.Tasks()
	if approvals.calls != 0 || len(tasks) != 1 || tasks[0].Media == nil || tasks[0].Media.BindingVersion != 3 {
		t.Fatal("attachment crossed approval/queue boundary")
	}
	message.Media = &channels.MediaReference{FileID: "different"}
	if err := g.acceptVerifiedMessage(context.Background(), binding, message); !errors.Is(err, ErrMessageConflict) {
		t.Fatal("changed attachment replay accepted")
	}
}
