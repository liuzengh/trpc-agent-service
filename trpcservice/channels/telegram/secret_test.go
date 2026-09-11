package telegram

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

func TestTelegramSecretScopeAndPurpose(t *testing.T) {
	t.Setenv("TG_TEST_WEBHOOK", "webhook-test-value")
	store, _ := secret.NewEnvStore([]secret.Grant{{TenantID: "tenant", Purpose: secret.TelegramWebhook, Reference: "env://TG_TEST_WEBHOOK"}})
	adapter, _ := New(store, nil)
	binding := testBindingWithConfig(t, bindingConfig{BotTokenRef: "env://TG_TEST_WEBHOOK", WebhookSecretRef: "env://TG_TEST_WEBHOOK"})
	req := httptest.NewRequest(http.MethodPost, "/callback", strings.NewReader(`{}`))
	req.Header.Set("X-Telegram-Bot-Api-Secret-Token", "webhook-test-value")
	if _, err := adapter.Callback(context.Background(), binding, req); err != nil {
		t.Fatalf("allowed webhook: %v", err)
	}
	if _, err := adapter.Send(context.Background(), binding, channels.OutboundMessage{}); !errors.Is(err, secret.ErrForbidden) {
		t.Fatalf("webhook secret used for send: %v", err)
	}
	binding.TenantID = "foreign-tenant"
	if _, err := adapter.Callback(context.Background(), binding, req); !errors.Is(err, secret.ErrForbidden) {
		t.Fatalf("cross tenant webhook: %v", err)
	}
}
