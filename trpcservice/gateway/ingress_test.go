package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type ingressTestAdapter struct {
	incoming channels.Incoming
}

func (a ingressTestAdapter) Verify(*http.Request, []byte) error      { return nil }
func (a ingressTestAdapter) Parse([]byte) (channels.Incoming, error) { return a.incoming, nil }

type ingressTestResolver struct {
	context tenant.TenantContext
}

func (r ingressTestResolver) Resolve(context.Context, tenant.ResolveRequest) (tenant.TenantContext, error) {
	return r.context, nil
}

func ingressTestContext() tenant.TenantContext {
	return tenant.TenantContext{
		TenantID: "tenant-ingress", AgentAppID: "agent-ingress", BindingID: "binding-ingress", Channel: "telegram",
		ExternalUser: "user-ingress", ExternalChat: "chat-ingress", RequestID: "message-ingress", MessageID: "message-ingress", TraceID: "trace-ingress",
		ConfigVersion: 4, BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: "none", Object: "none"},
	}
}

func newIngressTest(t *testing.T) (*Ingress, *queue.FakeQueue) {
	t.Helper()
	jobQueue := queue.NewFakeQueue(queue.FakeQueueConfig{})
	gateway, err := New(jobQueue)
	if err != nil {
		t.Fatal(err)
	}
	claims := storage.NewFakeCoordinationStore()
	tc := ingressTestContext()
	ingress, err := NewIngress(IngressConfig{
		Resolver: ingressTestResolver{context: tc},
		Claims:   claims,
		Gateway:  gateway,
		ResolveAgent: func(context.Context, tenant.TenantContext) (agent.AgentSpec, error) {
			return agent.AgentSpec{TenantID: tc.TenantID, AgentAppID: tc.AgentAppID, Version: tc.ConfigVersion, Name: "assistant", ModelProvider: "fake"}, nil
		},
		Adapters: map[string]WebhookAdapter{"telegram": ingressTestAdapter{incoming: channels.Incoming{ID: "message-ingress", UserID: "user-ingress", ChatID: "chat-ingress", Text: "hello"}}},
		OwnerID:  "owner-ingress",
		Now:      func() time.Time { return time.Now().UTC() },
	})
	if err != nil {
		t.Fatal(err)
	}
	return ingress, jobQueue
}

func TestIngressDurablySubmitsOnceAndDeduplicates(t *testing.T) {
	ingress, jobQueue := newIngressTest(t)
	request := httptest.NewRequest(http.MethodPost, "/webhook/telegram/telegram-app", strings.NewReader(`{"update_id":1}`))
	first := ingress.Handle(context.Background(), "telegram", "telegram-app", request, []byte(`{"update_id":1}`))
	if first.Status != http.StatusAccepted || !strings.Contains(string(first.Body), `"accepted":true`) || strings.Contains(string(first.Body), `"duplicate":true`) {
		t.Fatalf("first ingress result = %+v", first)
	}
	secondRequest := httptest.NewRequest(http.MethodPost, "/webhook/telegram/telegram-app", strings.NewReader(`{"update_id":1}`))
	second := ingress.Handle(context.Background(), "telegram", "telegram-app", secondRequest, []byte(`{"update_id":1}`))
	if second.Status != http.StatusAccepted || !strings.Contains(string(second.Body), `"duplicate":true`) {
		t.Fatalf("duplicate ingress result = %+v", second)
	}
	delivery, err := jobQueue.Receive(context.Background(), time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if delivery.Job.Tenant.TenantID != "tenant-ingress" || delivery.Job.Message.Content != "hello" {
		t.Fatalf("unexpected queued job: %+v", delivery.Job)
	}
}

func TestIngressRejectsUnknownChannelBeforeResolution(t *testing.T) {
	ingress, _ := newIngressTest(t)
	request := httptest.NewRequest(http.MethodPost, "/webhook/wecom/app", strings.NewReader("{}"))
	result := ingress.Handle(context.Background(), "wecom", "app", request, []byte("{}"))
	if result.Status != http.StatusNotFound {
		t.Fatalf("unknown channel status = %d", result.Status)
	}
}
