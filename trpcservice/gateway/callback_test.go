package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/routing"
)

type callbackTestAdapter struct{}

type approvalDecisionTestHandler struct{ calls int }

func (h *approvalDecisionTestHandler) HandleApprovalDecision(
	context.Context,
	ApprovalDecisionInput,
) (bool, error) {
	h.calls++
	return true, nil
}

func (callbackTestAdapter) Type() string { return "http" }
func (callbackTestAdapter) Capabilities() channels.Capabilities {
	return channels.Capabilities{MaxTextRunes: 8000}
}

func TestCallbackGatewayRoutesApprovalBeforeNormalIntake(t *testing.T) {
	repository := controlplane.NewMemoryRepository(controlplane.DefaultBootstrapData())
	journal := NewMemoryJournal()
	t.Cleanup(func() {
		_ = journal.Close()
		_ = repository.Close()
	})
	resolver, _ := routing.NewControlPlaneResolver(repository)
	intake, _ := NewIntake(resolver, journal)
	registry, _ := channels.NewRegistry(callbackTestAdapter{})
	approvals := &approvalDecisionTestHandler{}
	callbackGateway, err := NewCallbackGateway(
		repository, registry, intake, WithApprovalDecisionHandler(approvals),
	)
	if err != nil {
		t.Fatalf("new callback Gateway: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, "/callbacks/http/tutorial-http", nil)
	if _, err := callbackGateway.Handle(
		context.Background(), "http", "tutorial-http", request,
	); err != nil {
		t.Fatalf("handle callback: %v", err)
	}
	if approvals.calls != 1 || len(journal.Tasks()) != 0 {
		t.Fatalf("approval calls=%d tasks=%+v", approvals.calls, journal.Tasks())
	}
}
func (callbackTestAdapter) Send(
	context.Context,
	controlplane.ChannelBinding,
	channels.OutboundMessage,
) (channels.DeliveryReceipt, error) {
	return channels.DeliveryReceipt{}, nil
}
func (callbackTestAdapter) Callback(
	context.Context,
	controlplane.ChannelBinding,
	*http.Request,
) (channels.CallbackResult, error) {
	return channels.CallbackResult{
		StatusCode: http.StatusOK,
		Body:       []byte("ack"),
		Messages: []channels.InboundEnvelope{{
			ExternalMessageID: "external-1",
			ExternalUserID:    "raw-user",
			ExternalChatID:    "raw-chat",
			ChatType:          "direct",
			MessageType:       "text",
			Text:              "hello",
			ReplyTarget:       "raw-user",
			OccurredAt:        time.Now(),
		}},
	}, nil
}

func TestCallbackGatewayPersistsBeforeAck(t *testing.T) {
	repository := controlplane.NewMemoryRepository(controlplane.DefaultBootstrapData())
	journal := NewMemoryJournal()
	t.Cleanup(func() {
		_ = journal.Close()
		_ = repository.Close()
	})
	resolver, _ := routing.NewControlPlaneResolver(repository)
	intake, _ := NewIntake(resolver, journal)
	registry, _ := channels.NewRegistry(callbackTestAdapter{})
	callbackGateway, err := NewCallbackGateway(repository, registry, intake)
	if err != nil {
		t.Fatalf("new callback Gateway: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, "/callbacks/http/tutorial-http", nil)
	result, err := callbackGateway.Handle(
		context.Background(), "http", "tutorial-http", request,
	)
	if err != nil || string(result.Body) != "ack" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	tasks := journal.Tasks()
	if len(tasks) != 1 || tasks[0].UserID == "raw-user" ||
		tasks[0].SessionID == "raw-chat" || tasks[0].ReplyTarget != "raw-user" {
		t.Fatalf("tasks=%+v", tasks)
	}
}
