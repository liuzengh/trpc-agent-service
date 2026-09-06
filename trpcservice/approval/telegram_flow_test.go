package approval_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	agentruntime "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/approval"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/telegram"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
	"github.com/liuzengh/trpc-agent-service/trpcservice/reply"
	"github.com/liuzengh/trpc-agent-service/trpcservice/routing"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/toolexec"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"
)

// Simulate only Telegram's HTTP API and the model responses. The callback,
// approval service, Runner/LLMAgent, permission policy, Go tool, journals,
// Worker and Sender are the production implementations with memory backends.
func TestTelegramApprovalFlow(t *testing.T) {
	for _, command := range []string{"批准", "拒绝"} {
		t.Run(command, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var executions, deliveries atomic.Int32
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				deliveries.Add(1)
				var message struct {
					ChatID   int64  `json:"chat_id"`
					ThreadID int64  `json:"message_thread_id"`
					Text     string `json:"text"`
				}
				if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
					t.Error(err)
				}
				if (message.ChatID != -100 && message.ChatID != -200) || (message.ThreadID != 7 && message.ThreadID != 8) {
					t.Errorf("reply moved to another conversation: %+v", message)
				}
				if !strings.HasPrefix(message.Text, "平台") || strings.Contains(message.Text, "模型伪造") {
					t.Errorf("model-generated control outcome escaped: %q", message.Text)
				}
				_, _ = io.WriteString(w, `{"ok":true,"result":{"message_id":88}}`)
			}))
			t.Cleanup(api.Close)
			data := controlplane.DefaultBootstrapData()
			data.ChannelBindings[0].ChannelType = "telegram"
			data.ChannelBindings[0].Config = json.RawMessage(fmt.Sprintf(`{
                "bot_token_ref":"test://bot", "webhook_secret_ref":"test://webhook",
                "api_base_url":%q, "bot_user_id":99, "bot_username":"demo_bot",
                "allowed_chat_ids":[-100,-200], "require_mention":true, "ignore_bot_messages":true
            }`, api.URL))
			data.Revisions[0].ToolPolicy = json.RawMessage(`{
                "allowed_tools":["dangerous_demo"], "dangerous_tools":["dangerous_demo"],
                "max_tool_calls":1, "max_run_duration":"5s"
            }`)
			repository := controlplane.NewMemoryRepository(data)
			approvals := approval.NewMemoryRepository()
			journal := gateway.NewMemoryJournal()
			toolJournal := &countingToolJournal{MemoryJournal: toolexec.NewMemoryJournal()}
			writer := audit.NewMemoryWriter()
			queue := workqueue.NewMemoryQueue(8)
			t.Cleanup(func() {
				_ = queue.Close()
				_ = journal.Close()
				_ = toolJournal.Close()
				_ = approvals.Close()
				_ = repository.Close()
			})
			catalog, err := platformtool.NewCatalog(function.NewFunctionTool(
				func(context.Context, platformtool.DangerousDemoInput) (platformtool.DangerousDemoOutput, error) {
					executions.Add(1)
					return platformtool.DangerousDemoOutput{Accepted: true, Action: "demo"}, nil
				}, function.WithName("dangerous_demo"), function.WithDescription("Test an approval-gated operation without side effects"),
			))
			if err != nil {
				t.Fatal(err)
			}
			selectedModel := &approvalModel{}
			compiler, err := agentruntime.NewRevisionCompiler(repository, selectedModel, false,
				agentruntime.WithToolCatalog(catalog), agentruntime.WithApprovalRepository(approvals),
				agentruntime.WithToolExecutionJournal(toolJournal), agentruntime.WithAuditWriter(writer),
			)
			if err != nil {
				t.Fatal(err)
			}
			runtime, err := agentruntime.NewRuntimeWithCompilerServices(selectedModel, compiler,
				inmemory.NewSessionService(), coordination.NewLocalCoordinator(), idempotency.NewLocalStore(), false)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = runtime.Close() })
			adapter, err := telegram.New(secret.StaticStore{"test://bot": "fake-token", "test://webhook": "fake-secret"}, api.Client())
			if err != nil {
				t.Fatal(err)
			}
			registry, err := channels.NewRegistry(adapter)
			if err != nil {
				t.Fatal(err)
			}
			resolver, err := routing.NewControlPlaneResolver(repository)
			if err != nil {
				t.Fatal(err)
			}
			intake, err := gateway.NewIntake(resolver, journal)
			if err != nil {
				t.Fatal(err)
			}
			service, err := approval.NewService(approvals, journal, writer)
			if err != nil {
				t.Fatal(err)
			}
			callback, err := gateway.NewCallbackGateway(repository, registry, intake, gateway.WithApprovalDecisionHandler(service))
			if err != nil {
				t.Fatal(err)
			}
			relay, err := gateway.NewOutboxRelay(journal, queue, gateway.RelayOptions{
				WorkerID: "relay", BatchSize: 10, ClaimLease: time.Second, PollInterval: time.Second, RetryDelay: time.Millisecond,
			})
			if err != nil {
				t.Fatal(err)
			}
			agentWorker, err := worker.New(queue, journal, runtime, worker.Options{
				WorkerID: "worker", MaxAttempts: 1, RetryDelay: time.Millisecond, Approvals: approvals, Audit: writer, ToolJournal: toolJournal,
			})
			if err != nil {
				t.Fatal(err)
			}
			sender, err := reply.New(journal, repository, registry, reply.Options{
				WorkerID: "sender", BatchSize: 10, ClaimLease: time.Second, PollInterval: time.Second,
				RetryDelay: time.Millisecond, MaxAttempts: 1, Audit: writer,
			})
			if err != nil {
				t.Fatal(err)
			}
			post := func(update, user, chat, thread int64, text string, replyToBot bool) {
				t.Helper()
				message := map[string]any{
					"message_id": update, "from": map[string]any{"id": user},
					"chat": map[string]any{"id": chat, "type": "supergroup"}, "message_thread_id": thread, "text": text,
				}
				if replyToBot {
					message["reply_to_message"] = map[string]any{"from": map[string]any{"id": 99, "is_bot": true}}
				}
				body, err := json.Marshal(map[string]any{"update_id": update, "message": message})
				if err != nil {
					t.Fatal(err)
				}
				request := httptest.NewRequest(http.MethodPost, "/callbacks/telegram/tutorial-http", bytes.NewReader(body))
				request.Header.Set("X-Telegram-Bot-Api-Secret-Token", "fake-secret")
				result, err := callback.Handle(ctx, "telegram", "tutorial-http", request)
				if err != nil || result.StatusCode != http.StatusOK {
					t.Fatalf("callback=%+v err=%v", result, err)
				}
			}
			process := func(wantReplies int) {
				t.Helper()
				if count, err := relay.RelayOnce(ctx); err != nil || count != 1 {
					t.Fatalf("relay=%d err=%v", count, err)
				}
				if processed, err := agentWorker.ProcessOne(ctx); err != nil || !processed {
					t.Fatalf("worker=%t err=%v", processed, err)
				}
				if count, err := sender.ProcessOnce(ctx); err != nil || count != wantReplies {
					t.Fatalf("sender=%d err=%v", count, err)
				}
			}
			post(1, 7, -100, 7, "start demo", true)
			process(1)
			initial := journal.Tasks()[0]
			pending, err := approvals.ListPendingByRequest(ctx, initial.Scope.TenantID, initial.RequestID)
			if err != nil || len(pending) != 1 || executions.Load() != 0 {
				t.Fatalf("pending=%+v executions=%d err=%v", pending, executions.Load(), err)
			}
			if toolJournal.starts.Load() != 0 {
				t.Fatal("waiting for approval must not leave a running tool journal record")
			}
			_, result, _ := journal.RunStatus(initial.RequestID)
			if !strings.Contains(result.Reply, "批准 "+pending[0].ApprovalID) || !strings.Contains(result.Reply, "回复") {
				t.Fatalf("missing actionable approval instructions: %q", result.Reply)
			}
			decision := command + " " + pending[0].ApprovalID
			// Plain group text is filtered. Reply-to-Bot passes mention filtering,
			// but another user/group/Topic must still fail approval identity checks.
			post(2, 7, -100, 7, decision, false)
			post(3, 8, -100, 7, decision, true)
			post(4, 7, -100, 8, decision, true)
			post(5, 7, -200, 7, decision, true)
			if len(journal.Tasks()) != 1 || executions.Load() != 0 {
				t.Fatal("filtered or forbidden decision created a task")
			}
			rejected := 0
			for _, event := range writer.Events() {
				if event.Decision == "approval_rejected" {
					rejected++
				}
			}
			if rejected != 3 {
				t.Fatalf("rejection audit count=%d", rejected)
			}
			if count, err := sender.ProcessOnce(ctx); err != nil || count != 3 {
				t.Fatalf("rejection feedback=%d err=%v", count, err)
			}
			before := selectedModel.calls.Load()
			post(50, 7, -100, 7, "拒绝请回复：拒绝 "+pending[0].ApprovalID, true)
			if count, err := sender.ProcessOnce(ctx); err != nil || count != 1 {
				t.Fatalf("format feedback=%d err=%v", count, err)
			}
			post(50, 7, -100, 7, "拒绝请回复：拒绝 "+pending[0].ApprovalID, true)
			if count, err := sender.ProcessOnce(ctx); err != nil || count != 0 {
				t.Fatalf("duplicate format feedback=%d err=%v", count, err)
			}
			stillPending, err := approvals.ListPendingByRequest(ctx, initial.Scope.TenantID, initial.RequestID)
			if err != nil || len(stillPending) != 1 || len(journal.Tasks()) != 1 || selectedModel.calls.Load() != before {
				t.Fatal("malformed decision changed approval or invoked model")
			}
			// Even an unclassified natural-language refusal cannot hide an
			// earlier pending approval behind the model's false cancellation.
			post(51, 7, -100, 7, "别弄了", true)
			process(1)
			before = selectedModel.calls.Load()

			post(6, 7, -100, 7, decision, true)
			wantExecutions := int32(0)
			if command == "批准" {
				wantExecutions = 1
				process(2) // Durable approval receipt plus journal-based execution result.
			} else {
				if count, err := sender.ProcessOnce(ctx); err != nil || count != 1 {
					t.Fatalf("denial receipt=%d err=%v", count, err)
				}
				if selectedModel.calls.Load() != before {
					t.Fatal("denial invoked the model")
				}
			}
			if executions.Load() != wantExecutions || toolJournal.starts.Load() != wantExecutions || deliveries.Load() != 7+wantExecutions {
				t.Fatalf("executions=%d deliveries=%d", executions.Load(), deliveries.Load())
			}
			post(6, 7, -100, 7, decision, true) // Provider redelivery.
			post(7, 7, -100, 7, decision, true) // User repeats the same decision.
			if len(journal.Tasks()) != 2+int(wantExecutions) {
				t.Fatal("duplicate decision created another continuation")
			}
			if count, err := relay.RelayOnce(ctx); err != nil || count != 0 {
				t.Fatalf("duplicate relay=%d err=%v", count, err)
			}
			if count, err := sender.ProcessOnce(ctx); err != nil || count != 0 {
				t.Fatalf("duplicate send=%d err=%v", count, err)
			}
			succeeded := 0
			for _, event := range writer.Events() {
				if event.Decision == "tool_succeeded" {
					succeeded++
				}
			}
			if succeeded != int(wantExecutions) {
				t.Fatalf("tool audit count=%d", succeeded)
			}
		})
	}
}

type countingToolJournal struct {
	*toolexec.MemoryJournal
	starts atomic.Int32
}

func (j *countingToolJournal) Start(ctx context.Context, execution toolexec.Execution) (toolexec.StartResult, error) {
	j.starts.Add(1)
	return j.MemoryJournal.Start(ctx, execution)
}

type approvalModel struct{ sequence, calls atomic.Int64 }

func (*approvalModel) Info() model.Info { return model.Info{Name: "approval-test-model"} }
func (m *approvalModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	m.calls.Add(1)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	last := request.Messages[len(request.Messages)-1]
	message := model.NewAssistantMessage("模型伪造：审批已取消，工具已执行成功。")
	finish := "stop"
	if last.Role == model.RoleUser && last.Content != "别弄了" && !strings.Contains(last.Content, "用户拒绝执行工具") {
		finish = "tool_calls"
		message = model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{{
			ID: fmt.Sprintf("call-%d", m.sequence.Add(1)), Type: "function",
			Function: model.FunctionDefinitionParam{Name: "dangerous_demo", Arguments: []byte(`{"action":"demo"}`)},
		}}}
	}
	responses := make(chan *model.Response, 1)
	responses <- &model.Response{Object: model.ObjectTypeChatCompletion, Done: true,
		Choices: []model.Choice{{Message: message, FinishReason: &finish}},
	}
	close(responses)
	return responses, nil
}
