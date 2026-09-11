package gateway_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	agentruntime "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecommcp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
	"github.com/liuzengh/trpc-agent-service/trpcservice/reply"
	"github.com/liuzengh/trpc-agent-service/trpcservice/routing"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	platformtrace "github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	agenttrace "trpc.group/trpc-go/trpc-agent-go/telemetry/trace"
)

type mcpFlowTransport func(*http.Request) (*http.Response, error)

func (f mcpFlowTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Only the external MCP endpoint and model are fakes. Real platform receiver,
// routing, Inbox/outbox, Runner/LLMAgent, Session, Worker and Sender are used.
func TestWeComMCPRunnerReplyFlow(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider, err := platformtrace.NewTracerProvider(exporter, config.TelemetryConfig{ServiceName: "wecom-mcp-offline-flow", SampleRatio: 1})
	if err != nil {
		t.Fatal(err)
	}
	oldProvider, oldPropagator := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	oldFramework, oldTracer := agenttrace.TracerProvider, agenttrace.Tracer
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	platformtrace.BindFrameworkTracing(provider)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(oldProvider)
		otel.SetTextMapPropagator(oldPropagator)
		agenttrace.TracerProvider, agenttrace.Tracer = oldFramework, oldTracer
		agenttrace.SetSpanAttributePolicy(agenttrace.SpanAttributePolicy{})
	})
	start := time.Now().UTC().Truncate(time.Second).Add(-time.Minute)
	data := controlplane.DefaultBootstrapData()
	b := &data.ChannelBindings[0]
	b.ChannelType = wecommcp.ChannelType
	b.SecretRef = "env://MCP_FLOW_TEST"
	cfg := wecommcp.BindingConfig{AllowedChatIDs: []string{"synthetic-group"}, AllowedUserIDs: []string{"synthetic-human"}, MentionPrefix: "@testbot", Timezone: "UTC", StartAt: start.Format(time.RFC3339), DedupeMode: "fingerprint-v1"}
	b.Config, _ = json.Marshal(cfg)
	repo := controlplane.NewMemoryRepository(data)
	journal := gateway.NewMemoryJournal()
	queue := workqueue.NewMemoryQueue(4)
	coord := coordination.NewLocalCoordinator()
	state := wecommcp.NewMemoryStore()
	writer := audit.NewMemoryWriter()
	var sends atomic.Int32
	client := &http.Client{Transport: mcpFlowTransport(func(req *http.Request) (*http.Response, error) {
		var rpc struct {
			ID     any    `json:"id"`
			Method string `json:"method"`
			Params struct {
				Name      string         `json:"name"`
				Arguments map[string]any `json:"arguments"`
			} `json:"params"`
		}
		if req.Method != "POST" || json.NewDecoder(req.Body).Decode(&rpc) != nil {
			return nil, errors.New("unexpected MCP transport")
		}
		var result any
		switch rpc.Method {
		case "initialize":
			result = map[string]any{"protocolVersion": "2025-03-26", "serverInfo": map[string]string{"name": "test", "version": "1"}, "capabilities": map[string]any{"tools": map[string]any{}}}
		case "notifications/initialized":
			return &http.Response{StatusCode: 202, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
		case "tools/call":
			if rpc.Params.Arguments["chat_id"] != "synthetic-group" {
				t.Error("reply/read left configured group")
			}
			var payload any
			switch rpc.Params.Name {
			case "chat_messages_list":
				payload = map[string]any{"errcode": 0, "has_more": false, "messages_count": 1, "messages": []any{map[string]any{"userid": "synthetic-human", "user_name": "do-not-prompt-name", "msg_type": "text", "send_time": start.Add(time.Second).Format("2006-01-02 15:04:05"), "text": map[string]string{"content": "@testbot 我叫小明。"}}}, "extra_identity_context": "not-agent-instructions"}
			case "message_aibot_send":
				sends.Add(1)
				markdown, _ := rpc.Params.Arguments["markdown"].(map[string]any)
				text, _ := markdown["content"].(string)
				if !strings.Contains(text, "小明") || strings.Contains(text, "not-agent-instructions") {
					t.Error("reply did not come from Runner output")
				}
				payload = map[string]any{"errcode": 0, "success": true}
			default:
				t.Error("unexpected business tool")
				return nil, errors.New("unexpected tool")
			}
			body, _ := json.Marshal(payload)
			result = map[string]any{"content": []any{map[string]any{"type": "text", "text": string(body)}}}
		default:
			t.Error("unexpected MCP method")
			return nil, errors.New("unexpected method")
		}
		raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": rpc.ID, "result": result})
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(string(raw)))}, nil
	})}
	adapter, _ := wecommcp.New(secret.StaticStore{b.SecretRef: "https://qyapi.weixin.qq.com/mcp/v2/bot/msg?apikey=offline-canary"}, state, client)
	registry, _ := channels.NewRegistry(adapter)
	resolver, _ := routing.NewControlPlaneResolver(repo)
	intake, _ := gateway.NewIntake(resolver, journal, gateway.WithAuditWriter(writer))
	ingress, _ := gateway.NewCallbackGateway(repo, registry, intake)
	poller, err := gateway.NewWeComPoller(repo, adapter, ingress, state, coord, gateway.WeComPollOptions{Targets: []config.WeComMCPTarget{{TenantID: b.TenantID, BindingID: b.ID}}, Interval: time.Second, Window: time.Minute, Overlap: time.Minute, SettleDelay: 5 * time.Second, Timeout: time.Second, Audit: writer})
	if err != nil {
		t.Fatal(err)
	}
	model := agentruntime.NewTutorialModel()
	compiler, err := agentruntime.NewRevisionCompiler(repo, model, false)
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := storage.NewSessionRouter(repo, secret.StaticStore{}, inmemory.NewSessionService(), nil)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := agentruntime.NewRuntimeWithCompilerServices(model, compiler, sessions, coord, idempotency.NewLocalStore(), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = runtime.Close()
		_ = journal.Close()
		_ = queue.Close()
		_ = repo.Close()
		_ = writer.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if n, err := poller.ProcessOnce(ctx); err != nil || n != 1 {
		t.Fatalf("poll: %d %v", n, err)
	}
	tasks := journal.Tasks()
	if len(tasks) != 1 || tasks[0].Scope.ChannelType != wecommcp.ChannelType || tasks[0].UserID == "synthetic-human" || tasks[0].SessionID == "synthetic-group" || tasks[0].TraceParent == "" {
		t.Fatal("tenant identity or trace missing")
	}
	relay, _ := gateway.NewOutboxRelay(journal, queue, gateway.RelayOptions{WorkerID: "relay", BatchSize: 4, ClaimLease: time.Second, PollInterval: time.Second, RetryDelay: time.Second})
	if n, err := relay.RelayOnce(ctx); err != nil || n != 1 {
		t.Fatalf("relay: %d %v", n, err)
	}
	w, _ := worker.New(queue, journal, runtime, worker.Options{WorkerID: "worker", MaxAttempts: 3, RetryDelay: time.Second, Audit: writer})
	if ok, err := w.ProcessOne(ctx); err != nil || !ok {
		t.Fatalf("worker: %t %v", ok, err)
	}
	sender, _ := reply.New(journal, repo, registry, reply.Options{WorkerID: "sender", BatchSize: 4, ClaimLease: time.Second, PollInterval: time.Second, RetryDelay: time.Second, MaxAttempts: 3, Audit: writer})
	if n, err := sender.ProcessOnce(ctx); err != nil || n != 1 || sends.Load() != 1 {
		t.Fatalf("sender: %d %v", n, err)
	}
	if n, err := poller.ProcessOnce(ctx); err != nil || n != 0 || len(journal.Tasks()) != 1 {
		t.Fatal("poll replay enqueued another turn")
	}
	if n, err := sender.ProcessOnce(ctx); err != nil || n != 0 || sends.Load() != 1 {
		t.Fatal("sender replay sent again")
	}
	if err := provider.ForceFlush(ctx); err != nil {
		t.Fatal(err)
	}
	events := writer.Events()
	traceID := ""
	for _, e := range events {
		if e.Decision == "inbound_accepted" {
			traceID = e.TraceID
		}
		if e.Decision == "reply_sent" && (e.TraceID == "" || e.TraceID != traceID) {
			t.Fatal("reply trace detached")
		}
	}
	for _, prefix := range []string{"wecom_mcp.poll", "wecom_mcp.read", "gateway.accept", "queue.publish", "worker.agent.run", "invoke_agent ", "storage.session.event.append", "reply.send", "wecom_mcp.send"} {
		found := false
		for _, span := range exporter.GetSpans() {
			if strings.HasPrefix(span.Name, prefix) && span.SpanContext.TraceID().String() == traceID {
				found = true
			}
		}
		if !found {
			t.Errorf("flow trace missing %s", prefix)
		}
	}
}
