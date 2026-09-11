package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestRuntimeSummaryGeneratedAndReused(t *testing.T) {
	for _, backend := range []string{"inmemory", "redis"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			const marker = "SUMMARY_ONLY_MARKER_49183"
			var mu sync.Mutex
			var requests []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				data, _ := io.ReadAll(r.Body)
				mu.Lock()
				requests = append(requests, string(data))
				mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"id": "response", "object": "chat.completion", "model": "test", "choices": []any{map[string]any{"index": 0, "finish_reason": "stop", "message": map[string]any{"role": "assistant", "content": marker}}}})
			}))
			defer server.Close()
			t.Setenv("SUMMARY_TEST_KEY", "test")
			tenant := config.TenantConfig{
				TenantID: "summary-tenant", Version: "v1", App: config.AppConfig{Name: "app", AgentName: "agent"},
				Model: config.ModelConfig{Provider: "openai", Name: "test", BaseURL: server.URL + "/v1", APIKeyEnv: "SUMMARY_TEST_KEY", MaxTokens: 1024},
				Data:  config.DataConfig{Session: config.BackendConfig{Type: backend, DSNEnv: "SUMMARY_TEST_REDIS"}, Summary: config.BackendConfig{Type: backend}, Memory: config.BackendConfig{Type: "disabled"}},
			}
			if backend == "redis" {
				redis := miniredis.RunT(t)
				t.Setenv("SUMMARY_TEST_REDIS", "redis://"+redis.Addr()+"/0")
			}
			rt, err := buildRuntime(ctx, tenant, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer rt.close()
			key := session.Key{AppName: rt.AppNamespace, UserID: "user", SessionID: "conversation"}
			sess, err := rt.Session.CreateSession(ctx, key, nil)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 24; i++ {
				role, author := model.RoleUser, "user"
				if i%2 != 0 {
					role, author = model.RoleAssistant, "agent"
				}
				e := event.NewResponseEvent(fmt.Sprint(i/2), author, &model.Response{Done: true, Choices: []model.Choice{{Index: 0, Message: model.Message{Role: role, Content: fmt.Sprintf("Historical message %d about project decisions.", i)}}}})
				e.FilterKey = rt.AppNamespace
				if err := rt.Session.AppendEvent(ctx, sess, e); err != nil {
					t.Fatal(err)
				}
			}
			sess, err = rt.Session.GetSession(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			// Exercise the same asynchronous entry point used by Runner, without force.
			if err := rt.Session.EnqueueSummaryJob(ctx, sess, rt.AppNamespace, false); err != nil {
				t.Fatal(err)
			}
			for {
				current, err := rt.Session.GetSession(ctx, key)
				if err != nil {
					t.Fatal(err)
				}
				if text, ok := rt.Session.GetSessionSummaryText(ctx, current, session.WithSummaryFilterKey(rt.AppNamespace)); ok && strings.Contains(text, marker) {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("configured summary was never persisted")
				case <-time.After(10 * time.Millisecond):
				}
			}
			reader := rt
			if backend == "redis" {
				reader, err = buildRuntime(ctx, tenant, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer reader.close()
			}
			events, err := reader.Runner.Run(ctx, key.UserID, key.SessionID, model.NewUserMessage("Continue our discussion"))
			if err != nil {
				t.Fatal(err)
			}
			for e := range events {
				if e.Error != nil {
					t.Fatalf("runner error: %v", e.Error)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			reused := false
			for _, request := range requests {
				if strings.Contains(request, "Continue our discussion") && strings.Contains(request, marker) {
					reused = true
				}
			}
			if !reused {
				t.Fatal("next model invocation did not receive stored summary")
			}

		})
	}
}

func TestDisabledSummaryDoesNotCallModel(t *testing.T) {
	for _, backend := range []string{"inmemory", "redis"} {
		t.Run(backend, func(t *testing.T) {
			tenant := config.TenantConfig{TenantID: "disabled", App: config.AppConfig{Name: "app", AgentName: "agent"}, Model: config.ModelConfig{Provider: "mock"}, Data: config.DataConfig{Session: config.BackendConfig{Type: backend, DSNEnv: "SUMMARY_DISABLED_REDIS"}, Summary: config.BackendConfig{Type: "disabled"}, Memory: config.BackendConfig{Type: "disabled"}}}
			if backend == "redis" {
				r := miniredis.RunT(t)
				t.Setenv("SUMMARY_DISABLED_REDIS", "redis://"+r.Addr()+"/0")
			}
			rt, err := buildRuntime(context.Background(), tenant, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer rt.close()
			key := session.Key{AppName: rt.AppNamespace, UserID: "user", SessionID: "session"}
			sess, err := rt.Session.CreateSession(context.Background(), key, nil)
			if err != nil {
				t.Fatal(err)
			}
			e := event.NewResponseEvent("run", "user", &model.Response{Done: true, Choices: []model.Choice{{Message: model.NewUserMessage("do not summarize")}}})
			if err := rt.Session.AppendEvent(context.Background(), sess, e); err != nil {
				t.Fatal(err)
			}
			if err := rt.Session.CreateSessionSummary(context.Background(), sess, "", true); err != nil {
				t.Fatal(err)
			}
			if text, ok := rt.Session.GetSessionSummaryText(context.Background(), sess); ok || text != "" {
				t.Fatal("disabled summary generated content")
			}
		})
	}
}
