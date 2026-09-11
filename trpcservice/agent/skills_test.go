package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestRuntimeSkillAuthorizationAppliesToPromptAndLoading(t *testing.T) {
	root := t.TempDir()
	for name, body := range map[string]string{"greeting": "GREETING_BODY_789", "payroll": "PRIVATE_PAYROLL_BODY_321"} {
		if err := os.Mkdir(filepath.Join(root, name), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name, "SKILL.md"), []byte("---\nname: "+name+"\ndescription: "+name+" instructions\n---\n"+body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	repo := NewSkillRepository(root)
	for _, tc := range []struct {
		name           string
		allow          []string
		target, marker string
		granted        bool
	}{
		{"tenant_a_allowed", []string{"greeting"}, "greeting", "GREETING_BODY_789", true},
		{"tenant_a_denied", []string{"greeting"}, "payroll", "PRIVATE_PAYROLL_BODY_321", false},
		{"tenant_b_allowed", []string{"payroll"}, "payroll", "PRIVATE_PAYROLL_BODY_321", true},
		{"default_denies_all", nil, "payroll", "PRIVATE_PAYROLL_BODY_321", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var requests []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				mu.Lock()
				requests = append(requests, string(body))
				number := len(requests)
				mu.Unlock()
				message := map[string]any{"role": "assistant", "content": "finished"}
				reason := "stop"
				if number == 1 {
					args, _ := json.Marshal(map[string]string{"skill": tc.target})
					message = map[string]any{"role": "assistant", "tool_calls": []any{map[string]any{"id": "call-1", "type": "function", "function": map[string]any{"name": "skill_load", "arguments": string(args)}}}}
					reason = "tool_calls"
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"id": "test", "object": "chat.completion", "model": "test", "choices": []any{map[string]any{"index": 0, "finish_reason": reason, "message": message}}})
			}))
			defer server.Close()
			t.Setenv("SKILL_TEST_KEY", "test")
			tenant := config.TenantConfig{TenantID: tc.name, App: config.AppConfig{Name: "app", AgentName: "agent"}, Model: config.ModelConfig{Provider: "openai", Name: "test", BaseURL: server.URL + "/v1", APIKeyEnv: "SKILL_TEST_KEY", MaxTokens: 1024}, Skills: config.SkillPolicy{Allow: tc.allow}, Data: config.DataConfig{Session: config.BackendConfig{Type: "inmemory"}, Memory: config.BackendConfig{Type: "disabled"}, Summary: config.BackendConfig{Type: "disabled"}}}
			rt, err := buildRuntime(context.Background(), tenant, repo)
			if err != nil {
				t.Fatal(err)
			}
			defer rt.close()
			events, err := rt.Runner.Run(context.Background(), "user", "session", model.NewUserMessage("Load the requested skill"))
			if err != nil {
				t.Fatal(err)
			}
			for range events {
			}
			mu.Lock()
			defer mu.Unlock()
			if len(requests) < 2 {
				t.Fatal("tool result was not returned to the model")
			}
			exposed := strings.Contains(strings.Join(requests, "\n"), tc.marker)
			if exposed != tc.granted {
				t.Fatalf("skill body visibility = %v, want %v", exposed, tc.granted)
			}
			if !tc.granted && strings.Contains(requests[0], tc.target+" instructions") {
				t.Fatal("unauthorized skill metadata leaked into prompt")
			}
		})
	}
}
