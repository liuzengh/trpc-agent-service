package modelops

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type testModel struct {
	calls   int
	fail    bool
	noUsage bool
	wait    bool
}

func (m *testModel) Info() model.Info { return model.Info{Name: "accounted-test"} }
func (m *testModel) GenerateContent(ctx context.Context, r *model.Request) (<-chan *model.Response, error) {
	m.calls++
	if r.MaxTokens == nil || *r.MaxTokens > 10 {
		panic("output cap not applied")
	}
	if m.fail {
		return nil, errors.New("provider secret should not escape")
	}
	out := make(chan *model.Response, 3)
	if m.wait {
		go func() { <-ctx.Done(); close(out) }()
		return out, nil
	}
	for i := 1; i <= 3; i++ {
		response := &model.Response{Done: i == 3}
		if !m.noUsage {
			response.Usage = &model.Usage{PromptTokens: 10, CompletionTokens: i}
		}
		out <- response
	}
	close(out)
	return out, nil
}
func TestAccountingAllPurposesAndRepeatedStreamUsage(t *testing.T) {
	for _, purpose := range []string{"chat", "summary", "memory"} {
		t.Run(purpose, func(t *testing.T) {
			data := controlplane.DefaultBootstrapData()
			data.Tenants[0].QuotaConfig = json.RawMessage(`{"daily_prompt_tokens":1000,"daily_completion_tokens":16}`)
			g, _ := tenant.NewGuard(context.Background(), controlplane.NewMemoryRepository(data), config.QuotaConfig{Backend: "local"})
			defer g.Close()
			base := &testModel{}
			m, err := New(base, g, Options{TenantID: "tutorial-tenant", AppID: "tutorial-app", Purpose: purpose, MaxCompletionTokens: 10})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			for i := 0; i < 3; i++ {
				responses, err := m.GenerateContent(ctx, &model.Request{})
				if err != nil {
					t.Fatal(err)
				}
				for response := range responses {
					if response.Error != nil {
						t.Fatal(response.Error)
					}
				}
			}
			// Each call uses three cumulative completion tokens, not 1+2+3.
			if _, err := m.GenerateContent(ctx, &model.Request{}); !errors.Is(err, tenant.ErrBudgetExceeded) {
				t.Fatalf("fourth call=%v", err)
			}
			if base.calls != 3 {
				t.Fatal("blocked call reached provider")
			}
		})
	}
}
func TestUnknownOutcomesKeepConservativeDebit(t *testing.T) {
	for _, mode := range []string{"fail", "missing_usage", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			data := controlplane.DefaultBootstrapData()
			data.Tenants[0].QuotaConfig = json.RawMessage(`{"daily_completion_tokens":10}`)
			g, _ := tenant.NewGuard(context.Background(), controlplane.NewMemoryRepository(data), config.QuotaConfig{Backend: "local"})
			defer g.Close()
			base := &testModel{fail: mode == "fail", noUsage: mode == "missing_usage", wait: mode == "cancel"}
			m, _ := New(base, g, Options{TenantID: "tutorial-tenant", AppID: "tutorial-app", Purpose: "chat", MaxCompletionTokens: 10, Timeout: 10 * time.Millisecond})
			responses, err := m.GenerateContent(context.Background(), &model.Request{})
			if err == nil {
				for range responses {
				}
			}
			if _, err := m.GenerateContent(context.Background(), &model.Request{}); !errors.Is(err, tenant.ErrBudgetExceeded) {
				t.Fatal("unknown call was refunded")
			}
		})
	}
}
