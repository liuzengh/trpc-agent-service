package modelops

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type embeddingFake struct{ calls int }

func (e *embeddingFake) GetDimensions() int { return 1 }
func (e *embeddingFake) GetEmbedding(ctx context.Context, s string) ([]float64, error) {
	v, _, err := e.GetEmbeddingWithUsage(ctx, s)
	return v, err
}
func (e *embeddingFake) GetEmbeddingWithUsage(context.Context, string) ([]float64, map[string]any, error) {
	e.calls++
	return []float64{1}, map[string]any{"prompt_tokens": int64(10)}, nil
}
func TestEmbeddingCallsAreReservedAndSettled(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	data.Tenants[0].QuotaConfig = json.RawMessage(`{"daily_prompt_tokens":44}`)
	g, _ := tenant.NewGuard(context.Background(), controlplane.NewMemoryRepository(data), config.QuotaConfig{Backend: "local"})
	defer g.Close()
	base := &embeddingFake{}
	e, err := NewEmbedding(base, g, "tutorial-tenant", "tutorial-app", 1)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := e.GetEmbedding(context.Background(), "hi"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.GetEmbedding(context.Background(), "hi"); !errors.Is(err, tenant.ErrBudgetExceeded) {
		t.Fatal("embedding budget bypass")
	}
	if base.calls != 2 {
		t.Fatal("blocked embedding reached backend")
	}
}
