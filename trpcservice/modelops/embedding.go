package modelops

import (
	"context"
	"errors"
	"io"
	"math"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
)

type Embedding struct {
	base            embedder.Embedder
	guard           *tenant.Guard
	tenantID, appID string
	price           float64
	operation       *metrics.Operation
	usage           *metrics.Recorder
}

func NewEmbedding(base embedder.Embedder, guard *tenant.Guard, tenantID, appID string, price float64) (*Embedding, error) {
	if base == nil || guard == nil || tenantID == "" || appID == "" || !priceValid(price) {
		return nil, errors.New("invalid embedding accounting configuration")
	}
	recorder, err := metrics.New()
	if err != nil {
		return nil, err
	}
	return &Embedding{base: base, guard: guard, tenantID: tenantID, appID: appID, price: price, operation: metrics.NewOperation("model"), usage: recorder}, nil
}
func (e *Embedding) GetDimensions() int { return e.base.GetDimensions() }
func (e *Embedding) Close() error {
	if closer, ok := e.base.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
func (e *Embedding) GetEmbedding(ctx context.Context, text string) ([]float64, error) {
	v, _, err := e.GetEmbeddingWithUsage(ctx, text)
	return v, err
}
func (e *Embedding) GetEmbeddingWithUsage(parent context.Context, text string) (vector []float64, usage map[string]any, resultErr error) {
	started := time.Now()
	defer func() { e.operation.Finish(parent, started, e.tenantID, e.appID, "embedding", "embedding", resultErr) }()
	if len(text) > 131072 {
		return nil, nil, errors.New("embedding input exceeds per-call bound")
	}
	ctx, cancel := context.WithTimeout(parent, 60*time.Second)
	defer cancel()
	cost := func(tokens int64) float64 { return math.Ceil(float64(tokens)*e.price*1000) / 1e9 }
	r, err := e.guard.ReserveModel(ctx, e.tenantID, int64(len(text)+32), 0, cost(int64(len(text)+32)))
	if err != nil {
		return nil, nil, err
	}
	vector, usage, resultErr = e.base.GetEmbeddingWithUsage(ctx, text)
	p, known := embeddingTokens(usage["prompt_tokens"])
	if !known {
		p, known = embeddingTokens(usage["tokens"])
	}
	if !known || resultErr != nil {
		p = max(p, r.Prompt)
	}
	finish, stop := context.WithTimeout(context.WithoutCancel(parent), 3*time.Second)
	defer stop()
	first, settleErr := e.guard.SettleModel(finish, r, p, 0, cost(p))
	if first || settleErr != nil {
		e.usage.RecordSettlement(finish, e.tenantID, "embedding", !known || resultErr != nil, p > r.Prompt || cost(p) > r.Cost, settleErr)
	}
	if first && settleErr == nil {
		e.usage.RecordUsage(finish, e.tenantID, int(p), 0, cost(p))
	}
	if resultErr != nil {
		return nil, nil, errors.Join(errors.New("embedding provider failed"), settleErr)
	}
	return vector, usage, settleErr
}
func embeddingTokens(v any) (int64, bool) {
	var p int64
	switch n := v.(type) {
	case int:
		p = int64(n)
	case int64:
		p = n
	case float64:
		if math.IsNaN(n) || math.IsInf(n, 0) || n != math.Trunc(n) || n > 1e9 {
			return 0, false
		}
		p = int64(n)
	default:
		return 0, false
	}
	return p, p >= 0 && p <= 1e9
}
