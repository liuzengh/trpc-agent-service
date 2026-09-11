package trpcagent

import (
	"context"
	"errors"
	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/knowledgestore"
	"go.opentelemetry.io/otel/trace"
	"sync/atomic"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
)

var ErrKnowledge = errors.New("knowledge retrieval failed")

type KnowledgeConfig struct {
	Resource string
	Service  knowledge.Knowledge
}
type tracedKnowledge struct {
	service knowledge.Knowledge
	tracer  trace.Tracer
	failed  atomic.Bool
}

func (k *tracedKnowledge) Search(ctx context.Context, r *knowledge.SearchRequest) (out *knowledge.SearchResult, err error) {
	ctx, span := telemetrytrace.Start(k.tracer, ctx, "knowledge.search")
	defer func() { telemetrytrace.End(span, err) }()
	out, err = k.service.Search(ctx, r)
	if errors.Is(err, knowledgestore.ErrNoResults) {
		return &knowledge.SearchResult{Documents: []*knowledge.Result{}}, nil
	}
	if err != nil || out == nil {
		k.failed.Store(true)
		return nil, ErrKnowledge
	}
	return out, nil
}
