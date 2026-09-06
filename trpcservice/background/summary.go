package background

import (
	"context"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/summary"
)

type summaryModelKey struct{}

// No mutable shared model: each job carries its own revision-bound model.
// Outside an explicitly scoped job automatic summarization is disabled.
func NewJobSummarizer() summary.SessionSummarizer {
	return summary.NewDynamicSummarizer(func(ctx context.Context, _ *session.Session) (summary.SessionSummarizer, error) {
		m, ok := ctx.Value(summaryModelKey{}).(model.Model)
		if !ok {
			return nil, nil
		}
		return summary.NewSummarizer(m, summary.WithMaxSummaryWords(500)), nil
	})
}
