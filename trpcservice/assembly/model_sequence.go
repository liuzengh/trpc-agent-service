package assembly

import (
	"context"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

// modelSequence preserves the framework's caller-goroutine iterator when a
// model provides it and adapts the legacy channel API only when necessary.
// Model decorators in this package use one path so adding an observer cannot
// accidentally hide model.IterModel again.
func modelSequence(ctx context.Context, delegate model.Model, request *model.Request) (model.Seq[*model.Response], error) {
	if iterator, ok := delegate.(model.IterModel); ok {
		return iterator.GenerateContentIter(ctx, request)
	}
	responses, err := delegate.GenerateContent(ctx, request)
	if err != nil {
		return nil, err
	}
	return func(yield func(*model.Response) bool) {
		for response := range responses {
			if !yield(response) {
				return
			}
		}
	}, nil
}
