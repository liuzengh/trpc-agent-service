package assembly

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/modelusage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/safego"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type usageIdentityModel struct {
	delegate            model.Model
	providerID          string
	configuredModelName string
}

func observeActualModelUsage(delegate model.Model, providerID, configuredModelName string) model.Model {
	if delegate == nil {
		return nil
	}
	return &usageIdentityModel{delegate: delegate, providerID: providerID, configuredModelName: configuredModelName}
}

func (m *usageIdentityModel) Info() model.Info { return m.delegate.Info() }

func (m *usageIdentityModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	responses, err := m.delegate.GenerateContent(ctx, request)
	if err != nil {
		return nil, err
	}
	output := make(chan *model.Response, 1)
	safego.Go("model usage identity stream", func() {
		defer close(output)
		for response := range responses {
			recordCandidateUsage(ctx, m.providerID, m.configuredModelName, response)
			select {
			case output <- response:
			case <-ctx.Done():
				return
			}
		}
	})
	return output, nil
}

func (m *usageIdentityModel) GenerateContentIter(ctx context.Context, request *model.Request) (model.Seq[*model.Response], error) {
	sequence, err := modelSequence(ctx, m.delegate, request)
	if err != nil {
		return nil, err
	}
	return func(yield func(*model.Response) bool) {
		sequence(func(response *model.Response) bool {
			recordCandidateUsage(ctx, m.providerID, m.configuredModelName, response)
			return yield(response)
		})
	}, nil
}

func recordCandidateUsage(ctx context.Context, providerID, configuredModelName string, response *model.Response) {
	if response == nil || response.Usage == nil {
		return
	}
	recorder, ok := modelusage.FromContext(ctx)
	if !ok {
		return
	}
	usage := response.Usage
	recorder.Record(modelusage.Segment{
		ProviderID: providerID, ModelName: configuredModelName, ReportedModel: response.Model,
		PromptTokens:       usage.PromptTokens,
		CachedPromptTokens: max(usage.PromptTokensDetails.CachedTokens, usage.PromptTokensDetails.CacheReadTokens),
		CompletionTokens:   usage.CompletionTokens, TotalTokens: usage.TotalTokens,
	})
}

var _ model.IterModel = (*usageIdentityModel)(nil)
