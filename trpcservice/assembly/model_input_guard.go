package assembly

import (
	"context"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

// inputValidatingModel is the final provider-boundary check for multimodal
// requests. Runtime validates newly uploaded attachments earlier for a better
// user error, while this wrapper also catches non-text parts restored from
// historical Session events before they reach any provider or failover model.
type inputValidatingModel struct {
	inner       model.Model
	catalog     *config.ModelCatalog
	modelConfig config.ModelConfig
}

func newInputValidatingModel(inner model.Model, catalog *config.ModelCatalog, modelConfig config.ModelConfig) model.Model {
	return &inputValidatingModel{inner: inner, catalog: catalog, modelConfig: modelConfig}
}

func (m *inputValidatingModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	if m == nil || m.inner == nil || m.catalog == nil {
		return nil, fmt.Errorf("model input guard is not configured")
	}
	required := requiredRequestInputs(request)
	if err := m.catalog.ValidateInputs(m.modelConfig, required); err != nil {
		return nil, fmt.Errorf("reject unsupported model input before provider call: %w", err)
	}
	return m.inner.GenerateContent(ctx, request)
}

func (m *inputValidatingModel) Info() model.Info { return m.inner.Info() }

func requiredRequestInputs(request *model.Request) []config.ModelInputKind {
	if request == nil {
		return nil
	}
	seen := map[config.ModelInputKind]bool{}
	for _, message := range request.Messages {
		for _, part := range message.ContentParts {
			switch part.Type {
			case model.ContentTypeImage:
				seen[config.ModelInputImage] = true
			case model.ContentTypeAudio:
				seen[config.ModelInputAudio] = true
			case model.ContentTypeFile, model.ContentTypeVideo:
				seen[config.ModelInputFile] = true
			}
		}
	}
	result := make([]config.ModelInputKind, 0, len(seen))
	for _, kind := range []config.ModelInputKind{config.ModelInputImage, config.ModelInputAudio, config.ModelInputFile} {
		if seen[kind] {
			result = append(result, kind)
		}
	}
	return result
}
