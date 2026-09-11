package assembly

import (
	"context"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

// newInputValidationCallbacks performs the final multimodal capability check at
// the framework's model-callback seam. Runtime validates newly uploaded
// attachments earlier for a better user error; this callback also catches
// non-text parts restored from historical Session events immediately before a
// provider or failover model is invoked.
func newInputValidationCallbacks(catalog *config.ModelCatalog, modelConfig config.ModelConfig) *model.Callbacks {
	callbacks := model.NewCallbacks()
	callbacks.RegisterBeforeModel(func(_ context.Context, args *model.BeforeModelArgs) (*model.BeforeModelResult, error) {
		if catalog == nil {
			return nil, fmt.Errorf("model input guard is not configured")
		}
		var request *model.Request
		if args != nil {
			request = args.Request
		}
		if err := catalog.ValidateInputs(modelConfig, requiredRequestInputs(request)); err != nil {
			return nil, fmt.Errorf("reject unsupported model input before provider call: %w", err)
		}
		return nil, nil
	})
	return callbacks
}

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
